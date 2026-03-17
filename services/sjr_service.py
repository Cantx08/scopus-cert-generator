import pandas as pd
from collections import defaultdict
import logging

class SJRMapper:
    def __init__(self, csv_path_or_url: str):
        self.sjr_data = self._load_and_optimize_sjr(csv_path_or_url)

    def _load_and_optimize_sjr(self, filepath: str) -> dict:
        logging.info("Cargando histórico SJR y calculando el Top 10% por categoría...")
        
        df = pd.read_csv(
            filepath, 
            sep=';', 
            usecols=['Sourceid', 'Title', 'Issn', 'Categories', 'year', 'Rank'],
            dtype={'Sourceid': str, 'year': int}
        )

        # 1. Agrupar revistas por Año y Categoría para calcular percentiles
        # Estructura: anio -> nombre_categoria -> lista de (Rank, Sourceid)
        categorias_por_anio = defaultdict(lambda: defaultdict(list))
        
        # Guardamos la data para no iterar el DataFrame nuevamente
        datos_base = defaultdict(dict)
        
        for row in df.itertuples(index=False):
            source_id = str(row.Sourceid)
            anio = int(row.year)
            rank = float(row.Rank)
            cats_str = str(row.Categories)
            
            datos_base[source_id][anio] = {
                'rank': rank,
                'title': row.Title,
                'issn': row.Issn,
                'raw_cats': cats_str
            }
            
            if cats_str and cats_str != 'nan':
                for cat_item in cats_str.split(';'):
                    cat_item = cat_item.strip()
                    # Extraer categoría"
                    idx = cat_item.rfind(" (Q")
                    cat_name = cat_item[:idx].strip() if idx != -1 else cat_item
                    
                    categorias_por_anio[anio][cat_name].append((rank, source_id))

        # 2. Calcular posiciones y determinar se encuentran dentro del 10% superior
        # Estructura: source_id -> anio -> nombre_categoria -> porcentaje
        top_10_cache = defaultdict(lambda: defaultdict(dict))
        
        for anio, categorias in categorias_por_anio.items():
            for cat_name, revistas in categorias.items():
                # Ordenar por Rank
                revistas.sort(key=lambda x: x[0])
                total_revistas_en_categoria = len(revistas)
                
                for index, (rank, source_id) in enumerate(revistas):
                    posicion = index + 1
                    porcentaje = (posicion / total_revistas_en_categoria) * 100
                    
                    if porcentaje <= 10.0:
                        # Guardamos el porcentaje redondeado a 1 decimal
                        top_10_cache[source_id][anio][cat_name] = round(porcentaje, 1)

        # 3. Reconstruir el string final para guardarlo en la caché de la memoria
        sjr_dict = defaultdict(dict)
        
        for source_id, anios_data in datos_base.items():
            for anio, data in anios_data.items():
                cats_str = data['raw_cats']
                final_cats = []
                
                if cats_str and cats_str != 'nan':
                    for cat_item in cats_str.split(';'):
                        cat_item = cat_item.strip()
                        idx = cat_item.rfind(" (Q")
                        cat_name = cat_item[:idx].strip() if idx != -1 else cat_item
                        
                        # Verificamos si pertenece al 10%
                        porcentaje = top_10_cache.get(source_id, {}).get(anio, {}).get(cat_name)
                        
                        if porcentaje is not None:
                            final_cats.append(f"{cat_item}[Categoría dentro del 10% superior ({porcentaje})]")
                        else:
                            final_cats.append(cat_item)
                            
                    categorias_formateadas = "; ".join(final_cats)
                else:
                    categorias_formateadas = "N/A"
                    
                sjr_dict[source_id][anio] = {
                    'rank': data['rank'] if data['rank'] != 9999999 else "N/A",
                    'title': data['title'],
                    'issn': data['issn'],
                    'categories': categorias_formateadas
                }
                
        logging.info("SJR cacheado exitosamente.")
        return dict(sjr_dict)

    def map_publications(self, publicaciones: list) -> list:
        """
        Cruza la lista de publicaciones de Scopus con el diccionario histórico del SJR.
        """
        for pub in publicaciones:
            id_revista = str(pub.get("id_revista", "N/A"))
            try:
                pub_anio = int(pub.get("año", 0))
            except ValueError:
                pub_anio = 0

            # Si la revista existe en nuestro histórico SJR
            if id_revista in self.sjr_data and pub_anio > 0:
                sjr_years_data = self.sjr_data[id_revista]
                available_sjr_years = sorted(sjr_years_data.keys())

                if not available_sjr_years:
                    self._set_empty_sjr(pub)
                    continue

                # LÓGICA DE AÑOS (Reglas 4 y 5)
                anio_minimo = available_sjr_years[0]   # Ej. 1999
                anio_maximo = available_sjr_years[-1]  # Ej. 2024

                if pub_anio <= anio_minimo:
                    matched_year = anio_minimo
                elif pub_anio >= anio_maximo:
                    matched_year = anio_maximo
                else:
                    # Si cae en el medio, busca el año exacto o el más cercano
                    matched_year = min(available_sjr_years, key=lambda y: abs(y - pub_anio))

                # Extraer la data mapeada
                matched_data = sjr_years_data[matched_year]
                
                pub["sjr_encontrado"] = True
                pub["sjr_año_mapeado"] = matched_year
                pub["sjr_categorias"] = matched_data["categories"]
                pub["sjr_rank"] = matched_data["rank"]
            else:
                self._set_empty_sjr(pub)

        return publicaciones

    def _set_empty_sjr(self, pub: dict):
        pub["sjr_encontrado"] = False
        pub["sjr_año_mapeado"] = "N/A"
        pub["sjr_categorias"] = "N/A"
        pub["sjr_rank"] = "N/A"
