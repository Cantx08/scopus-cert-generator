"""
Servicio para mapear las publicaciones extraídas desde Scopus con el SJR.
"""

from collections import defaultdict
import logging
import pandas as pd

class SJRMapper:
    """
    Clase encargada de obtener los datos del SJR para el mapeo con las publicaciones de Scopus.
    Verifica si cada categoría de la revista mapeada se encuentra dentro del 10% superior en
    el año correspondiente.
    """
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

        # 1. Agrupar revistas por Año y Categoría para el cálculo de percentiles
        categories_by_year = defaultdict(lambda: defaultdict(list))
        sjr_base_data = defaultdict(dict)

        for row in df.itertuples(index=False):
            source_id = str(row.Sourceid)
            source_year = int(row.year)
            rank = float(row.Rank)
            cats_str = str(row.Categories)

            sjr_base_data[source_id][source_year] = {
                'rank': rank,
                'title': row.Title,
                'issn': row.Issn,
                'raw_cats': cats_str
            }

            # Se extraen las categorías de la columna Categories
            if cats_str and cats_str != 'nan':
                for cat_item in cats_str.split(';'):
                    cat_item = cat_item.strip()
                    idx = cat_item.rfind(" (Q")
                    cat_name = cat_item[:idx].strip() if idx != -1 else cat_item

                    categories_by_year[source_year][cat_name].append((rank, source_id))

        # 2. Verificar si la categoría se encuentra dentro del 10% superior
        top_10 = defaultdict(lambda: defaultdict(dict))

        for source_year, categories in categories_by_year.items():
            for cat_name, sources in categories.items():
                # Ordenar por Rank
                sources.sort(key=lambda x: x[0])
                total_sources_in_category = len(sources)

                for index, (rank, source_id) in enumerate(sources):
                    rank_position = index + 1
                    percentile_position = (rank_position / total_sources_in_category) * 100

                    # El porcentaje se almacena si se encuentra dentro del 10% superior
                    if percentile_position <= 10.0:
                        top_10[source_id][source_year][cat_name] = round(percentile_position, 1)

        # 3. Si corresponde al  10% superior, se agrega el porcentaje al diccionario del SJR
        sjr_dict = defaultdict(dict)

        for source_id, years_data in sjr_base_data.items():
            for source_year, data in years_data.items():
                cats_str = data['raw_cats']
                final_cats = []

                if cats_str and cats_str != 'nan':
                    for cat_item in cats_str.split(';'):
                        cat_item = cat_item.strip()
                        idx = cat_item.rfind(" (Q")
                        cat_name = cat_item[:idx].strip() if idx != -1 else cat_item

                        percentile_position = top_10.get(source_id, {}).get(source_year, {}).get(cat_name)

                        if percentile_position is not None:
                            final_cats.append(f"{cat_item}[Categoría dentro del 10% superior ({percentile_position})]")
                        else:
                            final_cats.append(cat_item)

                    formatted_categories = "; ".join(final_cats)
                else:
                    formatted_categories = "N/A"

                sjr_dict[source_id][source_year] = {
                    'rank': data['rank'] if data['rank'] != 9999999 else "N/A",
                    'title': data['title'],
                    'issn': data['issn'],
                    'categories': formatted_categories
                }

        logging.info("SJR cacheado exitosamente.")
        return dict(sjr_dict)

    def map_publications(self, publications: list) -> list:
        """
        Cruza la lista de publicaciones de Scopus con el diccionario histórico del SJR.
        """
        for pub in publications:
            source_id = str(pub.get("source_id", "N/A"))
            try:
                pub_year = int(pub.get("pub_year", 0))
            except ValueError:
                pub_year = 0

            # Si la revista existe en nuestro histórico SJR
            if source_id in self.sjr_data and pub_year > 0:
                sjr_years_data = self.sjr_data[source_id]
                available_sjr_years = sorted(sjr_years_data.keys())

                if not available_sjr_years:
                    self._set_empty_sjr(pub)
                    continue

                # Verificar el primer año y el último año disponible en el SJR
                min_year = available_sjr_years[0]   # Ej. 1999
                max_year = available_sjr_years[-1]  # Ej. 2024

                if pub_year <= min_year:
                    matched_year = min_year
                elif pub_year >= max_year:
                    matched_year = max_year
                else:
                    matched_year = min(available_sjr_years, key=lambda y: abs(y - pub_year))

                # Extraer la data mapeada
                matched_data = sjr_years_data[matched_year]

                pub["sjr_found"] = True
                pub["sjr_year"] = matched_year
                pub["sjr_categories"] = matched_data["categories"]
                pub["sjr_rank"] = matched_data["rank"]
            else:
                self._set_empty_sjr(pub)

        return publications

    def _set_empty_sjr(self, pub: dict):
        pub["sjr_found"] = False
        pub["sjr_year"] = "N/A"
        pub["sjr_categories"] = "N/A"
        pub["sjr_rank"] = "N/A"
