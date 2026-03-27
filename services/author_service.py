import pandas as pd
from azure.data.tables import TableClient
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
import os

TABLE_NAME = "Autores"
DEFAULT_PARTITION = "Docente"

class AuthorManager:
    """Clase para gestionar CRUD de autores en Azure Table Storage."""
    def __init__(self):
        self.conn_str = os.environ.get("AzureWebJobsStorage")

    def get_table_client(self):
        client = TableClient.from_connection_string(conn_str=self.conn_str, table_name=TABLE_NAME)
        
        try:
            client.create_table()
        except ResourceExistsError:
            pass

        return client

    def upsert_author(self, data):
        client = self.get_table_client()

        dni = data.get('cedula')
        if not dni:
            raise ValueError("El DNI/Cédula es obligatorio para registrar al docente.")
            
        entity = {
            "PartitionKey": DEFAULT_PARTITION,
            "RowKey": str(dni).strip(),
            "Nombres": data.get('nombres', ''),
            "Apellidos": data.get('apellidos', ''),
            "Titulo": data.get('titulo', ''),
            "Cargo": data.get('cargo', ''),
            "Departamento": data.get('departamento', ''),
            "Facultad": data.get('facultad', ''), # Añadido para permitir el filtro que mencionaste
            "ScopusIds": data.get('scopus_ids', '') 
        }
        
        # upsert_entity actualiza si la cédula ya existe, o crea si es nueva
        client.upsert_entity(entity=entity)
        return {"mensaje": "Autor guardado/actualizado correctamente", "cedula": dni}

    # R (Read) - Listar y Filtrar
    def get_authors(self, departamento=None, facultad=None):
        client = self.get_table_client()
        query_filters = [f"PartitionKey eq '{DEFAULT_PARTITION}'"]
        
        if departamento:
            query_filters.append(f"Departamento eq '{departamento}'")
        if facultad:
            query_filters.append(f"Facultad eq '{facultad}'")
            
        query = " and ".join(query_filters)
        entities = client.query_entities(query_filter=query)
            
        autores = []
        for e in entities:
            autores.append({
                "cedula": e["RowKey"],
                "nombres": e.get("Nombres", ""),
                "apellidos": e.get("Apellidos", ""),
                "titulo": e.get("Titulo", ""),
                "cargo": e.get("Cargo", ""),
                "departamento": e.get("Departamento", ""),
                "facultad": e.get("Facultad", ""),
                "scopus_ids": e.get("ScopusIds", "")
            })
        return autores

    # D (Delete) - Eliminar un autor
    def delete_author(self, cedula):
        client = self.get_table_client()
        try:
            client.delete_entity(partition_key=DEFAULT_PARTITION, row_key=str(cedula).strip())
            return {"mensaje": f"Autor con cédula {cedula} eliminado correctamente."}
        except ResourceNotFoundError:
            return {"error": "Autor no encontrado."}

    # Carga Masiva (CSV)
    def bulk_upload_authors(self, csv_content):
        from io import StringIO
        df = pd.read_csv(StringIO(csv_content))
        client = self.get_table_client()
        
        procesados = 0
        for _, row in df.iterrows():
            # Validar que la fila tenga cédula
            cedula = str(row.get('Cedula', '')).strip()
            if not cedula or cedula == 'nan':
                continue
                
            entity = {
                "PartitionKey": DEFAULT_PARTITION,
                "RowKey": cedula,
                "Nombres": str(row.get('Nombres', '')),
                "Apellidos": str(row.get('Apellidos', '')),
                "Titulo": str(row.get('Titulo', '')),
                "Cargo": str(row.get('Cargo', '')),
                "Departamento": str(row.get('Departamento', '')),
                "Facultad": str(row.get('Facultad', '')),
                "ScopusIds": str(row.get('ScopusIds', ''))
            }
            client.upsert_entity(entity=entity)
            procesados += 1
            
        return {"mensaje": f"{procesados} autores importados/actualizados correctamente."}
