import os
import uuid

import pandas as pd
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from azure.data.tables import TableClient

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

    @staticmethod
    def _sanitize(value):
        if value is None:
            return ""

        text = str(value).strip()
        return "" if text.lower() == "nan" else text

    @staticmethod
    def _is_valid_uuid(value):
        try:
            uuid.UUID(str(value))
            return True
        except (TypeError, ValueError):
            return False

    def _build_author_entity(self, author_id, data):
        return {
            "PartitionKey": DEFAULT_PARTITION,
            "RowKey": author_id,
            "Nombres": self._sanitize(data.get('nombres', '')),
            "Apellidos": self._sanitize(data.get('apellidos', '')),
            "Titulo": self._sanitize(data.get('titulo', '')),
            "Cargo": self._sanitize(data.get('cargo', '')),
            "Departamento": self._sanitize(data.get('departamento', '')),
            "Facultad": self._sanitize(data.get('facultad', '')),
            "ScopusIds": self._sanitize(data.get('scopus_ids', '')),
        }

    @staticmethod
    def _build_author_response(entity):
        return {
            "id": entity["RowKey"],
            "nombres": entity.get("Nombres", ""),
            "apellidos": entity.get("Apellidos", ""),
            "titulo": entity.get("Titulo", ""),
            "cargo": entity.get("Cargo", ""),
            "departamento": entity.get("Departamento", ""),
            "facultad": entity.get("Facultad", ""),
            "scopus_ids": entity.get("ScopusIds", ""),
        }

    def _ensure_uuid_row_key(self, client, entity):
        current_row_key = entity["RowKey"]
        if self._is_valid_uuid(current_row_key):
            return entity

        migrated_entity = dict(entity)
        migrated_entity["RowKey"] = str(uuid.uuid4())
        client.upsert_entity(entity=migrated_entity)
        client.delete_entity(partition_key=entity["PartitionKey"], row_key=current_row_key)
        return migrated_entity

    def upsert_author(self, data):
        client = self.get_table_client()

        author_id = self._sanitize(data.get('id', ''))
        if author_id and not self._is_valid_uuid(author_id):
            raise ValueError("El campo 'id' debe ser un UUID válido.")

        if not author_id:
            author_id = str(uuid.uuid4())

        entity = self._build_author_entity(author_id, data)
        
        client.upsert_entity(entity=entity)
        return {"mensaje": "Autor guardado/actualizado correctamente", "id": author_id}

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
            normalized_entity = self._ensure_uuid_row_key(client, e)
            autores.append(self._build_author_response(normalized_entity))
        return autores

    def delete_author(self, author_id):
        client = self.get_table_client()
        try:
            client.delete_entity(partition_key=DEFAULT_PARTITION, row_key=str(author_id).strip())
            return {"mensaje": f"Autor con id {author_id} eliminado correctamente."}
        except ResourceNotFoundError:
            return {"error": "Autor no encontrado."}

    def bulk_upload_authors(self, csv_content):
        from io import StringIO
        df = pd.read_csv(StringIO(csv_content))
        client = self.get_table_client()
        
        procesados = 0
        for _, row in df.iterrows():
            nombres = self._sanitize(row.get('Nombres', ''))
            apellidos = self._sanitize(row.get('Apellidos', ''))
            if not nombres and not apellidos:
                continue

            imported_id = self._sanitize(row.get('Id', ''))
            author_id = imported_id if self._is_valid_uuid(imported_id) else str(uuid.uuid4())

            entity = {
                "PartitionKey": DEFAULT_PARTITION,
                "RowKey": author_id,
                "Nombres": nombres,
                "Apellidos": apellidos,
                "Titulo": self._sanitize(row.get('Titulo', '')),
                "Cargo": self._sanitize(row.get('Cargo', '')),
                "Departamento": self._sanitize(row.get('Departamento', '')),
                "Facultad": self._sanitize(row.get('Facultad', '')),
                "ScopusIds": self._sanitize(row.get('ScopusIds', '')),
            }
            client.upsert_entity(entity=entity)
            procesados += 1
            
        return {"mensaje": f"{procesados} autores importados/actualizados correctamente."}
