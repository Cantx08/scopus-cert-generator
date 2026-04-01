import os
import pandas as pd
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from azure.data.tables import TableClient

TABLE_NAME = "Cargos"
DEFAULT_PARTITION = "Cargo"

class JobPositionManager:
    """Clase para gestionar CRUD de cargos en Azure Table Storage."""
    def __init__(self):
        self.conn_str = os.environ.get("AzureWebJobsStorage")

    def get_table_client(self):
        client = TableClient.from_connection_string(conn_str=self.conn_str, table_name=TABLE_NAME)
        
        try:
            client.create_table()
        except ResourceExistsError:
            pass

        return client

    def get_job_positions(self):
        """
        Obtiene todos los cargos que puede ocupar un docente.
        """
        client = self.get_table_client()
        entities = client.query_entities(query_filter=f"PartitionKey eq '{DEFAULT_PARTITION}'")
        
        cargos = []
        for e in entities:
            cargos.append({
                "nombre": e["RowKey"],
            })
        return cargos
