"""
DepartmentManager es una clase que permite enlistar departamentos y faciltades dentro de la EPN. Los departamentos académicos pueden ser filtrados por facultades.
"""
import os
from azure.data.tables import TableClient
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError


class DepartmentManager:
    """
    Clase para gestionar departamentos y facultades en Azure Table Storage.
    """
    def __init__(self):
        self.conn_str = os.environ.get("AzureWebJobsStorage")

    def get_table_client(self, table_name):
        """Obtiene un cliente para la tabla especificada, creando la tabla si no existe."""
        client = TableClient.from_connection_string(conn_str=self.conn_str, table_name=table_name)
        
        try:
            client.create_table()
        except ResourceExistsError:
            pass

        return client
    
    def get_facultades(self):
        """
        Obtiene la lista de facultades registradas.
        """
        client = self.get_table_client("Facultades")
        entities = client.query_entities(query_filter="PartitionKey eq 'Facultad'")
        
        facultades = []
        for e in entities:
            facultades.append({
                "sigla": e["RowKey"],
                "nombre": e.get("Nombre", "")
            })
        return facultades
    
    def get_departments(self, facultad=None):
        """
        Obtiene la lista de departamentos, permite filtrarlas por facultad.
        """

        client = self.get_table_client("Departamentos")

        if facultad:
            query = f"PartitionKey eq '{facultad}'"
            entities = client.query_entities(query_filter=query)
        else:
            entities = client.list_entities()
        
        departamentos = []
        for e in entities:
            departamentos.append({
                "facultad": e["PartitionKey"],
                "codigo": e["RowKey"],
                "nombre": e.get("Nombre", ""),
            })
        return departamentos
