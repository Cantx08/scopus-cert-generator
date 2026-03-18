"""
Módulo encargado de interactuar con la API de Scopus para extraer las publicaciones y
áreas de conocimiento de un autor dado los IDs de Scopus asociados a él.
"""

import os
import logging
from httpx import AsyncClient, Timeout

# Diccionario de Áreas de Conocimiento de Scopus
SUBJECT_AREAS = {
    "AGRI": "Agricultural and Biological Sciences",
    "ARTS": "Arts and Humanities",
    "BIOC": "Biochemistry, Genetics and Molecular Biology",
    "BUSI": "Business, Management and Accounting",
    "CENG": "Chemical Engineering",
    "CHEM": "Chemistry",
    "COMP": "Computer Science",
    "DECI": "Decision Sciences",
    "EART": "Earth and Planetary Sciences",
    "ECON": "Economics, Econometrics and Finance",
    "ENER": "Energy",
    "ENGI": "Engineering",
    "ENVI": "Environmental Science",
    "IMMU": "Immunology and Microbiology",
    "MATE": "Materials Science",
    "MATH": "Mathematics",
    "MEDI": "Medicine",
    "NEUR": "Neuroscience",
    "NURS": "Nursing",
    "PHAR": "Pharmacology, Toxicology and Pharmaceutics",
    "PHYS": "Physics and Astronomy",
    "PSYC": "Psychology",
    "SOCI": "Social Sciences",
    "VETE": "Veterinary",
    "DENT": "Dentistry",
    "HEAL": "Health Professions",
    "MULT": "Multidisciplinary",
}

class ScopusExtractor:
    """ Clase encargada de interactuar con la API de Scopus para extraer información relevante. """
    def __init__(self):
        self.api_key = os.environ.get("SCOPUS_API_KEY")
        self.inst_token = os.environ.get("SCOPUS_INST_TOKEN")
        self.target_afid = os.environ.get("EPN_AFFILIATION_ID")
        self.base_url = "https://api.elsevier.com"

        self.headers = {
            "Accept": "application/json",
            "X-ELS-APIKey": self.api_key
        }
        if self.inst_token:
            self.headers["X-ELS-Insttoken"] = self.inst_token

        self.timeout = Timeout(120.0, connect=10.0)

    async def get_publications(self, scopus_id: str, async_client: AsyncClient) -> list:
        """ Obtiene las publicaciones asociadas a un autor dado su Scopus ID. """
        publication_list = []
        start = 0
        count = 25

        while True:
            url = f"{self.base_url}/content/search/scopus"
            params = {
                "query": f"AU-ID({scopus_id})",
                "start": start,
                "count": count,
                "view": "COMPLETE"
            }

            response = await async_client.get(url, headers=self.headers, params=params)
            if response.status_code != 200:
                logging.error("Scopus Error %s: %s", response.status_code, response.text)
                break

            data = response.json()
            search_results = data.get("search-results", {})
            entries = search_results.get("entry", [])

            if not entries or (len(entries) == 1 and entries[0].get("error")):
                break

            for entry in entries:
                # Verificar si está afiliado a la EPN
                is_epn_affiliated = False
                authors = entry.get("author", [])
                if isinstance(authors, dict):
                    authors = [authors]

                for author in authors:
                    if str(author.get("authid", "")) == str(scopus_id):
                        afids = author.get("afid", [])
                        if isinstance(afids, dict):
                            afids = [afids]

                        for afid_obj in afids:
                            if str(afid_obj.get("$", "")) == str(self.target_afid):
                                is_epn_affiliated = True
                                break

                # Se obtienen los datos de cada publicación y se agrega a la lista de publicaciones
                pub_data = {
                    "scopus_id": scopus_id,
                    "pub_title": entry.get("dc:title", "N/A"),
                    "pub_year": entry.get("prism:coverDate", "N/A").split("-")[0],
                    "doi": entry.get("prism:doi", "N/A"),
                    "doc_type": entry.get("subtypeDescription", "N/A"),
                    "source_id": entry.get("source-id", "N/A"),
                    "source_title": entry.get("prism:publicationName", "N/A"),
                    "epn_affiliation": is_epn_affiliated
                }
                publication_list.append(pub_data)

            total_results = int(search_results.get("opensearch:totalResults", 0))
            if start + count >= total_results:
                break
            start += count

        return publication_list

    async def get_subject_areas(self, scopus_ids: list, async_client: AsyncClient) -> list:
        """ Obtiene las áreas de conocimiento correspondientes a un autor. """

        au_queries = [f"AU-ID({sid})" for sid in scopus_ids]
        query = " OR ".join(au_queries)

        url = f"{self.base_url}/content/search/scopus"
        params = {
            "query": query,
            "count": 1,
            "facets": "subjarea(sort=fd,count=50)"
        }

        response = await async_client.get(url, headers=self.headers, params=params)
        if response.status_code != 200:
            logging.error("Scopus Error %s: %s", response.status_code, response.text)
            return []

        data = response.json()
        search_results = data.get("search-results", {})
        facet_data = search_results.get("facet", [])
        if isinstance(facet_data, dict):
            facet_data = [facet_data]

        categories = []
        for facet in facet_data:
            if facet.get("name") == "subjarea" or facet.get("attribute") == "subjarea":
                categories = facet.get("category", [])
                break

        areas = []
        for cat in categories:
            abbrev = cat.get("value", cat.get("name", ""))
            hit_count = int(cat.get("hitCount", 0))
            subject_area = SUBJECT_AREAS.get(abbrev.upper(), cat.get("label", abbrev))
            areas.append({
                "abbrev": abbrev,
                "subject_area": subject_area,
                "documents": hit_count
            })

        return sorted(areas, key=lambda x: x["documents"], reverse=True)
