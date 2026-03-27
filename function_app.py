"""
Azure Function principal para extraer datos de Scopus y generar certificados en PDF.
Dividida en dos micro-servicios: Extracción y Generación.
"""

import logging
import json
import base64
import os
import asyncio
import azure.functions as func
from httpx import AsyncClient

# Importamos nuestros módulos limpios
from services.scopus_service import ScopusExtractor
from services.sjr_service import SJRMapper
from services.pdf_service import CertificadoPDFService
from services.author_service import AuthorManager

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

# ==========================================
# INICIALIZACIÓN GLOBAL (Cold Start)
# ==========================================
RUTA_SJR = os.environ.get("SJR_CSV_PATH", "df_sjr_24_04_2025.csv")

sjr_mapper = None 
try:
    sjr_mapper = SJRMapper(RUTA_SJR)
    logging.info("SJR Mapper inicializado correctamente en caché global.")
except Exception as e:
    logging.error("Error crítico: No se pudo cargar el archivo SJR en caché: %s", str(e))


# ==========================================
# FUNCIÓN 1: EXTRACCIÓN DE DATOS
# ==========================================
@app.route(route="ExtractScopusData", methods=["POST"])
async def ExtractScopusData(req: func.HttpRequest) -> func.HttpResponse:
    """
    Recibe los IDs de Scopus, consulta las APIs y cruza la data con el archivo SJR.
    Retorna únicamente los datos (JSON) listos para ser oxaminados o cacheados.
    """
    logging.info('Iniciando extracción de datos de Scopus.')

    try:
        req_body = req.get_json()
        scopus_ids = req_body.get('scopus_ids', [])

        if not scopus_ids or not isinstance(scopus_ids, list):
            return func.HttpResponse(json.dumps({"error": "Se requiere una lista válida de 'scopus_ids'."}), status_code=400, mimetype="application/json")

        extractor = ScopusExtractor()
        async with AsyncClient(timeout=extractor.timeout) as client:
            tasks = [extractor.get_publications(sid, client) for sid in scopus_ids]
            pubs_results = await asyncio.gather(*tasks)
            
            # Limpieza de duplicados por DOI
            all_publications = []
            seen_dois = set()
            for sublist in pubs_results:
                for pub in sublist:
                    doi = pub.get('doi')
                    if doi and doi != "N/A":
                        if doi not in seen_dois:
                            seen_dois.add(doi)
                            all_publications.append(pub)
                    else:
                        all_publications.append(pub)
            
            # Obtener áreas temáticas
            subject_areas = await extractor.get_subject_areas(scopus_ids, client)

        normalized_subject_areas = []
        if isinstance(subject_areas, list):
            for index, area in enumerate(subject_areas, start=1):
                if not isinstance(area, dict):
                    area = {}

                area_name = (
                    area.get("subject_area")
                    or area.get("name")
                    or area.get("area")
                    or area.get("nombre")
                    or f"Area {index}"
                )
                try:
                    area_documents = int(
                        area.get("documents", area.get("count", area.get("cantidad", area.get("value", 0))))
                    )
                except (TypeError, ValueError):
                    area_documents = 0

                normalized_subject_areas.append({
                    **area,
                    "name": area_name,
                    "count": area_documents,
                    "subject_area": area_name,
                    "documents": area_documents,
                })

        # Mapeo con SJR
        if sjr_mapper:
            all_publications = sjr_mapper.map_publications(all_publications)

        return func.HttpResponse(
            json.dumps({
                "mensaje": "Datos extraídos correctamente",
                "total_publicaciones": len(all_publications),
                "publications": all_publications,
                "subject_areas": normalized_subject_areas
            }),
            mimetype="application/json",
            status_code=200
        )

    except Exception as e:
        logging.error("Error en extracción: %s", str(e), exc_info=True)
        return func.HttpResponse(json.dumps({"error": f"Error interno en extracción: {str(e)}"}), status_code=500, mimetype="application/json")


# ==========================================
# FUNCIÓN 2: GENERACIÓN DE PDF
# ==========================================
@app.route(route="GenerateCertificate", methods=["POST"])
def GenerateCertificate(req: func.HttpRequest) -> func.HttpResponse:
    """
    Recibe el JSON previamente extraído de Scopus junto con los datos visuales
    (firmantes, fechas, memorandos) y genera el documento PDF al instante.
    """
    logging.info('Iniciando generación de PDF (Endpoint 2).')

    try:
        req_body = req.get_json()
        
        # Obtenemos todos los datos desde el body
        author_data = req_body.get('author', {})
        metadata = req_body.get('metadata', {})
        publications = req_body.get('publications', [])
        subject_areas = req_body.get('subject_areas', [])
        is_draft = req_body.get('is_draft', True)

        # Validaciones de integridad en los datos recibidos
        if not author_data or not metadata:
            return func.HttpResponse(json.dumps({"error": "Faltan datos en el objeto 'author' o 'metadata'."}), status_code=400, mimetype="application/json")

        if not publications:
            return func.HttpResponse(json.dumps({"error": "La lista de publicaciones está vacía. Debe extraer los datos primero."}), status_code=400, mimetype="application/json")

        # Validación de roles
        pdf_service = CertificadoPDFService()
        try:
            pdf_service.check_roles(author_data, metadata)
        except ValueError as ve:
            logging.warning(f"Validación de roles fallida: {ve}")
            return func.HttpResponse(json.dumps({"error": str(ve)}), status_code=400, mimetype="application/json")

        # Generación de PDF (Puro procesamiento de CPU, sin red)
        pdf_bytes = pdf_service.generate_pdf(
            author=author_data,
            metadata=metadata,
            publications=publications,
            subject_areas=subject_areas,
            is_draft=is_draft
        )

        pdf_base64 = base64.b64encode(pdf_bytes).decode('utf-8')
        tipo_doc = "Borrador" if is_draft else "Certificado Final"
        title_aux = author_data.get('titulo', '').replace('.', '').strip()

        return func.HttpResponse(
            json.dumps({
                "mensaje": f"{tipo_doc} generado exitosamente",
                "pdf_base64": pdf_base64,
                "nombre_archivo": f"{tipo_doc} - {title_aux} {author_data.get('nombres', 'Nombre')} {author_data.get('apellidos', 'Apellido')}.pdf"
            }),
            mimetype="application/json",
            status_code=200
        )

    except Exception as e:
        logging.error("Error en generación PDF: %s", str(e), exc_info=True)
        return func.HttpResponse(json.dumps({"error": f"Error interno en generación: {str(e)}"}), status_code=500, mimetype="application/json")
    
# ==========================================
# FUNCIÓN 3: GESTIÓN DE AUTORES
# ==========================================
@app.route(route="ManageAuthors", methods=["GET", "POST", "PUT"])
def ManageAuthors(req: func.HttpRequest) -> func.HttpResponse:
    method = req.method

    author_manager = AuthorManager()

    if method == "GET":
        # Listar y filtrar
        facultad = req.params.get('facultad')
        departamento = req.params.get('departamento')
        autores = author_manager.get_authors(facultad, departamento)
        return func.HttpResponse(json.dumps(autores), mimetype="application/json", status_code=200)

    elif method == "POST":
        # Revisar si es una subida de CSV en bloque
        content_type = req.headers.get("Content-Type", "")
        
        if "text/csv" in content_type or "multipart/form-data" in content_type:
            # Lógica para CSV (requiere extraer el texto del archivo)
            csv_content = req.get_body().decode('utf-8') 
            resultado = author_manager.bulk_upload_authors(csv_content)
            return func.HttpResponse(json.dumps(resultado), mimetype="application/json", status_code=200)
        else:
            # Lógica para crear un solo autor
            req_body = req.get_json()
            resultado = author_manager.upsert_author(req_body)
            return func.HttpResponse(json.dumps(resultado), mimetype="application/json", status_code=201)

    elif method == "PUT":
        # Lógica para actualizar (cambio de título, cargo, nuevos Scopus IDs)
        # Table Storage usa la misma lógica para crear o actualizar (upsert)
        req_body = req.get_json()
        if not req_body.get('id'):
            return func.HttpResponse("Falta el 'id' del autor para actualizar", status_code=400)
            
        resultado = author_manager.upsert_author(req_body)
        return func.HttpResponse(json.dumps(resultado), mimetype="application/json", status_code=200)
