"""
Azure Function principal para generar certificados en PDF basados en datos de Scopus y SJR.
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

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

# ==========================================
# INICIALIZACIÓN GLOBAL (Cold Start)
# ==========================================
# Cargar el CSV pesado en la memoria global optimiza los tiempos de respuesta
RUTA_SJR = os.environ.get("SJR_CSV_PATH", "df_sjr_24_04_2025.csv")

sjr_mapper = None 
try:
    sjr_mapper = SJRMapper(RUTA_SJR)
    logging.info("SJR Mapper inicializado correctamente en caché global.")
except Exception as e:
    logging.error("Error crítico: No se pudo cargar el archivo SJR en caché: %s", str(e))


@app.route(route="GenerateCertificate", methods=["POST"])
async def GenerateCertificate(req: func.HttpRequest) -> func.HttpResponse:
    """
    Genera un certificado en formato PDF basado en los datos proporcionados.
    """
    logging.info('Iniciando pipeline de generación de certificado.')

    try:
        # 1. RECIBIR Y VALIDAR DATOS DEL FRONTEND
        try:
            req_body = req.get_json()
        except ValueError:
            return func.HttpResponse(json.dumps({"error": "El cuerpo de la solicitud debe ser un JSON válido."}), status_code=400, mimetype="application/json")

        scopus_ids = req_body.get('scopus_ids', [])
        author_data = req_body.get('author', {})
        metadata = req_body.get('metadata', {})
        is_draft = req_body.get('is_draft', True)

        # Validaciones de integridad del payload
        if not scopus_ids or not isinstance(scopus_ids, list):
            return func.HttpResponse(json.dumps({"error": "Se requiere una lista válida de 'scopus_ids'."}), status_code=400, mimetype="application/json")
        
        if not author_data or not metadata:
            return func.HttpResponse(json.dumps({"error": "Faltan datos en el objeto 'author' o 'metadata'."}), status_code=400, mimetype="application/json")

        # 2. VALIDACIÓN DE ROLES (Reglas de negocio)
        pdf_service = CertificadoPDFService()
        try:
            pdf_service.check_roles(author_data, metadata)
        except ValueError as ve:
            logging.warning(f"Validación de roles fallida: {ve}")
            return func.HttpResponse(json.dumps({"error": str(ve)}), status_code=400, mimetype="application/json")

        # 3. EXTRACCIÓN DE SCOPUS (Paso 1)
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

        # 4. MAPEO SJR (Paso 2)
        if sjr_mapper:
            all_publications = sjr_mapper.map_publications(all_publications)
        else:
            logging.warning("SJR Mapper falló o no está inicializado. Se generará sin datos SJR.")

        # 5. GENERACIÓN DE PDF (Paso 3)
        pdf_bytes = pdf_service.generate_pdf(
            author=author_data,
            metadata=metadata,
            publications=all_publications,
            subject_areas=subject_areas,
            is_draft=is_draft
        )

        # 6. RESPUESTA PDF AL FRONTEND
        pdf_base64 = base64.b64encode(pdf_bytes).decode('utf-8')
        tipo_doc = "Borrador" if is_draft else "Certificado Final"

        return func.HttpResponse(
            json.dumps({
                "mensaje": f"{tipo_doc} generado exitosamente",
                "total_publicaciones": len(all_publications),
                "pdf_base64": pdf_base64,
                "nombre_sugerido": f"Certificado_{author_data.get('apellidos', 'Autor')}.pdf"
            }),
            mimetype="application/json",
            status_code=200
        )

    except Exception as e:
        # exc_info=True adjunta la traza del error (Stack Trace) a los logs de Azure
        logging.error("Error en el pipeline principal: %s", str(e), exc_info=True)
        return func.HttpResponse(json.dumps({"error": f"Error interno del servidor: {str(e)}"}), status_code=500, mimetype="application/json")