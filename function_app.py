import azure.functions as func
import logging
import json
import base64
import os
import asyncio
from httpx import AsyncClient

# Importamos nuestros módulos limpios
from services.scopus_service import ScopusExtractor
from services.sjr_service import SJRMapper
from services.pdf_service import CertificadoPDFService

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

# ==========================================
# INICIALIZACIÓN GLOBAL (Cold Start)
# ==========================================
RUTA_SJR = os.environ.get("SJR_CSV_PATH", "df_sjr_24_04_2025.csv")

sjr_mapper = None 
try:
    sjr_mapper = SJRMapper(RUTA_SJR)
except Exception as e:
    logging.error(f"Error crítico: No se pudo cargar el archivo SJR en caché: {e}")

@app.route(route="GenerarCertificado", methods=["POST"])
async def GenerarCertificado(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Iniciando pipeline de generación de certificado.')

    try:
        # 1. RECIBIR Y VALIDAR DATOS DEL FRONTEND
        req_body = req.get_json()
        scopus_ids = req_body.get('scopus_ids', [])
        author_data = req_body.get('author', {})
        metadata = req_body.get('metadata', {})
        is_draft = req_body.get('is_draft', True)

        if not scopus_ids or not isinstance(scopus_ids, list):
            return func.HttpResponse(json.dumps({"error": "Se requiere una lista válida de 'scopus_ids'."}), status_code=400)

        # 2. VALIDACIÓN DE REGLAS DE NEGOCIO (Roles)
        pdf_service = CertificadoPDFService()
        try:
            pdf_service.check_roles(author_data, metadata)
        except ValueError as ve:
            return func.HttpResponse(json.dumps({"error": str(ve)}), status_code=400, mimetype="application/json")

        # 3. EXTRACCIÓN DE SCOPUS (Paso 1)
        extractor = ScopusExtractor()
        async with AsyncClient(timeout=extractor.timeout) as client:
            tasks = [extractor.get_publications(sid, client) for sid in scopus_ids]
            pubs_results = await asyncio.gather(*tasks)
            
            # Limpieza de duplicados por DOI
            todas_publicaciones = []
            seen_dois = set()
            for sublist in pubs_results:
                for pub in sublist:
                    doi = pub.get('doi')
                    if doi and doi != "N/A":
                        if doi not in seen_dois:
                            seen_dois.add(doi)
                            todas_publicaciones.append(pub)
                    else:
                        todas_publicaciones.append(pub)
            
            # (Opcional) Si quieres incluir las áreas temáticas en el PDF, las extraes aquí
            # areas_tematicas = await extractor.get_subject_areas(scopus_ids, client)

        # 4. MAPEO SJR (Paso 2)
        if sjr_mapper:
            todas_publicaciones = sjr_mapper.map_publications(todas_publicaciones)
        else:
            logging.warning("SJR Mapper falló. Se generará sin datos SJR.")

        # 5. GENERACIÓN DE PDF (Paso 3)
        pdf_bytes = pdf_service.generate_pdf(
            author=author_data,
            metadata=metadata,
            publications=todas_publicaciones,
            is_draft=is_draft
        )

        # 6. RESPUESTA AL FRONTEND
        pdf_base64 = base64.b64encode(pdf_bytes).decode('utf-8')
        tipo_doc = "Borrador" if is_draft else "Certificado Final"

        return func.HttpResponse(
            json.dumps({
                "mensaje": f"{tipo_doc} generado exitosamente",
                "total_publicaciones": len(todas_publicaciones),
                "pdf_base64": pdf_base64,
                "nombre_sugerido": f"Certificado_{author_data.get('apellidos', 'Autor')}.pdf"
            }),
            mimetype="application/json",
            status_code=200
        )

    except Exception as e:
        logging.error(f"Error en el pipeline principal: {str(e)}")
        return func.HttpResponse(json.dumps({"error": f"Error interno del servidor: {str(e)}"}), status_code=500, mimetype="application/json")