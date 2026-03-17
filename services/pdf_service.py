import io
import os
import logging
from reportlab.lib.pagesizes import A4
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_JUSTIFY
from reportlab.lib import colors
from reportlab.lib.units import cm
from pypdf import PdfReader, PdfWriter

class CertificadoPDFService:
    def __init__(self, template_name="form.pdf"):
        self.template_path = os.path.join(os.path.dirname(__file__), "..", template_name)
        self.styles = self._configurar_estilos()

    def validar_roles(self, autor: dict, metadatos: dict):
        """Valida que los roles no se crucen según reglas de negocio."""
        nombre_autor = f"{autor.get('nombres', '')} {autor.get('apellidos', '')}".strip().lower()
        nombre_elaborador = metadatos.get('elaborador', '').strip().lower()
        nombre_firmante = metadatos.get('firmante_nombre', '').strip().lower()

        if nombre_elaborador == nombre_firmante:
            raise ValueError("El elaborador del informe y el firmante no pueden ser la misma persona.")
        if nombre_autor == nombre_elaborador:
            raise ValueError("El autor de las publicaciones no puede ser el elaborador del certificado.")
        if nombre_autor == nombre_firmante:
            raise ValueError("El autor de las publicaciones no puede ser el firmante del certificado.")

    def _configurar_estilos(self):
        """Configura los estilos basándose en style_manager.py"""
        styles = getSampleStyleSheet()
        
        styles.add(ParagraphStyle(name='MainTitle', parent=styles['Title'], fontSize=20, spaceAfter=30,
                                  alignment=TA_LEFT, textColor=colors.black, fontName='Helvetica-Bold'))
        styles.add(ParagraphStyle(name='SubTitle', parent=styles['Heading2'], fontSize=12,
                                  spaceAfter=10, spaceBefore=10, fontName='Helvetica-Bold'))
        styles.add(ParagraphStyle(name='Justified', parent=styles['Normal'], fontSize=11,
                                  alignment=TA_JUSTIFY, spaceAfter=6, fontName='Times-Roman'))
        styles.add(ParagraphStyle(name='Publication', parent=styles['Normal'], fontSize=11, leftIndent=20,
                                  spaceAfter=8, alignment=TA_JUSTIFY, fontName='Times-Roman'))
        styles.add(ParagraphStyle(name='Signature', parent=styles['Normal'], fontSize=11, alignment=TA_LEFT,
                                  fontName='Times-Roman', textColor=colors.black))
        styles.add(ParagraphStyle(name='AuthorTable', parent=styles['Normal'], fontSize=8,
                                  alignment=TA_LEFT, fontName='Times-Roman', textColor=colors.black))
        return styles

    def generar_documento(self, autor: dict, metadatos: dict, publicaciones: list, es_borrador: bool) -> bytes:
        self.validar_roles(autor, metadatos)

        buffer = io.BytesIO()
        # Usamos SimpleDocTemplate con márgenes ajustados para dar espacio al membrete
        doc = SimpleDocTemplate(buffer, pagesize=A4, rightMargin=2*cm, leftMargin=2*cm, topMargin=3*cm, bottomMargin=2.5*cm)
        story = []

        # --- 1. ENCABEZADO ---
        fecha = metadatos.get("fecha_generacion", "")
        left_title = Paragraph("Certificación de Publicaciones", self.styles['MainTitle'])
        right_date = Paragraph(f"<font size=10>{fecha}</font>", self.styles['MainTitle'])
        
        # Tabla para alinear título y fecha en la misma línea
        title_table = Table([[left_title, right_date]], colWidths=[11*cm, 6*cm], hAlign='LEFT')
        title_table.setStyle(TableStyle([
            ('VALIGN', (0, 0), (-1, -1), 'BOTTOM'),
            ('ALIGN', (0, 0), (0, 0), 'LEFT'),
            ('ALIGN', (1, 0), (1, 0), 'RIGHT'),
            ('LEFTPADDING', (0, 0), (-1, -1), 0),
            ('RIGHTPADDING', (0, 0), (-1, -1), 0),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 0),
        ]))
        story.append(title_table)
        story.append(Spacer(1, 20))

        # Información del docente
        nombre_completo = f"{autor.get('nombres', '')} {autor.get('apellidos', '')}"
        depto = autor.get('departamento', 'Departamento no especificado')
        author_info = f"<b>{nombre_completo}</b><br/><br/>{depto}<br/><br/>Escuela Politécnica Nacional"
        story.append(Paragraph(f"<font size=12>{author_info}</font>", self.styles['Normal']))
        story.append(Spacer(1, 20))

        # --- 2. RESUMEN ---
        story.append(Paragraph("<b>RESUMEN</b>", self.styles['SubTitle']))
        genero = autor.get("genero", "M").upper()
        genero_text = "del profesor" if genero == "M" else "de la profesora"
        pub_text = "las publicaciones" if len(publicaciones) > 1 else "la publicación"
        memo = metadatos.get("memorando", "")
        
        if memo:
            resumen = f"El presente informe se realiza en base a la solicitud del memorando {memo}, con la finalidad de certificar {pub_text} {genero_text} {nombre_completo}."
        else:
            resumen = f"El presente informe se realiza con la finalidad de certificar {pub_text} {genero_text} {nombre_completo}."
        
        story.append(Paragraph(resumen, self.styles['Justified']))
        story.append(Spacer(1, 15))

        # --- 3. INFORME TÉCNICO (SCOPUS) ---
        story.append(Paragraph("Publicaciones Scopus", self.styles['SubTitle']))
        articulo = "El" if genero == "M" else "La"
        intro_scopus = f"{articulo} {nombre_completo}, es {autor.get('cargo', 'Docente')} de la Escuela Politécnica Nacional y miembro del {depto}."
        story.append(Paragraph(intro_scopus, self.styles['Justified']))
        story.append(Spacer(1, 10))
        
        stats_text = f"Ha participado en un total de {len(publicaciones)} publicaciones Scopus. Tal como se detalla a continuación:"
        story.append(Paragraph(stats_text, self.styles['Justified']))
        story.append(Spacer(1, 15))

        # Lista de Publicaciones
        for i, pub in enumerate(publicaciones, 1):
            categorias = str(pub.get("sjr_categorias", "N/A"))
            has_q1 = "Q1" in categorias.upper()
            
            pub_str = f"{i}. ({pub.get('año', '')}) \"{pub.get('titulo', '')}\". {pub.get('revista', '')}. "
            if categorias != "N/A":
                pub_str += f"<b>Indexada en Scopus - {categorias}</b>."
            else:
                pub_str += "<b>Indexada en Scopus</b>."
            
            doi = pub.get("doi", "")
            if doi and doi != "N/A":
                pub_str += f" DOI: {doi}"
            
            # Nota de filiación si no pertenece a la institución
            if not pub.get("pertenece_a_institucion_en_publicacion", True):
                pub_str += " <u>(Sin Filiación)</u>"
            
            # Si tiene Q1, aplicar negritas a todo el párrafo
            if has_q1:
                pub_str = f"<b>{pub_str}</b>"

            story.append(Paragraph(pub_str, self.styles['Publication']))
        
        story.append(Spacer(1, 15))

        # --- 4. CONCLUSIÓN ---
        story.append(Paragraph("Conclusión", self.styles['SubTitle']))
        articulo_min = "el" if genero == "M" else "la"
        texto_conclusion = f"Por los antecedentes expuestos, la autoridad competente certifica que {articulo_min} {nombre_completo}, cuenta con un total de {len(publicaciones)} publicaciones. {articulo_min.capitalize()} {nombre_completo} puede hacer uso del presente certificado para lo que considere necesario."
        story.append(Paragraph(texto_conclusion, self.styles['Justified']))
        
        # --- 5. FIRMAS (Empujadas al final) ---
        story.append(Spacer(1, 60))
        story.append(Paragraph(f"<b>{metadatos.get('firmante_nombre', '').upper()}</b>", self.styles['Signature']))
        story.append(Paragraph(f"<b>{metadatos.get('firmante_cargo', '').upper()} DE LA ESCUELA POLITÉCNICA NACIONAL</b>", self.styles['Signature']))
        story.append(Spacer(1, 15))

        # Tabla del elaborador con bordes
        table_details = [[Paragraph("Elaborado por:", self.styles['AuthorTable']), Paragraph(metadatos.get('elaborador', ''), self.styles['AuthorTable'])]]
        author_table = Table(table_details, colWidths=[2.5*cm, 4*cm], hAlign='LEFT')
        author_table.setStyle(TableStyle([
            ('GRID', (0, 0), (-1, -1), 0.5, colors.black),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('LEFTPADDING', (0, 0), (-1, -1), 2),
            ('TOPPADDING', (0, 0), (-1, -1), 1),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 1),
        ]))
        story.append(author_table)

        # --- CALLBACK DE MARCA DE AGUA ---
        def draw_watermark(canvas, doc):
            """Dibuja la marca de agua 'BORRADOR NO VÁLIDO' en el centro de cada página."""
            canvas.saveState()
            canvas.setFont("Helvetica-Bold", 60)
            canvas.setFillColorRGB(0.85, 0.85, 0.85) # Gris claro
            canvas.translate(A4[0]/2, A4[1]/2)
            canvas.rotate(45)
            canvas.drawCentredString(0, 0, "BORRADOR NO VÁLIDO")
            canvas.restoreState()

        # Construcción del PDF
        if es_borrador:
            doc.build(story, onFirstPage=draw_watermark, onLaterPages=draw_watermark)
        else:
            doc.build(story)

        pdf_bytes = buffer.getvalue()
        buffer.close()

        # Si es final, superponer sobre la plantilla
        if not es_borrador:
            pdf_bytes = self._aplicar_plantilla(pdf_bytes)

        return pdf_bytes

    def _aplicar_plantilla(self, content_bytes: bytes) -> bytes:
        if not os.path.exists(self.template_path):
            logging.warning("No se encontró form.pdf en el servidor. Retornando PDF sin plantilla.")
            return content_bytes

        try:
            content_reader = PdfReader(io.BytesIO(content_bytes))
            writer = PdfWriter()

            for page in content_reader.pages:
                # Copia fresca del fondo institucional por cada página generada
                template_reader = PdfReader(self.template_path)
                page_merge = template_reader.pages[0]
                page_merge.merge_page(page)
                writer.add_page(page_merge)

            output_buffer = io.BytesIO()
            writer.write(output_buffer)
            return output_buffer.getvalue()
            
        except Exception as e:
            logging.error(f"Error al aplicar plantilla: {e}")
            return content_bytes
        