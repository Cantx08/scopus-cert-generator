"""
Servicio para generar certificados en formato PDF.
"""

import io
import os
import logging
from collections import Counter
import matplotlib
# Usar el backend 'Agg' es crucial en Azure Functions para evitar errores de hilos GUI
matplotlib.use('Agg') 
import matplotlib.pyplot as plt

from reportlab.lib.pagesizes import A4
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image as RLImage
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_LEFT, TA_JUSTIFY, TA_CENTER
from reportlab.lib import colors
from reportlab.lib.units import cm
from pypdf import PdfReader, PdfWriter

class CertificadoPDFService:
    """
    Servicio para generar certificados y borradores en formato PDF.
    """
    def __init__(self, template_name="form.pdf"):
        self.template_path = os.path.join(os.path.dirname(__file__), "..", template_name)
        self.styles = self._configure_styles()

    def check_roles(self, author: dict, metadata: dict):
        """
        Verifica que tanto el docente, como el elaborador y la autoridad firmante sean diferentes.
        """
        author_name = f"{author.get('nombres', '')} {author.get('apellidos', '')}".strip().lower()
        report_creator_name = metadata.get('elaborador', '').strip().lower()
        authority_name = metadata.get('firmante_nombre', '').strip().lower()

        if report_creator_name == authority_name:
            raise ValueError("El elaborador del informe y el firmante no pueden ser la misma persona.")
        if author_name == report_creator_name:
            raise ValueError("El autor de las publicaciones no puede ser el elaborador del certificado.")
        if author_name == authority_name:
            raise ValueError("El autor de las publicaciones no puede ser el firmante del certificado.")

    def _configure_styles(self):
        """Configura los estilos basándose en style_manager.py"""
        styles = getSampleStyleSheet()
        
        styles.add(ParagraphStyle(name='MainTitle', parent=styles['Title'], fontSize=20, spaceAfter=30,
                                  alignment=TA_LEFT, textColor=colors.black, fontName='Helvetica-Bold'))
        styles.add(ParagraphStyle(name='SubTitle', parent=styles['Heading2'], fontSize=14,
                                  spaceAfter=10, spaceBefore=10, fontName='Helvetica-Bold'))
        styles.add(ParagraphStyle(name='Section', parent=styles['Heading3'], fontSize=12,
                                  spaceAfter=10, spaceBefore=10, fontName='Helvetica-Bold'))
        styles.add(ParagraphStyle(name='Justified', parent=styles['Normal'], fontSize=11,
                                  alignment=TA_JUSTIFY, spaceAfter=6, fontName='Times-Roman'))
        styles.add(ParagraphStyle(name='CaptionCenter', parent=styles['Normal'], alignment=TA_CENTER,
                                  fontSize=10, textColor=colors.black, fontName='Times-Roman'))
        styles.add(ParagraphStyle(name='Publication', parent=styles['Normal'], fontSize=11, leftIndent=20,
                                  spaceAfter=8, alignment=TA_JUSTIFY, fontName='Times-Roman'))
        styles.add(ParagraphStyle(name='Signature', parent=styles['Normal'], fontSize=11, alignment=TA_LEFT,
                                  fontName='Times-Roman', textColor=colors.black))
        styles.add(ParagraphStyle(name='AuthorTable', parent=styles['Normal'], fontSize=8,
                                  alignment=TA_LEFT, fontName='Times-Roman', textColor=colors.black))
        return styles

    def generate_pdf(self, author: dict, metadata: dict, publications: list, subject_areas: list, is_draft: bool) -> bytes:
        """Genera el PDF del certificado o borrador."""
        self.check_roles(author, metadata)

        buffer = io.BytesIO()
        
        # Configuración de formato y márgenes del documento
        doc = SimpleDocTemplate(buffer, pagesize=A4, rightMargin=2*cm, leftMargin=2*cm, topMargin=3*cm, bottomMargin=2.5*cm)
        story = []

        # --- 1. ENCABEZADO ---
        certificate_date = metadata.get("fecha_generacion", "")
        left_title = Paragraph("Certificación de Publicaciones", self.styles['MainTitle'])
        right_date = Paragraph(f"<font size=10>{certificate_date}</font>", self.styles['MainTitle'])
        
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
        full_name = f"{author.get('titulo', '')} {author.get('nombres', '')} {author.get('apellidos', '')}"
        department = author.get('departamento', 'Departamento no especificado')
        author_info = f"<b>{full_name}</b><br/><br/>{department}<br/><br/>Escuela Politécnica Nacional"
        story.append(Paragraph(f"<font size=12>{author_info}</font>", self.styles['Normal']))
        story.append(Spacer(1, 20))

        # --- 2. RESUMEN ---
        story.append(Paragraph("<b>RESUMEN</b>", self.styles['SubTitle']))
        gender = author.get("genero", "M").upper()
        gender_text = "del profesor" if gender == "M" else "de la profesora"
        pub_text = "las publicaciones" if len(publications) > 1 else "la publicación"
        memo = metadata.get("memorando", "")

        if memo:
            summary_text = f"El presente informe se realiza en base a la solicitud del memorando {memo}, con la finalidad de certificar {pub_text} {gender_text} {full_name}."
        else:
            summary_text = f"El presente informe se realiza con la finalidad de certificar {pub_text} {gender_text} {full_name}."
        
        story.append(Paragraph(summary_text, self.styles['Justified']))
        story.append(Spacer(1, 15))

        # --- 3. INFORME TÉCNICO (SCOPUS) ---
        story.append(Paragraph("Publicaciones Scopus", self.styles['SubTitle']))

        gender_section = "del" if gender == "M" else "de la"
        article = "El" if gender == "M" else "La"

        # Introducción Scopus
        intro_scopus = f"{article} {full_name}, es {author.get('cargo', 'Docente')} de la Escuela Politécnica Nacional y miembro del {department}."
        story.append(Paragraph(intro_scopus, self.styles['Justified']))
        story.append(Spacer(1, 10))
        
        stats_text = f"Ha participado en un total de {len(publications)} publicaciones Scopus. Tal como se detalla a continuación:"
        story.append(Paragraph(stats_text, self.styles['Justified']))
        story.append(Spacer(1, 15))

        any_top_10 = False
        any_no_filiation = False

        # Lista de Publicaciones
        for i, pub in enumerate(publications, 1):
            categories = str(pub.get("sjr_categories", "N/A"))
            is_top_10 = "[Categoría dentro del 10% superior" in categories
            
            # Compatibilidad con las llaves enviadas (epn_affiliation o pertenece_a_institucion_en_publicacion)
            pertenece_institucion = pub.get("epn_affiliation", pub.get("pertenece_a_institucion_en_publicacion", True))
            is_no_filiation = not pertenece_institucion

            if is_top_10:
                any_top_10 = True
            if is_no_filiation:
                any_no_filiation = True

            prefix = "** " if is_top_10 else ""
            
            # Manejo de compatibilidad de llaves para año y título
            pub_year = pub.get('pub_year') or pub.get('año', '')
            pub_title = pub.get('pub_title') or pub.get('titulo', '')
            source_title = pub.get('source_title') or pub.get('revista', '')
            
            pub_str = f"{prefix}{i}. ({pub_year}) \"{pub_title}\". {source_title}. "
            
            # Formato de indexación y Q1 (negritas)
            index_text = f"<b>Indexada en Scopus - {categories}</b>." if categories != "N/A" else "<b>Indexada en Scopus</b>."
            pub_str += index_text
            
            if pub.get("doi") and pub.get("doi") != "N/A":
                pub_str += f" DOI: {pub.get('doi')}"
            
            if is_no_filiation:
                pub_str += " <u>(Sin Filiación)</u>"
            
            # Aplicar negrita total si es Q1
            if "Q1" in categories.upper():
                pub_str = f"<b>{pub_str}</b>"

            story.append(Paragraph(pub_str, self.styles['Publication']))
        
        # --- LEYENDAS CONDICIONALES ---
        story.append(Spacer(1, 10))
        if any_top_10:
            story.append(Paragraph("** Publicación dentro del 10% superior en al menos una categoría de Scimago SJR.", self.styles['Justified']))
        if any_no_filiation:
            story.append(Paragraph("Sin Filiación: Publicación sin filiación de la Escuela Politécnica Nacional.", self.styles['Justified']))
        
        # --- GRÁFICA DE TENDENCIA ---
        story.append(Spacer(1, 15))
        story.append(Paragraph(f"Adicionalmente, en la Figura 1 se muestra la tendencia por año de las publicaciones en Scopus {gender_section} {full_name}.", self.styles['Justified']))
        story.append(Spacer(1, 10))
        
        grafica_buffer = self._draw_chart(publications)
        img = RLImage(grafica_buffer, width=15*cm, height=7.5*cm)
        story.append(img)
        story.append(Spacer(1, 10))
        story.append(Paragraph("<b>Figura 1.</b> Publicaciones Scopus por Año - Fuente web de Scopus.", self.styles['CaptionCenter']))

        # --- ÁREAS TEMÁTICAS ---
        story.append(Spacer(1, 20))
        story.append(Paragraph(f"Áreas Temáticas de publicaciones Scopus {gender_section} {full_name}", self.styles['Section']))
        
        num_areas = len(subject_areas)
        story.append(Paragraph(f"{article} {full_name}, ha publicado en {num_areas} áreas temáticas, las cuales se detallan a continuación:", self.styles['Justified']))
        story.append(Spacer(1, 10))
        
        for idx, area in enumerate(subject_areas, 1):
            area_text = f"{idx}. {area.get('subject_area')}"
            story.append(Paragraph(area_text, self.styles['Publication']))
        
        # --- 4. CONCLUSIÓN ---
        story.append(Spacer(1, 10))
        story.append(Paragraph("Conclusión", self.styles['SubTitle']))
        articulo_min = "el" if gender == "M" else "la"
        texto_conclusion = f"Por los antecedentes expuestos, la autoridad competente certifica que {articulo_min} {full_name}, cuenta con un total de {len(publications)} publicaciones. {articulo_min.capitalize()} {full_name} puede hacer uso del presente certificado para lo que considere necesario."
        story.append(Paragraph(texto_conclusion, self.styles['Justified']))
        
        # --- 5. FIRMAS (Empujadas al final) ---
        story.append(Spacer(1, 60))
        story.append(Paragraph(f"<b>{metadata.get('firmante_nombre', '').upper()}</b>", self.styles['Signature']))
        story.append(Paragraph(f"<b>{metadata.get('firmante_cargo', '').upper()} DE LA ESCUELA POLITÉCNICA NACIONAL</b>", self.styles['Signature']))
        story.append(Spacer(1, 15))

        # Tabla del elaborador con bordes
        table_details = [[Paragraph("Elaborado por:", self.styles['AuthorTable']), Paragraph(metadata.get('elaborador', ''), self.styles['AuthorTable'])]]
        author_table = Table(table_details, colWidths=[2.5*cm, 4*cm], hAlign='LEFT')
        author_table.setStyle(TableStyle([
            ('GRID', (0, 0), (-1, -1), 0.5, colors.black),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('LEFTPADDING', (0, 0), (-1, -1), 2),
            ('TOPPADDING', (0, 0), (-1, -1), 1),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 1),
        ]))
        story.append(author_table)

        # Construcción del PDF
        doc.build(story)
        pdf_bytes = buffer.getvalue()
        buffer.close()

        # Añadir la plantilla solo si es un documento final (no es borrador)
        if not is_draft:
            pdf_bytes = self._add_template(pdf_bytes)

        return pdf_bytes

    def _draw_chart(self, publications: list) -> io.BytesIO:
        """Genera un gráfico de tendencias por año con estilos personalizados."""
        # Preparar datos a partir de la lista de publicaciones
        years = []
        for p in publications:
            year_str = str(p.get("pub_year") or p.get("año", ""))
            if year_str.isdigit():
                years.append(int(year_str))
                
        pub_count = Counter(years)
        if not pub_count:
            # Fallback en caso de que no haya publicaciones válidas
            pub_count = {2024: 0}
            
        years = sorted(pub_count.keys())
        counts = [pub_count[y] for y in years]

        # Crear figura
        plt.figure(figsize=(8, 4))
        
        # Crear gráfico de línea con colores personalizados
        plt.plot(years, counts, marker='o', linewidth=2, markersize=6, color='#009ece')
        
        # Configurar etiquetas y título
        plt.xlabel('Year', fontsize=10, ha='center', color='#2e2e2e')
        plt.ylabel('Documents', fontsize=10, ha='center', color='#2e2e2e')
        plt.title('Documents by year', fontsize=13, pad=15, color='#2e2e2e', loc='left')
        
        # Configurar grid - solo líneas horizontales
        plt.grid(axis='y', alpha=0.3, color='#cccccc')
        
        # Hacer transparentes los bordes de la gráfica
        for spine in plt.gca().spines.values():
            spine.set_visible(False)
        
        # Eliminar márgenes
        plt.margins(0)
        plt.tight_layout(pad=0)
        
        # Configurar límites de ejes
        plt.xlim(min(years) - 0.5, max(years) + 0.5)
        plt.ylim(0, max(counts) + 1)
        
        # Configurar ticks dinámicamente según la cantidad de datos
        # X-axis (años): determinar el paso según el rango de años
        year_range = max(years) - min(years) + 1
        if year_range <= 15:
            x_step = 1
        elif year_range <= 30:
            x_step = 2
        elif year_range <= 45:
            x_step = 3
        else:
            x_step = 5
        
        x_ticks = list(range(min(years), max(years) + 1, x_step))
        plt.xticks(x_ticks, color='#2e2e2e')
        
        # Y-axis (número de publicaciones): determinar el paso según el máximo
        max_count = max(counts) if counts else 1
        if max_count <= 5:
            y_step = 1
        elif max_count <= 10:
            y_step = 2
        elif max_count <= 20:
            y_step = 3
        else:
            y_step = 5
        
        y_ticks = list(range(0, max_count + y_step + 1, y_step))
        plt.yticks(y_ticks, color='#2e2e2e')
        
        # Guardar como imagen en memoria
        img_buffer = io.BytesIO()
        plt.savefig(img_buffer, format='png', dpi=150, bbox_inches='tight', transparent=True)
        plt.close()
        img_buffer.seek(0)
        
        return img_buffer
    
    def _add_template(self, content_bytes: bytes) -> bytes:
        """Aplica el template para generar el certificado final."""
        if not os.path.exists(self.template_path):
            logging.warning("No se encontró la template. Retornando borrador.")
            return content_bytes

        try:
            content_reader = PdfReader(io.BytesIO(content_bytes))
            writer = PdfWriter()

            # Agregar template para generar el certificado final
            for page in content_reader.pages:
                template_reader = PdfReader(self.template_path)
                page_merge = template_reader.pages[0]
                page_merge.merge_page(page)
                writer.add_page(page_merge)

            output_buffer = io.BytesIO()
            writer.write(output_buffer)
            return output_buffer.getvalue()

        except Exception as e:
            logging.error("Error al agregar template: %s", str(e))
            return content_bytes
