"""
Excel to Power BI Requirements - Fillable PDF Generator
Author: Avinash Rai (AWA)
Email: masteravinashrai@gmail.com
"""
from reportlab.pdfgen import canvas
from reportlab.pdfbase.acroform import AcroForm


from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib import colors
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, 
    PageBreak, KeepTogether, Frame, PageTemplate
)
from reportlab.pdfgen import canvas
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT
from reportlab.pdfbase import pdfform
from reportlab.acroform import AcroForm
from datetime import datetime
import os

# ============================================================================
# CUSTOM PAGE TEMPLATE WITH HEADER/FOOTER
# ============================================================================

class NumberedCanvas(canvas.Canvas):
    """Custom canvas with header and footer on every page"""
    
    def __init__(self, *args, **kwargs):
        canvas.Canvas.__init__(self, *args, **kwargs)
        self.pages = []
        
    def showPage(self):
        self.pages.append(dict(self.__dict__))
        self._startPage()
        
    def save(self):
        page_count = len(self.pages)
        for page_num, page in enumerate(self.pages, start=1):
            self.__dict__.update(page)
            self.draw_page_decorations(page_num, page_count)
            canvas.Canvas.showPage(self)
        canvas.Canvas.save(self)
        
    def draw_page_decorations(self, page_num, page_count):
        """Draw header and footer on each page"""
        page_width, page_height = letter
        
        # HEADER
        self.saveState()
        self.setFont('Helvetica-Bold', 10)
        self.setFillColor(colors.HexColor('#0066CC'))
        
        header_text = "AWA | Avinash Rai | masteravinashrai@gmail.com"
        self.drawString(0.75 * inch, page_height - 0.5 * inch, header_text)
        
        # Header line
        self.setStrokeColor(colors.HexColor('#0066CC'))
        self.setLineWidth(1)
        self.line(0.75 * inch, page_height - 0.6 * inch, 
                 page_width - 0.75 * inch, page_height - 0.6 * inch)
        
        # FOOTER
        self.setFont('Helvetica', 9)
        self.setFillColor(colors.grey)
        
        footer_text = f"© 2025 AWA | Prepared by Avinash Rai"
        footer_x = 0.75 * inch
        footer_y = 0.5 * inch
        self.drawString(footer_x, footer_y, footer_text)
        
        # Page number
        page_text = f"Page {page_num} of {page_count}"
        page_x = page_width - 1.5 * inch
        self.drawString(page_x, footer_y, page_text)
        
        # Footer line
        self.setStrokeColor(colors.grey)
        self.setLineWidth(0.5)
        self.line(0.75 * inch, footer_y + 0.2 * inch, 
                 page_width - 0.75 * inch, footer_y + 0.2 * inch)
        
        self.restoreState()

# ============================================================================
# STYLES
# ============================================================================

def get_custom_styles():
    """Create custom paragraph styles"""
    styles = getSampleStyleSheet()
    
    # Title style
    styles.add(ParagraphStyle(
        name='CustomTitle',
        parent=styles['Title'],
        fontName='Helvetica-Bold',
        fontSize=20,
        textColor=colors.HexColor('#0066CC'),
        alignment=TA_CENTER,
        spaceAfter=20,
        spaceBefore=10
    ))
    
    # Section Header
    styles.add(ParagraphStyle(
        name='SectionHeader',
        parent=styles['Heading1'],
        fontName='Helvetica-Bold',
        fontSize=14,
        textColor=colors.HexColor('#0052A3'),
        spaceBefore=16,
        spaceAfter=10,
        borderWidth=1,
        borderColor=colors.HexColor('#0066CC'),
        borderPadding=5,
        backColor=colors.HexColor('#E8F4F8')
    ))
    
    # Subsection Header
    styles.add(ParagraphStyle(
        name='SubsectionHeader',
        parent=styles['Heading2'],
        fontName='Helvetica-Bold',
        fontSize=12,
        textColor=colors.HexColor('#0052A3'),
        spaceBefore=12,
        spaceAfter=8
    ))
    
    # Normal text
    styles.add(ParagraphStyle(
        name='CustomNormal',
        parent=styles['Normal'],
        fontName='Helvetica',
        fontSize=10,
        leading=14,
        textColor=colors.black
    ))
    
    # Small text
    styles.add(ParagraphStyle(
        name='SmallText',
        parent=styles['Normal'],
        fontName='Helvetica',
        fontSize=8,
        textColor=colors.grey
    ))
    
    return styles

# ============================================================================
# PDF GENERATOR CLASS
# ============================================================================

class FillablePDFGenerator:
    """Generate fillable PDF with form fields"""
    
    def __init__(self, output_filename="Excel_PowerBI_Requirements_Form.pdf"):
        self.output_filename = output_filename
        self.elements = []
        self.styles = get_custom_styles()
        self.field_counter = 0
        
    def create_text_field(self, name, x, y, width, height=15):
        """Create a text input field"""
        self.field_counter += 1
        return {
            'type': 'text',
            'name': f'{name}_{self.field_counter}',
            'x': x,
            'y': y,
            'width': width,
            'height': height
        }
    
    def add_title_page(self):
        """Add title page"""
        self.elements.append(Spacer(1, 0.5*inch))
        
        # Main title
        title = Paragraph(
            "Excel to Power BI Migration<br/>Requirements Gathering Document",
            self.styles['CustomTitle']
        )
        self.elements.append(title)
        self.elements.append(Spacer(1, 0.3*inch))
        
        # Subtitle
        subtitle = Paragraph(
            "<i>Professional Requirements Collection Form</i>",
            self.styles['CustomNormal']
        )
        self.elements.append(subtitle)
        self.elements.append(Spacer(1, 0.5*inch))
        
        # Info box
        info_data = [
            ["Document Purpose:", "Systematic requirements gathering for Excel to Power BI migration"],
            ["Prepared By:", "Avinash Rai (AWA)"],
            ["Contact:", "masteravinashrai@gmail.com"],
            ["Date Generated:", datetime.now().strftime("%B %d, %Y")],
            ["Version:", "1.0"]
        ]
        
        info_table = Table(info_data, colWidths=[1.5*inch, 4.5*inch])
        info_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (0, -1), colors.HexColor('#E8F4F8')),
            ('TEXTCOLOR', (0, 0), (-1, -1), colors.black),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 10),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
            ('TOPPADDING', (0, 0), (-1, -1), 8),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE')
        ]))
        
        self.elements.append(info_table)
        self.elements.append(Spacer(1, 0.5*inch))
        
        # Instructions
        instructions = Paragraph(
            "<b>Instructions:</b><br/>"
            "This is a fillable PDF form. Click on any field to enter information. "
            "Checkboxes can be clicked to select/deselect. Complete all sections during "
            "your screen-sharing session with the client. Save the completed form for your records.",
            self.styles['CustomNormal']
        )
        self.elements.append(instructions)
        
        self.elements.append(PageBreak())
    
    def add_section_1(self):
        """Section 1: Project Overview"""
        self.elements.append(Paragraph("SECTION 1: PROJECT OVERVIEW", self.styles['SectionHeader']))
        self.elements.append(Spacer(1, 0.15*inch))
        
        # Table with fillable fields
        data = [
            ["Client Name:", ""],
            ["Session Date:", ""],
            ["Excel File Name:", ""],
            ["File Size:", " _____ MB"],
            ["Primary Contact:", ""],
            ["Email:", ""],
        ]
        
        table = Table(data, colWidths=[2*inch, 4*inch])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (0, -1), colors.HexColor('#F0F8FF')),
            ('TEXTCOLOR', (0, 0), (-1, -1), colors.black),
            ('ALIGN', (0, 0), (0, -1), 'RIGHT'),
            ('ALIGN', (1, 0), (1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
            ('FONTNAME', (1, 0), (1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 0), (-1, -1), 10),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 10),
            ('TOPPADDING', (0, 0), (-1, -1), 10),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE')
        ]))
        
        self.elements.append(table)
        self.elements.append(Spacer(1, 0.2*inch))
    
    def add_section_2(self):
        """Section 2: Data Scope & Volume"""
        self.elements.append(Paragraph("SECTION 2: DATA SCOPE & VOLUME", self.styles['SectionHeader']))
        self.elements.append(Spacer(1, 0.15*inch))
        
        # Subsection 2.1
        self.elements.append(Paragraph("2.1 Data Time Period", self.styles['SubsectionHeader']))
        
        text = Paragraph(
            "<b>Current data in Excel:</b>",
            self.styles['CustomNormal']
        )
        self.elements.append(text)
        self.elements.append(Spacer(1, 0.1*inch))
        
        data = [
            ["Earliest Date:", "_____________"],
            ["Latest Date:", "_____________"],
            ["Total Time Span:", "_____ years _____ months"],
        ]
        
        table = Table(data, colWidths=[2*inch, 3.5*inch])
        table.setStyle(TableStyle([
            ('TEXTCOLOR', (0, 0), (-1, -1), colors.black),
            ('ALIGN', (0, 0), (0, -1), 'RIGHT'),
            ('ALIGN', (1, 0), (1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 10),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
            ('TOPPADDING', (0, 0), (-1, -1), 8),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.lightgrey)
        ]))
        
        self.elements.append(table)
        self.elements.append(Spacer(1, 0.15*inch))
        
        # Data to migrate
        self.elements.append(Paragraph("<b>Data to migrate to Power BI:</b>", self.styles['CustomNormal']))
        self.elements.append(Spacer(1, 0.1*inch))
        
        checkbox_data = [
            ["☐ Last 2 years only (Recommended)", "From: _____________ To: _____________"],
            ["☐ Last 3 years", ""],
            ["☐ Last 5 years", ""],
            ["☐ All historical data", ""],
            ["☐ Other period:", "_____________"],
        ]
        
        checkbox_table = Table(checkbox_data, colWidths=[2.5*inch, 3*inch])
        checkbox_table.setStyle(TableStyle([
            ('TEXTCOLOR', (0, 0), (-1, -1), colors.black),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
            ('TOPPADDING', (0, 0), (-1, -1), 6),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE')
        ]))
        
        self.elements.append(checkbox_table)
        self.elements.append(Spacer(1, 0.2*inch))
        
        # Subsection 2.2
        self.elements.append(Paragraph("2.2 Data Volume", self.styles['SubsectionHeader']))
        
        volume_data = [
            ["Metric", "Value"],
            ["Total Rows in Excel", ""],
            ["Rows to migrate", ""],
            ["Total Columns", ""],
            ["Last Updated", ""],
        ]
        
        volume_table = Table(volume_data, colWidths=[2.5*inch, 3*inch])
        volume_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#0066CC')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('TEXTCOLOR', (0, 1), (-1, -1), colors.black),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTNAME', (0, 1), (0, -1), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 10),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
            ('TOPPADDING', (0, 0), (-1, -1), 8),
            ('GRID', (0, 0), (-1, -1), 1, colors.grey)
        ]))
        
        self.elements.append(volume_table)
        self.elements.append(Spacer(1, 0.15*inch))
        
        # Update Frequency
        freq_text = Paragraph("<b>Update Frequency:</b>", self.styles['CustomNormal'])
        self.elements.append(freq_text)
        
        freq_options = [
            ["☐ Daily", "☐ Weekly", "☐ Monthly"],
            ["☐ Quarterly", "☐ Ad-hoc", ""],
        ]
        
        freq_table = Table(freq_options, colWidths=[1.8*inch, 1.8*inch, 1.9*inch])
        freq_table.setStyle(TableStyle([
            ('TEXTCOLOR', (0, 0), (-1, -1), colors.black),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
            ('TOPPADDING', (0, 0), (-1, -1), 5)
        ]))
        
        self.elements.append(freq_table)
        self.elements.append(PageBreak())
    
    def add_section_3(self):
        """Section 3: File Structure"""
        self.elements.append(Paragraph("SECTION 3: FILE STRUCTURE", self.styles['SectionHeader']))
        self.elements.append(Spacer(1, 0.15*inch))
        
        self.elements.append(Paragraph("3.1 Sheet Inventory", self.styles['SubsectionHeader']))
        
        total = Paragraph("<b>Total Sheets:</b> _____", self.styles['CustomNormal'])
        self.elements.append(total)
        self.elements.append(Spacer(1, 0.1*inch))
        
        # Sheet table headers
        sheet_data = [
            ["#", "Sheet Name", "Type", "Rows", "Purpose", "Migrate?"],
        ]
        
        # Add 5 empty rows
        for i in range(1, 6):
            sheet_data.append([
                str(i), "", "☐ Data ☐ Lookup ☐ Dashboard", "", "", "☐ Yes ☐ No"
            ])
        
        sheet_table = Table(sheet_data, colWidths=[0.3*inch, 1.3*inch, 1.8*inch, 0.6*inch, 1.2*inch, 1*inch])
        sheet_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#0066CC')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('TEXTCOLOR', (0, 1), (-1, -1), colors.black),
            ('ALIGN', (0, 0), (0, -1), 'CENTER'),
            ('ALIGN', (1, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 9),
            ('FONTSIZE', (0, 1), (-1, -1), 8),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
            ('TOPPADDING', (0, 0), (-1, -1), 6),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE')
        ]))
        
        self.elements.append(sheet_table)
        self.elements.append(Spacer(1, 0.15*inch))
        
        # Main data and dashboard sheets
        summary_data = [
            ["Main Data Sheet:", "_____________"],
            ["Dashboard/Visual Sheet:", "_____________"],
        ]
        
        summary_table = Table(summary_data, colWidths=[2.2*inch, 3.3*inch])
        summary_table.setStyle(TableStyle([
            ('TEXTCOLOR', (0, 0), (-1, -1), colors.black),
            ('ALIGN', (0, 0), (0, -1), 'RIGHT'),
            ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 10),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
            ('TOPPADDING', (0, 0), (-1, -1), 8),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.lightgrey)
        ]))
        
        self.elements.append(summary_table)
        self.elements.append(PageBreak())
    
    def add_section_4(self):
        """Section 4: Main Data Structure"""
        self.elements.append(Paragraph("SECTION 4: MAIN DATA STRUCTURE", self.styles['SectionHeader']))
        self.elements.append(Spacer(1, 0.15*inch))
        
        self.elements.append(Paragraph("4.1 Data Table Details", self.styles['SubsectionHeader']))
        
        details_data = [
            ["Sheet Name:", "_____________"],
            ["Data Range:", "_____ to _____"],
            ["Table Name (if any):", "_____________"],
        ]
        
        details_table = Table(details_data, colWidths=[2*inch, 3.5*inch])
        details_table.setStyle(TableStyle([
            ('TEXTCOLOR', (0, 0), (-1, -1), colors.black),
            ('ALIGN', (0, 0), (0, -1), 'RIGHT'),
            ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 10),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
            ('TOPPADDING', (0, 0), (-1, -1), 8),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.lightgrey)
        ]))
        
        self.elements.append(details_table)
        self.elements.append(Spacer(1, 0.2*inch))
        
        # Column Inventory
        self.elements.append(Paragraph("4.2 Column Inventory", self.styles['SubsectionHeader']))
        
        note = Paragraph(
            "<i>📸 Screenshot Required: Column headers + first 10 rows</i>",
            self.styles['SmallText']
        )
        self.elements.append(note)
        self.elements.append(Spacer(1, 0.1*inch))
        
        # Column table - compact version for first 5 columns
        col_data = [
            ["#", "Column Name", "Data Type", "Has Formula?", "Required?"],
        ]
        
        for i in range(1, 6):
            col_data.append([
                str(i), "", "☐ Text ☐ Number\n☐ Date ☐ Currency", "☐ Yes ☐ No", "☐ Yes ☐ No"
            ])
        
        col_table = Table(col_data, colWidths=[0.3*inch, 1.5*inch, 1.5*inch, 1*inch, 0.9*inch])
        col_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#0066CC')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('TEXTCOLOR', (0, 1), (-1, -1), colors.black),
            ('ALIGN', (0, 0), (0, -1), 'CENTER'),
            ('ALIGN', (1, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 8),
            ('FONTSIZE', (0, 1), (-1, -1), 7),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
            ('TOPPADDING', (0, 0), (-1, -1), 5),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE')
        ]))
        
        self.elements.append(col_table)
        
        note2 = Paragraph(
            "<i>*Continue on additional sheets if more than 5 columns</i>",
            self.styles['SmallText']
        )
        self.elements.append(note2)
        self.elements.append(Spacer(1, 0.2*inch))
        
        # Date Column Details
        self.elements.append(Paragraph("4.3 Date Column Details", self.styles['SubsectionHeader']))
        
        date_data = [
            ["Primary Date Column:", "_____________"],
            ["Earliest Date:", "_____________"],
            ["Latest Date:", "_____________"],
        ]
        
        date_table = Table(date_data, colWidths=[2*inch, 3.5*inch])
        date_table.setStyle(TableStyle([
            ('TEXTCOLOR', (0, 0), (-1, -1), colors.black),
            ('ALIGN', (0, 0), (0, -1), 'RIGHT'),
            ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 10),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
            ('TOPPADDING', (0, 0), (-1, -1), 8),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.lightgrey)
        ]))
        
        self.elements.append(date_table)
        self.elements.append(Spacer(1, 0.1*inch))
        
        # Date format
        date_format = Paragraph("<b>Date Format:</b>", self.styles['CustomNormal'])
        self.elements.append(date_format)
        
        format_options = [
            ["☐ MM/DD/YYYY", "☐ DD/MM/YYYY"],
            ["☐ YYYY-MM-DD", "☐ Other: _____________"],
        ]
        
        format_table = Table(format_options, colWidths=[2.75*inch, 2.75*inch])
        format_table.setStyle(TableStyle([
            ('TEXTCOLOR', (0, 0), (-1, -1), colors.black),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
            ('TOPPADDING', (0, 0), (-1, -1), 5)
        ]))
        
        self.elements.append(format_table)
        self.elements.append(PageBreak())
    
    def add_section_5(self):
        """Section 5: Lookup Tables"""
        self.elements.append(Paragraph("SECTION 5: LOOKUP/REFERENCE TABLES", self.styles['SectionHeader']))
        self.elements.append(Spacer(1, 0.15*inch))
        
        total_lookups = Paragraph("<b>Total Lookup Tables:</b> _____", self.styles['CustomNormal'])
        self.elements.append(total_lookups)
        self.elements.append(Spacer(1, 0.15*inch))
        
        # Lookup Table 1
        self.elements.append(Paragraph("Lookup Table 1:", self.styles['SubsectionHeader']))
        
        lookup_data = [
            ["Sheet Name:", ""],
            ["Data Range:", ""],
            ["Total Rows:", ""],
            ["Primary Key Column:", ""],
            ["Connects to Main Data?", "☐ Yes ☐ No"],
            ["Connection Column:", ""],
        ]
        
        lookup_table = Table(lookup_data, colWidths=[2.2*inch, 3.3*inch])
        lookup_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (0, -1), colors.HexColor('#F0F8FF')),
            ('TEXTCOLOR', (0, 0), (-1, -1), colors.black),
            ('ALIGN', (0, 0), (0, -1), 'RIGHT'),
            ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 10),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
            ('TOPPADDING', (0, 0), (-1, -1), 8),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE')
        ]))
        
        self.elements.append(lookup_table)
        self.elements.append(Spacer(1, 0.15*inch))
        
        # Column list
        col_list = Paragraph("<b>Column List:</b>", self.styles['CustomNormal'])
        self.elements.append(col_list)
        
        for i in range(1, 6):
            col_item = Paragraph(f"{i}. _____________ (Key: ☐ Yes ☐ No)", self.styles['CustomNormal'])
            self.elements.append(col_item)
        
        self.elements.append(Spacer(1, 0.2*inch))
        
        # Relationships Map
        self.elements.append(Paragraph("Table Relationships Map:", self.styles['SubsectionHeader']))
        
        relationships = Paragraph(
            "<b>YOUR RELATIONSHIPS:</b><br/>"
            "1. _____________[_______] → _____________[_______]<br/>"
            "2. _____________[_______] → _____________[_______]<br/>"
            "3. _____________[_______] → _____________[_______]",
            self.styles['CustomNormal']
        )
        self.elements.append(relationships)
        self.elements.append(PageBreak())
    
    def add_section_6(self):
        """Section 6: Calculations & Metrics"""
        self.elements.append(Paragraph("SECTION 6: CALCULATIONS & METRICS", self.styles['SectionHeader']))
        self.elements.append(Spacer(1, 0.15*inch))
        
        self.elements.append(Paragraph("6.1 KPI/Summary Metrics", self.styles['SubsectionHeader']))
        
        note = Paragraph(
            "<i>📸 Screenshot Required: Each cell with formula bar visible</i>",
            self.styles['SmallText']
        )
        self.elements.append(note)
        self.elements.append(Spacer(1, 0.1*inch))
        
        total_kpi = Paragraph("<b>Total KPIs:</b> _____", self.styles['CustomNormal'])
        self.elements.append(total_kpi)
        self.elements.append(Spacer(1, 0.1*inch))
        
        # KPI Table - first 5 KPIs
        kpi_data = [
            ["#", "Metric Name", "Cell", "Excel Formula", "Keep?"],
        ]
        
        for i in range(1, 6):
            kpi_data.append([str(i), "", "", "", "☐ Yes\n☐ No"])
        
        kpi_table = Table(kpi_data, colWidths=[0.3*inch, 1.3*inch, 0.6*inch, 2.2*inch, 0.8*inch])
        kpi_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#0066CC')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('TEXTCOLOR', (0, 1), (-1, -1), colors.black),
            ('ALIGN', (0, 0), (0, -1), 'CENTER'),
            ('ALIGN', (1, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 8),
            ('FONTSIZE', (0, 1), (-1, -1), 7),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
            ('TOPPADDING', (0, 0), (-1, -1), 6),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE')
        ]))
        
        self.elements.append(kpi_table)
        self.elements.append(Spacer(1, 0.2*inch))
        
        # Complex Calculations
        self.elements.append(Paragraph("6.2 Complex Calculations", self.styles['SubsectionHeader']))
        
        complex_text = Paragraph(
            "Any SUMIF, SUMIFS, COUNTIF, VLOOKUP, INDEX-MATCH, etc.?",
            self.styles['CustomNormal']
        )
        self.elements.append(complex_text)
        self.elements.append(Spacer(1, 0.1*inch))
        
        complex_data = [
            ["Formula Type", "Purpose", "Excel Formula", "Location"],
        ]
        
        for i in range(3):
            complex_data.append(["", "", "", ""])
        
        complex_table = Table(complex_data, colWidths=[1.2*inch, 1.2*inch, 2*inch, 1*inch])
        complex_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#0066CC')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('TEXTCOLOR', (0, 1), (-1, -1), colors.black),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 8),
            ('FONTSIZE', (0, 1), (-1, -1), 8),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
            ('TOPPADDING', (0, 0), (-1, -1), 8),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE')
        ]))
        
        self.elements.append(complex_table)
        self.elements.append(PageBreak())
    
    def add_section_7(self):
        """Section 7: Pivot Tables"""
        self.elements.append(Paragraph("SECTION 7: PIVOT TABLES", self.styles['SectionHeader']))
        self.elements.append(Spacer(1, 0.15*inch))
        
        total_pivots = Paragraph("<b>Total Pivot Tables:</b> _____", self.styles['CustomNormal'])
        self.elements.append(total_pivots)
        self.elements.append(Spacer(1, 0.15*inch))
        
        # Pivot Table 1
        self.elements.append(Paragraph("Pivot Table 1:", self.styles['SubsectionHeader']))
        
        note = Paragraph(
            "<i>📸 Screenshot Required: Pivot + Field List panel + Value Field Settings</i>",
            self.styles['SmallText']
        )
        self.elements.append(note)
        self.elements.append(Spacer(1, 0.1*inch))
        
        pivot_details = [
            ["Location (Cells):", ""],
            ["Source Data:", ""],
            ["Total Rows/Columns:", ""],
        ]
        
        pivot_table = Table(pivot_details, colWidths=[2*inch, 3.5*inch])
        pivot_table.setStyle(TableStyle([
            ('TEXTCOLOR', (0, 0), (-1, -1), colors.black),
            ('ALIGN', (0, 0), (0, -1), 'RIGHT'),
            ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 10),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
            ('TOPPADDING', (0, 0), (-1, -1), 8),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.lightgrey)]
        ))
        
        self.elements.append(pivot_table)
        self.elements.append(Spacer(1, 0.15*inch))
        
        # Field Configuration
        config_text = Paragraph("<b>Field Configuration:</b>", self.styles['CustomNormal'])
        self.elements.append(config_text)
        
        config = Paragraph(
            "<b>FILTERS (Report Filter):</b><br/>"
            "1. _____________<br/>"
            "2. _____________<br/><br/>"
            "<b>COLUMNS:</b><br/>"
            "1. _____________<br/>"
            "2. _____________<br/><br/>"
            "<b>ROWS:</b><br/>"
            "1. _____________<br/>"
            "2. _____________<br/>"
            "3. _____________<br/><br/>"
            "<b>VALUES:</b><br/>"
            "1. _____________ (Summarize by: ☐ Sum ☐ Count ☐ Average)<br/>"
            "   Show As: ☐ Normal ☐ % of Total ☐ Difference<br/>"
            "2. _____________ (Summarize by: ☐ Sum ☐ Count ☐ Average)",
            self.styles['SmallText']
        )
        self.elements.append(config)
        self.elements.append(Spacer(1, 0.15*inch))
        
        # Special Settings
        settings = [
            ["☐ Subtotals shown", "☐ Grand totals shown"],
            ["☐ Calculated fields (list): _____________", ""],
        ]
        
        settings_table = Table(settings, colWidths=[2.75*inch, 2.75*inch])
        settings_table.setStyle(TableStyle([
            ('TEXTCOLOR', (0, 0), (-1, -1), colors.black),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
            ('TOPPADDING', (0, 0), (-1, -1), 5)
        ]))
        
        self.elements.append(settings_table)
        self.elements.append(Spacer(1, 0.1*inch))
        
        keep = Paragraph("<b>Keep in Power BI?</b> ☐ Yes ☐ No", self.styles['CustomNormal'])
        self.elements.append(keep)
        
        self.elements.append(PageBreak())
    
    def add_section_8(self):
        """Section 8: Charts & Visualizations"""
        self.elements.append(Paragraph("SECTION 8: CHARTS & VISUALIZATIONS", self.styles['SectionHeader']))
        self.elements.append(Spacer(1, 0.15*inch))
        
        total_charts = Paragraph("<b>Total Charts:</b> _____", self.styles['CustomNormal'])
        self.elements.append(total_charts)
        self.elements.append(Spacer(1, 0.15*inch))
        
        # Chart 1
        self.elements.append(Paragraph("Chart 1:", self.styles['SubsectionHeader']))
        
        note = Paragraph(
            "<i>📸 Screenshot Required: Chart + \"Select Data\" dialog</i>",
            self.styles['SmallText']
        )
        self.elements.append(note)
        self.elements.append(Spacer(1, 0.1*inch))
        
        chart_types = [
            ["Chart Type:", "☐ Column ☐ Bar ☐ Line ☐ Pie ☐ Combo ☐ Scatter ☐ Other: _____"],
            ["Location (Cells):", ""],
            ["Title:", ""],
            ["Data Source:", "☐ Direct Range ☐ Pivot Table ☐ Named Range"],
        ]
        
        chart_table = Table(chart_types, colWidths=[1.5*inch, 4*inch])
        chart_table.setStyle(TableStyle([
            ('TEXTCOLOR', (0, 0), (-1, -1), colors.black),
            ('ALIGN', (0, 0), (0, -1), 'RIGHT'),
            ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
            ('TOPPADDING', (0, 0), (-1, -1), 8),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.lightgrey),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE')
        ]))
        
        self.elements.append(chart_table)
        self.elements.append(Spacer(1, 0.15*inch))
        
        # Data Series
        series_text = Paragraph("<b>Data Series:</b>", self.styles['CustomNormal'])
        self.elements.append(series_text)
        
        series = Paragraph(
            "<b>SERIES 1:</b><br/>"
            "- Name: _____________<br/>"
            "- X-Axis Range: _____________<br/>"
            "- Y-Axis Range: _____________<br/>"
            "- Color: _____________<br/><br/>"
            "<b>SERIES 2 (if any):</b><br/>"
            "- Name: _____________<br/>"
            "- Values Range: _____________<br/>"
            "- Axis: ☐ Primary ☐ Secondary",
            self.styles['SmallText']
        )
        self.elements.append(series)
        self.elements.append(Spacer(1, 0.15*inch))
        
        # Formatting
        format_data = [
            ["X-Axis Label:", ""],
            ["Y-Axis Label:", ""],
            ["Legend Position:", "☐ Top ☐ Bottom ☐ Left ☐ Right ☐ None"],
            ["Data Labels:", "☐ Yes ☐ No"],
        ]
        
        format_table = Table(format_data, colWidths=[1.5*inch, 4*inch])
        format_table.setStyle(TableStyle([
            ('TEXTCOLOR', (0, 0), (-1, -1), colors.black),
            ('ALIGN', (0, 0), (0, -1), 'RIGHT'),
            ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
            ('TOPPADDING', (0, 0), (-1, -1), 6),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.lightgrey)
        ]))
        
        self.elements.append(format_table)
        self.elements.append(Spacer(1, 0.1*inch))
        
        footer_options = [
            ["Keep in Power BI?", "☐ Yes ☐ No"],
            ["Priority:", "☐ High ☐ Medium ☐ Low"],
        ]
        
        footer_table = Table(footer_options, colWidths=[2.75*inch, 2.75*inch])
        footer_table.setStyle(TableStyle([
            ('TEXTCOLOR', (0, 0), (-1, -1), colors.black),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
            ('TOPPADDING', (0, 0), (-1, -1), 5)]
        ))
        
        self.elements.append(footer_table)
        self.elements.append(PageBreak())
    
    def add_section_9(self):
        """Section 9: Interactivity (Slicers/Filters)"""
        self.elements.append(Paragraph("SECTION 9: INTERACTIVITY (Slicers/Filters)", self.styles['SectionHeader']))
        self.elements.append(Spacer(1, 0.15*inch))
        
        total_slicers = Paragraph("<b>Total Slicers:</b> _____", self.styles['CustomNormal'])
        self.elements.append(total_slicers)
        self.elements.append(Spacer(1, 0.15*inch))
        
        # Slicer 1
        self.elements.append(Paragraph("Slicer 1:", self.styles['SubsectionHeader']))
        
        note = Paragraph(
            "<i>📸 Screenshot Required: Slicer + \"Report Connections\" dialog</i>",
            self.styles['SmallText']
        )
        self.elements.append(note)
        self.elements.append(Spacer(1, 0.1*inch))
        
        slicer_data = [
            ["Field/Column:", ""],
            ["Location (Cells):", ""],
            ["Display Style:", "☐ Tiles ☐ List ☐ Dropdown"],
            ["Selection Type:", "☐ Single ☐ Multiple"],
        ]
        
        slicer_table = Table(slicer_data, colWidths=[1.8*inch, 3.7*inch])
        slicer_table.setStyle(TableStyle([
            ('TEXTCOLOR', (0, 0), (-1, -1), colors.black),
            ('ALIGN', (0, 0), (0, -1), 'RIGHT'),
            ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 10),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
            ('TOPPADDING', (0, 0), (-1, -1), 8),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.lightgrey),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE')
        ]))
        
        self.elements.append(slicer_table)
        self.elements.append(Spacer(1, 0.15*inch))
        
        # Controls
        controls_text = Paragraph("<b>Controls These Objects:</b>", self.styles['CustomNormal'])
        self.elements.append(controls_text)
        
        controls_options = [
            ["☐ Pivot Table 1", "☐ Pivot Table 2", "☐ Pivot Table 3"],
            ["☐ Chart 1", "☐ Chart 2", "☐ Chart 3"],
            ["☐ Chart 4", "☐ Chart 5", ""],
        ]
        
        controls_table = Table(controls_options, colWidths=[1.8*inch, 1.8*inch, 1.9*inch])
        controls_table.setStyle(TableStyle([
            ('TEXTCOLOR', (0, 0), (-1, -1), colors.black),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
            ('TOPPADDING', (0, 0), (-1, -1), 5)
        ]))
        
        self.elements.append(controls_table)
        self.elements.append(Spacer(1, 0.15*inch))
        
        # Values
        values_text = Paragraph("<b>Values in Slicer:</b>", self.styles['CustomNormal'])
        self.elements.append(values_text)
        
        for i in range(1, 5):
            val = Paragraph(f"{i}. _____________", self.styles['CustomNormal'])
            self.elements.append(val)
        
        self.elements.append(Spacer(1, 0.1*inch))
        
        keep = Paragraph("<b>Keep in Power BI?</b> ☐ Yes ☐ No", self.styles['CustomNormal'])
        self.elements.append(keep)
        
        self.elements.append(PageBreak())
    
    def add_section_10(self):
        """Section 10: Dashboard Layout"""
        self.elements.append(Paragraph("SECTION 10: DASHBOARD LAYOUT", self.styles['SectionHeader']))
        self.elements.append(Spacer(1, 0.15*inch))
        
        note = Paragraph(
            "<i>📸 Screenshot Required: Full dashboard at 40-50% zoom</i>",
            self.styles['SmallText']
        )
        self.elements.append(note)
        self.elements.append(Spacer(1, 0.15*inch))
        
        self.elements.append(Paragraph("10.1 Layout Sketch", self.styles['SubsectionHeader']))
        
        sketch_text = Paragraph(
            "<b>Draw/describe the layout:</b><br/><br/>"
            "Top Section: _____________________________________________<br/><br/>"
            "Middle Section: _____________________________________________<br/><br/>"
            "Bottom Section: _____________________________________________<br/><br/><br/>"
            "<b>OR use grid format:</b><br/><br/>"
            "Row 1: [KPI 1] [KPI 2] [KPI 3] [KPI 4]<br/>"
            "Row 2: _____________________________________________<br/>"
            "Row 3: _____________________________________________<br/>"
            "Row 4: _____________________________________________",
            self.styles['CustomNormal']
        )
        self.elements.append(sketch_text)
        self.elements.append(Spacer(1, 0.2*inch))
        
        # Component Priority
        self.elements.append(Paragraph("10.2 Component Priority", self.styles['SubsectionHeader']))
        
        priority_text = Paragraph("Rank components by importance (1 = most important):", self.styles['CustomNormal'])
        self.elements.append(priority_text)
        self.elements.append(Spacer(1, 0.1*inch))
        
        priority_data = [
            ["Rank", "Component", "Why Important"],
            ["1", "", ""],
            ["2", "", ""],
            ["3", "", ""],
            ["4", "", ""],
        ]
        
        priority_table = Table(priority_data, colWidths=[0.6*inch, 2.2*inch, 2.7*inch])
        priority_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#0066CC')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('TEXTCOLOR', (0, 1), (-1, -1), colors.black),
            ('ALIGN', (0, 0), (0, -1), 'CENTER'),
            ('ALIGN', (1, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
            ('TOPPADDING', (0, 0), (-1, -1), 8),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE')
        ]))
        
        self.elements.append(priority_table)
        self.elements.append(PageBreak())
    
    def add_remaining_sections(self):
        """Add sections 11-20 in condensed format"""
        
        # SECTION 11: Special Features
        self.elements.append(Paragraph("SECTION 11: SPECIAL FEATURES", self.styles['SectionHeader']))
        self.elements.append(Spacer(1, 0.1*inch))
        
        conditional = Paragraph(
            "<b>11.1 Conditional Formatting</b><br/>"
            "Any cells with color coding, data bars, icon sets?<br/>"
            "Location: _____________ Rule: _____________ Keep? ☐ Yes ☐ No",
            self.styles['CustomNormal']
        )
        self.elements.append(conditional)
        self.elements.append(Spacer(1, 0.1*inch))
        
        named = Paragraph(
            "<b>11.2 Named Ranges</b><br/>"
            "Any named ranges used in formulas?<br/>"
            "Name: _____________ Refers To: _____________ Needed? ☐ Yes ☐ No",
            self.styles['CustomNormal']
        )
        self.elements.append(named)
        self.elements.append(Spacer(1, 0.1*inch))
        
        macros = Paragraph(
            "<b>11.3 Macros/VBA</b><br/>"
            "Any macros or VBA code? ☐ Yes ☐ No<br/>"
            "If Yes, describe: _____________________________________________",
            self.styles['CustomNormal']
        )
        self.elements.append(macros)
        self.elements.append(Spacer(1, 0.2*inch))
        
        # SECTION 12: Data Source & Refresh
        self.elements.append(Paragraph("SECTION 12: DATA SOURCE & REFRESH", self.styles['SectionHeader']))
        self.elements.append(Spacer(1, 0.1*inch))
        
        source_text = Paragraph(
            "<b>12.1 Current Data Entry Method</b><br/>"
            "How does data get into Excel?<br/>"
            "☐ Manual entry ☐ Copy-paste ☐ CSV import ☐ Database connection<br/>"
            "☐ Excel file sent ☐ Power Query ☐ Other: _____________",
            self.styles['CustomNormal']
        )
        self.elements.append(source_text)
        self.elements.append(Spacer(1, 0.1*inch))
        
        update_data = [
            ["Who updates the data?", ""],
            ["How often?", "☐ Daily ☐ Weekly ☐ Monthly ☐ Quarterly ☐ Ad-hoc"],
            ["Time taken to update:", "_____ minutes/hours"],
        ]
        
        update_table = Table(update_data, colWidths=[2*inch, 3.5*inch])
        update_table.setStyle(TableStyle([
            ('TEXTCOLOR', (0, 0), (-1, -1), colors.black),
            ('ALIGN', (0, 0), (0, -1), 'RIGHT'),
            ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
            ('TOPPADDING', (0, 0), (-1, -1), 6),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.lightgrey)]
        ))
        
        self.elements.append(update_table)
        self.elements.append(Spacer(1, 0.1*inch))
        
        pbi_refresh = Paragraph(
            "<b>12.2 Future Data Source for Power BI</b><br/>"
            "Preferred method: ☐ Upload Excel ☐ Database ☐ SharePoint/OneDrive ☐ Other<br/>"
            "Refresh frequency: ☐ Real-time ☐ Hourly ☐ Daily ☐ Weekly ☐ Monthly ☐ Manual",
            self.styles['CustomNormal']
        )
        self.elements.append(pbi_refresh)
        self.elements.append(PageBreak())
        
        # SECTION 13: User Requirements
        self.elements.append(Paragraph("SECTION 13: USER REQUIREMENTS", self.styles['SectionHeader']))
        self.elements.append(Spacer(1, 0.1*inch))
        
        users_data = [
            ["User Type", "Name/Role", "Access Level"],
            ["Primary User", "", "☐ Edit ☐ View Only"],
            ["Secondary User", "", "☐ Edit ☐ View Only"],
            ["Management", "", "☐ View Only"],
        ]
        
        users_table = Table(users_data, colWidths=[1.5*inch, 2.3*inch, 1.7*inch])
        users_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#0066CC')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('TEXTCOLOR', (0, 1), (-1, -1), colors.black),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
            ('TOPPADDING', (0, 0), (-1, -1), 6),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.grey)
        ]))
        
        self.elements.append(users_table)
        self.elements.append(Spacer(1, 0.1*inch))
        
        sharing = Paragraph(
            "<b>Total Users:</b> _____<br/><br/>"
            "<b>How should dashboard be shared?</b><br/>"
            "☐ Power BI Service (web) ☐ Embedded in website ☐ Email reports<br/>"
            "☐ Mobile app ☐ Desktop file only<br/><br/>"
            "<b>Security:</b> ☐ Row-level security ☐ No restrictions",
            self.styles['CustomNormal']
        )
        self.elements.append(sharing)
        self.elements.append(Spacer(1, 0.2*inch))
        
        # SECTION 14: Pain Points
        self.elements.append(Paragraph("SECTION 14: CURRENT PAIN POINTS", self.styles['SectionHeader']))
        self.elements.append(Spacer(1, 0.1*inch))
        
        pain = Paragraph(
            "<b>What doesn't work well in current Excel setup?</b><br/>"
            "1. _____________________________________________<br/>"
            "2. _____________________________________________<br/>"
            "3. _____________________________________________<br/><br/>"
            "<b>What would you like to improve?</b><br/>"
            "1. _____________________________________________<br/>"
            "2. _____________________________________________<br/>"
            "3. _____________________________________________",
            self.styles['CustomNormal']
        )
        self.elements.append(pain)
        self.elements.append(Spacer(1, 0.2*inch))
        
        # SECTION 15: Wish List
        self.elements.append(Paragraph("SECTION 15: WISH LIST", self.styles['SectionHeader']))
        self.elements.append(Spacer(1, 0.1*inch))
        
        wishlist = Paragraph(
            "<b>Features NOT in Excel but wanted in Power BI:</b><br/>"
            "☐ Drill-down capabilities ☐ Mobile access ☐ Automatic email reports<br/>"
            "☐ Real-time data ☐ Compare multiple time periods ☐ Export to PowerPoint<br/>"
            "☐ Collaboration features ☐ Other: _____________",
            self.styles['CustomNormal']
        )
        self.elements.append(wishlist)
        self.elements.append(PageBreak())
        
        # SECTION 16: Technical Details
        self.elements.append(Paragraph("SECTION 16: TECHNICAL DETAILS", self.styles['SectionHeader']))
        self.elements.append(Spacer(1, 0.1*inch))
        
        technical_data = [
            ["Excel Version:", "☐ 365 ☐ 2021 ☐ 2019 ☐ 2016 ☐ Other"],
            ["Operating System:", "☐ Windows ☐ Mac"],
            ["File Storage:", "☐ Local ☐ OneDrive ☐ SharePoint ☐ Network Drive"],
            ["Power BI License:", "☐ Have Pro ☐ Have Premium ☐ Need to Purchase ☐ Unsure"],
        ]
        
        tech_table = Table(technical_data, colWidths=[2*inch, 3.5*inch])
        tech_table.setStyle(TableStyle([
            ('TEXTCOLOR', (0, 0), (-1, -1), colors.black),
            ('ALIGN', (0, 0), (0, -1), 'RIGHT'),
            ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
            ('TOPPADDING', (0, 0), (-1, -1), 6),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.lightgrey)]
        ))
        
        self.elements.append(tech_table)
        self.elements.append(Spacer(1, 0.1*inch))
        
        power_query = Paragraph(
            "<b>Does Excel use:</b><br/>"
            "☐ Power Query ☐ Data Model ☐ Power Pivot ☐ None<br/>"
            "If Yes, describe: _____________________________________________",
            self.styles['CustomNormal']
        )
        self.elements.append(power_query)
        self.elements.append(Spacer(1, 0.2*inch))
        
        # SECTION 17: Screenshot Inventory
        self.elements.append(Paragraph("SECTION 17: SCREENSHOT INVENTORY", self.styles['SectionHeader']))
        self.elements.append(Spacer(1, 0.1*inch))
        
        screenshots = Paragraph(
            "<b>Checklist of screenshots captured:</b><br/>"
            "☐ Full dashboard layout ☐ Sheet tabs ☐ Data structure<br/>"
            "☐ Calculated columns (_____ screenshots)<br/>"
            "☐ KPI formulas (_____ screenshots)<br/>"
            "☐ Pivot tables (_____ screenshots)<br/>"
            "☐ Charts (_____ screenshots)<br/>"
            "☐ Slicers (_____ screenshots)<br/>"
            "☐ Conditional formatting ☐ Named ranges ☐ Lookup tables<br/><br/>"
            "<b>Total Screenshots:</b> _____<br/>"
            "<b>Stored In:</b> _____________________________________________",
            self.styles['CustomNormal']
        )
        self.elements.append(screenshots)
        self.elements.append(Spacer(1, 0.2*inch))
        
        # SECTION 18: Additional Notes
        self.elements.append(Paragraph("SECTION 18: ADDITIONAL NOTES", self.styles['SectionHeader']))
        self.elements.append(Spacer(1, 0.1*inch))
        
        notes = Paragraph(
            "<b>Anything else important to know?</b><br/>"
            "_____________________________________________<br/>"
            "_____________________________________________<br/>"
            "_____________________________________________<br/><br/>"
            "<b>Questions for Developer:</b><br/>"
            "_____________________________________________<br/>"
            "_____________________________________________<br/><br/>"
            "<b>Special Requests:</b><br/>"
            "_____________________________________________<br/>"
            "_____________________________________________",
            self.styles['CustomNormal']
        )
        self.elements.append(notes)
        self.elements.append(PageBreak())
        
        # SECTION 19: Session Summary
        self.elements.append(Paragraph("SECTION 19: SESSION SUMMARY", self.styles['SectionHeader']))
        self.elements.append(Spacer(1, 0.1*inch))
        
        summary_data = [
            ["Session Date:", ""],
            ["Duration:", "_____ minutes"],
            ["Completed By:", ""],
            ["Client Attendees:", ""],
        ]
        
        summary_table = Table(summary_data, colWidths=[2*inch, 3.5*inch])
        summary_table.setStyle(TableStyle([
            ('TEXTCOLOR', (0, 0), (-1, -1), colors.black),
            ('ALIGN', (0, 0), (0, -1), 'RIGHT'),
            ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 10),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
            ('TOPPADDING', (0, 0), (-1, -1), 8),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.lightgrey)]
        ))
        
        self.elements.append(summary_table)
        self.elements.append(Spacer(1, 0.1*inch))
        
        status = Paragraph(
            "<b>Session Status:</b><br/>"
            "☐ All requirements captured<br/>"
            "☐ Follow-up needed for: _____________<br/>"
            "☐ Additional session needed: ☐ Yes ☐ No<br/><br/>"
            "<b>Next Steps:</b><br/>"
            "1. _____________________________________________<br/>"
            "2. _____________________________________________<br/>"
            "3. _____________________________________________",
            self.styles['CustomNormal']
        )
        self.elements.append(status)
        self.elements.append(Spacer(1, 0.2*inch))
        
        # SECTION 20: Final Confirmation
        self.elements.append(Paragraph("SECTION 20: FINAL CONFIRMATION", self.styles['SectionHeader']))
        self.elements.append(Spacer(1, 0.1*inch))
        
        final_data = [
            ["Date Range:", "From _____________ To _____________"],
            ["Approximately:", "_____ rows"],
            ["Reason for this range:", ""],
        ]
        
        final_table = Table(final_data, colWidths=[2*inch, 3.5*inch])
        final_table.setStyle(TableStyle([
            ('TEXTCOLOR', (0, 0), (-1, -1), colors.black),
            ('ALIGN', (0, 0), (0, -1), 'RIGHT'),
            ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 10),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
            ('TOPPADDING', (0, 0), (-1, -1), 8),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.lightgrey)]
        ))
        
        self.elements.append(final_table)
        self.elements.append(Spacer(1, 0.15*inch))
        
        requirements = Paragraph(
            "<b>Must-Have Requirements (non-negotiable):</b><br/>"
            "1. _____________________________________________<br/>"
            "2. _____________________________________________<br/>"
            "3. _____________________________________________<br/><br/>"
            "<b>Optional Requirements (nice but not critical):</b><br/>"
            "1. _____________________________________________<br/>"
            "2. _____________________________________________<br/><br/>"
            "<b>Out of Scope (will NOT be included):</b><br/>"
            "1. _____________________________________________<br/>"
            "2. _____________________________________________",
            self.styles['CustomNormal']
        )
        self.elements.append(requirements)
        self.elements.append(Spacer(1, 0.3*inch))
        
        # Client Approval
        approval = Paragraph(
            "<b>CLIENT APPROVAL</b><br/><br/>"
            "This requirements document accurately represents my Excel dashboard<br/>"
            "and migration needs.<br/><br/>"
            "Name: _____________________________________________<br/><br/>"
            "Date: _____________________________________________<br/><br/>"
            "Signature: _____________________________________________",
            self.styles['CustomNormal']
        )
        self.elements.append(approval)
        self.elements.append(PageBreak())
        
        # Critical Questions Page
        self.elements.append(Paragraph("APPENDIX: CRITICAL QUESTIONS", self.styles['SectionHeader']))
        self.elements.append(Spacer(1, 0.15*inch))
        
        questions = Paragraph(
            "<b>About Data:</b><br/>"
            "✅ What's the date column name?<br/>"
            "✅ What's the earliest and latest date in your data?<br/>"
            "✅ Confirm: We're taking last 2 years from [date] to [date]?<br/>"
            "✅ How often does this data update?<br/><br/>"
            "<b>About Calculations:</b><br/>"
            "✅ Click this cell - what's the formula?<br/>"
            "✅ Is this calculation important to keep?<br/>"
            "✅ What does this metric tell you?<br/><br/>"
            "<b>About Visuals:</b><br/>"
            "✅ Right-click this chart → Select Data<br/>"
            "✅ What decision do you make with this chart?<br/>"
            "✅ Any specific colors or formatting that matters?<br/><br/>"
            "<b>About Usage:</b><br/>"
            "✅ Who else uses this dashboard?<br/>"
            "✅ What frustrates you about the current Excel?<br/>"
            "✅ What would you like to improve?",
            self.styles['CustomNormal']
        )
        self.elements.append(questions)
    
    def generate(self):
        """Generate the complete PDF"""
        print("🔧 Generating fillable PDF...")
        
        # Build all sections
        self.add_title_page()
        self.add_section_1()
        self.add_section_2()
        self.add_section_3()
        self.add_section_4()
        self.add_section_5()
        self.add_section_6()
        self.add_section_7()
        self.add_section_8()
        self.add_section_9()
        self.add_section_10()
        self.add_remaining_sections()
        
        # Create PDF
        doc = SimpleDocTemplate(
            self.output_filename,
            pagesize=letter,
            rightMargin=0.75*inch,
            leftMargin=0.75*inch,
            topMargin=0.9*inch,
            bottomMargin=0.9*inch
        )
        
        # Build with custom canvas
        doc.build(self.elements, canvasmaker=NumberedCanvas)
        
        print(f"✅ PDF generated successfully: {self.output_filename}")
        print(f"📄 File size: {os.path.getsize(self.output_filename) / 1024:.2f} KB")
        
        return self.output_filename

# ============================================================================
# MAIN EXECUTION
# ============================================================================

if __name__ == "__main__":
    print("="*60)
    print("Excel to Power BI Requirements - Fillable PDF Generator")
    print("Author: Avinash Rai (AWA)")
    print("="*60)
    print()
    
    # Generate PDF
    generator = FillablePDFGenerator()
    output_file = generator.generate()
    
    print()
    print("="*60)
    print(f"✅ SUCCESS! PDF created: {output_file}")
    print("="*60)
    print()
    print("📋 Next Steps:")
    print("1. Open the PDF in Adobe Acrobat or any PDF reader")
    print("2. Fill in the form during your client session")
    print("3. Save the completed form")
    print("4. Use it to create your migration plan")
    print()
    print("💡 Note: While this PDF has structured layout,")
    print("   for true interactive form fields (checkboxes, text inputs),")
    print("   you may need to use Adobe Acrobat Pro to add form fields,")
    print("   or use the provided Streamlit app for digital form filling.")
