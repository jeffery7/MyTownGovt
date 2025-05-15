#!/usr/bin/env python3

import pandas as pd
import os
import subprocess
import tempfile
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, PageBreak
from reportlab.lib.styles import getSampleStyleSheet
from datetime import datetime

# Configuration
board_name = "Planning_Board"  # Board to process
data_dir = "Hardwick_Data"
year = 2024  # Year for the report (set to None to process all years)

# List of document extensions that LibreOffice can convert to PDF
SUPPORTED_DOC_EXTENSIONS = {'.doc', '.docx', '.odf', '.odt', '.rtf'}

def convert_doc_to_pdf(input_path, output_path):
    """
    Convert a document file (.doc, .docx, .odf, etc.) to PDF using LibreOffice.

    Args:
        input_path (str): Path to the input document file.
        output_path (str): Path to save the output PDF in Attachments folder.

    Returns:
        bool: True if conversion succeeded, False otherwise.
    """
    try:
        # Run libreoffice command to convert document to PDF
        subprocess.run(
            ["libreoffice", "--headless", "--convert-to", "pdf", "--outdir", os.path.dirname(output_path), input_path],
            check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )
        # libreoffice saves the PDF with the same base name in the output directory; rename if needed
        generated_pdf = os.path.join(os.path.dirname(output_path), os.path.splitext(os.path.basename(input_path))[0] + ".pdf")
        if os.path.exists(generated_pdf):
            os.rename(generated_pdf, output_path)
            if os.path.getsize(output_path) > 0:
                print(f"Generated PDF {output_path} ({os.path.getsize(output_path)} bytes)")
                return True
            else:
                print(f"Error: Generated PDF {output_path} is empty")
                os.remove(output_path)
                return False
        else:
            print(f"Error: LibreOffice did not generate expected PDF for {input_path}")
            return False
    except subprocess.CalledProcessError as e:
        print(f"Error converting {input_path} to PDF with LibreOffice: {e.stderr}")
        return False
    except Exception as e:
        print(f"Unexpected error converting {input_path} to PDF: {e}")
        return False

def generate_yearly_report(board_name, year, data_dir="Hardwick_Data"):
    """
    Generate a yearly PDF report for a board, combining meeting agendas and attachments.

    Args:
        board_name (str): Name of the board (e.g., "Planning_Board").
        year (int or None): Year to filter meetings (e.g., 2022), or None for all years.
        data_dir (str): Root directory for data (default: "Hardwick_Data").
    """
    # Paths
    board_dir = os.path.join(data_dir, board_name)
    meeting_csv = os.path.join(board_dir, "meeting_data.csv")
    documents_csv = os.path.join(board_dir, "meeting_documents.csv")
    attachments_dir = os.path.join(board_dir, "Attachments")
    output_suffix = f"All_Years" if year is None else str(year)
    output_pdf = os.path.join(board_dir, f"Yearly_Minutes_and_Agendas_{output_suffix}.pdf")

    # Check if input files exist
    if not os.path.exists(meeting_csv):
        print(f"Error: {meeting_csv} not found")
        return
    if not os.path.exists(documents_csv):
        print(f"Warning: {documents_csv} not found, proceeding without attachments")

    # Read CSV files
    try:
        meetings_df = pd.read_csv(meeting_csv)
    except Exception as e:
        print(f"Error reading {meeting_csv}: {e}")
        return
    documents_df = pd.DataFrame(columns=["Board", "Time", "Timestamp", "File Name", "Download URL", "File Path"])
    if os.path.exists(documents_csv):
        try:
            documents_df = pd.read_csv(documents_csv)
        except Exception as e:
            print(f"Error reading {documents_csv}: {e}")

    # Normalize timestamps to seconds
    meetings_df["Timestamp"] = pd.to_datetime(meetings_df["Timestamp"], errors='coerce').dt.floor("s")
    if not documents_df.empty:
        documents_df["Timestamp"] = pd.to_datetime(documents_df["Timestamp"], errors='coerce').dt.floor("s")

    # Debug: Print unique timestamps
    print(f"Meeting timestamps: {meetings_df['Timestamp'].dropna().unique()}")
    if not documents_df.empty:
        print(f"Document timestamps: {documents_df['Timestamp'].dropna().unique()}")

    # Filter meetings by year (or include all if year is None)
    if year is not None:
        meetings_df = meetings_df[meetings_df["Timestamp"].dt.year == year]
    meetings_df = meetings_df.sort_values(by="Timestamp", ascending=True)  # Chronological order
    if meetings_df.empty:
        print(f"No meetings found for {board_name} in {'all years' if year is None else year}")
        return

    # Calculate summary statistics
    total_meetings = len(meetings_df)
    meeting_dates = meetings_df["Timestamp"].dt.strftime("%B %d").tolist()
    meeting_dates_str = ", ".join(meeting_dates)
    total_attachments = len(documents_df[documents_df["Timestamp"].dt.year == year]) if year is not None and not documents_df.empty else len(documents_df)

    # Prepare PDF components
    styles = getSampleStyleSheet()
    temp_files = []
    meeting_pdfs = []

    # Title page with summary
    title_pdf = os.path.join(tempfile.gettempdir(), "title_page.pdf")
    title_doc = SimpleDocTemplate(title_pdf, pagesize=letter)
    title_story = []

    # Add title
    title = f"{board_name.replace('_', ' ')} Minutes and Agendas - {'All Years' if year is None else year}"
    title_story.append(Paragraph(title, styles["Title"]))
    title_story.append(Spacer(1, 12))

    # Add summary
    summary_title = "Summary of Meetings"
    if year is not None:
        summary_title += f" for {year}"
    summary_title += ":"
    title_story.append(Paragraph(summary_title, styles["Heading2"]))
    title_story.append(Spacer(1, 12))

    summary_lines = [
        f"- Total Meetings: {total_meetings}",
        f"- Meeting Dates: {meeting_dates_str}",
        f"- Total Attachments: {total_attachments}"
    ]
    for line in summary_lines:
        title_story.append(Paragraph(line, styles["Normal"]))
        title_story.append(Spacer(1, 6))

    title_doc.build(title_story)
    temp_files.append(title_pdf)

    # Process each meeting
    for idx, meeting in meetings_df.iterrows():
        timestamp = meeting["Timestamp"]
        agenda = meeting.get("Agenda", "")
        if pd.isna(agenda):
            agenda = "No agenda available"

        # Create text content for this meeting
        story = []
        meeting_date = timestamp.strftime("%B %d, %Y %I:%M %p")
        story.append(Paragraph(f"Meeting: {meeting_date}", styles["Heading2"]))
        story.append(Spacer(1, 12))
        story.append(Paragraph("Agenda", styles["Heading3"]))
        agenda_lines = agenda.split("\n")
        for line in agenda_lines:
            story.append(Paragraph(line.strip(), styles["Normal"]))
        story.append(Spacer(1, 12))

        # Add attachment list
        pdf_files = []
        attachment_page_count = 0
        if not documents_df.empty:
            meeting_docs = documents_df[documents_df["Timestamp"] == timestamp]
            if meeting_docs.empty:
                print(f"Warning: No documents found for {meeting_date} (Timestamp: {timestamp})")
            else:
                story.append(Paragraph("Attachments", styles["Heading3"]))
                for _, doc in meeting_docs.iterrows():
                    file_path = doc.get("File Path", "")
                    file_name = doc.get("File Name", "Unknown")
                    if not os.path.exists(file_path):
                        print(f"Warning: Attachment {file_path} not found for {meeting_date}")
                        continue

                    # Handle supported document formats or .pdf attachments
                    final_pdf_path = file_path
                    original_file_name = file_name  # Preserve original name for display
                    file_ext = os.path.splitext(file_path.lower())[1]
                    if file_ext in SUPPORTED_DOC_EXTENSIONS:
                        pdf_name = f"{os.path.splitext(os.path.basename(file_path))[0]}.pdf"
                        final_pdf_path = os.path.join(attachments_dir, pdf_name)
                        if not os.path.exists(final_pdf_path):
                            if convert_doc_to_pdf(file_path, final_pdf_path):
                                print(f"Converted {file_name} to PDF {final_pdf_path} for {meeting_date}")
                            else:
                                print(f"Warning: Failed to convert {file_name} to PDF for {meeting_date}")
                                continue
                        else:
                            print(f"Using existing PDF {final_pdf_path} for {file_name}")
                        file_path = final_pdf_path
                    elif file_ext != '.pdf':
                        print(f"Warning: Skipping unsupported file format {file_name} (extension {file_ext}) for {meeting_date}")
                        continue

                    # Add original file name to the list
                    story.append(Paragraph(f"Attachment: {original_file_name}", styles["Normal"]))
                    story.append(Spacer(1, 6))

                    # Add attachment PDF to merge list
                    if os.path.exists(file_path):
                        pdf_files.append(file_path)
                        # Estimate page count
                        try:
                            result = subprocess.run(
                                ["pdftk", file_path, "dump_data"],
                                capture_output=True, text=True, check=True
                            )
                            for line in result.stdout.splitlines():
                                if line.startswith("NumberOfPages"):
                                    page_count = int(line.split(":")[1].strip())
                                    attachment_page_count += page_count
                                    print(f"Prepared {original_file_name} for {meeting_date} ({page_count} pages)")
                                    break
                        except Exception as e:
                            print(f"Error reading {original_file_name} for page count: {e}")
                    else:
                        print(f"Warning: {original_file_name} not found at {file_path} for {meeting_date}")

        # Generate text PDF for this meeting
        meeting_text_pdf = os.path.join(tempfile.gettempdir(), f"meeting_{idx}.pdf")
        meeting_doc = SimpleDocTemplate(meeting_text_pdf, pagesize=letter)
        meeting_doc.build(story)
        text_page_count = 0
        try:
            result = subprocess.run(
                ["pdftk", meeting_text_pdf, "dump_data"],
                capture_output=True, text=True, check=True
            )
            for line in result.stdout.splitlines():
                if line.startswith("NumberOfPages"):
                    text_page_count = int(line.split(":")[1].strip())
                    break
        except Exception as e:
            print(f"Error reading text PDF page count for {meeting_date}: {e}")
            text_page_count = 1  # Fallback estimate
        print(f"Generated text PDF for {meeting_date} ({text_page_count} pages)")

        # Merge meeting text PDF with its attachments
        meeting_pdf = os.path.join(tempfile.gettempdir(), f"meeting_{idx}_full.pdf")
        meeting_pdf_files = [meeting_text_pdf]
        if pdf_files:
            blank_page_pdf = os.path.join(tempfile.gettempdir(), "blank_page.pdf")
            # Create a single blank page PDF with minimal content
            blank_doc = SimpleDocTemplate(blank_page_pdf, pagesize=letter)
            blank_doc.build([Paragraph("", styles["Normal"])])  # Empty paragraph
            temp_files.append(blank_page_pdf)
            for pdf_file in pdf_files:
                meeting_pdf_files.append(blank_page_pdf)  # Add blank page for separation
                meeting_pdf_files.append(pdf_file)

        if len(meeting_pdf_files) == 1:  # Only text PDF, no attachments
            meeting_pdf_files.append(meeting_text_pdf)  # Duplicate to avoid pdftk error
        try:
            subprocess.run(
                ["pdftk"] + meeting_pdf_files + ["cat", "output", meeting_pdf],
                check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
            )
            meeting_pdfs.append(meeting_pdf)
            temp_files.append(meeting_text_pdf)
            temp_files.append(meeting_pdf)
            print(f"Merged meeting PDF for {meeting_date} (text: {text_page_count}, attachments: {attachment_page_count} pages)")
        except subprocess.CalledProcessError as e:
            print(f"Error merging meeting PDF for {meeting_date}: {e.stderr}")
            continue

    # Merge all meeting PDFs into the final report
    try:
        # Merge all PDFs with pdftk
        final_pdf_files = [title_pdf] + meeting_pdfs
        subprocess.run(
            ["pdftk"] + final_pdf_files + ["cat", "output", output_pdf],
            check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )

        # Get final page count
        try:
            result = subprocess.run(
                ["pdftk", output_pdf, "dump_data"],
                capture_output=True, text=True, check=True
            )
            final_page_count = 0
            for line in result.stdout.splitlines():
                if line.startswith("NumberOfPages"):
                    final_page_count = int(line.split(":")[1].strip())
                    break
            print(f"Generated {output_pdf} successfully (total pages: {final_page_count})")
        except Exception as e:
            print(f"Error reading final PDF page count: {e}")
            print(f"Generated {output_pdf} successfully")

        # Clean up temporary PDFs
        for temp_file in temp_files:
            if os.path.exists(temp_file):
                try:
                    os.remove(temp_file)
                    print(f"Cleaned up temporary PDF {temp_file}")
                except Exception as e:
                    print(f"Error cleaning up {temp_file}: {e}")
    except subprocess.CalledProcessError as e:
        print(f"Error merging final PDF: {e.stderr}")
    except Exception as e:
        print(f"Error generating PDF {output_pdf}: {e}")

def main():
    generate_yearly_report(board_name, year, data_dir)

if __name__ == "__main__":
    main()