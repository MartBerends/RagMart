import os
import pandas as pd
import requests
import time
from PyPDF2 import PdfReader
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configuration
CSV_FILE = "document_data.csv"
PDF_FOLDER = "pdfs/"
TEXT_FOLDER = "texts/"
FAILED_FILES = "failed_files.txt"  # File to track failed PDFs
BATCH_SIZE = 5000  # Number of PDFs to process in one run
MAX_THREADS = 5  # Number of parallel threads for faster processing

# Create directories if they don't exist
os.makedirs(PDF_FOLDER, exist_ok=True)
os.makedirs(TEXT_FOLDER, exist_ok=True)

# Load previously failed files to skip them
if os.path.exists(FAILED_FILES):
    with open(FAILED_FILES, "r") as f:
        failed_files = set(f.read().splitlines())  # Store failed file IDs
else:
    failed_files = set()

def download_pdf(document_id):
    """Download a PDF document using its ID."""
    url = f"https://gegevensmagazijn.tweedekamer.nl/OData/v4/2.0/Document({document_id})/resource"
    pdf_path = os.path.join(PDF_FOLDER, f"{document_id}.pdf")
    
    try:
        response = requests.get(url, stream=True)
        if response.status_code == 200:
            with open(pdf_path, "wb") as f:
                f.write(response.content)
            return pdf_path
        elif response.status_code == 429:
            print(f"Rate limit reached (429) for {url}. Pausing...")
            time.sleep(20)
            return None
        else:
            print(f"Failed to download {url}: {response.status_code}")
            return None
    except Exception as e:
        print(f"Error downloading PDF {document_id}: {e}")
        return None

def extract_text_from_pdf(pdf_path, document_id):
    """Extract text from a PDF and save it as a text file. Skip corrupt files."""
    try:
        reader = PdfReader(pdf_path)
        text = "".join(page.extract_text() or "" for page in reader.pages)
        
        if not text.strip():  # If no text is extracted, consider it a failed file
            raise ValueError("No text found")

        text_path = os.path.join(TEXT_FOLDER, os.path.basename(pdf_path).replace(".pdf", ".txt"))
        with open(text_path, "w", encoding="utf-8") as f:
            f.write(text)

        os.remove(pdf_path)  # Delete the PDF after extracting text
        return text_path

    except Exception as e:
        print(f"Error extracting text from {pdf_path}: {e}")
        failed_files.add(document_id)  # Mark as failed to skip next time
        with open(FAILED_FILES, "a") as f:
            f.write(document_id + "\n")
        return None

def is_already_processed(document_id):
    """Check if a document's text file already exists or was previously marked as failed."""
    return (os.path.exists(os.path.join(TEXT_FOLDER, f"{document_id}.txt")) or
            document_id in failed_files)

def count_total_documents(csv_file):
    """Count total valid documents (excluding deleted or missing IDs)."""
    df = pd.read_csv(csv_file)
    valid_docs = df[(~df["Id"].isna()) & (df["Verwijderd"] != True)]
    return len(valid_docs)

def count_processed_documents():
    """Count already processed text files."""
    return len([f for f in os.listdir(TEXT_FOLDER) if f.endswith(".txt")])

def process_documents(csv_file, batch_size):
    """Process documents in parallel using multithreading while skipping failed ones."""
    df = pd.read_csv(csv_file)
    total_documents = count_total_documents(csv_file)
    processed_count = count_processed_documents()
    
    print(f"Total valid documents: {total_documents}")
    print(f"Already processed: {processed_count}")

    # Get list of unprocessed document IDs (excluding failed files)
    document_ids = [
        row["Id"] for _, row in df.iterrows()
        if not pd.isna(row["Id"]) and not is_already_processed(row["Id"]) and row.get("Verwijderd", False) != True
    ]
    
    # Limit batch size
    document_ids = document_ids[:batch_size]

    # Process PDFs in parallel
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        future_to_id = {executor.submit(download_pdf, doc_id): doc_id for doc_id in document_ids}
        
        for future in as_completed(future_to_id):
            doc_id = future_to_id[future]
            pdf_path = future.result()
            if pdf_path:
                executor.submit(extract_text_from_pdf, pdf_path, doc_id)
            
            # Track progress
            processed_count += 1
            if processed_count % 2 == 0 or processed_count == len(document_ids):  # Log every 10 files
                progress_percentage = (processed_count / total_documents) * 100
                print(f"Progress: {processed_count}/{total_documents} ({progress_percentage:.2f}%)")

    print(f"Final Progress: {processed_count}/{total_documents} ({(processed_count / total_documents) * 100:.2f}%)")

if __name__ == "__main__":
    process_documents(CSV_FILE, BATCH_SIZE)
