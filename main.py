import functions_framework
import fetch_pdfs
import fetchDocuments
import sendData
import logging

# Initialize logging
logging.basicConfig(level=logging.INFO)

@functions_framework.http
def fetch_and_process_documents(request):
    """
    This function is triggered by an HTTP request.
    It fetches PDF documents, extracts text, and uploads it to BigQuery.
    """
    logging.info("Starting document fetching process...")
    
    # Fetch document metadata
    logging.info("Fetching document metadata...")
    fetchDocuments.main()

    # Download PDFs and extract text
    logging.info("Downloading PDFs and extracting text...")
    fetch_pdfs.main()

    # Send extracted text to BigQuery
    logging.info("Uploading text data to BigQuery...")
    sendData.main()

    logging.info("Process completed successfully.")
    return "Documents processed successfully", 200

