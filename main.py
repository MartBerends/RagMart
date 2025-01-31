import functions_framework
import fetchDocuments
import fetchData
import logging

# Initialize logging
logging.basicConfig(level=logging.INFO)

@functions_framework.http
def fetch_and_process_documents(request):
    """
    This function is triggered by an HTTP request.
    It first fetches document metadata, then downloads PDFs, extracts text,
    and uploads everything to BigQuery.
    """
    logging.info("Starting document fetching and processing pipeline...")

    # Step 1: Fetch document metadata from overheid API and store in BigQuery
    logging.info("Fetching document metadata...")
    fetchData.fetch_and_store_documents(request)  # <-- Call fetchData function

    # Step 2: Process the documents (download, extract text, upload text)
    logging.info("Processing and extracting text from PDFs...")
    fetchDocuments.fetch_and_process_documents(request)  # <-- Call fetchDocuments function

    logging.info("All documents processed successfully.")
    return "Documents fetched and processed successfully", 200
