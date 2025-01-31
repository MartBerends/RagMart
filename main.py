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
    It fetches PDF documents, extracts text, and uploads it to BigQuery.
    """
    logging.info("Starting document fetching process...")
    
    # Fetch document metadata
    logging.info("Fetching document metadata...")
    fetchDocuments.main()

    logging.info("Process completed successfully.")
    return "Documents processed successfully", 200

