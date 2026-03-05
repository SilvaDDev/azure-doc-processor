import azure.functions as func
import logging
import json
import os
import uuid
from datetime import datetime
from azure.ai.formrecognizer import DocumentAnalysisClient
from azure.core.credentials import AzureKeyCredential
from azure.cosmos import CosmosClient

app = func.FunctionApp()


def get_document_intelligence_client():
    """
    Creates a client to communicate with Document Intelligence.
    """
    endpoint = os.environ["DOCUMENT_INTELLIGENCE_ENDPOINT"]
    key = os.environ["DOCUMENT_INTELLIGENCE_KEY"]
    
    credential = AzureKeyCredential(key)
    client = DocumentAnalysisClient(endpoint=endpoint, credential=credential)
    
    return client


def get_cosmos_container():
    """
    Creates a client to communicate with Cosmos DB and returns the container.
    """
    connection_string = os.environ["CosmosDBConnection"]
    
    client = CosmosClient.from_connection_string(connection_string)
    database = client.get_database_client("DocumentsDB")
    container = database.get_container_client("ProcessedDocuments")
    
    return container


def analyze_document(file_content: bytes):
    """
    Sends document to Document Intelligence and returns extracted data.
    """
    client = get_document_intelligence_client()
    
    poller = client.begin_analyze_document("prebuilt-document", file_content)
    result = poller.result()
    
    extracted_data = {
        "key_value_pairs": [],
        "tables": [],
        "content": result.content if result.content else ""
    }
    
    if result.key_value_pairs:
        for pair in result.key_value_pairs:
            if pair.key and pair.value:
                extracted_data["key_value_pairs"].append({
                    "key": pair.key.content,
                    "value": pair.value.content
                })
    
    if result.tables:
        for table in result.tables:
            table_data = {
                "row_count": table.row_count,
                "column_count": table.column_count,
                "cells": []
            }
            for cell in table.cells:
                table_data["cells"].append({
                    "row": cell.row_index,
                    "column": cell.column_index,
                    "content": cell.content
                })
            extracted_data["tables"].append(table_data)
    
    return extracted_data


def save_to_cosmos(file_name: str, extracted_data: dict):
    """
    Saves the extracted data to Cosmos DB.
    """
    container = get_cosmos_container()
    
    # Create the document to save
    document = {
        "id": str(uuid.uuid4()),
        "fileName": file_name,
        "documentType": "invoice",
        "status": "completed",
        "extractedData": extracted_data,
        "processedAt": datetime.utcnow().isoformat(),
        "keyValuePairCount": len(extracted_data["key_value_pairs"]),
        "tableCount": len(extracted_data["tables"])
    }
    
    # Save to Cosmos DB
    container.create_item(body=document)
    
    return document["id"]


@app.blob_trigger(
    arg_name="myblob",
    path="uploads/{name}",
    connection="AzureWebJobsStorage"
)
def process_document(myblob: func.InputStream):
    """
    Triggered when a file is uploaded to the 'uploads' container.
    """
    logging.info(f"Processing blob: {myblob.name}")
    logging.info(f"Blob size: {myblob.length} bytes")
    
    # Step 1: Read the file content
    file_content = myblob.read()
    logging.info("File content read successfully")
    
    # Step 2: Send to Document Intelligence
    logging.info("Sending to Document Intelligence for analysis...")
    extracted_data = analyze_document(file_content)
    logging.info("Document analysis complete")
    
    logging.info(f"Extracted {len(extracted_data['key_value_pairs'])} key-value pairs")
    logging.info(f"Extracted {len(extracted_data['tables'])} tables")
    
    # Step 3: Save to Cosmos DB
    logging.info("Saving to Cosmos DB...")
    document_id = save_to_cosmos(myblob.name, extracted_data)
    logging.info(f"Saved to Cosmos DB with ID: {document_id}")
    
    logging.info("Document processing complete")
@app.route(route="documents", methods=["GET"])
def get_documents(req: func.HttpRequest) -> func.HttpResponse:
    """
    Returns a list of all processed documents.
    """
    logging.info("GET /api/documents called")
    
    try:
        container = get_cosmos_container()
        
        # Query all documents
        query = "SELECT c.id, c.fileName, c.documentType, c.status, c.processedAt, c.keyValuePairCount, c.tableCount FROM c"
        items = list(container.query_items(query=query, enable_cross_partition_query=True))
        
        logging.info(f"Found {len(items)} documents")
        
        return func.HttpResponse(
            body=json.dumps(items, indent=2),
            status_code=200,
            mimetype="application/json"
        )
    
    except Exception as e:
        logging.error(f"Error fetching documents: {str(e)}")
        return func.HttpResponse(
            body=json.dumps({"error": str(e)}),
            status_code=500,
            mimetype="application/json"
        )


@app.route(route="documents/{id}", methods=["GET"])
def get_document_by_id(req: func.HttpRequest) -> func.HttpResponse:
    """
    Returns a specific document by ID.
    """
    document_id = req.route_params.get("id")
    logging.info(f"GET /api/documents/{document_id} called")
    
    if not document_id:
        return func.HttpResponse(
            body=json.dumps({"error": "Document ID is required"}),
            status_code=400,
            mimetype="application/json"
        )
    
    try:
        container = get_cosmos_container()
        
        # Query for specific document
        query = "SELECT * FROM c WHERE c.id = @id"
        parameters = [{"name": "@id", "value": document_id}]
        items = list(container.query_items(query=query, parameters=parameters, enable_cross_partition_query=True))
        
        if not items:
            return func.HttpResponse(
                body=json.dumps({"error": "Document not found"}),
                status_code=404,
                mimetype="application/json"
            )
        
        logging.info(f"Found document: {document_id}")
        
        return func.HttpResponse(
            body=json.dumps(items[0], indent=2),
            status_code=200,
            mimetype="application/json"
        )
    
    except Exception as e:
        logging.error(f"Error fetching document: {str(e)}")
        return func.HttpResponse(
            body=json.dumps({"error": str(e)}),
            status_code=500,
            mimetype="application/json"
        )