import azure.functions as func
import logging
import json
import os
import uuid
from datetime import datetime
from azure.ai.formrecognizer import DocumentAnalysisClient
from azure.core.credentials import AzureKeyCredential
from azure.cosmos import CosmosClient
import requests
from jose import jwt, JWTError

app = func.FunctionApp()

# ============================================================================
# AUTHENTICATION FUNCTIONS
# ============================================================================

_jwks_cache = None

def get_signing_keys():
    """
    Fetches Microsoft's public signing keys.
    These keys are used to verify that tokens are genuinely from Microsoft.
    """
    global _jwks_cache
    
    if _jwks_cache is not None:
        return _jwks_cache
    
    tenant_id = os.environ["AUTH_TENANT_ID"]
    
    jwks_url = f"https://login.microsoftonline.com/{tenant_id}/discovery/v2.0/keys"
    
    response = requests.get(jwks_url)
    _jwks_cache = response.json()
    
    return _jwks_cache


def validate_token(req: func.HttpRequest):
    """
    Extracts and validates the token from the request.
    Returns the token claims (user info) if valid, None if invalid.
    """
    auth_header = req.headers.get("Authorization")
    
    if not auth_header:
        logging.warning("No Authorization header found")
        return None
    
    parts = auth_header.split()
    
    if len(parts) != 2 or parts[0].lower() != "bearer":
        logging.warning("Authorization header format is not 'Bearer <token>'")
        return None
    
    token = parts[1]
    
    try:
        jwks = get_signing_keys()
    except Exception as e:
        logging.error(f"Failed to fetch signing keys: {str(e)}")
        return None
    
    try:
        unverified_header = jwt.get_unverified_header(token)
        kid = unverified_header.get("kid")
    except JWTError as e:
        logging.warning(f"Failed to decode token header: {str(e)}")
        return None
    
    signing_key = None
    for key in jwks.get("keys", []):
        if key.get("kid") == kid:
            signing_key = key
            break
    
    if not signing_key:
        logging.warning("No matching signing key found")
        return None
    
    tenant_id = os.environ["AUTH_TENANT_ID"]
    client_id = os.environ["AUTH_CLIENT_ID"]
    
    try:
        claims = jwt.decode(
            token,
            signing_key,
            algorithms=["RS256"],
            audience=client_id,
            issuer=f"https://login.microsoftonline.com/{tenant_id}/v2.0"
        )
        
        logging.info(f"Token validated for user: {claims.get('sub')}")
        return claims
        
    except JWTError as e:
        logging.warning(f"Token validation failed: {str(e)}")
        return None


def get_user_id(claims):
    """
    Extracts the user ID from token claims.
    """
    return claims.get("sub")


# ============================================================================
# DOCUMENT INTELLIGENCE AND COSMOS DB FUNCTIONS
# ============================================================================

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


def save_to_cosmos(file_name: str, extracted_data: dict, user_id: str = None):
    """
    Saves the extracted data to Cosmos DB.
    Now includes user_id to associate documents with users.
    """
    container = get_cosmos_container()
    
    document = {
        "id": str(uuid.uuid4()),
        "userId": user_id,
        "fileName": file_name,
        "documentType": "invoice",
        "status": "completed",
        "extractedData": extracted_data,
        "processedAt": datetime.utcnow().isoformat(),
        "keyValuePairCount": len(extracted_data["key_value_pairs"]),
        "tableCount": len(extracted_data["tables"])
    }
    
    container.create_item(body=document)
    
    return document["id"]


# ============================================================================
# BLOB TRIGGER FUNCTION
# ============================================================================

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
    
    file_content = myblob.read()
    logging.info("File content read successfully")
    
    logging.info("Sending to Document Intelligence for analysis...")
    extracted_data = analyze_document(file_content)
    logging.info("Document analysis complete")
    
    logging.info(f"Extracted {len(extracted_data['key_value_pairs'])} key-value pairs")
    logging.info(f"Extracted {len(extracted_data['tables'])} tables")
    
    logging.info("Saving to Cosmos DB...")
    document_id = save_to_cosmos(myblob.name, extracted_data, user_id=None)
    logging.info(f"Saved to Cosmos DB with ID: {document_id}")
    
    logging.info("Document processing complete")


# ============================================================================
# HTTP TRIGGER FUNCTIONS (API Endpoints)
# ============================================================================

@app.route(route="documents", methods=["GET"])
def get_documents(req: func.HttpRequest) -> func.HttpResponse:
    """
    Returns a list of documents for the authenticated user.
    """
    logging.info("GET /api/documents called")
    
    claims = validate_token(req)
    
    if not claims:
        return func.HttpResponse(
            body=json.dumps({"error": "Unauthorized. Valid token required."}),
            status_code=401,
            mimetype="application/json"
        )
    
    user_id = get_user_id(claims)
    logging.info(f"Fetching documents for user: {user_id}")
    
    try:
        container = get_cosmos_container()
        
        query = """
            SELECT c.id, c.fileName, c.documentType, c.status, 
                   c.processedAt, c.keyValuePairCount, c.tableCount 
            FROM c 
            WHERE c.userId = @userId
        """
        parameters = [{"name": "@userId", "value": user_id}]
        
        items = list(container.query_items(
            query=query,
            parameters=parameters,
            enable_cross_partition_query=True
        ))
        
        logging.info(f"Found {len(items)} documents for user")
        
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
    Returns a specific document by ID (only if it belongs to the user).
    """
    document_id = req.route_params.get("id")
    logging.info(f"GET /api/documents/{document_id} called")
    
    claims = validate_token(req)
    
    if not claims:
        return func.HttpResponse(
            body=json.dumps({"error": "Unauthorized. Valid token required."}),
            status_code=401,
            mimetype="application/json"
        )
    
    user_id = get_user_id(claims)
    
    if not document_id:
        return func.HttpResponse(
            body=json.dumps({"error": "Document ID is required"}),
            status_code=400,
            mimetype="application/json"
        )
    
    try:
        container = get_cosmos_container()
        
        query = "SELECT * FROM c WHERE c.id = @id AND c.userId = @userId"
        parameters = [
            {"name": "@id", "value": document_id},
            {"name": "@userId", "value": user_id}
        ]
        
        items = list(container.query_items(
            query=query,
            parameters=parameters,
            enable_cross_partition_query=True
        ))
        
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