import azure.functions as func
import logging
import os
import requests
import msal
import json

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)


# Service principal configuration (set these in local.settings.json for local dev)
TENANT_ID = os.getenv("TENANT_ID") or os.getenv("AZURE_TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID") or os.getenv("AZURE_CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET") or os.getenv("AZURE_CLIENT_SECRET")
ONELAKE_SCOPE = os.getenv("ONELAKE_SCOPE", "https://storage.azure.com/.default")
ONELAKE_TEST_ENDPOINT = os.getenv("ONELAKE_TEST_ENDPOINT")  # optional test endpoint to call
AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}" if TENANT_ID else None

BaseURL = "https://onelake.table.fabric.microsoft.com/"

def acquire_onelake_token():
    """Acquire an access token using client credentials (service principal) via MSAL."""
    if not (TENANT_ID and CLIENT_ID and CLIENT_SECRET):
        raise RuntimeError("TENANT_ID, CLIENT_ID and CLIENT_SECRET must be set in environment (local.settings.json for local dev).")

    app = msal.ConfidentialClientApplication(
        CLIENT_ID,
        authority=AUTHORITY,
        client_credential=CLIENT_SECRET,
    )

    result = app.acquire_token_for_client(scopes=["https://storage.azure.com/.default"])
    if "access_token" in result:    
        return result["access_token"]
    else:
        logging.error("MSAL token acquisition failed: %s", result)
        raise RuntimeError("Failed to acquire token: " + str(result.get("error_description") or result))

@app.route(route="fn_get_irc_configuration")

def fn_get_irc_configuration(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    # Read input params
    workspace = req.params.get('workspace')
    dataitem = req.params.get('dataitem')

    warehouse_id = f"/{workspace}/{dataitem}"
    #warehouse_id = "/iceberg-westus-workspace/LH_WestUS_Iceberg_Demo.Lakehouse"

    # Acquire token (non-fatal for request; used for display and for API call)
    token = None
    try:
        token = acquire_onelake_token()
    except Exception as e:
        logging.warning("Could not acquire OneLake token: %s", e)
    
    # List all table identifiers under the namespace
    tables_url = f"https://onelake.dfs.fabric.microsoft.com/iceberg/v1/config?warehouse={warehouse_id}"
    
    if token is None:
        logging.error("Token required for OneLake tables call but not available")
        return func.HttpResponse("Authentication required for OneLake tables call", status_code=401)

    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

    try:
        tables_response = requests.get(tables_url, headers=headers, timeout=30)
        tables_response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logging.error("OneLake tables API request failed: %s", e)
        # try to return upstream response body if available
        resp = getattr(e, 'response', None)
        if resp is not None:
            try:
                return func.HttpResponse(resp.text, status_code=resp.status_code)
            except Exception:
                pass
        return func.HttpResponse(str(e), status_code=502)

    # Parse and return JSON body
    try:
        tables = tables_response.json()
        return func.HttpResponse(json.dumps(tables), status_code=tables_response.status_code, mimetype="application/json")        
    except ValueError:
        return func.HttpResponse(tables_response.text, status_code=tables_response.status_code)


@app.route(route="fn_list_namespaces")

def fn_list_namespaces(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    # Read input params
    workspace = req.params.get('workspace')
    dataitem = req.params.get('dataitem')
    
    warehouse_id = f"/{workspace}/{dataitem}"
    #warehouse_id = "/iceberg-westus-workspace/LH_WestUS_Iceberg_Demo.Lakehouse"

    # Acquire token (non-fatal for request; used for display and for API call)
    token = None
    try:
        token = acquire_onelake_token()
    except Exception as e:
        logging.warning("Could not acquire OneLake token: %s", e)
    
    # List all table identifiers under the namespace
    tables_url = f"https://onelake.dfs.fabric.microsoft.com/iceberg/v1/{warehouse_id}/namespaces"
    
    if token is None:
        logging.error("Token required for OneLake tables call but not available")
        return func.HttpResponse("Authentication required for OneLake tables call", status_code=401)

    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

    try:
        tables_response = requests.get(tables_url, headers=headers, timeout=30)
        tables_response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logging.error("OneLake tables API request failed: %s", e)
        # try to return upstream response body if available
        resp = getattr(e, 'response', None)
        if resp is not None:
            try:
                return func.HttpResponse(resp.text, status_code=resp.status_code)
            except Exception:
                pass
        return func.HttpResponse(str(e), status_code=502)

    # Parse and return JSON body
    try:
        tables = tables_response.json()
        return func.HttpResponse(json.dumps(tables), status_code=tables_response.status_code, mimetype="application/json")        
    except ValueError:
        return func.HttpResponse(tables_response.text, status_code=tables_response.status_code)

@app.route(route="fn_get_schema_details")

def fn_get_schema_details(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    # Read input params
    workspace = req.params.get('workspace')
    dataitem = req.params.get('dataitem')
    schema = req.params.get('schema')
    
    warehouse_id = f"/{workspace}/{dataitem}"
    #warehouse_id = "/iceberg-westus-workspace/LH_WestUS_Iceberg_Demo.Lakehouse"

    # Acquire token (non-fatal for request; used for display and for API call)
    token = None
    try:
        token = acquire_onelake_token()
    except Exception as e:
        logging.warning("Could not acquire OneLake token: %s", e)
    
    # List all table identifiers under the namespace
    tables_url = f"https://onelake.dfs.fabric.microsoft.com/iceberg/v1/{warehouse_id}/namespaces/{schema}"
    
    if token is None:
        logging.error("Token required for OneLake tables call but not available")
        return func.HttpResponse("Authentication required for OneLake tables call", status_code=401)

    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

    try:
        tables_response = requests.get(tables_url, headers=headers, timeout=30)
        tables_response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logging.error("OneLake tables API request failed: %s", e)
        # try to return upstream response body if available
        resp = getattr(e, 'response', None)
        if resp is not None:
            try:
                return func.HttpResponse(resp.text, status_code=resp.status_code)
            except Exception:
                pass
        return func.HttpResponse(str(e), status_code=502)

    # Parse and return JSON body
    try:
        tables = tables_response.json()
        return func.HttpResponse(json.dumps(tables), status_code=tables_response.status_code, mimetype="application/json")        
    except ValueError:
        return func.HttpResponse(tables_response.text, status_code=tables_response.status_code)

@app.route(route="fn_read_iceberg_catalog")

def fn_read_iceberg_catalog(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    # Read input params
    workspace = req.params.get('workspace')
    dataitem = req.params.get('dataitem')
    schema = req.params.get('schema')

    warehouse_id = f"/{workspace}/{dataitem}"
    #warehouse_id = "/iceberg-westus-workspace/LH_WestUS_Iceberg_Demo.Lakehouse"

    # Acquire token (non-fatal for request; used for display and for API call)
    token = None
    try:
        token = acquire_onelake_token()
    except Exception as e:
        logging.warning("Could not acquire OneLake token: %s", e)
    
    # List all table identifiers under the namespace
    tables_url = f"https://onelake.dfs.fabric.microsoft.com/iceberg/v1/{warehouse_id}/namespaces/{schema}/tables"

    if token is None:
        logging.error("Token required for OneLake tables call but not available")
        return func.HttpResponse("Authentication required for OneLake tables call", status_code=401)

    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

    try:
        tables_response = requests.get(tables_url, headers=headers, timeout=30)
        tables_response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logging.error("OneLake tables API request failed: %s", e)
        # try to return upstream response body if available
        resp = getattr(e, 'response', None)
        if resp is not None:
            try:
                return func.HttpResponse(resp.text, status_code=resp.status_code)
            except Exception:
                pass
        return func.HttpResponse(str(e), status_code=502)

    # Parse and return JSON body
    try:
        tables = tables_response.json()
        return func.HttpResponse(json.dumps(tables), status_code=tables_response.status_code, mimetype="application/json")        
    except ValueError:
        return func.HttpResponse(tables_response.text, status_code=tables_response.status_code)

@app.route(route="fn_read_tables")

def fn_read_tables(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    # Read input params
    workspace = req.params.get('workspace')
    dataitem = req.params.get('dataitem')
    schema = req.params.get('schema')
    table = req.params.get('table')

    warehouse_id = f"/{workspace}/{dataitem}"
    #warehouse_id = "/iceberg-westus-workspace/LH_WestUS_Iceberg_Demo.Lakehouse"

    # Acquire token (non-fatal for request; used for display and for API call)
    token = None
    try:
        token = acquire_onelake_token()
    except Exception as e:
        logging.warning("Could not acquire OneLake token: %s", e)
    
    # List all table identifiers under the namespace
    tables_url = f"https://onelake.dfs.fabric.microsoft.com/iceberg/v1/{warehouse_id}/namespaces/{schema}/tables/{table}"

    if token is None:
        logging.error("Token required for OneLake tables call but not available")
        return func.HttpResponse("Authentication required for OneLake tables call", status_code=401)

    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

    try:
        tables_response = requests.get(tables_url, headers=headers, timeout=30)
        tables_response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logging.error("OneLake tables API request failed: %s", e)
        # try to return upstream response body if available
        resp = getattr(e, 'response', None)
        if resp is not None:
            try:
                return func.HttpResponse(resp.text, status_code=resp.status_code)
            except Exception:
                pass
        return func.HttpResponse(str(e), status_code=502)

    # Parse and return JSON body
    try:
        tables = tables_response.json()
        return func.HttpResponse(json.dumps(tables), status_code=tables_response.status_code, mimetype="application/json")        
    except ValueError:
        return func.HttpResponse(tables_response.text, status_code=tables_response.status_code)



