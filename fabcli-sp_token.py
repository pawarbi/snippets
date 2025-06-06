import msal

def get_fabric_service_principal_token(client_id, client_secret, tenant_id):
    app = msal.ConfidentialClientApplication(
        client_id=client_id,
        client_credential=client_secret,
        authority=f"https://login.microsoftonline.com/{tenant_id}"
    )
    fabric_scope = "https://analysis.windows.net/powerbi/api/.default"
    result = app.acquire_token_for_client(scopes=[fabric_scope])
    if "access_token" in result:
        return result["access_token"]
    else:
        print(f"Error: {result.get('error')}")
        print(f"Description: {result.get('error_description')}")
        return None

client_id = "your-service-principal-client-id"
client_secret = "your-service-principal-secret"
tenant_id = "your-tenant-id"

fabric_token = get_fabric_service_principal_token(client_id, client_secret, tenant_id)

if fabric_token:
    print("Successfully obtained Fabric token")
else:
    print("Failed to obtain token")


#get token for fab-cli

os.environ['FAB_TOKEN'] = fabric_token
os.environ['FAB_TOKEN_ONELAKE'] = fabric_token

