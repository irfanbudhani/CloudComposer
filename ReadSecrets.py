import pendulum
from airflow.decorators import dag, task

# Import the Secret Manager client library
from google.cloud import secretmanager

# --- Define your secret details ---
# TODO: Change these to your values
GCP_PROJECT_ID = "your-gcp-project-id"
SECRET_ID = "your-secret-name-in-gcp"
SECRET_VERSION = "latest"  # Usually 'latest' is what you want

def get_secret(project_id: str, secret_id: str, version_id: str) -> str:
    """
    Helper function to retrieve a secret from GCP Secret Manager.
    """
    # Create the Secret Manager client
    client = secretmanager.SecretManagerServiceClient()

    # Build the full resource name of the secret version
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

    # Access the secret version
    print(f"Accessing secret: {name}")
    response = client.access_secret_version(request={"name": name})

    # Decode the secret payload
    secret_value = response.payload.data.decode("UTF-8")
    return secret_value


@task
def fetch_and_use_secret():
    """
    This task fetches the secret and then uses it.
    """
    try:
        # 1. Fetch the secret
        api_key = get_secret(GCP_PROJECT_ID, SECRET_ID, SECRET_VERSION)

        # -----------------------------------------------------------------
        # ! SECURITY WARNING !
        #
        # DO NOT print the actual secret value to the logs like this:
        # print(f"My secret is: {api_key}")
        #
        # This will expose it in your Airflow task logs.
        # Instead, just confirm it was fetched.
        # -----------------------------------------------------------------

        print(f"Successfully fetched secret '{SECRET_ID}'.")
        print(f"The secret's first 3 chars are: {api_key[:3]}...")

        # 2. Use the secret for authentication
        #
        # This is a fake example of using the fetched secret (e.g., an API key)
        # to authenticate with another service.
        
        print("Authenticating to external_api.com...")
        # http_client = ExternalApiClient(base_url="https://api.external_api.com")
        # http_client.set_auth(token=api_key)
        # data = http_client.get_data()
        # print("Successfully authenticated and fetched data.")

        return "Authentication successful"

    except Exception as e:
        print(f"Error while fetching or using secret: {e}")
        raise

@dag(
    dag_id="gcp_secret_manager_example",
    start_date=pendulum.today('UTC').add(days=-1),
    schedule=None,
    catchup=False,
    tags=["gcp", "secret-manager", "example"],
)
def secret_manager_dag():
    """
    DAG to demonstrate fetching and using a secret from GCP Secret Manager.
    """
    fetch_and_use_secret()

# Instantiate the DAG
secret_manager_dag()
