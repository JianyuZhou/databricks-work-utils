import requests

# Define the Databricks domain and endpoint
DATABRICKS_DOMAIN = "https://<YOUR-DATABRICKS-WORKSPACE-URL>"
ENDPOINT = "/api/2.0/workspace/shares/list"

# Your personal access token or API token
TOKEN = "Bearer <YOUR-TOKEN>"

# Set up the headers for the request
headers = {
    "Authorization": TOKEN,
    "Content-Type": "application/json",
}

# Make the GET request
response = requests.get(DATABRICKS_DOMAIN + ENDPOINT, headers=headers)

# Print the response
print(response.json())
