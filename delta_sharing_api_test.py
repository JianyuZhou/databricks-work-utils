import requests

# Define the Databricks domain and endpoint
DATABRICKS_DOMAIN = "<workspace-url>"
ENDPOINT = "/api/2.1/unity-catalog/shares"

# Your personal access token or API token
TOKEN = "<bear token>"

# Set up the headers for the request
headers = {
    "Authorization": TOKEN,
    "Content-Type": "application/json",
}

# Make the GET request
response = requests.get(DATABRICKS_DOMAIN + ENDPOINT, headers=headers)

# Print the response
print(response.json())
