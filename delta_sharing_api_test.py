import requests

# Define the Databricks domain and endpoint
DATABRICKS_DOMAIN = "https://dbc-64aa1604-7419.dev.databricks.com"
ENDPOINT = "/api/2.1/unity-catalog/shares"

# Your personal access token or API token
TOKEN = "Bearer dapi1b05f73b8ee741eccd1de61430a8fd47"

# Set up the headers for the request
headers = {
    "Authorization": TOKEN,
    "Content-Type": "application/json",
}

# Make the GET request
response = requests.get(DATABRICKS_DOMAIN + ENDPOINT, headers=headers)

# Print the response
print(response.json())
