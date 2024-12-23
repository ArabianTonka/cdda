import pandas as pd
import logging
from azure.core.exceptions import AzureError
from azure.storage.blob import BlobServiceClient
from fastapi import FastAPI, UploadFile, File, HTTPException
from io import StringIO

# Azure Blob Storage configuration
CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=storageacc2134;AccountKey=AJuTRpbMbJX1nY21oeDjgce2ztvqctMyhOkJkb5cf1z9abk4STK9f31owP8bUjDmjwWjfbQi3xiZ+ASt+RTfWQ==;EndpointSuffix=core.windows.net"
CONTAINER_NAME = "mycontainer"
BLOB_NAME = "marketing_campaign.csv"

# Initialize the FastAPI app
app = FastAPI()

# Load local CSV as default
data = pd.read_csv("marketing_campaign.csv", delimiter="\t")
data.columns = data.columns.str.strip()  # Clean column names


def load_data_from_blob():
    """
    Load data from Azure Blob Storage and return a pandas DataFrame.
    """
    try:
        blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
        blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=BLOB_NAME)

        # Download blob content and parse CSV
        blob_data = blob_client.download_blob().readall()
        csv_string = blob_data.decode("utf-8")

        # Try reading the CSV with a comma delimiter
        try:
            data = pd.read_csv(StringIO(csv_string), delimiter=",")
        except Exception as e:
            print(f"Error reading with comma delimiter: {e}")
            # If comma fails, try tab delimiter
            try:
                data = pd.read_csv(StringIO(csv_string), delimiter="\t")
            except Exception as e:
                print(f"Error reading with tab delimiter: {e}")
                # If both fail, raise an exception
                raise HTTPException(status_code=400, detail="Failed to parse CSV file: Invalid format.")

        # Check if data is empty or no valid columns found
        if data.empty:
            raise HTTPException(status_code=400, detail="No valid data columns found in the file.")

        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to load data from Azure Blob: {str(e)}")


@app.get("/")
def read_root():
    """
    Root endpoint.
    """
    return {"message": "Welcome to the Azure-integrated FastAPI Kaggle data API!"}


@app.get("/data")
def get_data(limit: int = 10):
    """
    Get a sample of the data.
    """
    global data
    return data.head(limit).to_dict(orient="records")


@app.get("/columns")
def get_columns():
    """
    Get the list of column names.
    """
    global data
    if data.empty:
        return {"error": "Data is empty or not loaded."}
    return {"columns": data.columns.tolist()}


@app.get("/data/{column}")
def get_column_values(column: str):
    """
    Get unique values from a specific column.
    """
    global data
    if column not in data.columns:
        return {"error": f"The column {column} does not exist."}
    return data[column].dropna().unique().tolist()


@app.get("/stats/{column}")
def get_column_stats(column: str):
    """
    Get basic statistics for a numeric column.
    """
    global data
    if column not in data.columns or not pd.api.types.is_numeric_dtype(data[column]):
        return {"error": f"The column {column} is not numeric or does not exist."}
    stats = {
        "mean": data[column].mean(),
        "min": data[column].min(),
        "max": data[column].max(),
        "std": data[column].std(),
    }
    return stats


@app.get("/response")
def get_response_column():
    """
    Retrieves all values from the 'Response' column.
    """
    global data
    if "Response" not in data.columns:
        return {"error": "The 'Response' column does not exist in the data."}
    
    # Return all values, including duplicates, as a list
    return data["Response"].tolist()


@app.post("/upload")
async def upload_file_to_blob(file: UploadFile = File(...)):
    """
    Uploads a file to Azure Blob Storage.
    """
    try:
        blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
        blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob="marketing_campaign.csv")

        file_content = await file.read()

        if len(file_content) == 0:
            raise HTTPException(status_code=400, detail="Uploaded file is empty.")

        blob_client.upload_blob(file_content, overwrite=True)
        return {"message": f"File {file.filename} uploaded successfully."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to upload the file: {str(e)}")






@app.get("/load-from-blob")
def load_csv_from_blob():
    """
    Load CSV data from Azure Blob Storage into the application.
    """
    global data
    try:
        data = load_data_from_blob()
        return {"message": "Data successfully loaded from Azure Blob Storage."}
    except HTTPException as e:
        raise e
    
def load_data_from_blob():
    """
    Load data from Azure Blob Storage and return a pandas DataFrame.
    """
    try:
        print("Starting the process of loading data from Azure Blob Storage...")
        
        # Create Blob Service Client
        blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
        blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=BLOB_NAME)

        # Check if the blob exists
        blob_exists = blob_client.exists()
        if not blob_exists:
            print(f"Blob {BLOB_NAME} does not exist in the container.")
            raise HTTPException(status_code=404, detail="Blob not found.")

        # Download blob content
        blob_data = blob_client.download_blob().readall()
        if not blob_data:
            print("Blob content is empty.")
            raise HTTPException(status_code=400, detail="Blob data is empty.")

        print(f"Blob data fetched successfully. Size: {len(blob_data)} bytes.")
        
        # Decode the content
        csv_string = blob_data.decode("utf-8-sig")  # Handle BOM if present
        print(f"First 500 characters of blob content:\n{csv_string[:500]}")

        # Try to read the CSV file
        data = pd.read_csv(StringIO(csv_string), sep=r"\s+")
        print("CSV data loaded successfully.")

        # Check if the data is empty
        if data.empty:
            print("The CSV data is empty.")
            raise HTTPException(status_code=400, detail="No valid data columns found in the file.")

        print(f"Data loaded with {len(data)} rows and {len(data.columns)} columns.")
        return data

    except Exception as e:
        print(f"Error during blob data loading: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to load data from Azure Blob: {str(e)}")









