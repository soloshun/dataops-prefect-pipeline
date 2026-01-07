from prefect import task
import pandas as pd

@task
def store_results(
        hourly_data: pd.DataFrame,
        path: str,
        file_name: str,
        date: str,
        file_type: str = "csv"
    ) -> None:
    """
    Store the fetched hourly weather data to a CSV file.
    
    This is the data persistence layer of the pipeline - all data storage
    operations occur here. This keeps data handling logic separate from
    data retrieval logic for clarity and maintainability.

    Args:
    hourly_data (pd.DataFrame): DataFrame containing hourly weather data.
    path (str): Directory path where the file will be stored.
    file_name (str): Base name for the output file.
    date (str): Date string used for naming the output file.
    file_type (str): Type of file to save the data as. Defaults to "csv".
    """
    
    if file_type not in ["csv", "json"]:
        raise ValueError("Unsupported file type. Currently only 'csv' and 'json' are supported.")
    
    # DATA STORAGE CHECKPOINT: Persisting processed data to disk
    if file_type == "json":
        hourly_data.to_json(f'{path}/{file_name}_{date}.json', orient='records', lines=True)
    else:  # Default to CSV
        hourly_data.to_csv(f'{path}/{file_name}_{date}.csv', index=False)