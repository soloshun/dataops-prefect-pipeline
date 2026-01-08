from prefect import task
import pandas as pd

@task
def compute_daily_average(hourly_data: pd.DataFrame) -> pd.DataFrame:
    """
    Compute daily average from hourly data.

    Args:
        hourly_data (pd.DataFrame): DataFrame containing hourly weather data.
    Returns:
        pd.DataFrame: DataFrame containing daily averages with column names.
    """
    summary = hourly_data[[
        'temperature_2m', 
        'precipitation', 
        'relative_humidity_2m', 
        'wind_speed_10m', 
        'wind_gusts_10m', 
        'surface_pressure'
    ]].mean()
    return summary.to_frame().T
