import os
import sys

# Ensure 'src' is in the path so we can import modules from dataops_prefect_pipeline
# this is needed because we are not using 'pip install .' in the remote environment
repo_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

from prefect import flow, get_run_logger

logger = get_run_logger()

@flow
def daily_london_weather_flow() -> dict:
    """
    Flow to fetch daily weather data for London.
    """
    from dataops_prefect_pipeline.tasks.fetch_weather import fetch_weather_data
    from dataops_prefect_pipeline.tasks.compute_daily_average import compute_daily_average
    from dataops_prefect_pipeline.utils.dates import get_yesterdays_date
    from dataops_prefect_pipeline.tasks.store_results import store_results
    from dataops_prefect_pipeline.utils.config import (
        RAW_DATA_PATH, 
        PROCESSED_DATA_PATH
    )

    # Get yesterday's date
    date = get_yesterdays_date()

    # Fetch hourly weather data for London
    logger.info(f"Fetching hourly weather data for London on {date}...")
    hourly_data = fetch_weather_data(date=date)
    logger.info("Hourly weather data fetched.")

    # Store hourly raw data
    logger.info("Storing raw hourly data... to", RAW_DATA_PATH)
    store_results (
        hourly_data=hourly_data,
        path=RAW_DATA_PATH,
        file_name="london_hourly_weather",
        date=date,
        file_type="csv"
    )
    logger.info("Raw hourly data stored.")

    # Compute daily average temperature and total precipitation
    logger.info("Calculating daily summary...")
    daily_summary = compute_daily_average(hourly_data)
    logger.info("Daily summary calculated.")

    # Store processed daily summary data
    logger.info("Storing daily summary data... to", PROCESSED_DATA_PATH)
    store_results (
        hourly_data=daily_summary,
        path=PROCESSED_DATA_PATH,
        file_name="london_daily_weather_summary",
        date=date,
        file_type="json"
    )
    logger.info(f"Daily summary data stored at {PROCESSED_DATA_PATH}")

    logger.info(f"Daily London weather summary for {date}: {daily_summary.to_dict()}")
    return daily_summary.to_dict()


# uncomment this code run `python daily_weather_flow.py` for local to could deployment
"""
if __name__ == "__main__":
    daily_london_weather_flow.deploy(
        name="daily-london-weather-pipeline",
        work_pool_name="london-weather-cloud-pool",
        image="prefecthq/prefect-client:3-latest",
        schedule=schedules.Cron( 
            "0 9 * * *", # create a daily schedule at 9 AM London time
            timezone="Europe/London"
        )
    )
"""