from prefect import flow, schedules

@flow
def daily_london_weather_flow() -> dict:
    """
    Flow to fetch daily weather data for London.
    """
    from src.tasks.fetch_weather import fetch_weather_data
    from src.tasks.compute_daily_average import compute_daily_average
    from src.utils.dates import get_yesterdays_date
    from src.tasks.store_results import store_results
    from src.utils.config import RAW_DATA_PATH, PROCESSED_DATA_PATH

    # Get yesterday's date
    date = get_yesterdays_date()

    # Fetch hourly weather data for London
    hourly_data = fetch_weather_data(date=date)

    # Store hourly raw data
    store_results(
        hourly_data=hourly_data,
        path=RAW_DATA_PATH,
        file_name="london_hourly_weather",
        date=date,
        file_type="csv"
    )

    # Compute daily average temperature and total precipitation
    daily_summary = compute_daily_average(hourly_data)

    # Store processed daily summary data
    store_results(
        hourly_data=daily_summary,
        path=PROCESSED_DATA_PATH,
        file_name="london_daily_weather_summary",
        date=date,
        file_type="json"
    )

    return daily_summary.to_dict()


if __name__ == "__main__":
    import os, sys
    repo_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    if repo_root not in sys.path:
        sys.path.insert(0, repo_root)
    daily_london_weather_flow.deploy(
        name="daily-london-weather-pipeline",
        work_pool_name="london-weather-cloud-pool",
        image="prefecthq/prefect-client:3-latest",
        schedule=schedules.Cron( 
            "0 9 * * *", # create a daily schedule at 9 AM London time
            timezone="Europe/London"
        )
    )