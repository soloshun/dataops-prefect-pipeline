from prefect import flow, schedules

if __name__ == "__main__":
    flow.from_source(
        source="https://github.com/soloshun/dataops-prefect-pipeline.git",
        entrypoint="src/dataops_prefect_pipeline/flows/daily_weather_flow.py:daily_london_weather_flow",
    ).deploy(
        name="prefect-pipeline-github-deployment",
        work_pool_name="london-weather-cloud-pool",
        image="prefecthq/prefect-client:3-latest",
        # build=True,
        schedule=schedules.Cron( 
            "30 8 * * *", # create a daily schedule at 8:30 AM London time
            # "0 9 * * *", # create a daily schedule at 9 AM London time
            timezone="Europe/London"
        ),
        job_variables={
            "pip_packages": [
                "openmeteo-requests",
                "requests-cache",
                "retry-requests",
                "numpy",
                "pandas"
            ]
        }
    )