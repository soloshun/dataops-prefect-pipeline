from datetime import datetime, timedelta, timezone

def get_yesterdays_date() -> str:
    yesterday = datetime.now(timezone.utc) - timedelta(days=1)
    return yesterday.strftime("%Y-%m-%d")