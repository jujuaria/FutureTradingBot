import databento as db
import pandas as pd
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pytz
import os
from dotenv import load_dotenv
load_dotenv(".env")
api_key = os.getenv('DATABENTO_API_KEY')
print(api_key)
# Initialize the historical client with your API key.
client = db.Historical(key=api_key)

# Get current time in US/Eastern timezone.
eastern = pytz.timezone("US/Eastern")
now_est = datetime.now(eastern)

# Define yesterday's end in Eastern Time.
# For example, if now is 2025-03-06 09:37:00 EST, then yesterday's end is 2025-03-05 23:59:59 EST.
yesterday_est = now_est - timedelta(days=1)
yesterday_end_est = yesterday_est.replace(hour=23, minute=59, second=59, microsecond=0)

# Convert yesterday's end time to UTC.
end_dt = yesterday_end_est.astimezone(pytz.utc)

# Define the start date as 6 months before the end date.
start_dt = end_dt - relativedelta(months=1)

# Convert datetime objects to ISO format strings.
start_date = start_dt.isoformat()
end_date = end_dt.isoformat()

print("Start Date (UTC):", start_date)
print("End Date (UTC):", end_date)

# Define your data request parameters.
dataset = "GLBX.MDP3"
symbols = ["ES.C.0"]
schema = "ohlcv-1m"
stype_in = "continuous"

# Request the data for the computed date range.
data = client.timeseries.get_range(
    dataset=dataset,
    symbols=symbols,
    start=start_date,
    end=end_date,
    schema=schema,
    stype_in=stype_in,
)

# Convert the data to a Pandas DataFrame.
df = data.to_df()
print(df.head())