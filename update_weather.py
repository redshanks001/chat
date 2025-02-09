import requests
import os
from supabase import create_client
from datetime import datetime, timedelta
import time

# Load environment variables
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY")

# Create Supabase client
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# OpenWeather One Call API URL template
ONE_CALL_API_URL = "https://api.openweathermap.org/data/3.0/onecall?lat={}&lon={}&exclude=minutely,alerts&appid={}&units=metric"

# Air Quality Index (AQI) categories mapping
AQI_CATEGORIES = {
    1: "Good",
    2: "Fair",
    3: "Moderate",
    4: "Poor",
    5: "Very Poor"
}

# Rate limiting
RATE_LIMIT = 60  # requests per minute
request_timestamps = []

def get_aqi_category(aqi_value):
    """Map AQI value to air quality category."""
    return AQI_CATEGORIES.get(aqi_value, "Unknown")

def wait_for_rate_limit():
    """Wait if we're approaching the rate limit."""
    global request_timestamps
    now = datetime.now()
    minute_ago = now - timedelta(minutes=1)
    
    # Remove timestamps older than 1 minute
    request_timestamps = [ts for ts in request_timestamps if ts > minute_ago]
    
    # If we're at the rate limit, wait until we're under it
    while len(request_timestamps) >= RATE_LIMIT:
        time.sleep(1)
        now = datetime.now()
        minute_ago = now - timedelta(minutes=1)
        request_timestamps = [ts for ts in request_timestamps if ts > minute_ago]

def fetch_weather(latitude, longitude):
    """Fetch weather and air pollution data from OpenWeather One Call API using coordinates."""
    wait_for_rate_limit()
    
    url = ONE_CALL_API_URL.format(latitude, longitude, OPENWEATHER_API_KEY)
    
    response = requests.get(url)
    request_timestamps.append(datetime.now())
    
    if response.status_code == 200:
        data = response.json()
        current = data['current']
        weather_data = {
            "temperature": current['temp'],
            "humidity": current['humidity'],
            "wind_speed": current['wind_speed'],
            "wind_direction": current['wind_deg'],
            "pressure": current['pressure'],
            "visibility": current.get('visibility'),
            "weather_desc": current['weather'][0]['description'],
            "air_pollution": get_aqi_category(current.get('air_quality', {}).get('aqi', 0)),
            "updated_at": datetime.utcnow().isoformat()
        }
        return weather_data
    else:
        print(f"Failed to fetch weather for coordinates ({latitude}, {longitude}): {response.status_code}")
        return {
            "temperature": None,
            "humidity": None,
            "wind_speed": None,
            "wind_direction": None,
            "pressure": None,
            "visibility": None,
            "weather_desc": "*",
            "air_pollution": None,
            "updated_at": datetime.utcnow().isoformat()
        }

def update_weather():
    """Fetch districts from Supabase, get weather data, and update the table."""
    districts = supabase.table("districts").select("id, name, latitude, longitude").execute()

    if districts:
        for district in districts.data:
            district_id = district["id"]
            city_name = district["name"]
            latitude = district.get("latitude")
            longitude = district.get("longitude")
            
            if latitude is not None and longitude is not None:
                weather_data = fetch_weather(latitude, longitude)
                
                # Upsert weather data even if fetching fails
                supabase.table("weather").upsert({
                    "district_id": district_id,
                    **weather_data
                }).execute()
                print(f"Updated weather for {city_name}")
            else:
                print(f"Skipping {city_name} due to missing coordinates")

if __name__ == "__main__":
    update_weather()
