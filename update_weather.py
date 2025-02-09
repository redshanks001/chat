import requests
import os
import time
from supabase import create_client
from datetime import datetime

# Load environment variables
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY")

# Create Supabase client
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# OpenWeather One Call API (Only Free Version 2.5 is Used)
ONE_CALL_API_URL = "https://api.openweathermap.org/data/2.5/onecall?lat={}&lon={}&appid={}&units=metric&exclude=minutely"

# OpenWeather Air Pollution API (Separate Call for AQI)
AIR_POLLUTION_API_URL = "https://api.openweathermap.org/data/2.5/air_pollution?lat={}&lon={}&appid={}"

# Air Quality Index (AQI) categories mapping
AQI_CATEGORIES = {
    1: "Good",
    2: "Fair",
    3: "Moderate",
    4: "Poor",
    5: "Very Poor"
}

def get_aqi_category(aqi_value):
    """Map AQI value to air quality category."""
    return AQI_CATEGORIES.get(aqi_value, "Unknown")

def unix_to_datetime(unix_timestamp):
    """Convert Unix timestamp to readable date format."""
    return datetime.utcfromtimestamp(unix_timestamp).strftime('%Y-%m-%d %H:%M:%S')

def fetch_weather_and_forecast(latitude, longitude):
    """Fetch current weather, hourly & daily forecasts from OpenWeather API."""
    url = ONE_CALL_API_URL.format(latitude, longitude, OPENWEATHER_API_KEY)
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()

        # Extract current weather data
        current = data["current"]
        weather_data = {
            "temperature": current["temp"],
            "humidity": current["humidity"],
            "wind_speed": current["wind_speed"],
            "wind_direction": current["wind_deg"],
            "pressure": current["pressure"],
            "visibility": current.get("visibility"),
            "weather_desc": current["weather"][0]["description"],
            "updated_at": datetime.utcnow().isoformat()
        }

        # Extract hourly & daily temperature forecast
        weather_data["hourly_forecast"] = [
            {"time": unix_to_datetime(h["dt"]), "temperature": h["temp"]} for h in data["hourly"][:24]
        ]
        weather_data["daily_forecast"] = [
            {"date": unix_to_datetime(d["dt"]), "temperature": d["temp"]["day"]} for d in data["daily"][:8]
        ]

        return weather_data

    else:
        print(f"Failed to fetch weather data for ({latitude}, {longitude}): {response.status_code}")
        return None

def fetch_air_quality(latitude, longitude):
    """Fetch air pollution data from OpenWeather API."""
    url = AIR_POLLUTION_API_URL.format(latitude, longitude, OPENWEATHER_API_KEY)
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        aqi_value = data["list"][0]["main"]["aqi"]
        return get_aqi_category(aqi_value)
    
    print(f"Failed to fetch air quality for ({latitude}, {longitude}): {response.status_code}")
    return None

def update_weather():
    """Fetch districts from Supabase, get weather data, and update the table."""
    districts = supabase.table("districts").select("id, name, latitude, longitude").execute()

    if districts and districts.data:
        request_count = 0  # Track number of requests in the last minute
        start_time = time.time()  # Track when the minute started

        for district in districts.data:
            district_id = district["id"]
            city_name = district["name"]
            latitude = district.get("latitude")
            longitude = district.get("longitude")

            if latitude is None or longitude is None:
                print(f"Skipping {city_name} due to missing coordinates.")
                continue

            # Fetch weather and forecast data
            weather_data = fetch_weather_and_forecast(latitude, longitude)
            request_count += 1  # Increment request count

            # Fetch air quality separately
            air_pollution = fetch_air_quality(latitude, longitude)
            request_count += 1  # Increment request count

            if weather_data:
                weather_data["air_pollution"] = air_pollution  # Add AQI to weather data
                
                # Upsert into Supabase
                supabase.table("weather").upsert({
                    "district_id": district_id,
                    **weather_data
                }).execute()
                print(f"Updated weather for {city_name}")

            # Ensure we stay under 60 requests per minute
            elapsed_time = time.time() - start_time  # Time elapsed since start of minute
            if request_count >= 58:  # Leave some buffer for safety
                sleep_time = 60 - elapsed_time
                if sleep_time > 0:
                    print(f"Rate limit reached. Sleeping for {sleep_time:.2f} seconds...")
                    time.sleep(sleep_time)
                request_count = 0  # Reset request count
                start_time = time.time()  # Restart time tracking

if __name__ == "__main__":
    update_weather()
