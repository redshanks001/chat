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

# OpenWeather One Call API (Single API for Weather & Air Pollution)
ONE_CALL_API_URL = "https://api.openweathermap.org/data/3.0/onecall?lat={}&lon={}&appid={}&units=metric&exclude=minutely"

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

def fetch_weather_and_forecast(latitude, longitude):
    """Fetch current weather, air pollution, hourly & daily forecasts from OpenWeather API."""
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
            "visibility": current.get("visibility", None),
            "weather_desc": current["weather"][0]["description"],
            "updated_at": datetime.utcnow().isoformat()
        }

        # Extract air pollution data
        if "components" in current:
            aqi_value = current["aqi"]
            weather_data["air_pollution"] = get_aqi_category(aqi_value)
        else:
            weather_data["air_pollution"] = None

        # Extract hourly & daily temperature forecast
        weather_data["hourly_forecast"] = [
            {"time": h["dt"], "temperature": h["temp"]} for h in data["hourly"][:24]
        ]
        weather_data["daily_forecast"] = [
            {"date": d["dt"], "temperature": d["temp"]["day"]} for d in data["daily"][:8]
        ]

        return weather_data

    else:
        print(f"Failed to fetch data for ({latitude}, {longitude}): {response.status_code}")
        return None

def update_weather():
    """Fetch districts from Supabase, get weather data, and update the table."""
    districts = supabase.table("districts").select("id, name, latitude, longitude").execute()

    if districts and districts.data:
        for district in districts.data:
            district_id = district["id"]
            latitude = district.get("latitude")
            longitude = district.get("longitude")

            if latitude is None or longitude is None:
                print(f"Skipping {district['name']} due to missing coordinates.")
                continue

            weather_data = fetch_weather_and_forecast(latitude, longitude)
            if weather_data:
                supabase.table("weather").upsert({
                    "district_id": district_id,
                    **weather_data
                }).execute()
                print(f"Updated weather for {district['name']}")

            # Add a delay to avoid exceeding the rate limit (60 RPM)
            time.sleep(1.1)

if __name__ == "__main__":
    update_weather()
