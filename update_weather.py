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

# OpenWeather API URLs (Free Plan)
WEATHER_API_URL = "https://api.openweathermap.org/data/2.5/weather?lat={}&lon={}&appid={}&units=metric"
FORECAST_API_URL = "https://api.openweathermap.org/data/2.5/forecast?lat={}&lon={}&appid={}&units=metric"
AIR_POLLUTION_API_URL = "https://api.openweathermap.org/data/2.5/air_pollution?lat={}&lon={}&appid={}"

# Air Quality Index (AQI) categories
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

def fetch_weather(latitude, longitude):
    """Fetch current weather from OpenWeather API."""
    url = WEATHER_API_URL.format(latitude, longitude, OPENWEATHER_API_KEY)

    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            weather_data = {
                "temperature": data["main"]["temp"],
                "humidity": data["main"]["humidity"],
                "wind_speed": data["wind"]["speed"],
                "wind_direction": data["wind"]["deg"],
                "pressure": data["main"]["pressure"],
                "visibility": data.get("visibility"),
                "weather_desc": data["weather"][0]["description"],
                "updated_at": datetime.utcnow().isoformat()
            }
            return weather_data
        else:
            print(f"Failed to fetch weather data for ({latitude}, {longitude}): {response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Error fetching weather data for ({latitude}, {longitude}): {e}")
        return None

def fetch_air_quality(latitude, longitude):
    """Fetch air pollution data from OpenWeather API."""
    url = AIR_POLLUTION_API_URL.format(latitude, longitude, OPENWEATHER_API_KEY)

    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            aqi_value = data["list"][0]["main"]["aqi"]
            return get_aqi_category(aqi_value)
        return None
    except requests.exceptions.RequestException as e:
        print(f"Error fetching air quality for ({latitude}, {longitude}): {e}")
        return None

def update_weather():
    """Fetch districts from Supabase, get weather data, and update the table."""
    districts = supabase.table("districts").select("id, name, latitude, longitude").execute()

    if districts and districts.data:
        for district in districts.data:
            district_id = district["id"]
            city_name = district["name"]
            latitude = district.get("latitude")
            longitude = district.get("longitude")

            if latitude is None or longitude is None:
                print(f"Skipping {city_name} due to missing coordinates.")
                continue

            # Fetch weather data
            weather_data = fetch_weather(latitude, longitude)

            # Fetch air quality separately
            air_pollution = fetch_air_quality(latitude, longitude)

            if weather_data:
                weather_data["air_pollution"] = air_pollution  # Add AQI to weather data
                
                # Upsert into Supabase
                supabase.table("weather").upsert({
                    "district_id": district_id,
                    **weather_data
                }).execute()
                print(f"Updated weather for {city_name}")

            # Add delay to prevent exceeding API limit
            time.sleep(1.1)

if __name__ == "__main__":
    update_weather()
