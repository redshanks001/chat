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

# OpenWeather API URL templates
WEATHER_API_URL_COORDS = "https://api.openweathermap.org/data/2.5/weather?lat={}&lon={}&appid={}&units=metric"
AIR_POLLUTION_API_URL = "https://api.openweathermap.org/data/2.5/air_pollution?lat={}&lon={}&appid={}"
ONE_CALL_API_URL = "https://api.openweathermap.org/data/2.5/onecall?lat={}&lon={}&appid={}&units=metric&exclude=minutely"

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
    """Fetch current weather, hourly forecast, and daily forecast from OpenWeather API."""
    url = ONE_CALL_API_URL.format(latitude, longitude, OPENWEATHER_API_KEY)

    try:
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
                {"date": unix_to_datetime(d["dt"]), "temperature": d["temp"]["day"]} for d in data["daily"][:7]
            ]

            return weather_data

        else:
            print(f"Failed to fetch weather data for ({latitude}, {longitude}): {response.status_code}")
            print("Response:", response.text)  # Debugging
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

        print(f"Failed to fetch air quality for ({latitude}, {longitude}): {response.status_code}")
        print("Response:", response.text)  # Debugging
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

            # Fetch weather and forecast data
            weather_data = fetch_weather_and_forecast(latitude, longitude)

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

            # Add delay to prevent exceeding OpenWeather API limit (60 requests per minute)
            time.sleep(1.1)

if __name__ == "__main__":
    update_weather()
