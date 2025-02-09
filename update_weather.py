import requests
import os
from supabase import create_client
from datetime import datetime

# Load environment variables
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY")

# Create Supabase client
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# OpenWeather API URLs
WEATHER_API_URL = "https://api.openweathermap.org/data/2.5/weather?lat={}&lon={}&appid={}&units=metric"
AIR_POLLUTION_API_URL = "https://api.openweathermap.org/data/2.5/air_pollution?lat={}&lon={}&appid={}"
HOURLY_FORECAST_API_URL = "https://api.openweathermap.org/data/2.5/forecast?lat={}&lon={}&appid={}&units=metric"
DAILY_FORECAST_API_URL = "https://api.openweathermap.org/data/2.5/onecall?lat={}&lon={}&appid={}&units=metric&exclude=hourly,minutely"

# Air Quality Index (AQI) categories
AQI_CATEGORIES = {1: "Good", 2: "Fair", 3: "Moderate", 4: "Poor", 5: "Very Poor"}

def get_aqi_category(aqi_value):
    """Map AQI value to air quality category."""
    return AQI_CATEGORIES.get(aqi_value, "Unknown")

def fetch_weather_and_forecast(latitude, longitude):
    """Fetch current weather, hourly forecast, and daily forecast from OpenWeather API."""
    try:
        # Fetch current weather
        weather_response = requests.get(WEATHER_API_URL.format(latitude, longitude, OPENWEATHER_API_KEY))
        hourly_response = requests.get(HOURLY_FORECAST_API_URL.format(latitude, longitude, OPENWEATHER_API_KEY))
        daily_response = requests.get(DAILY_FORECAST_API_URL.format(latitude, longitude, OPENWEATHER_API_KEY))
        air_pollution_response = requests.get(AIR_POLLUTION_API_URL.format(latitude, longitude, OPENWEATHER_API_KEY))

        # Ensure all API requests are successful
        if weather_response.status_code != 200 or hourly_response.status_code != 200 or daily_response.status_code != 200:
            print(f"Failed to fetch data for ({latitude}, {longitude})")
            return None

        weather_data = weather_response.json()
        hourly_data = hourly_response.json()
        daily_data = daily_response.json()

        # Extract current weather details
        weather_details = {
            "temperature": weather_data["main"]["temp"],
            "humidity": weather_data["main"]["humidity"],
            "wind_speed": weather_data["wind"]["speed"],
            "wind_direction": weather_data["wind"]["deg"],
            "pressure": weather_data["main"]["pressure"],
            "visibility": weather_data.get("visibility"),
            "weather_desc": weather_data["weather"][0]["description"],
            "updated_at": datetime.utcnow().isoformat()
        }

        # Extract air pollution data
        if air_pollution_response.status_code == 200:
            air_data = air_pollution_response.json()
            aqi_value = air_data["list"][0]["main"]["aqi"]
            weather_details["air_pollution"] = get_aqi_category(aqi_value)

        # Extract hourly forecast temperatures (next 24 hours)
        weather_details["hourly_forecast"] = {
            f"hour_{i + 1}": hourly_data["list"][i]["main"]["temp"] for i in range(24)
        }

        # Extract daily forecast temperatures (next 8 days)
        weather_details["daily_forecast"] = {
            f"day_{i + 1}": daily_data["daily"][i]["temp"]["day"] for i in range(8)
        }

        return weather_details
    except Exception as e:
        print(f"Error fetching weather data: {e}")
        return None

def update_weather():
    """Fetch districts from Supabase, get weather and forecast data, and update the table."""
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
                supabase.table("weather").upsert({"district_id": district_id, **weather_data}).execute()
                print(f"Updated weather for {district['name']}")

if __name__ == "__main__":
    update_weather()
