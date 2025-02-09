import requests
import os
from supabase import create_client
from datetime import datetime
import json

# Load environment variables
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY")

# Create Supabase client
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# OpenWeather API URL templates
WEATHER_API_URL_CITY = "https://api.openweathermap.org/data/2.5/weather?q={}&appid={}&units=metric"
WEATHER_API_URL_COORDS = "https://api.openweathermap.org/data/2.5/weather?lat={}&lon={}&appid={}&units=metric"
AIR_POLLUTION_API_URL = "https://api.openweathermap.org/data/2.5/air_pollution?lat={}&lon={}&appid={}"
ONE_CALL_API_URL = "https://api.openweathermap.org/data/2.5/onecall?lat={}&lon={}&exclude=current,minutely,alerts&appid={}&units=metric"

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

def fetch_weather(city_name=None, latitude=None, longitude=None):
    """Fetch weather, air pollution, and forecast data from OpenWeather API."""
    if latitude is not None and longitude is not None:
        url = WEATHER_API_URL_COORDS.format(latitude, longitude, OPENWEATHER_API_KEY)
    else:
        url = WEATHER_API_URL_CITY.format(city_name, OPENWEATHER_API_KEY)
    
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json()
        weather_data = {
            "temperature": data["main"]["temp"],
            "humidity": data["main"]["humidity"],
            "wind_speed": data["wind"]["speed"],
            "wind_direction": data["wind"]["deg"],
            "pressure": data["main"]["pressure"],
            "visibility": data.get("visibility", None),
            "weather_desc": data["weather"][0]["description"],
            "updated_at": datetime.utcnow().isoformat()
        }
        
        # Fetch air pollution data if coordinates are available
        if latitude is not None and longitude is not None:
            air_pollution_response = requests.get(AIR_POLLUTION_API_URL.format(latitude, longitude, OPENWEATHER_API_KEY))
            if air_pollution_response.status_code == 200:
                air_data = air_pollution_response.json()
                aqi_value = air_data["list"][0]["main"]["aqi"]
                weather_data["air_pollution"] = get_aqi_category(aqi_value)
            else:
                weather_data["air_pollution"] = None

            # Fetch forecast data
            forecast_response = requests.get(ONE_CALL_API_URL.format(latitude, longitude, OPENWEATHER_API_KEY))
            if forecast_response.status_code == 200:
                forecast_data = forecast_response.json()
                weather_data["daily_forecast"] = json.dumps(forecast_data["daily"][:7])  # 7-day forecast
                weather_data["hourly_forecast"] = json.dumps(forecast_data["hourly"][:24])  # 24-hour forecast
            else:
                weather_data["daily_forecast"] = None
                weather_data["hourly_forecast"] = None
        else:
            weather_data["air_pollution"] = None
            weather_data["daily_forecast"] = None
            weather_data["hourly_forecast"] = None
        
        return weather_data
    else:
        print(f"Failed to fetch weather for {city_name or (latitude, longitude)}: {response.status_code}")
        return {
            "temperature": None,
            "humidity": None,
            "wind_speed": None,
            "wind_direction": None,
            "pressure": None,
            "visibility": None,
            "weather_desc": "*",
            "air_pollution": None,
            "daily_forecast": None,
            "hourly_forecast": None,
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
            
            weather_data = fetch_weather(city_name=city_name, latitude=latitude, longitude=longitude)
            
            # Upsert weather data even if fetching fails
            supabase.table("weather").upsert({
                "district_id": district_id,
                **weather_data
            }).execute()
            print(f"Updated weather for {city_name}")

if __name__ == "__main__":
    update_weather()

