import requests
import os
from supabase import create_client
from datetime import datetime, timedelta

# Load environment variables
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY")

# Create Supabase client
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# OpenWeather One Call API URL
ONE_CALL_API_URL = "https://api.openweathermap.org/data/2.5/onecall?lat={}&lon={}&appid={}&units=metric"

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
    """Fetch current weather, hourly forecast, and daily forecast from OpenWeather One Call API."""
    url = ONE_CALL_API_URL.format(latitude, longitude, OPENWEATHER_API_KEY)
    
    weather_data = {}
    
    # Fetch weather data
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        
        # Current weather
        current = data["current"]
        weather_data.update({
            "temperature": current["temp"],
            "humidity": current["humidity"],
            "wind_speed": current["wind_speed"],
            "wind_direction": current["wind_deg"],
            "pressure": current["pressure"],
            "visibility": current.get("visibility"),
            "weather_desc": current["weather"][0]["description"],
            "updated_at": datetime.utcnow().isoformat()
        })
        
        # Hourly forecast for the next 24 hours
        hourly_forecast = [
            {"time": datetime.utcfromtimestamp(item["dt"]).isoformat(), "temperature": item["temp"]}
            for item in data["hourly"][:8]  # Next 24 hours (3-hour intervals)
        ]
        
        # Daily forecast for the next 5 days
        daily_forecast = [
            {"date": datetime.utcfromtimestamp(item["dt"]).date().isoformat(), 
             "min_temp": item["temp"]["min"], "max_temp": item["temp"]["max"]}
            for item in data["daily"][:5]
        ]
        
        weather_data["hourly_forecast"] = hourly_forecast
        weather_data["daily_forecast"] = daily_forecast
    
    return weather_data

def fetch_air_quality(latitude, longitude):
    """Fetch air pollution data from OpenWeather API."""
    url = f"https://api.openweathermap.org/data/2.5/air_pollution?lat={latitude}&lon={longitude}&appid={OPENWEATHER_API_KEY}"
    response = requests.get(url)
    if response.status_code == 200:
        air_data = response.json()
        aqi_value = air_data["list"][0]["main"]["aqi"]
        return get_aqi_category(aqi_value)
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
            
            # Fetch weather and forecast data using One Call API
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

if __name__ == "__main__":
    update_weather()
