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

# OpenWeather API URL templates
WEATHER_API_URL_COORDS = "https://api.openweathermap.org/data/2.5/weather?lat={}&lon={}&appid={}&units=metric"
FORECAST_API_URL = "https://api.openweathermap.org/data/2.5/forecast?lat={}&lon={}&appid={}&units=metric"
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

def fetch_weather_and_forecast(latitude, longitude):
    """Fetch current weather, hourly forecast, and daily forecast from OpenWeather API."""
    url_weather = WEATHER_API_URL_COORDS.format(latitude, longitude, OPENWEATHER_API_KEY)
    url_forecast = FORECAST_API_URL.format(latitude, longitude, OPENWEATHER_API_KEY)
   
    weather_data = {}
   
    # Fetch current weather
    weather_response = requests.get(url_weather)
    if weather_response.status_code == 200:
        data = weather_response.json()
        weather_data.update({
            "temperature": data["main"]["temp"],
            "humidity": data["main"]["humidity"],
            "wind_speed": data["wind"]["speed"],
            "wind_direction": data["wind"]["deg"],
            "pressure": data["main"]["pressure"],
            "visibility": data.get("visibility"),
            "weather_desc": data["weather"][0]["description"],
            "updated_at": datetime.utcnow().isoformat()
        })
   
    # Fetch hourly and daily forecasts
    forecast_response = requests.get(url_forecast)
    if forecast_response.status_code == 200:
        forecast_data = forecast_response.json()["list"]
       
        # Extract hourly forecast for the next 24 hours
        hourly_forecast = [
            {"time": item["dt_txt"], "temperature": item["main"]["temp"]}
            for item in forecast_data[:8]  # Next 24 hours (3-hour intervals)
        ]
       
        # Extract daily forecast for the next 5 days
        daily_forecast = {}
        for item in forecast_data:
            date = item["dt_txt"].split(" ")[0]
            if date not in daily_forecast:
                daily_forecast[date] = []
            daily_forecast[date].append(item["main"]["temp"])
       
        daily_forecast_avg = [
            {"date": date, "temperature": sum(temps) / len(temps)}
            for date, temps in list(daily_forecast.items())[:5]
        ]
       
        weather_data["hourly_forecast"] = hourly_forecast
        weather_data["daily_forecast"] = daily_forecast_avg
   
    return weather_data

def fetch_air_quality(latitude, longitude):
    """Fetch air pollution data from OpenWeather API."""
    url = AIR_POLLUTION_API_URL.format(latitude, longitude, OPENWEATHER_API_KEY)
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

if __name__ == "__main__":
    update_weather()
