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

# OpenWeather API URL template
WEATHER_API_URL = "https://api.openweathermap.org/data/2.5/weather?q={}&appid={}&units=metric"

def fetch_weather(city_name):
    """Fetch weather data from OpenWeather API."""
    url = WEATHER_API_URL.format(city_name, OPENWEATHER_API_KEY)
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json()
        return {
            "temperature": data["main"]["temp"],
            "humidity": data["main"]["humidity"],
            "wind_speed": data["wind"]["speed"],
            "wind_direction": data["wind"]["deg"],
            "pressure": data["main"]["pressure"],
            "visibility": data.get("visibility", None),
            "weather_desc": data["weather"][0]["description"],
            "updated_at": datetime.utcnow().isoformat()
        }
    else:
        print(f"Failed to fetch weather for {city_name}: {response.status_code}")
        return {
            "temperature": None,
            "humidity": None,
            "wind_speed": None,
            "wind_direction": None,
            "pressure": None,
            "visibility": None,
            "weather_desc": "*",
            "updated_at": datetime.utcnow().isoformat()
        }

def update_weather():
    """Fetch districts from Supabase, get weather data, and update the table."""
    districts = supabase.table("districts").select("id, name").execute()

    if districts:
        for district in districts.data:
            district_id = district["id"]
            city_name = district["name"]
            
            weather_data = fetch_weather(city_name)
            
            # Upsert weather data even if fetching fails
            supabase.table("weather").upsert({
                "district_id": district_id,
                **weather_data
            }).execute()
            print(f"Updated weather for {city_name}")

if __name__ == "__main__":
    update_weather()
