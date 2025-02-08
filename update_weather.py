import requests
from datetime import datetime
import os
from supabase import create_client, Client

# Supabase credentials
SUPABASE_URL = os.getenv("SUPABASE_URL")  # Your Supabase Project URL
SUPABASE_ANON_KEY = os.getenv("SUPABASE_ANON_KEY")  # Your Supabase Anon Key

# Create a Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)

# OpenWeather API details
OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY")
WEATHER_API_URL = "https://api.openweathermap.org/data/2.5/weather?q={}&appid={}&units=metric"

# District mappings for unrecognized names
district_name_mapping = {
    "Nicobars": "Port Blair,IN",
    "North and Middle Andaman": "Port Blair,IN",
    "South Andaman": "Port Blair,IN"
}

def fetch_weather(city_name):
    """Fetch weather data from OpenWeather API."""
    mapped_name = district_name_mapping.get(city_name, city_name)
    url = WEATHER_API_URL.format(mapped_name, OPENWEATHER_API_KEY)
    
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
        return None  # No data, keep fields empty

def update_weather_table():
    """Fetch all districts from Supabase and update the weather table."""
    try:
        # Fetch all districts
        response = supabase.table("districts").select("id, name").execute()
        districts = response.data

        for district in districts:
            district_id = district["id"]
            district_name = district["name"]
            weather_data = fetch_weather(district_name)

            if weather_data is None:
                # Insert NULL values if no weather data is found
                supabase.table("weather").upsert({
                    "district_id": district_id,
                    "temperature": None,
                    "humidity": None,
                    "wind_speed": None,
                    "wind_direction": None,
                    "pressure": None,
                    "visibility": None,
                    "weather_desc": None,
                    "updated_at": datetime.utcnow().isoformat()
                }).execute()
            else:
                # Insert real weather data
                supabase.table("weather").upsert({
                    "district_id": district_id,
                    "temperature": weather_data["temperature"],
                    "humidity": weather_data["humidity"],
                    "wind_speed": weather_data["wind_speed"],
                    "wind_direction": weather_data["wind_direction"],
                    "pressure": weather_data["pressure"],
                    "visibility": weather_data["visibility"],
                    "weather_desc": weather_data["weather_desc"],
                    "updated_at": weather_data["updated_at"]
                }).execute()

        print("Weather data updated successfully.")

    except Exception as e:
        print(f"Error updating weather table: {e}")

# Run the update function
update_weather_table()
