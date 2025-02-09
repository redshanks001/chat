import requests
import os
from supabase import create_client
from datetime import datetime
import time

# Load environment variables
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
OPENWEATHER_API_KEY_1 = os.getenv("OPENWEATHER_API_KEY_1")
OPENWEATHER_API_KEY_2 = os.getenv("OPENWEATHER_API_KEY_2")

# Create Supabase client
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# OpenWeather One Call API URL template
ONE_CALL_API_URL = "https://api.openweathermap.org/data/3.0/onecall?lat={}&lon={}&exclude=minutely,alerts&appid={}&units=metric"

# Air Quality Index (AQI) categories mapping
AQI_CATEGORIES = {
    1: "Good",
    2: "Fair",
    3: "Moderate",
    4: "Poor",
    5: "Very Poor"
}

# API key management
api_keys = [OPENWEATHER_API_KEY_1, OPENWEATHER_API_KEY_2]
current_key_index = 0

def get_aqi_category(aqi_value):
    """Map AQI value to air quality category."""
    return AQI_CATEGORIES.get(aqi_value, "Unknown")

def switch_api_key():
    """Switch to the other API key."""
    global current_key_index
    current_key_index = (current_key_index + 1) % len(api_keys)
    print(f"Switched to API key {current_key_index + 1}")

def fetch_weather(latitude, longitude):
    """Fetch weather and air pollution data from OpenWeather One Call API using coordinates."""
    global current_key_index
    
    for _ in range(len(api_keys)):  # Try all available keys
        url = ONE_CALL_API_URL.format(latitude, longitude, api_keys[current_key_index])
        
        response = requests.get(url)
        
        if response.status_code == 200:
            data = response.json()
            current = data['current']
            weather_data = {
                "temperature": current['temp'],
                "humidity": current['humidity'],
                "wind_speed": current['wind_speed'],
                "wind_direction": current['wind_deg'],
                "pressure": current['pressure'],
                "visibility": current.get('visibility'),
                "weather_desc": current['weather'][0]['description'],
                "air_pollution": get_aqi_category(current.get('air_quality', {}).get('aqi', 0)),
                "updated_at": datetime.utcnow().isoformat(),
                "daily_forecast": data['daily'],
                "hourly_forecast": data['hourly'][:24]  # First 24 hours
            }
            return weather_data
        elif response.status_code == 429:  # Too Many Requests
            print(f"Rate limit reached for API key {current_key_index + 1}. Switching keys.")
            switch_api_key()
            time.sleep(1)  # Wait a bit before trying again
        else:
            print(f"Failed to fetch weather for coordinates ({latitude}, {longitude}): {response.status_code}")
            switch_api_key()
    
    # If all keys fail, return null data
    print(f"All API keys failed for coordinates ({latitude}, {longitude})")
    return {
        "temperature": None,
        "humidity": None,
        "wind_speed": None,
        "wind_direction": None,
        "pressure": None,
        "visibility": None,
        "weather_desc": "*",
        "air_pollution": None,
        "updated_at": datetime.utcnow().isoformat(),
        "daily_forecast": None,
        "hourly_forecast": None
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
            
            if latitude is not None and longitude is not None:
                weather_data = fetch_weather(latitude, longitude)
                
                # Upsert weather data even if fetching fails
                supabase.table("weather").upsert({
                    "district_id": district_id,
                    **weather_data
                }).execute()
                print(f"Updated weather for {city_name}")
            else:
                print(f"Skipping {city_name} due to missing coordinates")

if __name__ == "__main__":
    update_weather()

