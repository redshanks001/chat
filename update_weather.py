import requests
import psycopg2
from datetime import datetime
import os

# Supabase credentials from environment variables
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
DATABASE_URL = os.getenv("DATABASE_URL")  # Your Supabase Postgres connection string
OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY")

# OpenWeather API URL template
WEATHER_API_URL = "https://api.openweathermap.org/data/2.5/weather?q={}&appid={}&units=metric"

# Map districts to valid city names for OpenWeather API
district_name_mapping = {
    "Nicobars": "Port Blair,IN",
    "North and Middle Andaman": "Port Blair,IN",
    "South Andaman": "Port Blair,IN"
}

def fetch_weather(city_name):
    """Fetch weather data from OpenWeather API."""
    mapped_name = district_name_mapping.get(city_name, city_name)  # Use mapped name if available
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
    """Fetch all districts and update the weather table."""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()

        # Fetch all districts
        cursor.execute("SELECT id, name FROM districts;")
        districts = cursor.fetchall()

        for district_id, district_name in districts:
            weather_data = fetch_weather(district_name)

            # If weather data is None, insert NULL values
            if weather_data is None:
                cursor.execute("""
                    INSERT INTO weather (district_id, temperature, humidity, wind_speed, wind_direction, pressure, visibility, weather_desc, updated_at)
                    VALUES (%s, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NOW())
                    ON CONFLICT (district_id) DO UPDATE 
                    SET temperature = EXCLUDED.temperature,
                        humidity = EXCLUDED.humidity,
                        wind_speed = EXCLUDED.wind_speed,
                        wind_direction = EXCLUDED.wind_direction,
                        pressure = EXCLUDED.pressure,
                        visibility = EXCLUDED.visibility,
                        weather_desc = EXCLUDED.weather_desc,
                        updated_at = NOW();
                """, (district_id,))
            else:
                cursor.execute("""
                    INSERT INTO weather (district_id, temperature, humidity, wind_speed, wind_direction, pressure, visibility, weather_desc, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (district_id) DO UPDATE 
                    SET temperature = EXCLUDED.temperature,
                        humidity = EXCLUDED.humidity,
                        wind_speed = EXCLUDED.wind_speed,
                        wind_direction = EXCLUDED.wind_direction,
                        pressure = EXCLUDED.pressure,
                        visibility = EXCLUDED.visibility,
                        weather_desc = EXCLUDED.weather_desc,
                        updated_at = EXCLUDED.updated_at;
                """, (district_id, weather_data["temperature"], weather_data["humidity"], 
                      weather_data["wind_speed"], weather_data["wind_direction"], weather_data["pressure"], 
                      weather_data["visibility"], weather_data["weather_desc"], weather_data["updated_at"]))

        conn.commit()
        cursor.close()
        conn.close()
        print("Weather data updated successfully.")

    except Exception as e:
        print(f"Error updating weather table: {e}")

# Run the update function
update_weather_table()
