import pandas as pd
from datetime import datetime

#Sample data copied from https://www.weatherapi.com/
json_sample_data = {
  "location": {
    "name": "London",
    "region": "City of London, Greater London",
    "country": "United Kingdom",
    "lat": 51.52,
    "lon": -0.11,
    "tz_id": "Europe/London",
    "localtime_epoch": 1572750389,
    "localtime": "2019-11-03 3:06"
  },
  "forecast": {
    "forecastday": [
      {
        "date": "2018-01-31",
        "date_epoch": 1517356800,
        "day": {
          "maxtemp_c": 9.7,
          "maxtemp_f": 49.5,
          "mintemp_c": 4.5,
          "mintemp_f": 40,
          "avgtemp_c": 7.8,
          "avgtemp_f": 46,
          "maxwind_mph": 17.7,
          "maxwind_kph": 28.4,
          "totalprecip_mm": 0.22,
          "totalprecip_in": 0.01,
          "avgvis_km": 9,
          "avgvis_miles": 5,
          "avghumidity": 69,
          "condition": {
            "text": "Sunny",
            "icon": "//cdn.weatherapi.com/weather/64x64/day/113.png",
            "code": 1000
          },
          "uv": 0
        },
        "astro": {
          "sunrise": "07:41 AM",
          "sunset": "04:48 PM",
          "moonrise": "04:55 PM",
          "moonset": "07:33 AM",
          "moon_phase": "Waxing Gibbous",
          "moon_illumination": "97"
        },
        "hour": [
          {
            "time_epoch": 1517356800,
            "time": "2018-01-31 00:00",
            "temp_c": 9.6,
            "temp_f": 49.3,
            "is_day": 0,
            "condition": {
              "text": "Overcast",
              "icon": "//cdn.weatherapi.com/weather/64x64/night/122.png",
              "code": 1009
            },
            "wind_mph": 14.3,
            "wind_kph": 23,
            "wind_degree": 231,
            "wind_dir": "SW",
            "pressure_mb": 1013,
            "pressure_in": 30.4,
            "precip_mm": 0,
            "precip_in": 0,
            "humidity": 90,
            "cloud": 100,
            "feelslike_c": 6.6,
            "feelslike_f": 43.9,
            "windchill_c": 6.6,
            "windchill_f": 43.9,
            "heatindex_c": 9.6,
            "heatindex_f": 49.3,
            "dewpoint_c": 8,
            "dewpoint_f": 46.4,
            "will_it_rain": 0,
            "chance_of_rain": "0",
            "will_it_snow": 0,
            "chance_of_snow": "0",
            "vis_km": 10,
            "vis_miles": 6
          },
          {
            "time_epoch": 1517360400,
            "time": "2018-01-31 01:00",
            "temp_c": 9.7,
            "temp_f": 49.4,
            "is_day": 0,
            "condition": {
              "text": "Overcast",
              "icon": "//cdn.weatherapi.com/weather/64x64/night/122.png",
              "code": 1009
            },
            "wind_mph": 15.2,
            "wind_kph": 24.5,
            "wind_degree": 234,
            "wind_dir": "SW",
            "pressure_mb": 1012,
            "pressure_in": 30.4,
            "precip_mm": 0,
            "precip_in": 0,
            "humidity": 86,
            "cloud": 100,
            "feelslike_c": 6.5,
            "feelslike_f": 43.7,
            "windchill_c": 6.5,
            "windchill_f": 43.7,
            "heatindex_c": 9.6,
            "heatindex_f": 49.3,
            "dewpoint_c": 7.4,
            "dewpoint_f": 45.2,
            "will_it_rain": 0,
            "chance_of_rain": "3",
            "will_it_snow": 0,
            "chance_of_snow": "0",
            "vis_km": 10,
            "vis_miles": 6
          }
        ]
      }
    ]
  }
  }

class WeatherForecastTransformer:
    def __init__(self, weather_json):
        self.weather_json = weather_json

    def transform_to_dataframe(self):
        # Extracting location name
        location_name = self.weather_json['location']['name']

        # Extracting capture date
        capture_date = datetime.fromtimestamp(self.weather_json['location']['localtime_epoch']).strftime('%Y-%m-%d %H:%M:%S')

        # List to store each row of data
        data_rows = []

        # Iterating through each forecast day
        for forecast_day in self.weather_json['forecast']['forecastday']:
            for hour_data in forecast_day['hour']:
                # Mapping to the columns
                row = {
                    'capture_date': capture_date,
                    'forecast_dateunix': hour_data['time_epoch'],
                    'forecast_datetime': hour_data['time'],
                    'name': location_name,
                    'rain_category': 'rain' if hour_data['will_it_rain'] else 'no rain',
                    'rain_category_value': hour_data['chance_of_rain'],
                    'temp': hour_data['temp_c'],
                    'temp_min': forecast_day['day']['mintemp_c'],
                    'temp_max': forecast_day['day']['maxtemp_c'],
                    'temp_humidity': hour_data['humidity'],
                    'weather_description': hour_data['condition']['text']
                }
                data_rows.append(row)

        # Creating DataFrame
        return pd.DataFrame(data_rows)

# Usage
historic_weather_json = json_sample_data
transformer = WeatherForecastTransformer(historic_weather_json)
dataframe = transformer.transform_to_dataframe()
