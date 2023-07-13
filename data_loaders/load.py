import io
import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """
    base_url = "https://earthquake.usgs.gov/fdsnws/event/1/query?"
    start_year = 2018
    end_year = 2024

    data_list = []

    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            start_date = f"{year}-{month:02d}-01"
            end_date = f"{year}-{month:02d}-31"
            params = {
                "format": "geojson",
                "starttime": start_date,
                "endtime": end_date,
                "limit": 20000  # Limitamos la cantidad de eventos por solicitud
            }

            response = requests.get(base_url, params=params)
            if response.status_code == 200:
                data = response.json()

                for feature in data["features"]:
                    properties = feature["properties"]
                    magnitude = properties["mag"]
                    place = properties["place"]
                    date_time = pd.to_datetime(properties["time"], unit='ms')
                    mag_type = properties["magType"]
                    event_type = properties["type"]
                    latitude = feature["geometry"]["coordinates"][1]
                    longitude = feature["geometry"]["coordinates"][0]
                    depth = feature["geometry"]["coordinates"][2]
                    event_id = feature["id"]

                    data_list.append([magnitude, place, date_time, mag_type, event_type, latitude, longitude, depth, event_id])
            else:
                print("Error en la solicitud:", response.status_code)

    df = pd.DataFrame(data_list, columns=["Magnitud", "Lugar", "Fecha", "Tipo de Magnitud", "Tipo de Evento", "Latitud", "Longitud", "Profundidad", "ID"])

    return df
    
@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
