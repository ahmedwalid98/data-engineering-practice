import requests
import pandas as pd
from bs4 import BeautifulSoup
import os
from io import BytesIO

URL = 'https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/'


def request_url(url):
    response = requests.get(url)
    return response


def parse_response(response, el):
    content = BeautifulSoup(response, 'html.parser')
    return content.find_all(el)


def get_max_temp(content):
    csv_data = BytesIO(content)
    df = pd.read_csv(csv_data)
    temperature_data = pd.to_numeric(
        df["HourlyDryBulbTemperature"], errors="coerce"
    )

    # Find the row with the maximum temperature
    max_temperature_row = df.loc[temperature_data.idxmax()]

    # Print the row with the maximum temperature
    print(max_temperature_row)


def main():
    body = request_url(URL)
    elements = parse_response(body.text, 'tr')
    mapped_elements = map(lambda el: [el.find_all(
        'td')[0].a.text, el.find_all(
        'td')[1].text], elements[3:-1])
    filtered_element = filter(
        lambda el: el[1] == '2024-01-19 15:43', list(mapped_elements))
    for el in list(filtered_element):
        url = URL + el[0]
        response = request_url(url)
        get_max_temp(response.content)


if __name__ == "__main__":
    main()
