import csv
from pathlib import Path

def check_for_required_files(dir_path: str, file_name: str, var_names):
    """Verify the existance of the csv files used to store the api results. If the csv
    files are not found in the directory provided in the ``dir_path`` param, create
    the file with a header definition as described in the ``var_names`` param.

    :param str dir_path: The directory where the weather data is stored or where the desired
    location of the data will be.

    :param str file_name: The name of the file.

    :param list var_names: This is a list containing the columns names expected to be returned
    by the api. The column ordering should be the same.
    """
    file_path = dir_path+'/'+file_name
    if Path(file_path).exists() is False:
        with open(file_path,'w') as f:
           csvWriter = csv.DictWriter(f, fieldnames = var_names)
           csvWriter.writeheader()
           f.close()

def _get_master_str(self, granularity: str):
    json_str = {"daily": [{"time": [],
                            "summary": [],
                            "icon": [],
                            "sunriseTime": [],
                            "sunsetTime": [],
                            "moonPhase": [],
                            "precipIntensity": [],
                            "precipIntensityMax": [],
                            "precipIntensityMaxTime": [],
                            "precipProbability": [],
                            "precipType": [],
                            "precipAccumulation": [],
                            "temperatureHigh": [],
                            "temperatureHighTime": [],
                            "temperatureLow": [],
                            "temperatureLowTime": [],
                            "apparentTemperatureHigh": [],
                            "apparentTemperatureHighTime": [],
                            "apparentTemperatureLow": [],
                            "apparentTemperatureLowTime": [],
                            "dewPoint": [],
                            "humidity": [],
                            "windSpeed": [],
                            "windGust": [],
                            "windGustTime": [],
                            "windBearing": [],
                            "cloudCover": [],
                            "uvIndex": [],
                            "uvIndexTime": [],
                            "visibility": [],
                            "temperatureMin": [],
                            "temperatureMinTime": [],
                            "temperatureMax": [],
                            "temperatureMaxTime": [],
                            "apparentTemperatureMin": [],
                            "apparentTemperatureMinTime": [],
                            "apparentTemperatureMax": [],
                            "apparentTemperatureMaxTime": [],
                            "pressure": [],
                            "ozone": []}],
            "hourly": [{"time": [],
                            "summary": [],
                            "icon": [],
                            "precipIntensity": [],
                            "precipProbability": [],
                            "precipType": [],
                            "precipAccumulation": [],
                            "temperature": [],
                            "apparentTemperature": [],
                            "dewPoint": [],
                            "humidity": [],
                            "pressure": [],
                            "windSpeed": [],
                            "windGust": [],
                            "windBearing": [],
                            "cloudCover": [],
                            "uvIndex": [],
                            "visibility": [],
                            "ozone": []}]}
    master_dict = json_str[granularity][0]
    return pandas.DataFrame.from_dict(master_dict)