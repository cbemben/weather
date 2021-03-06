import requests
import pandas
import time
import itertools
from datetime import datetime, timedelta
import json
import os
import logging
import csv
from pathlib import Path, PurePath
from thunderclap.utils import check_for_required_files, get_darksky_api_structure

class WeatherApiConnect:
    """ This class is the connector to the api. It also acts as the file 
    handler at this time. The api calls return daily and hourly climate data and store
    the data in seperate csv files.

    :param str startdate: the first day climate data should be collected.

    :param str enddate: the last day climate data should be collected.

    :param dict latlong: dictionary of specific latlongs as the key and a descriptive value
    such as zipcode, block group or other user-defined identifier.

    :param str darksky_token: your darksky token

    :param str data_dir: the location where this process will store the returned data.
    """
    def __init__(self, startdate: str, enddate: str, latlong: dict, darksky_token: str,
                 data_dir: str):
        self._startdate = startdate
        self._enddate = enddate
        self._latlong = latlong
        self._darksky_token = darksky_token
        self._df_hourly = pandas.DataFrame()
        self._df_daily = pandas.DataFrame()
        self._raw_response = None
        self._data_dir = data_dir
        self._app_start_time = None
        self._logger = logging.getLogger(__name__)
        self._missing_daily_rng = None

    def run_daily_refresh(self):
        self._app_start_time = pandas.Timestamp.now()
        self.get_csv_files_for_appending()
        self.delete_last_two_weeks_of_records()
        self.override_config_values_for_daily_refresh()
        self.get_historical_data()
        self.get_forecasted_data()
        self.write_data_to_file()
        self._logger.info('%s ~ %s', (len(self._df_daily) + len(self._df_hourly)),
           (pandas.Timestamp.now()-self._app_start_time).seconds/60)

    def get_historical_data(self):
        all_locs = self.get_list_of_locations()
        all_dates = list(self.get_list_of_dates(self._startdate, self._enddate))
        #get historical
        for latlong, date in itertools.product(all_locs, all_dates):
            date = date.isoformat()
            self._raw_response = self.api_retrieve(latlong, date)
            self.append_api_results('hourly', self._raw_response, latlong)
            self.append_api_results('daily', self._raw_response, latlong)

    def get_forecasted_data(self):
        all_locs = self.get_list_of_locations()
        #get available forecast/future data by omitting dates
        for latlong in all_locs:
            self._raw_response = self.api_retrieve(latlong, '')
            self.append_api_results('daily', self._raw_response, latlong)
            self.append_api_results('hourly', self._raw_response, latlong)

    def get_list_of_dates(self, startdate: str, enddate: str) -> pandas.DataFrame: 
        """Generate a complete sequence of dates.

        :param str startdate: the first date of the dataframe.

        :param str enddate: the last date of the dataframe.
        """
        start = datetime.strptime(startdate, '%Y-%m-%d')
        end = datetime.strptime(enddate, '%Y-%m-%d')
        all_days = pandas.date_range(start, end, freq='D')
        return all_days

    def get_list_of_locations(self):
        all_locs = self._latlong.keys()
        return list(all_locs)

    def api_retrieve(self, latlong: str, date: str):
        if date == '':
            URL = 'https://api.darksky.net/forecast/' + self._darksky_token + '/' + latlong
            PARAMS = {"lang":"en",
                      "units":"auto",
                      "extend": "hourly"}
        else:
            URL = 'https://api.darksky.net/forecast/' + self._darksky_token + '/' + latlong + ',' + date
        #move params to config
            PARAMS = {"lang":"en",
                      "units":"auto"}
        return requests.get(url=URL, params=PARAMS)

    def append_api_results(self, granularity: str, results, latlong: str):
        #extract data from response
        result = results.json()[granularity]['data']
        latitude = results.json()['latitude']
        longitude = results.json()['longitude']
        #convert to a dataframe
        df = pandas.DataFrame.from_dict(result)
        df = pandas.concat([get_darksky_api_structure(granularity),df], sort=False)
        #adds descriptive cols to df
        df['latitude'] = latitude
        df['longitude'] = longitude
        df['zipcode'] = self._latlong.get(latlong)
        #append api call to correct df, convert timestamp and return
        if granularity == 'hourly':
            df['time'] = df['time'].apply(lambda x: 
                time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(x)))
            df = self.reorder_columns(df)
            self._df_hourly = df.append(self._df_hourly, sort=False)
            return self._df_hourly
        else:
            df['time'] = df['time'].apply(lambda x: time.strftime('%Y-%m-%d', time.localtime(x)))
            # df = df.rename(columns={'time': 'date'})
            df = self.reorder_columns(df)
            self._df_daily = df.append(self._df_daily, sort=False)
            return self._df_daily

    def reorder_columns(self, data: pandas.DataFrame):
        cols = data.columns.tolist()
        cols.insert(1, cols.pop(cols.index('latitude')))
        cols.insert(2, cols.pop(cols.index('longitude')))
        cols.insert(3, cols.pop(cols.index('zipcode')))
        df = data.reindex(columns = cols)
        return df

    def get_csv_files_for_appending(self):
        dir_path = self._data_dir

        check_for_required_files(dir_path, 'weather-daily.csv',
            list(get_darksky_api_structure('daily').columns))
        check_for_required_files(dir_path, 'weather-daily-bu.csv',
            list(get_darksky_api_structure('daily').columns))
        check_for_required_files(dir_path, 'weather-hourly.csv',
            list(get_darksky_api_structure('hourly').columns))
        check_for_required_files(dir_path, 'weather-hourly-bu.csv',
            list(get_darksky_api_structure('hourly').columns))

        #open or create file
        hourly_path = PurePath(dir_path).joinpath('weather-hourly.csv')
        daily_path = PurePath(dir_path).joinpath('weather-daily.csv')

        dtyp = {"time": str,
                "summary": str,
                "icon": str,
                "sunriseTime": float,
                "sunsetTime": float,
                "moonPhase": float,
                "precipIntensity": float,
                "precipIntensityMax": float,
                "precipIntensityMaxTime": float,
                "precipProbability": float,
                "precipType": str,
                "precipAccumulation": float,
                "temperatureHigh": float,
                "temperatureHighTime": float,
                "temperatureLow": float,
                "temperatureLowTime": float,
                "apparentTemperatureHigh": float,
                "apparentTemperatureHighTime": float,
                "apparentTemperatureLow": float,
                "apparentTemperatureLowTime": float,
                "dewPoint": float,
                "humidity": float,
                "windSpeed": float,
                "windGust": float,
                "windGustTime": float,
                "windBearing": float,
                "cloudCover": float,
                "uvIndex": float,
                "uvIndexTime": float,
                "visibility": float,
                "temperatureMin": float,
                "temperatureMinTime": float,
                "temperatureMax": float,
                "temperatureMaxTime": float,
                "apparentTemperatureMin": float,
                "apparentTemperatureMinTime": float,
                "apparentTemperatureMax": float,
                "apparentTemperatureMaxTime": float,
                "pressure": str,
                "ozone": str}

        daily_df = pandas.read_csv(daily_path, dtype=dtyp)
        hourly_df = pandas.read_csv(hourly_path)

        daily_df['time'] = pandas.to_datetime(daily_df.time)
        hourly_df['time'] = pandas.to_datetime(hourly_df.time)

        self._df_daily = daily_df
        self._df_hourly = hourly_df

    def override_config_values_for_daily_refresh(self):
        """ Check daily csv file for the max date and geographies needed for the api calls.
            This relies on the dates in the daily weather file and disregards hourly.
            Future work will need to be done so that each file is handled seperately."""
        df = self._df_daily.groupby(['latitude','longitude','zipcode'])['time'].max()
        df = pandas.DataFrame(df).reset_index()
        df = df.rename(columns={'time': 'startdate'})
        # max date plus 1 so the last day isn't duplicated
        df['startdate'] = df['startdate'].dt.date + timedelta(days=1)
        df['enddate'] = datetime.today().date()
        df['latlong'] = df['latitude'].astype(str) + ',' + df['longitude'].astype(str)
        # override config values when running daily update.
        self._startdate = str(df['startdate'].min())
        self._enddate = str(df['enddate'].min())
        self._latlong = dict(df[['latlong','zipcode']].values)
        return df

    def delete_last_two_weeks_of_records(self):
        """ look in csv file and delete the future dates, they will be overwritten with fresh data
            The darksky FAQ's mention that historical data can change and can lag up to 2 weeks.
            This method purges records within the last 2 weeks as part of the daily refresh """
        two_weeks = pandas.to_datetime(datetime.today().date() - timedelta(days=14))
        self._df_daily = self._df_daily[self._df_daily['time'] < two_weeks]
        self._df_hourly = self._df_hourly[self._df_hourly['time'] < two_weeks]


    def write_data_to_file(self):
        """ write the historical that is needed and the future forecast """
        dir_path = self._data_dir

        #open or create file
        daily_path = PurePath(dir_path).joinpath('weather-daily.csv')
        daily_bu_path = PurePath(dir_path).joinpath('weather-daily-bu.csv')
        hourly_path = PurePath(dir_path).joinpath('weather-hourly.csv')
        hourly_bu_path = PurePath(dir_path).joinpath('weather-hourly-bu.csv')        
        #backup existing data before rewriting csv
        os.remove(daily_bu_path)
        os.remove(hourly_bu_path)
        os.rename(daily_path,daily_bu_path)
        os.rename(hourly_path,hourly_bu_path)

        self._df_daily['time'] = pandas.to_datetime(self._df_daily.time).dt.date
        self._df_hourly['time'] = pandas.to_datetime(self._df_hourly.time)

        self._df_daily.to_csv(daily_path, index=False)
        self._df_hourly.to_csv(hourly_path, index=False)

    def validate_complete_sequence_of_dates(self):
        """Identify incomplete date sequence if it exists. This prevents
        incomplete data from being written to final file.
        """
        min_dt = str(self._df_daily['time'].min().date())
        max_dt = str(self._df_daily['time'].max().date())
        cs = pandas.DataFrame(self.get_list_of_dates(min_dt, max_dt), columns=['time'])
        df = cs.merge(self._df_daily, how='left', on='time')
        miss_dt = df[df['latitude'].isna()]
        try:
            if len(miss_dt) is not 0:
                self._missing_daily_rng = miss_dt
                raise ValueError('Broken date sequence in daily file!')
        except ValueError:
            raise


if __name__ == '__main__':
    import configparser
    config = configparser.ConfigParser()
    config.read('config.ini')
    WA = WeatherApiConnect(config['TimePeriod']['startdate'],config['TimePeriod']['enddate'],
                           dict(config.items('LocationDetail')), os.environ['DARKSKY_TOKEN'],
                            '/Users/FBIMAC/raw-data')