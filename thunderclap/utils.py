import csv
import json
from pathlib import Path
import pandas

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

def get_master_str(granularity: str):
    with open('thunderclap/data/darksky_api_return_structure.json') as json_file:   
        json_str = json.load(json_file)
    master_dict = json_str[granularity][0]
    return pandas.DataFrame.from_dict(master_dict)