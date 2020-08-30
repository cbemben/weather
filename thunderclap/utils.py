import csv

def check_for_required_files(self, file_name: str, var_names):
    file_path = self._data_dir+'/'+file_name
    if Path(file_path).exists() is False:
        with open(file_path,'w') as f:
           csvWriter = csv.DictWriter(f, fieldnames = var_names)
           csvWriter.writeheader()
           #csvWriter
           f.close()