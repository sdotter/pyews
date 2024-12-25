import csv
from datetime import datetime
import os

class CustomWeatherStore:
    def __init__(self, data_dir):
        self.data_dir = data_dir
        self.directory_names = {
            'raw': 'raw',
            'calib': 'calib',
            'hourly': 'hourly',
            'daily': 'daily',
            'monthly': 'monthly'
        }
        self.key_lists = {
            'raw': [
                'idx', 'delay', 'hum_in', 'temp_in', 'hum_out', 'temp_out',
                'abs_pressure', 'wind_ave', 'wind_gust', 'wind_dir', 'rain',
                'status', 'illuminance', 'uv',
            ],
        }

    def _prepare_data_line(self, data):
        # Reorder or select data as needed to match specific output formatting
        ordered_data = [
            data['idx'],
            str(data.get('delay', '')),
            str(data.get('hum_in', '')),
            str(data.get('temp_in', '')),
            '', '',  # Assuming some spaces are left intentionally blank
            str(data.get('abs_pressure', '')),
            str(data.get('wind_ave', '')),
            str(data.get('wind_gust', '')),
            str(data.get('wind_dir', '')),
            str(data.get('rain', '')),
            str(data.get('status', '')),
            str(data.get('illuminance', '')),
            str(data['uv']),
        ]
        return ','.join(ordered_data)

    def save_data(self, data, datatype='raw'):
        if datatype not in self.directory_names:
            raise ValueError("Unsupported datatype: " + datatype)

        # Prepare the directory path based on the datatype
        dt = datetime.strptime(data['idx'], "%Y-%m-%dT%H:%M:%S.%f")
        year_month_dir = os.path.join(self.data_dir, self.directory_names[datatype], dt.strftime('%Y'), dt.strftime('%Y-%m'))
        os.makedirs(year_month_dir, exist_ok=True)

        if datatype == 'raw':
            filename = dt.strftime('%Y-%m-%d.txt')
        elif datatype in ['daily', 'monthly']:
            filename = datatype + "-" + dt.strftime('%Y-%m') + ".txt"
        elif datatype == 'hourly':
            filename = datatype + "-" + dt.strftime('%Y-%m-%d-%H') + ".txt"
        else:  # For calib or potentially other datatypes
            filename = datatype + "-" + dt.strftime('%Y-%m-%d') + ".txt"

        file_path = os.path.join(year_month_dir, filename)
        
        dt_format = "%Y-%m-%dT%H:%M:%S.%f"
        with open(file_path, 'a') as file:
            if isinstance(data, dict):
                # Convert 'idx' datetime from ISO 8601 to desired format 
                if 'idx' in data and isinstance(data['idx'], str):
                    try:
                        idx_dt = datetime.strptime(data['idx'].split('.')[0], dt_format.split('.')[0])
                        data['idx'] = idx_dt.strftime('%Y-%m-%d %H:%M:%S')
                    except ValueError:
                        # In case the format does not match, indicate parsing failure or ignore
                        print("Datetime parsing failed for idx:", data['idx'])
                
                values = [str(data.get(key, '')) for key in self.key_lists[datatype]]
                line = ','.join(values)
            else:
                line = data
            file.write(line + '\n')