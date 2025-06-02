import logging
import os

LENGTH_DATA_BYTES = 3
FILENAME_KEYS_SEPARATOR = '_'
KEY_VALUE_SEPARATOR = ','

class StorageAdapter:
    def __init__(self, storage_path):
        self.storage_path = storage_path
        try:
            os.makedirs(os.path.dirname(storage_path), exist_ok=True)
            logging.debug(f"Storage directory created at: {storage_path}")
        except Exception as e:
            logging.error(f"Failed to create storage directory at {storage_path}: {e}")
        pass

    def append(self, file_key, key, value, secondary_file_key=None):
        path = FILENAME_KEYS_SEPARATOR.join([file_key, secondary_file_key]) if secondary_file_key else file_key
        file_path = os.path.join(self.storage_path, path)
        try:
            data_bytes = f'{key}{KEY_VALUE_SEPARATOR}{value}'.encode('utf-8')
            len_data_bytes = len(data_bytes).to_bytes(LENGTH_DATA_BYTES, 'big')
            with open(file_path, 'ab') as f:
                f.write(len_data_bytes + data_bytes)
                f.flush()
            logging.debug(f"action: append_data_to_storage | result: success | key: {key} | value: {value} | path: {path}")
        except Exception as e:
            logging.error(f"action: append_data_to_storage | result: fail | error: {e}")
    
    def load_data_from_file(self, file_path):
        data = {}
        try:
            with open(file_path, 'rb') as f:
                while True:
                    len_data_bytes = f.read(LENGTH_DATA_BYTES)
                    if not len_data_bytes:
                        break
                    len_data = int.from_bytes(len_data_bytes, 'big')
                    data_bytes = f.read(len_data)
                    if not data_bytes:
                        break
                    if len(data_bytes) == len_data:
                        key, value = data_bytes.decode('utf-8').split(KEY_VALUE_SEPARATOR, 1)
                        data[key] = value
                    else:
                        logging.debug(f"action: load_data_from_storage | result: fail | data corruption detected | path: {file_path}")  
            logging.debug(f"action: load_data_from_storage | result: success | path: {file_path}")
            return data
        except FileNotFoundError:
            return {}
        except Exception as e:
            logging.error(f"action: load_data_from_storage | result: fail | error: {e}")
            
    def load_data(self, key):
        data = {}
        with os.scandir(self.storage_path) as files:
            for f in files:
                file_keys = f.name.split(FILENAME_KEYS_SEPARATOR)
                if file_keys[0] == key:
                    file_path = os.path.join(self.storage_path, f.name)
                    if len(file_keys) == 2:
                        secondary_file_key = file_keys[1]
                        data[secondary_file_key] = self.load_data_from_file(file_path)
                    else:
                        data = self.load_data_from_file(file_path)
        return data
