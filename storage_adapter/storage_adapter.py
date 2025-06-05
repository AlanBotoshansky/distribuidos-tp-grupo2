import logging
import os
import uuid
import ast

LENGTH_DATA_BYTES = 3
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
    
    def __get_file_path(self, file_key, secondary_file_key=None):
        path = f"{file_key}{secondary_file_key}" if secondary_file_key else file_key
        return os.path.join(self.storage_path, path)
    
    def __get_temp_file_path(self):
        path = f"{uuid.uuid4()}"
        return os.path.join(self.storage_path, path)
    
    def __load(self, file_key, load_function):
        data = {}
        with os.scandir(self.storage_path) as files:
            for f in files:
                if f.name.startswith(file_key):
                    file_path = os.path.join(self.storage_path, f.name)
                    if f.name == file_key:
                        data = load_function(file_path)
                    else:
                        secondary_file_key = f.name[len(file_key):]
                        data[secondary_file_key] = load_function(file_path)
        return data if data else None
    
    def delete(self, file_key, secondary_file_key=None):
        file_path = self.__get_file_path(file_key, secondary_file_key)
        try:
            os.remove(file_path)
            logging.debug(f"action: delete_file_from_storage | result: success | path: {file_path}")
        except Exception as e:
            logging.error(f"action: delete_file_from_storage | result: fail | error: {e} | path: {file_path}")
    
    def __encode_key_value(self, key, value=None):
        if value is None:
            data_bytes = repr(key).encode('utf-8')
        else:
            data_bytes = f'{repr(key)}{KEY_VALUE_SEPARATOR}{repr(value)}'.encode('utf-8')
        len_data_bytes = len(data_bytes).to_bytes(LENGTH_DATA_BYTES, 'big')
        return len_data_bytes + data_bytes

    def append(self, file_key, key, value=None, secondary_file_key=None):
        file_path = self.__get_file_path(file_key, secondary_file_key)
        try:
            with open(file_path, 'ab') as f:
                f.write(self.__encode_key_value(key, value))
                f.flush()
            logging.debug(f"action: append_data_to_storage | result: success | key: {key} | value: {value} | file_path: {file_path}")
        except Exception as e:
            logging.error(f"action: append_data_to_storage | result: fail | error: {e}")
            
    def __load_key_values_from_file(self, file_path):
        key_values = {}
        keys = set()
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
                    decoded_data = data_bytes.decode('utf-8')
                    if KEY_VALUE_SEPARATOR in decoded_data:
                        key, value = decoded_data.split(KEY_VALUE_SEPARATOR, 1)
                        key_values[ast.literal_eval(key)] = ast.literal_eval(value)
                    else:
                        keys.add(ast.literal_eval(decoded_data))
                else:
                    logging.debug(f"action: load_data_from_storage | result: fail | data corruption detected | path: {file_path}")  
        logging.debug(f"action: load_data_from_storage | result: success | path: {file_path}")
        return keys if not key_values and keys else key_values
            
    def load_key_values(self, file_key):
        return self.__load(file_key, self.__load_key_values_from_file)
            
    def update(self, file_key, data, secondary_file_key=None):
        file_path = self.__get_file_path(file_key, secondary_file_key)
        temp_file_path = self.__get_temp_file_path()
        try:
            with open(temp_file_path, 'wb') as temp_f:
                temp_f.write(repr(data).encode('utf-8'))
                temp_f.flush()
            os.replace(temp_file_path, file_path)
            logging.debug(f"action: update_data_in_storage | result: success | file_path: {file_path}")
        except Exception as e:
            logging.error(f"action: update_data_in_storage | result: fail | error: {e} | path: {file_path}")
    
    def __load_data_from_file(self, file_path):
        data = None
        with open(file_path, 'rb') as f:
            data_bytes = f.read()
            if not data_bytes:
                logging.debug(f"action: load_data_from_storage | result: fail | empty file | path: {file_path}")
                return None
            try:
                data = ast.literal_eval(data_bytes.decode('utf-8'))
            except Exception as e:
                logging.error(f"action: load_data_from_storage | result: fail | error: {e} | path: {file_path}")
                return None
        logging.debug(f"action: load_data_from_storage | result: success | path: {file_path}")
        return data
    
    def load_data(self, file_key):
        return self.__load(file_key, self.__load_data_from_file)
