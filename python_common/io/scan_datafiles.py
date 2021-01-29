import os
from types import FunctionType, MethodType

def false_filter(filepath,filename,dir_list):
    return False

def true_filter(filepath,filename,dir_list):
    return True

class ScanDataFiles():
    def __init__(self,job_name,data_path_base,file_filters=None,dir_filters=None, ckpt_dir_base='data/scan_data_ckpt'):
        self._name = job_name
        self._files = []
        self._ckpt_dir_base = ckpt_dir_base
        self._ckpt_dir = f'{self._ckpt_dir_base}/{job_name}'
        if not os.path.exists(self._ckpt_dir):
            os.makedirs(self._ckpt_dir)
        self._data_path_base = data_path_base
        self._dir_filters = dir_filters
        self._file_filters = file_filters

    def init_filters(self,filters,default_filter,default_filters=None):
        apply_filter = None
        if filters is None:
            filters = default_filters
            if filters is None or (isinstance( filters,list) and len(filters) == 0):
                filters = default_filter

        if isinstance( filters,list):
            apply_filter = filters[0]
            #print(f'list filters get apply_filter:{apply_filter}')
            filters = filters[1:]
        elif  isinstance( filters,FunctionType):
            apply_filter = filters

        return apply_filter,filters




    def scan(self,base_dir =None,data_dir=None,file_filters=None,dir_filters=None,dir_list=None):
        if base_dir is None:
            base_dir = self._data_path_base
        if data_dir is None:
            data_dir = ''
        
        dir_path = os.path.join(base_dir,data_dir)

        dir_filter,dir_next_filters = self.init_filters(dir_filters,true_filter,default_filters =self._dir_filters)
        file_filter,file_next_filters = self.init_filters(file_filters,true_filter,default_filters = self._file_filters)

        for filename in os.listdir(dir_path):
            filepath = os.path.join(dir_path, filename)
            if os.path.isdir(filepath) and dir_filter(filepath,filename,dir_list):
                print(f"will check dir:{filepath}")
                if dir_list is None:
                    cur_dir_list = []
                else:
                    cur_dir_list =  dir_list.copy()
                cur_dir_list.append(filename)
                self.scan(base_dir,os.path.join(data_dir,filename),file_filters=file_next_filters,dir_filters=dir_next_filters,dir_list=cur_dir_list)
            if os.path.isfile(filepath) and file_filter(filepath,filename,dir_list):
                print(f'will process file:{filepath}')
                self._files.append((base_dir,data_dir,filename))


    def process_files(self):
        print(self._files)
