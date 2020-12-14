from python_common.io.file_utils import chunked_file_line_reader

file_path = 'data/test.txt'
with open(file_path, 'r') as fp:
    for line in chunked_file_line_reader(fp,block_size=1028* 32):
        print(f'ret:{line}')