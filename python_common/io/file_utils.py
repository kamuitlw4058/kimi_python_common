def chunked_file_line_reader(fp,sep = '\n', block_size=1024 * 32):
    """生成器函数：分块读取文件内容"""
    remain = ''
    while True:
        chunk = fp.read(block_size)
        if not chunk:
            #print(f'yield remain :{remain}')
            yield remain
            break
        #print(f'chunk :{chunk}')
        lines = (remain +  chunk).split(sep)
        if len(lines) > 1:
            for line in lines[:-1]:
                yield line
        remain = lines[-1]
    
