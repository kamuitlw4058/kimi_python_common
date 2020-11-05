from python_common.database.client.clients import clients
#import pandas_profiling
import json

if __name__ == "__main__":
     client = clients.client()
     print(client.tables())