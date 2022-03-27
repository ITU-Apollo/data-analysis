#!/usr/bin/python3

from pymongo import MongoClient
from os.path import exists
import pyarrow as pa
import sys
import pymongo 
import pyarrow.parquet as pq
import pandas as pd
import multiprocessing
import boto3 as boto

def convert_collection_to_parque(cname, fname):
    client = MongoClient("") #connection string
    db = client.apollo
    collection = db[cname]
    ctr = 0
    print(f'doing {cname}')
    batch_size = 1000
    df = pd.DataFrame().from_records([collection.find_one()]).astype(str)
    table = pa.Table.from_pandas(df)
    writer = pq.ParquetWriter(fname, table.schema)
    cursor = collection.find().batch_size(batch_size)
    while True:
        rows_batch = []
        while len(rows_batch) < batch_size:
            try:
                row = cursor.next()
            except StopIteration:
                break
            rows_batch.append(row)
        if len(rows_batch) == 0: break
        ctr = ctr + len(rows_batch)
        if (ctr % 10000) == 0: print(f'{cname} done {ctr}')
        df = pd.DataFrame().from_records(rows_batch).astype(str)
        table = pa.Table.from_pandas(df)
        writer.write_table(table)
        #if ctr > 5000: return

if __name__ == '__main__':
    client = MongoClient("") #connection string
    db = client.apollo
    
    cnames = db.list_collection_names()
    #cnames = ['CommitFile']

    cnames2 = {}
    for cname in cnames:
        cnames2[cname] = f'{cname}.parquet'
    cnames = cnames2

    convert_parq = {}
    for cname, fname in cnames.items():
        if exists(fname):
            yn = input(f'{fname} exists. Overwrite? [yN] ')
            if yn.lower() != "y": continue
        convert_parq[cname] = fname

    with multiprocessing.Pool(8) as p:
        rets = []
        for cname, fname in convert_parq.items():
            rets.append(p.apply_async(convert_collection_to_parque, (cname, fname, )))
        for ret in rets: ret.get() # wait until it's finished

    upload_s3 = {}
    for cname, fname in cnames.items():
        yn = input(f'Upload {cname} to s3? [Yn] ')
        if yn.lower() == "n": continue
        upload_s3[cname] = fname

    boto_c = boto.resource('s3')

    for cname, fname in upload_s3.items():
        boto_c.Object('bucket-name', f'/{cname}.parquet').put(Body=open(fname, 'rb'))
