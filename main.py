from collections import namedtuple
import pandas as pd
from hdfs import InsecureClient
import avro.schema
from avro.datafile import DataFileWriter
from avro.io import DatumWriter

client_hdfs = InsecureClient('http://192.168.88.148:50070')
fields = ("AirlineID", "Name", "Alias", "IATA", "ICAO", "Callsign", "Country", "Active")


with client_hdfs.read('/user/student/airlines/airlines.dat', encoding='utf-8') as reader:
    df = pd.read_csv(reader, header=None, keep_default_na=False)

schema = avro.schema.parse(open("airlines.avsc", "rb").read())

writer = DataFileWriter(open("airlines.avro", "wb"), DatumWriter(), schema)
with open("airlines.avro", "wb") as out:
    writer = DataFileWriter(out, DatumWriter(), schema)
    for _, row in df.iterrows():
        record = {field: value for field, value in zip(fields, row.values)}
        writer.append(record)

client_hdfs.upload(hdfs_path='/user/student/airlines/airlines.avro', local_path='airlines.avro')
