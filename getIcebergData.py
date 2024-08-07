#!pip install pandas numpy scikit-learn
#!mkdir data
#!touch data/features.txt

import os
import warnings
import sys
import json
import pprint
from datetime import datetime
#import pandas as pd
import numpy as np

import cml.data_v1 as cmldata
conn = cmldata.get_connection("go01-aw-dl")
spark = conn.get_spark_session()

from pyspark.sql.functions import input_file_name

argv_snap=5551603955198434466
file="s3a://go01-demo/warehouse/tablespace/external/hive/fnb_bryce.db/metal/metadata/00008-94432d62-f212-40d7-ad82-16af39de508f.metadata.json"

df_meta = spark.read.option("nullValue", "null") \
    .option("dateFormat", "yyyy-MM-dd") \
    .option("multiLine", "true") \
    .json("s3a://go01-demo/warehouse/tablespace/external/hive/fnb_bryce.db/metal/metadata/*.metadata.json" )


df_meta_filename = df_meta.withColumn("filename", input_file_name())


# Display the DataFrame schema
#df_meta_filename.printSchema()

# Show the DataFrame content
#df_meta_filename.show(vertical=True, truncate=False)

# Collect the data as a list of Row objects
rows = df_meta_filename.collect()

# Convert the Row objects to dictionaries
data = [row.asDict() for row in rows]

# Convert to a JSON string
json_string = json.dumps(data)

# If you want a JSON object instead of a string, you can do:
json_chkpt = json.loads(json_string)

for chkptdata in json_chkpt:
  curr_snap=chkptdata['current-snapshot-id']

  if(curr_snap == argv_snap):
    print('\n  Checkpoint Dump\n-------------------')
    print(f"Checkpoint Metadata Filename               : {chkptdata['filename']}")


    print(f"Current Snapshot                           : {curr_snap}")


    dt_object = datetime.fromtimestamp(chkptdata['last-updated-ms']/1000)
    date_string = dt_object.strftime('%Y-%m-%d %H:%M:%S.%f')
    print(f"Last Updated                               : {date_string}")

    print(f"Location                                   : {chkptdata['location']}")

    print(f"storage_handler                            : {chkptdata['properties'][2]}")
    print(f"iceberg.mr.table.format                    : {chkptdata['properties'][3]}")
    print(f"iceberg.mr.write.format.default            : {chkptdata['properties'][4]}")
    print(f"iceberg.mr.write.parquet.compression-codec : {chkptdata['properties'][5]}")
    print(f"iceberg.mr.read.merge-on-read              : {chkptdata['properties'][6]}")


    metadatalog = chkptdata['metadata-log']
    snapshotdata= chkptdata['snapshots']

    for snap in snapshotdata:
      if(argv_snap == snap[4]):

      #  pprint.pprint(snap)
        print("==============================================================")
        print(f"snapshot-id        : {snap[4]}")
        print(f"parent-snapshot-id : {snap[1]}")
        print(f"sequence-number    : {snap[3]}")
        #print("A string map that summarizes the snapshot changes, including operation (see below)")
        print(f"summary	           : {snap[5][7]}")
        #print("ID of the table's current schema when the snapshot was created")
        print(f"schema-id          : {snap[2]}")
        #print("A timestamp when the snapshot was created, used for garbage collection and table inspection")
        dt_object = datetime.fromtimestamp(snap[6]/1000)
        date_string = dt_object.strftime('%Y-%m-%d %H:%M:%S.%f')
        print(f"timestamp-ms 	     : {date_string}")
        #print("The location of a manifest list for this snapshot that tracks manifest files with additional metadata")
        print(f"manifest-list 	   : {snap[0]}")
        avrodf = spark.read.format("avro").load(snap[0])
        mplist= avrodf.select('manifest_path').collect()
        print("manifest paths")
        for mp in mplist:
          print(f" - {mp['manifest_path']}")
          mpdf = spark.read.format("avro").load(mp['manifest_path'])
          dflist=mpdf.collect()
          for df in dflist:
            if(argv_snap == df['snapshot_id']):
              print(f"  - snapshot_id: {df['snapshot_id']}")
              print(f"  - data_file: {df['data_file']['file_path']}")
              print("\n")
        #avrodf.show(vertical=True, truncate=False)
        #avrodf.show(vertical=False, truncate=False)
      #  manifestdf= spark.read.format("avro").load(

        #print("A list of manifest file locations. Must be omitted if manifest-list is present")
        #print(f"manifests          : {snap[]}")

        print("\n")
