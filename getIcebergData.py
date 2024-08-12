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

tablename='FNB_Bryce.metal'

import cml.data_v1 as cmldata
conn = cmldata.get_connection("go01-aw-dl")
spark = conn.get_spark_session()
# 'spark.sql.iceberg.handle-timestamp-without-timezone'
spark.conf.set("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
spark.sql(f"select * from {tablename}.files").show(truncate=False)

spark.sql(f"select snapshot_id from {tablename}.history").show(truncate=False)

json_tableformat = spark.sql(f"describe formatted {tablename}").toJSON().collect()
file_location = json.loads(json_tableformat[19])['data_type']
tableinfo = json.loads(json_tableformat[21])['data_type']
tableinfo_json=json.loads(tableinfo.replace('[','{"').replace('=','" : "').replace(',','", "').replace(']','"}'))

current_snapshot_id = tableinfo_json['current-snapshot-id']

#spark.sparkContext.getConf().getAll()
  #"tag", first_snapshot
  # get a snapshot so we always train with the same data
#data = spark.read\
#.format("iceberg")\
#.load(f"{tablename}").toPandas()


from pyspark.sql.functions import input_file_name
argv_snap= 5135327718516679476
#argv_snap=5049826933335619187
#argv_snap=5551603955198434466
#file="s3a://go01-demo/warehouse/tablespace/external/hive/jing_airlines.db/flights/metadata/00005-09c2e8cd-1461-4388-9058-bff94accf579.metadata.json"
file="s3a://go01-demo/warehouse/tablespace/external/hive/fnb_bryce.db/metal/metadata/00034-3321592c-426f-4dde-ada0-7df5562a08dc.metadata.json"
df_meta = spark.read.option("nullValue", "null") \
    .option("dateFormat", "yyyy-MM-dd") \
    .option("multiLine", "true") \
    .json(file)


df_meta_filename = df_meta.withColumn("filename", input_file_name())


# Display the DataFrame schema
#df_meta_filename.printSchema()

# Show the DataFrame content
#df_meta_filename.show(vertical=True, truncate=False)


# Convert DataFrame to JSON strings
df_json_strings = df_meta_filename.toJSON().collect()

# Parse JSON strings into JSON objects
df_json_objects = [json.loads(json_str) for json_str in df_json_strings]

CONTENT_MAP= {0: 'DATA', 1: 'POSITION DELETES', 2: 'EQUALITY DELETES' }
STATUS_MAP= {0: 'EXISTING' ,1: 'ADDED', 2: 'DELETED'}
for chkptdata in df_json_objects:
  curr_snap=chkptdata['current-snapshot-id']

  if(curr_snap == argv_snap):
    print('\n  Checkpoint Dump\n-------------------')
    print(f"Checkpoint Metadata Filename               : {chkptdata['filename']}")


    print(f"Current Snapshot                           : {curr_snap}")


    dt_object = datetime.fromtimestamp(chkptdata['last-updated-ms']/1000)
    date_string = dt_object.strftime('%Y-%m-%d %H:%M:%S.%f')
    print(f"Last Updated                               : {date_string}")

    print(f"Location                                   : {chkptdata['location']}")
    print(f"bucketing_version                          : {chkptdata['properties']['bucketing_version']}")
    print(f"serialization.format                       : {chkptdata['properties']['serialization.format']}")

    print(f"storage_handler                            : {chkptdata['properties']['storage_handler']}")
    print(f"write.update.mode                          : {chkptdata['properties']['write.update.mode']}")

    print(f"write.delete.mode                          : {chkptdata['properties']['write.delete.mode']}")
    print(f"write.merge.mode                           : {chkptdata['properties']['write.merge.mode']}")
    print(f"write.parquet.compression-codec            : {chkptdata['properties']['write.parquet.compression-codec']}")


    metadatalog = chkptdata['metadata-log']
    snapshotdata= chkptdata['snapshots']

    for snap in snapshotdata:
      if(argv_snap == snap['snapshot-id']):

      #  pprint.pprint(snap)
        print("==============================================================")
        print(f"snapshot-id        : {snap['snapshot-id']}")
        print(f"parent-snapshot-id : {snap['parent-snapshot-id']}")
        print(f"sequence-number    : {snap['sequence-number']}")
        #print("A string map that summarizes the snapshot changes, including operation (see below)")
        print(f"summary	          ")
        print(f"  added-data-files             : {snap['summary']['added-data-files']}")
        if 'added-delete-files'in snap['summary']:
          print(f"' added-delete-files           : {snap['summary']['added-delete-files']}")
        else:
          print(f"' added-delete-files           : NONE")
        
        print(f"  added-files-size             : {snap['summary']['added-files-size']}")
        if 'added-position-delete-files'in snap['summary']:
          print(f"  added-position-delete-files  : {snap['summary']['added-position-delete-files']}")
        else:
          print(f"' added-position-delete-files  : NONE")          

        if 'added-position-deletes'in snap['summary']:
          print(f"  added-position-deletes       : {snap['summary']['added-position-deletes']}")
        else:
           print(f"  added-position-deletes       : NONE")
        print(f"  added-records                : {snap['summary']['added-records']}")
        print(f"  changed-partition-count      : {snap['summary']['changed-partition-count']}")
        print(f"  operation                    : {snap['summary']['operation']}")
        print(f"  total-data-files             : {snap['summary']['total-data-files']}")
        print(f"  total-delete-files           : {snap['summary']['total-delete-files']}")
        print(f"  total-equality-deletes       : {snap['summary']['total-equality-deletes']}")
        print(f"  total-files-size             : {snap['summary']['total-files-size']}")
        print(f"  total-position-deletes       : {snap['summary']['total-position-deletes']}")
        print(f"  total-records                : {snap['summary']['total-records']}")
        
        #print("ID of the table's current schema when the snapshot was created")
        #print(f"schema-id          : {snap['schema-id']}")
        #print("A timestamp when the snapshot was created, used for garbage collection and table inspection")
        dt_object = datetime.fromtimestamp(snap['timestamp-ms']/1000)
        date_string = dt_object.strftime('%Y-%m-%d %H:%M:%S.%f')
        print(f"timestamp-ms 	     : {date_string}")
        #print("The location of a manifest list for this snapshot that tracks manifest files with additional metadata")
        print(f"manifest-list 	   : {snap['manifest-list']}")
        print("")
        avrodf = spark.read.format("avro").load(snap['manifest-list'])
        mplist= avrodf.select('manifest_path').toJSON().collect()

        # Convert DataFrame to JSON strings
        mplist_json_strings = mplist

        # Parse JSON strings into JSON objects
        mplist = [json.loads(json_str) for json_str in mplist_json_strings]        
        
        
        print("Manifest Paths")
        for mp in mplist:
          print(f"  - Manifest Path    :  {mp['manifest_path']}")

          mpdf = spark.read.format("avro").load(mp['manifest_path'])
          #dflist=mpdf.collect()

          # Convert DataFrame to JSON strings
          mpdf_json_strings = mpdf.toJSON().collect()

          # Parse JSON strings into JSON objects
          mpdf_json_objects = [json.loads(json_str) for json_str in mpdf_json_strings]
          
          

          sorted_dflist = sorted(mpdf_json_objects, key=lambda x: x['snapshot_id'])
          for df in sorted_dflist:
            print(f"  - snapshot_id: {df['snapshot_id']}")
            print(f"  - status.    : {STATUS_MAP[df['status']]}") 
            print(f"  - Content    : {CONTENT_MAP[df['data_file']['content']]}")

            print(f"  - data_file  : {df['data_file']['file_path']}")
            print("\n")
        #avrodf.show(vertical=True, truncate=False)
        #avrodf.show(vertical=False, truncate=False)
      #  manifestdf= spark.read.format("avro").load(

        #print("A list of manifest file locations. Must be omitted if manifest-list is present")
        #print(f"manifests          : {snap[]}")

        print("\n")
