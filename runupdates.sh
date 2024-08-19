#!/bin/bash
set -f

BEELINE="/home/cdsw/apache-hive-beeline-3.1.3000.2024.0.17.0-25/bin/beeline"
URL='jdbc:hive2://xxxxxx-demo-aws.ylcu-atmi.cloudera.site/default;transportMode=http;httpPath=cliservice;socketTimeout=60;ssl=true;retries=3;'
USER="jprosser"
PASS="SXXXXXXXX"

MODE="cowtag"
#WMODE="merge-on-read"
#MODE="cow"
WMODE="commit-on-write"

CREATE_TABLE="
drop table if exists  leqembi.alzh_narrow_p_change_dts_${MODE};
create table  leqembi.alzh_narrow_p_change_dts_${MODE}
 (
   PatientID integer,
   Age integer ,
   Gender integer ,
   Ethnicity integer,
   action string,
   sat_end_dts STRING
)PARTITIONED BY SPEC ( sat_end_dts )
STORED BY ICEBERG TBLPROPERTIES (\"format-version\" = \"2\",\"external.table.purge\"=\"true\",\"write.delete.mode\"=\"${WMODE}\",\"write.update.mode\"=\"${WMODE}\",\"write.merge.mode\"=\"${WMODE}\");
insert into  leqembi.alzh_narrow_p_change_dts_${MODE} select * from  leqembi.alzh_narrow order by patientid;
select * from  leqembi.alzh_narrow_p_change_dts_${MODE} limit 5;
"

CREATE_ACTION_DETAILS_VIEW="
DROP VIEW IF EXISTS leqembi.action_details;
CREATE VIEW leqembi.action_details AS
SELECT d.useg, d.ACTION, 
              coalesce(upd.PatientID,t.PatientID) PatientID, 
              coalesce(upd.age,t.age) age,
              coalesce(upd.Gender,    t.Gender) Gender,
              coalesce(upd.Ethnicity,    t.Ethnicity) Ethnicity

  FROM (SELECT c.patientid cseg, coalesce(u.PatientID,c.PatientID) useg
                ,CASE
                    when c.md5 != u.md5  then 'U'
                    when c.PatientID is NULL then 'I'
                    when u.PatientID is NULL then 'D'
                END 'ACTION'
        FROM  leqembi.alzh_narrow_p_change_dts_CKSUMS c
        FULL OUTER JOIN leqembi.alzh_narrow_p_change_dts_UPD_CKSUMS u
            ON c.PatientID = u.PatientID
       )  D
  LEFT OUTER JOIN leqembi.alzh_narrow_p_change_dts_${MODE} t   ON  D.useg = t.PatientID  
  LEFT OUTER JOIN leqembi.alzh_narrow_p_change_dts_UPD_VW upd ON D.useg = upd.PatientID order by useg;
"

TEST_SQL='SELECT * from leqembi.alzh_narrow_p_change_dts_UPD_VW order by patientid LIMIT 5;'

LOOK_AT_ACTION_DETAILS="SELECT count(patientid) , patientid  FROM leqembi.action_details group by patientid order by count(patientid) desc,patientid limit 5;"


RUN_SQL_CMD() {
  local sqlvar=$1
  local sql=${!sqlvar}
  echo "---------"
  echo "Running command:\n${sql}\n"
  echo ${sql} |  $BEELINE --silent=true --verbose=false  -n${USER}  -p ${PASS}   -u ${URL}


  if [ $? -eq 0 ]; then
      echo "-------------------"
  else
      echo "Command failed"
      exit
  fi
  

}


RUN_SQL_CMD CREATE_TABLE

RUN_SQL_CMD CREATE_ACTION_DETAILS_VIEW


echo "STARTING MERGES"

for ((i=1; i<=31; i++))
do
    echo "UPDATE $i"
    CREATE_UPD_VW="drop view if exists leqembi.alzh_narrow_p_change_dts_UPD_VW;create view leqembi.alzh_narrow_p_change_dts_UPD_VW as select * from leqembi.alzh_narrow_p_change_dts_UPD${i} order by patientid;"

    CREATE_CKSUM_TABS="truncate table  leqembi.alzh_narrow_p_change_dts_upd_CKSUMS;truncate table leqembi.alzh_narrow_p_change_dts_CKSUMS;\
insert into leqembi.alzh_narrow_p_change_dts_CKSUMS select patientid , md5(concat(CAST(patientid AS STRING),CAST(age AS STRING),CAST(gender AS STRING), CAST(ethnicity AS STRING))) as md5 from leqembi.alzh_narrow_p_change_dts_${MODE}  order by patientid; \
insert into leqembi.alzh_narrow_p_change_dts_UPD_CKSUMS select patientid ,md5(concat(CAST(patientid AS STRING), CAST(age AS STRING),CAST(gender AS STRING), CAST(ethnicity AS STRING))) as md5 from leqembi.alzh_narrow_p_change_dts_UPD_VW order by patientid;"
    
    DO_MERGE="MERGE INTO leqembi.alzh_narrow_p_change_dts_${MODE} c USING (select               DELTAS.useg patientid,               coalesce(updates.age,DELTAS.age) age,               coalesce(updates.gender,DELTAS.gender) gender,              coalesce(updates.Ethnicity,    DELTAS.Ethnicity) Ethnicity,              DELTAS.action        from  leqembi.alzh_narrow_p_change_dts_UPD_VW updates        RIGHT OUTER JOIN leqembi.action_details DELTAS                ON updates.patientid = DELTAS.useg        ) as NC ON c.patientid = nc.patientid WHEN MATCHED and NC.ACTION = 'D' THEN DELETE WHEN MATCHED and NC.ACTION = 'U' THEN UPDATE SET sat_end_dts = date_add(current_date(), ${i}),              patientid = nc.patientid,               age = nc.age,              gender = nc.gender,              ethnicity = nc.ethnicity,              action='U' WHEN NOT MATCHED AND NC.ACTION = 'I' THEN   INSERT (patientid, age, gender, ethnicity,action,sat_end_dts) VALUES   (nc.patientid, nc.age, nc.gender, nc.ethnicity,'I',date_add(current_date(), ${i})) ;"

    # Add commands here
    echo "CREATE UPDATE VIEW"
    RUN_SQL_CMD CREATE_UPD_VW 
    echo
    echo
    echo "CREATE CHECKSUM TABS"
    RUN_SQL_CMD CREATE_CKSUM_TABS
    echo
    echo "LOOK AT ACTION_DETAILS"
    RUN_SQL_CMD LOOK_AT_ACTION_DETAILS
    echo "DO MERGE"
    RUN_SQL_CMD DO_MERGE 
    echo
    echo
    echo
    echo "GET SNAPSHOTS"
    echo "select * from leqembi.alzh_narrow_p_change_dts_${MODE}.snapshots;"  | $BEELINE --silent=true --verbose=false   -n${USER}  -p ${PASS}   -u ${URL}
done

CHECK_TABLE="select count(patientid) , patientid from  leqembi.alzh_narrow_p_change_dts_${MODE}  group by patientid order by count(patientid) desc,patientid LIMIT 5;"

RUN_SQL_CMD CHECK_TABLE

echo "done"
