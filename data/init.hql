SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
SET hive.support.concurrency=true;
SET hive.enforce.bucketing=true; -- Optional in Hive 3.x and later
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.compactor.initiator.on=true;
SET hive.compactor.worker.threads=1;

DROP TABLE IF EXISTS graderoster;

CREATE TABLE graderoster (
    student_id STRING,
    course_id STRING,
    roll_no STRING,
    email_id STRING,
    grade STRING
)
STORED AS ORC
TBLPROPERTIES (
    "transactional"="true",
    "orc.compress"="ZLIB"
);

-- Step 1: Create a staging table using TEXTFILE
DROP TABLE IF EXISTS graderoster_stage;

CREATE EXTERNAL TABLE graderoster_stage (
    student_id STRING,
    course_id STRING,
    roll_no STRING,
    email_id STRING,
    grade STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1");

-- Step 2: Load data into the staging table
-- LOAD DATA LOCAL INPATH '/opt/hive/mydata/student_course_grades.csv'
-- INTO TABLE graderoster_stage;

-- Step 3: Insert into ORC table
-- INSERT INTO TABLE graderoster SELECT * FROM graderoster_stage;
