-- Create the table (external if you want to manage files yourself)
CREATE TABLE IF NOT EXISTS student_data (
    student_ID STRING,
    course_id STRING,
    roll_no STRING,
    email_ID STRING,
    grade STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1");

-- Load the data from a CSV file (replace with your HDFS/local path)
LOAD DATA LOCAL INPATH '/opt/hive/mydata/student_course_grades.csv'
INTO TABLE student_data;
