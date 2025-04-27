-- Create the table
CREATE TABLE IF NOT EXISTS graderoster (
    student_id TEXT,
    course_id TEXT,
    roll_no TEXT,
    email_id TEXT,
    grade TEXT
);

-- Load data from a CSV file
-- \COPY graderoster(student_id, course_id, roll_no, email_id, grade) FROM '/mydata/student_course_grades.csv' DELIMITER ',' CSV HEADER;
