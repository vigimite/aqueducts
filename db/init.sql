CREATE TABLE temp_readings (
    location_id INTEGER,
    timestamp TIMESTAMP,
    temperature_c FLOAT,
    humidity FLOAT,
    weather_condition VARCHAR(50)
);

CREATE TABLE temp_readings_empty (
    location_id INTEGER,
    timestamp TIMESTAMP,
    temperature_c FLOAT,
    humidity FLOAT,
    weather_condition VARCHAR(50)
);

COPY temp_readings FROM '/opt/temp_readings_jan_2024.csv' DELIMITER ',' CSV HEADER;
COPY temp_readings FROM '/opt/temp_readings_feb_2024.csv' DELIMITER ',' CSV HEADER;
