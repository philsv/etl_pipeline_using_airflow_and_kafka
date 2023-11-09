CREATE DATABASE IF NOT EXISTS tolldata;

USE tolldata;

CREATE TABLE livetolldata (
    timestamp DATETIME,
    vehicle_id INT,
    vehicle_type CHAR(15),
    toll_plaza_id SMALLINT
);
