DROP DATABASE IF EXISTS dwh_hotel;
CREATE DATABASE IF NOT EXISTS dwh_hotel;

USE dwh_hotel;

CREATE TABLE `stg_room` (
  `id` integer PRIMARY KEY,
  `type` integer,
  `updated_at` datetime,
  `is_deleted` boolean DEFAULT false
);

CREATE TABLE `stg_guest` (
  `id` integer PRIMARY KEY,
  `state` varchar(255),
  `country` varchar(255),
  `updated_at` datetime,
  `is_deleted` boolean DEFAULT false
);

CREATE TABLE `stg_booking` (
  `id` integer PRIMARY KEY,
  `checkin` date,
  `checkout` date,
  `updated_at` datetime,
  `is_deleted` boolean DEFAULT false
);

CREATE TABLE `stg_booking_room` (
  `id` integer PRIMARY KEY,
  `booking` integer,
  `room` integer,
  `guest` integer,
  `updated_at` datetime,
  `processed` boolean DEFAULT false,
  `is_deleted` boolean DEFAULT false
);

CREATE TABLE `stg_booking_addon` (
  `id` integer PRIMARY KEY,
  `booking_room` integer,
  `addon` integer,
  `quantity` integer,
  `datetime` timestamp,
  `updated_at` datetime,
  `processed` boolean DEFAULT false,
  `is_deleted` boolean DEFAULT false
);

CREATE TABLE `dim_date` (
  `id` bigint PRIMARY KEY,
  `datetime` datetime,
  `date` date,
  `month` date,
  `quarter` date,
  `year` date
);

CREATE TABLE `dim_roomtype` (
  `id` integer PRIMARY KEY AUTO_INCREMENT,
  `_id` integer,
  `name` varchar(255),
  `price` float,
  `created_at` datetime
);

CREATE TABLE `dim_addon` (
  `id` integer PRIMARY KEY AUTO_INCREMENT,
  `_id` integer,
  `name` varchar(255),
  `price` float,
  `created_at` datetime
);

CREATE TABLE `dim_guest` (
  `id` integer PRIMARY KEY AUTO_INCREMENT,
  `_id` integer,
  `email` varchar(255),
  `dob` date,
  `gender` varchar(25),
  `created_at` datetime
);

CREATE TABLE `dim_location` (
  `id` integer PRIMARY KEY AUTO_INCREMENT,
  `fips` varchar(10),
  `state` varchar(255),
  `country` varchar(255)
);

CREATE TABLE `fct_amenities` (
  `id` integer PRIMARY KEY AUTO_INCREMENT,
  `datetime` bigint,
  `guest` integer,
  `guest_location` integer,
  `roomtype` integer,
  `addon` integer,
  `addon_quantity` integer
);

CREATE TABLE `fct_bookings` (
  `id` integer PRIMARY KEY AUTO_INCREMENT,
  `datetime` bigint,
  `guest` integer,
  `guest_location` integer,
  `roomtype` integer
);

ALTER TABLE `fct_bookings` ADD FOREIGN KEY (`datetime`) REFERENCES `dim_date` (`id`);

ALTER TABLE `fct_bookings` ADD FOREIGN KEY (`guest`) REFERENCES `dim_guest` (`id`);

ALTER TABLE `fct_bookings` ADD FOREIGN KEY (`roomtype`) REFERENCES `dim_roomtype` (`id`);

ALTER TABLE `fct_bookings` ADD FOREIGN KEY (`guest_location`) REFERENCES `dim_location` (`id`);

ALTER TABLE `fct_amenities` ADD FOREIGN KEY (`datetime`) REFERENCES `dim_date` (`id`);

ALTER TABLE `fct_amenities` ADD FOREIGN KEY (`addon`) REFERENCES `dim_addon` (`id`);

ALTER TABLE `fct_amenities` ADD FOREIGN KEY (`guest`) REFERENCES `dim_guest` (`id`);

ALTER TABLE `fct_amenities` ADD FOREIGN KEY (`roomtype`) REFERENCES `dim_roomtype` (`id`);

ALTER TABLE `fct_amenities` ADD FOREIGN KEY (`guest_location`) REFERENCES `dim_location` (`id`);

CREATE TABLE `full_picture` (
  `date` date,
  `guest` integer,
  `dob` date,
  `age` integer,
  `age_range` varchar(255),
  `country` varchar(255),
  `roomtype` varchar(255),
  `price` integer,
  `addon` json,
  PRIMARY KEY (`date`, `guest`)
);

CREATE FUNCTION get_age_range(age INTEGER) RETURNS VARCHAR(25) DETERMINISTIC
RETURN CASE
		WHEN age < 20 THEN 'less than 20'
		WHEN age BETWEEN 20 AND 29 THEN '20 - 29'
		WHEN age BETWEEN 30 AND 39 THEN '30 - 39'
		WHEN age BETWEEN 40 AND 49 THEN '40 - 49'
		WHEN age BETWEEN 50 AND 59 THEN '50 - 59'
		ELSE 'at least 60'
END;
