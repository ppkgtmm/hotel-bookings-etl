DROP DATABASE IF EXISTS olap_hotel;
CREATE DATABASE IF NOT EXISTS olap_hotel;

USE olap_hotel;

CREATE TABLE `stg_guest` (
  `id` integer,
  `email` varchar(255),
  `dob` date,
  `gender` varchar(25),
  `location` integer,
  `updated_at` datetime
);

CREATE TABLE `stg_room` (
  `id` integer,
  `type` integer,
  `updated_at` datetime
);

CREATE TABLE `stg_booking` (
  `id` integer PRIMARY KEY,
  `checkin` date,
  `checkout` date
);

CREATE TABLE `stg_booking_room` (
  `id` integer PRIMARY KEY,
  `booking` integer,
  `room` integer,
  `guest` integer,
  `updated_at` datetime,
  `processed` boolean DEFAULT false
);

CREATE TABLE `stg_booking_addon` (
  `id` integer PRIMARY KEY,
  `booking_room` integer,
  `addon` integer,
  `quantity` integer,
  `datetime` timestamp,
  `updated_at` datetime,
  `processed` boolean DEFAULT false
);

CREATE TABLE `dim_date` (
  `id` bigint PRIMARY KEY,
  `datetime` datetime,
  `date` date,
  `month` date,
  `quarter` date,
  `year` integer
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
  `id` integer PRIMARY KEY,
  `email` varchar(255),
  `dob` date,
  `gender` varchar(25)
);

CREATE TABLE `dim_location` (
  `id` integer PRIMARY KEY,
  `state` varchar(255),
  `country` varchar(255)
);

CREATE TABLE `fct_purchase` (
  `datetime` bigint,
  `guest` integer,
  `guest_location` integer,
  `roomtype` integer,
  `addon` integer,
  `addon_quantity` integer,
  PRIMARY KEY (`datetime`, `guest`, `guest_location`, `roomtype`, `addon`)
);

CREATE TABLE `fct_booking` (
  `datetime` bigint,
  `guest` integer,
  `guest_location` integer,
  `roomtype` integer,
  PRIMARY KEY (`datetime`, `guest`, `guest_location`, `roomtype`)
);

ALTER TABLE `fct_booking` ADD FOREIGN KEY (`datetime`) REFERENCES `dim_date` (`id`);

ALTER TABLE `fct_booking` ADD FOREIGN KEY (`guest`) REFERENCES `dim_guest` (`id`);

ALTER TABLE `fct_booking` ADD FOREIGN KEY (`roomtype`) REFERENCES `dim_roomtype` (`id`);

ALTER TABLE `fct_booking` ADD FOREIGN KEY (`guest_location`) REFERENCES `dim_location` (`id`);

ALTER TABLE `fct_purchase` ADD FOREIGN KEY (`datetime`) REFERENCES `dim_date` (`id`);

ALTER TABLE `fct_purchase` ADD FOREIGN KEY (`addon`) REFERENCES `dim_addon` (`id`);

ALTER TABLE `fct_purchase` ADD FOREIGN KEY (`guest`) REFERENCES `dim_guest` (`id`);

ALTER TABLE `fct_purchase` ADD FOREIGN KEY (`roomtype`) REFERENCES `dim_roomtype` (`id`);

ALTER TABLE `fct_purchase` ADD FOREIGN KEY (`guest_location`) REFERENCES `dim_location` (`id`);
