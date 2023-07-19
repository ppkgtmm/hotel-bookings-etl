DROP DATABASE IF EXISTS olap_hotel;
CREATE DATABASE IF NOT EXISTS olap_hotel;

USE olap_hotel;

CREATE TABLE `stg_location` (
  `id` integer PRIMARY KEY,
  `state` varchar(255),
  `country` varchar(255),
  `created_at` timestamp,
  `updated_at` timestamp
);

CREATE TABLE `stg_users` (
  `id` integer PRIMARY KEY,
  `firstname` varchar(255),
  `lastname` varchar(255),
  `email` varchar(255),
  `location` integer,
  `created_at` timestamp,
  `updated_at` timestamp
);

CREATE TABLE `stg_guests` (
  `id` integer PRIMARY KEY,
  `firstname` varchar(255),
  `lastname` varchar(255),
  `email` varchar(255),
  `dob` date,
  `location` integer,
  `created_at` timestamp,
  `updated_at` timestamp
);

CREATE TABLE `stg_addons` (
  `id` integer PRIMARY KEY,
  `name` varchar(255),
  `price` float,
  `created_at` timestamp,
  `updated_at` timestamp
);

CREATE TABLE `stg_roomtypes` (
  `id` integer PRIMARY KEY,
  `name` varchar(255),
  `price` float,
  `created_at` timestamp,
  `updated_at` timestamp
);

CREATE TABLE `stg_rooms` (
  `id` integer PRIMARY KEY,
  `floor` integer,
  `number` integer,
  `type` integer,
  `created_at` timestamp,
  `updated_at` timestamp
);

CREATE TABLE `stg_bookings` (
  `id` integer PRIMARY KEY,
  `user` integer,
  `checkin` date,
  `checkout` date,
  `payment` timestamp,
  `created_at` timestamp,
  `updated_at` timestamp
);

CREATE TABLE `stg_booking_rooms` (
  `id` integer PRIMARY KEY,
  `booking` integer,
  `room` integer,
  `guest` integer,
  `created_at` timestamp,
  `updated_at` timestamp
);

CREATE TABLE `stg_booking_addons` (
  `id` integer PRIMARY KEY,
  `booking_room` integer,
  `addon` integer,
  `quantity` integer,
  `datetime` timestamp,
  `created_at` timestamp,
  `updated_at` timestamp
);

CREATE TABLE `dim_date` (
  `id` integer PRIMARY KEY,
  `date` date,
  `year` integer,
  `month` integer,
  `day` integer,
  `weekday` varchar(255)
);

CREATE TABLE `dim_time` (
  `id` integer PRIMARY KEY,
  `hour` integer,
  `min` integer
);

CREATE TABLE `dim_roomtype` (
  `id` integer PRIMARY KEY,
  `_id` integer,
  `name` varchar(255),
  `price` float
);

CREATE TABLE `dim_room` (
  `id` integer PRIMARY KEY,
  `floor` integer,
  `number` integer
);

CREATE TABLE `dim_addon` (
  `id` integer PRIMARY KEY,
  `_id` integer,
  `name` varchar(255),
  `price` float
);

CREATE TABLE `dim_guest` (
  `id` integer PRIMARY KEY,
  `_id` integer,
  `email` varchar(255),
  `dob` date,
  `state` varchar(255),
  `country` varchar(255)
);

CREATE TABLE `fct_transaction` (
  `date` integer,
  `time` integer,
  `guest` integer,
  `room` integer,
  `roomtype` integer,
  `addon` integer,
  `addon_quantity` integer,
  PRIMARY KEY (`date`, `time`, `guest`, `room`, `roomtype`, `addon`)
);

ALTER TABLE `fct_transaction` ADD FOREIGN KEY (`date`) REFERENCES `dim_date` (`id`);

ALTER TABLE `fct_transaction` ADD FOREIGN KEY (`time`) REFERENCES `dim_time` (`id`);

ALTER TABLE `fct_transaction` ADD FOREIGN KEY (`addon`) REFERENCES `dim_addon` (`id`);

ALTER TABLE `fct_transaction` ADD FOREIGN KEY (`guest`) REFERENCES `dim_guest` (`id`);

ALTER TABLE `fct_transaction` ADD FOREIGN KEY (`room`) REFERENCES `dim_room` (`id`);

ALTER TABLE `fct_transaction` ADD FOREIGN KEY (`roomtype`) REFERENCES `dim_roomtype` (`id`);
