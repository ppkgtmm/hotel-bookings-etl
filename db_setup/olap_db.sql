DROP DATABASE IF EXISTS olap_hotel;
CREATE DATABASE IF NOT EXISTS olap_hotel;

USE olap_hotel;

CREATE TABLE `users` (
  `id` integer PRIMARY KEY,
  `firstname` varchar(255),
  `lastname` varchar(255),
  `email` varchar(255),
  `state` varchar(255),
  `country` varchar(255),
  `created_at` timestamp,
  `updated_at` timestamp
);

CREATE TABLE `guests` (
  `id` integer PRIMARY KEY,
  `firstname` varchar(255),
  `lastname` varchar(255),
  `email` varchar(255),
  `dob` date,
  `state` varchar(255),
  `country` varchar(255),
  `created_at` timestamp,
  `updated_at` timestamp
);

CREATE TABLE `addons` (
  `id` integer PRIMARY KEY,
  `name` varchar(255),
  `price` float,
  `created_at` timestamp,
  `updated_at` timestamp
);

CREATE TABLE `roomtypes` (
  `id` integer PRIMARY KEY,
  `name` varchar(255),
  `price` float,
  `created_at` timestamp,
  `updated_at` timestamp
);

CREATE TABLE `rooms` (
  `id` integer PRIMARY KEY,
  `floor` integer,
  `number` integer,
  `type` integer,
  `created_at` timestamp,
  `updated_at` timestamp
);

CREATE TABLE `bookings` (
  `id` integer PRIMARY KEY,
  `user` integer,
  `guest` integer,
  `checkin` date,
  `checkout` date,
  `payment` timestamp,
  `created_at` timestamp,
  `updated_at` timestamp
);

CREATE TABLE `booking_rooms` (
  `id` integer PRIMARY KEY,
  `booking` integer,
  `room` integer,
  `created_at` timestamp,
  `updated_at` timestamp
);

CREATE TABLE `booking_room_addons` (
  `id` integer PRIMARY KEY,
  `bookingrooms` integer,
  `addon` integer,
  `quantity` integer,
  `date` date,
  `created_at` timestamp,
  `updated_at` timestamp
);
