DROP DATABASE IF EXISTS oltp_hotel;
CREATE DATABASE IF NOT EXISTS oltp_hotel;
USE oltp_hotel;

CREATE TABLE `location` (
  `id` integer PRIMARY KEY AUTO_INCREMENT,
  `state` varchar(255),
  `country` varchar(255)
);

CREATE TABLE `users` (
  `id` integer PRIMARY KEY AUTO_INCREMENT,
  `firstname` varchar(255),
  `lastname` varchar(255),
  `gender` varchar(25),
  `email` varchar(255),
  `location` integer
);

CREATE TABLE `guests` (
  `id` integer PRIMARY KEY AUTO_INCREMENT,
  `firstname` varchar(255),
  `lastname` varchar(255),
  `gender` varchar(25),
  `email` varchar(255),
  `dob` date,
  `location` integer
);

CREATE TABLE `addons` (
  `id` integer PRIMARY KEY AUTO_INCREMENT,
  `name` varchar(255),
  `price` float
);

CREATE TABLE `roomtypes` (
  `id` integer PRIMARY KEY AUTO_INCREMENT,
  `name` varchar(255),
  `price` float
);

CREATE TABLE `rooms` (
  `id` integer PRIMARY KEY AUTO_INCREMENT,
  `floor` integer,
  `number` integer,
  `type` integer
);

CREATE TABLE `bookings` (
  `id` integer PRIMARY KEY AUTO_INCREMENT,
  `user` integer,
  `checkin` date,
  `checkout` date,
  `payment` datetime
);

CREATE TABLE `booking_rooms` (
  `id` integer PRIMARY KEY AUTO_INCREMENT,
  `booking` integer,
  `room` integer,
  `guest` integer
);

CREATE TABLE `booking_addons` (
  `id` integer PRIMARY KEY AUTO_INCREMENT,
  `booking_room` integer,
  `addon` integer,
  `quantity` integer,
  `datetime` datetime
);

ALTER TABLE `rooms` ADD FOREIGN KEY (`type`) REFERENCES `roomtypes` (`id`);

ALTER TABLE `bookings` ADD FOREIGN KEY (`user`) REFERENCES `users` (`id`);

ALTER TABLE `booking_rooms` ADD FOREIGN KEY (`guest`) REFERENCES `guests` (`id`);

ALTER TABLE `booking_rooms` ADD FOREIGN KEY (`room`) REFERENCES `rooms` (`id`);

ALTER TABLE `booking_rooms` ADD FOREIGN KEY (`booking`) REFERENCES `bookings` (`id`);

ALTER TABLE `booking_addons` ADD FOREIGN KEY (`addon`) REFERENCES `addons` (`id`);

ALTER TABLE `booking_addons` ADD FOREIGN KEY (`booking_room`) REFERENCES `booking_rooms` (`id`);

ALTER TABLE `users` ADD FOREIGN KEY (`location`) REFERENCES `location` (`id`);

ALTER TABLE `guests` ADD FOREIGN KEY (`location`) REFERENCES `location` (`id`);

ALTER TABLE `location`
ADD COLUMN `created_at` datetime DEFAULT CURRENT_TIMESTAMP,
ADD COLUMN `updated_at` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP;

ALTER TABLE `users`
ADD COLUMN `created_at` datetime DEFAULT CURRENT_TIMESTAMP,
ADD COLUMN `updated_at` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP;

ALTER TABLE `guests`
ADD COLUMN `created_at` datetime DEFAULT CURRENT_TIMESTAMP,
ADD COLUMN `updated_at` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP;

ALTER TABLE `addons`
ADD COLUMN `created_at` datetime DEFAULT CURRENT_TIMESTAMP,
ADD COLUMN `updated_at` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP;

ALTER TABLE `roomtypes`
ADD COLUMN `created_at` datetime DEFAULT CURRENT_TIMESTAMP,
ADD COLUMN `updated_at` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP;

ALTER TABLE `rooms`
ADD COLUMN `created_at` datetime DEFAULT CURRENT_TIMESTAMP,
ADD COLUMN `updated_at` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP;

ALTER TABLE `bookings`
ADD COLUMN `created_at` datetime DEFAULT CURRENT_TIMESTAMP,
ADD COLUMN `updated_at` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP;

ALTER TABLE `booking_rooms`
ADD COLUMN `created_at` datetime DEFAULT CURRENT_TIMESTAMP,
ADD COLUMN `updated_at` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP;

ALTER TABLE `booking_addons`
ADD COLUMN `created_at` datetime DEFAULT CURRENT_TIMESTAMP,
ADD COLUMN `updated_at` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP;
