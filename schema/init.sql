CREATE DATABASE IF NOT EXISTS  `assignment`;

USE `assignment`;

-- TABLES --
CREATE TABLE IF NOT EXISTS `people` (
  `id` int NOT NULL AUTO_INCREMENT,
  `given_name` VARCHAR(100) NOT NULL,
  `family_name` VARCHAR(100) NOT NULL,
  `date_of_birth` DATE NOT NULL,
  `place_of_birth` VARCHAR(100),
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


CREATE TABLE IF NOT EXISTS `places` (
  `id` int NOT NULL AUTO_INCREMENT,
  `city` VARCHAR(100) NOT NULL,
  `county` VARCHAR(100) NOT NULL,
  `country` VARCHAR(100) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;