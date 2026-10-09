-- Official MySQL image also creates MYSQL_DATABASE / MYSQL_USER from env.
-- This script is a safety net if those vars are missing on first boot.
CREATE DATABASE IF NOT EXISTS ridehail CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
