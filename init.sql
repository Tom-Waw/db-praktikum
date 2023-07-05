CREATE TABLE `product` (
    `id` INT PRIMARY KEY AUTO_INCREMENT,
    `asin` VARCHAR(10) UNIQUE NOT NULL,
    `name` VARCHAR(255) NOT NULL,
    `image` TEXT,
    `rank` INT,
    `rating` REAL CHECK (
        `rating` IS NULL
        OR (
            `rating` >= 1
            AND `rating` <= 5
        )
    )
);
CREATE TABLE `recommendation` (
    `product_id` INT NOT NULL,
    `recommended_product_id` INT NOT NULL,
    PRIMARY KEY (`product_id`, `recommended_product_id`),
    FOREIGN KEY (`product_id`) REFERENCES `product` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
    FOREIGN KEY (`recommended_product_id`) REFERENCES `product` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
);
-- BOOKS TABLE
CREATE TABLE `book`(
    `id` INT PRIMARY KEY,
    `ISBN` VARCHAR(13) NOT NULL,
    `n_pages` INT NOT NULL,
    `date_published` DATE NOT NULL,
    `publisher_id` INT NOT NULL,
    FOREIGN KEY (`id`) REFERENCES `product` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
);
CREATE TABLE `publisher` (
    `id` INT PRIMARY KEY AUTO_INCREMENT,
    `name` VARCHAR(255) UNIQUE NOT NULL
);
ALTER TABLE `book`
ADD FOREIGN KEY (`publisher_id`) REFERENCES `publisher` (`id`) ON DELETE CASCADE ON UPDATE CASCADE;
-- DVD TABLE
CREATE TABLE `dvd` (
    `id` INT PRIMARY KEY,
    `format` VARCHAR(255) NOT NULL,
    `duration` INT NOT NULL,
    `region_code` VARCHAR(255) NOT NULL,
    FOREIGN KEY (`id`) REFERENCES `product` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
);
-- MUSIC TABLE
CREATE TABLE `cd` (
    `id` INT PRIMARY KEY,
    `label` VARCHAR(255) NOT NULL,
    `date_published` DATE NOT NULL,
    FOREIGN KEY (`id`) REFERENCES `product` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
);
CREATE TABLE `track` (
    `id` INT PRIMARY KEY AUTO_INCREMENT,
    `cd_id` INT NOT NULL,
    `title` VARCHAR(255) NOT NULL,
    FOREIGN KEY (`cd_id`) REFERENCES `cd` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
);
-- INVOLVED PERSONS
CREATE TABLE `person` (
    `id` INT PRIMARY KEY AUTO_INCREMENT,
    `name` VARCHAR(255) UNIQUE NOT NULL
);
CREATE TABLE `person_product` (
    `person_id` INT NOT NULL,
    `product_id` INT NOT NULL,
    `role` ENUM(
        'AUTHOR',
        -- default for books
        'ARTIST',
        -- default for music/cds
        'ACTOR',
        'CREATOR',
        'DIRECTOR'
    ) NOT NULL,
    PRIMARY KEY (`person_id`, `product_id`, `role`),
    FOREIGN KEY (`person_id`) REFERENCES `person` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
    FOREIGN KEY (`product_id`) REFERENCES `product` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
);
-- CATEGORIES
CREATE TABLE `category` (
    `id` INT PRIMARY KEY AUTO_INCREMENT,
    `name` VARCHAR(255) NOT NULL,
    `parent_id` INT,
    UNIQUE KEY (`name`, `parent_id`),
    FOREIGN KEY (`parent_id`) REFERENCES `category` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
);
CREATE TABLE `product_category` (
    `product_id` INT NOT NULL,
    `category_id` INT NOT NULL,
    PRIMARY KEY (`product_id`, `category_id`),
    FOREIGN KEY (`product_id`) REFERENCES `product` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
    FOREIGN KEY (`category_id`) REFERENCES `category` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
);
-- BRANCHES
CREATE TABLE `address` (
    `id` INT PRIMARY KEY AUTO_INCREMENT,
    `street` VARCHAR(255) NOT NULL,
    `zip` VARCHAR(255) NOT NULL
);
CREATE TABLE `branch` (
    `id` INT PRIMARY KEY AUTO_INCREMENT,
    `name` VARCHAR(255) UNIQUE NOT NULL,
    `address_id` INT NOT NULL,
    FOREIGN KEY (`address_id`) REFERENCES `address` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
);
CREATE TABLE `branch_product` (
    `id` INT PRIMARY KEY AUTO_INCREMENT,
    `branch_id` INT NOT NULL,
    `product_id` INT NOT NULL,
    `price` DECIMAL(8, 2),
    `stock` BOOLEAN NOT NULL,
    `state` ENUM(
        'NEW',
        'AS_NEW',
        'USED' -- TODO more states?
    ) NOT NULL,
    UNIQUE KEY (`branch_id`, `product_id`, `state`),
    FOREIGN KEY (`branch_id`) REFERENCES `branch` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
    FOREIGN KEY (`product_id`) REFERENCES `product` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
);
-- CUSTOMERS
CREATE TABLE `customer` (
    `id` INT PRIMARY KEY AUTO_INCREMENT,
    `name` VARCHAR(255) NOT NULL,
    `iban` VARCHAR(50),
    `address_id` INT,
    FOREIGN KEY (`address_id`) REFERENCES `address` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
);
CREATE TABLE `order` (
    `id` INT PRIMARY KEY AUTO_INCREMENT,
    `customer_id` INT,
    `branch_product_id` INT,
    `date` DATE NOT NULL,
    `price` DECIMAL(8, 2) NOT NULL,
    FOREIGN KEY (`customer_id`) REFERENCES `customer` (`id`) ON DELETE
    SET NULL ON UPDATE CASCADE,
        FOREIGN KEY (`branch_product_id`) REFERENCES `branch_product` (`id`) ON DELETE
    SET NULL ON UPDATE CASCADE
);
CREATE TABLE `review` (
    `customer_id` INT NOT NULL,
    `product_id` INT NOT NULL,
    `rating` INT NOT NULL CHECK (
        `rating` >= 1
        AND `rating` <= 5
    ),
    `summary` VARCHAR(255) NOT NULL,
    `content` TEXT NOT NULL,
    PRIMARY KEY (`product_id`, `customer_id`),
    FOREIGN KEY (`customer_id`) REFERENCES `customer` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
    FOREIGN KEY (`product_id`) REFERENCES `product` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
);
-- TRIGGERS
CREATE TRIGGER `product_rating` BEFORE
INSERT ON `review` FOR EACH ROW
UPDATE `product`
SET `rating` = (
        SELECT AVG(`rating`)
        FROM `review`
        WHERE `product_id` = NEW.`product_id`
    )
WHERE `id` = NEW.`product_id`;
DELIMITER $$ --
CREATE TRIGGER `person_product_role` BEFORE
INSERT ON `person_product` FOR EACH ROW BEGIN IF EXISTS(
        SELECT 1
        FROM `book`
        WHERE `id` = NEW.`product_id`
    ) THEN
SET NEW.`role` = 'AUTHOR';
ELSEIF EXISTS(
    SELECT 1
    FROM `cd`
    WHERE `id` = NEW.`product_id`
) THEN
SET NEW.`role` = 'ARTIST';
END IF;
END $$ --
DELIMITER ;
CREATE TRIGGER `order_price` BEFORE
INSERT ON `order` FOR EACH ROW
SET NEW.`price` = (
        SELECT `price`
        FROM `branch_product`
        WHERE `id` = NEW.`branch_product_id`
    );