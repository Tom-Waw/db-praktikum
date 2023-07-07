-- PRODUCTS TABLE
CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    asin VARCHAR(10) UNIQUE NOT NULL,
    name VARCHAR(255) NOT NULL,
    image TEXT,
    rank INTEGER,
    rating REAL CHECK (rating BETWEEN 1 AND 5)
);

CREATE TABLE recommendations (
    product_id INTEGER NOT NULL,
    recommended_product_id INTEGER NOT NULL,
    PRIMARY KEY (product_id, recommended_product_id),
    FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE,
    FOREIGN KEY (recommended_product_id) REFERENCES products(id) ON DELETE CASCADE
);

-- BOOKS TABLE
CREATE TABLE publishers (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) UNIQUE NOT NULL
);

CREATE TABLE books (
    product_id INTEGER PRIMARY KEY,
    isbn VARCHAR(13) NOT NULL,
    n_pages INTEGER NOT NULL,
    date_published DATE NOT NULL,
    publisher_id INTEGER NOT NULL,
    FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE,
    FOREIGN KEY (publisher_id) REFERENCES publishers(id) ON DELETE CASCADE
);

-- DVD TABLE
CREATE TABLE dvds (
    product_id INTEGER PRIMARY KEY,
    format VARCHAR(255) NOT NULL,
    duration INTEGER NOT NULL,
    region_code VARCHAR(255) NOT NULL,
    FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE
);

-- MUSIC TABLE
CREATE TABLE cds (
    product_id INTEGER PRIMARY KEY,
    label VARCHAR(255) NOT NULL,
    date_published DATE NOT NULL,
    FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE
);

CREATE TABLE tracks (
    id SERIAL PRIMARY KEY,
    cd_id INTEGER NOT NULL,
    title VARCHAR(255) NOT NULL,
    FOREIGN KEY (cd_id) REFERENCES cds(product_id) ON DELETE CASCADE
);

-- INVOLVED PERSONS
CREATE TABLE persons (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) UNIQUE NOT NULL
);

CREATE TABLE person_product (
    person_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    role VARCHAR(255) NOT NULL CHECK (role IN ('AUTHOR', 'ARTIST', 'ACTOR', 'CREATOR', 'DIRECTOR')),
    PRIMARY KEY (person_id, product_id, role),
    FOREIGN KEY (person_id) REFERENCES persons(id) ON DELETE CASCADE,
    FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE
);

CREATE OR REPLACE FUNCTION set_role()
RETURNS TRIGGER AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM books WHERE product_id = NEW.product_id) THEN
        NEW.role = 'AUTHOR';
    ELSIF EXISTS (SELECT 1 FROM cds WHERE product_id = NEW.product_id) THEN
        NEW.role = 'ARTIST';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER set_role_trigger
BEFORE INSERT OR UPDATE OR DELETE ON person_product
FOR EACH ROW EXECUTE PROCEDURE set_role();

-- CATEGORIES
CREATE TABLE categories (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    parent_id INTEGER,
    UNIQUE (name, parent_id),
    FOREIGN KEY (parent_id) REFERENCES categories(id) ON DELETE CASCADE
);

CREATE TABLE product_category (
    product_id INTEGER NOT NULL,
    category_id INTEGER NOT NULL,
    PRIMARY KEY (product_id, category_id),
    FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE,
    FOREIGN KEY (category_id) REFERENCES categories(id) ON DELETE CASCADE
);

-- SHOPS
CREATE TABLE addresses (
    id SERIAL PRIMARY KEY,
    street VARCHAR(255) NOT NULL,
    zip VARCHAR(255) NOT NULL
);

CREATE TABLE shops (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) UNIQUE NOT NULL,
    address_id INTEGER NOT NULL,
    FOREIGN KEY (address_id) REFERENCES addresses(id) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE TABLE shop_product (
    id SERIAL PRIMARY KEY,
    shop_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    price DECIMAL(8, 2),
    stock BOOLEAN NOT NULL,
    state VARCHAR(255) NOT NULL CHECK (state IN ('NEW', 'AS_NEW', 'USED')),
    UNIQUE (shop_id, product_id, state),
    FOREIGN KEY (shop_id) REFERENCES shops(id) ON DELETE CASCADE,
    FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE
);

-- CUSTOMERS
CREATE TABLE customers (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    iban CHAR(34),
    address_id INTEGER,
    FOREIGN KEY (address_id) REFERENCES addresses(id) ON DELETE SET NULL
);

CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER,
    shop_product_id INTEGER,
    date DATE NOT NULL,
    price DECIMAL(8, 2),
    FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE SET NULL,
    FOREIGN KEY (shop_product_id) REFERENCES shop_product(id) ON DELETE SET NULL
);

CREATE TABLE reviews (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    rating INTEGER NOT NULL CHECK (rating BETWEEN 1 AND 5),
    summary VARCHAR(255) NOT NULL,
    content TEXT NOT NULL,
    FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE CASCADE,
    FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE
);

CREATE OR REPLACE FUNCTION update_product_rating()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE products
    SET rating = (SELECT AVG(rating) FROM reviews WHERE product_id = NEW.product_id)
    WHERE id = NEW.product_id;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_rating_trigger
AFTER INSERT OR UPDATE OR DELETE ON reviews
FOR EACH ROW EXECUTE PROCEDURE update_product_rating();