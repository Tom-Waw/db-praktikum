-- Anzahl der Produkte jedes Typs (Buch, Musik-CD, DVD)
SELECT 'book' AS type, COUNT(*) AS count FROM book
UNION ALL
SELECT 'music_cd' AS type, COUNT(*) AS count FROM cd
UNION ALL
SELECT 'dvd' AS type, COUNT(*) AS count FROM dvd;

-- Die 5 besten Produkte jedes Typs
WITH products_with_ratings AS (
SELECT 'book' AS type, book.id AS product_id, product.rating AS rating FROM book JOIN product ON book.id = product.id
UNION ALL
SELECT 'music_cd' AS type, cd.id AS product_id, product.rating AS rating FROM cd JOIN product ON cd.id = product.id
UNION ALL
SELECT 'dvd' AS type, dvd.id AS product_id, product.rating AS rating FROM dvd JOIN product ON dvd.id = product.id
)
SELECT * FROM (
SELECT type, product_id, rating,
ROW_NUMBER() OVER(PARTITION BY type ORDER BY rating DESC) AS rn
FROM products_with_ratings
) t WHERE rn <= 5;

-- Produkte ohne Angebot
SELECT product.id AS product_id FROM product LEFT JOIN branch_product ON product.id = branch_product.product_id WHERE branch_product.product_id IS NULL;

-- Produkte, bei denen das teuerste Angebot mehr als das Doppelte des billigsten kostet
SELECT product_id FROM branch_product GROUP BY product_id HAVING MAX(price) > 2 * MIN(price);

-- Produkte mit mindestens einer sehr schlechten (1) und mindestens einer sehr guten (5) Bewertung
SELECT product_id FROM review WHERE rating = 1 INTERSECT SELECT product_id FROM review WHERE rating = 5;

-- Anzahl der Produkte ohne Bewertung
SELECT COUNT(*) FROM product LEFT JOIN review ON product.id = review.product_id WHERE review.product_id IS NULL;

-- Rezensenten, die mindestens 10 Rezensionen geschrieben haben
SELECT customer_id FROM review GROUP BY customer_id HAVING COUNT(*) >= 10;

-- Autoren, die auch an DVDs oder Musik-CDs beteiligt sind
SELECT DISTINCT person.name FROM person
JOIN person_product pp1 ON person.id = pp1.person_id
JOIN book ON pp1.product_id = book.id
WHERE EXISTS (
SELECT 1 FROM person_product pp2
JOIN cd ON pp2.product_id = cd.id
WHERE pp2.person_id = person.id
) OR EXISTS (
SELECT 1 FROM person_product pp3
JOIN dvd ON pp3.product_id = dvd.id
WHERE pp3.person_id = person.id
)
ORDER BY person.name;

-- Durchschnittliche Anzahl von Songs auf einer CD
SELECT AVG(track_count) FROM (
SELECT COUNT(*) AS track_count FROM track GROUP BY cd_id
) AS track_counts;

-- Produkte, die in anderen Hauptkategorien ähnliche Produkte haben
WITH recursive main_categories AS (
SELECT id, name, parent_id FROM category WHERE parent_id IS NULL
UNION ALL
SELECT category.id, category.name, category.parent_id FROM category JOIN main_categories ON category.parent_id = main_categories.id
)
SELECT product_id FROM product_category pc1
WHERE EXISTS (
SELECT 1 FROM product_category pc2
JOIN main_categories ON pc2.category_id = main_categories.id
WHERE pc1.product_id <> pc2.product_id AND pc1.category_id <> pc2.category_id
);

-- Produkte, die in allen Filialen angeboten werden
SELECT product_id FROM branch_product GROUP BY product_id HAVING COUNT(DISTINCT branch_id) = (SELECT COUNT(*) FROM branch);

-- Anteil der Fälle, in denen das preiswerteste Angebot in Leipzig liegt
WITH cheapest_offers AS (
SELECT product_id, MIN(price) AS min_price FROM branch_product GROUP BY product_id
), leipzig_offers AS (
SELECT branch_product.product_id, branch_product.price FROM branch_product
JOIN branch ON branch_product.branch_id = branch.id
JOIN address ON branch.address_id = address.id
WHERE address.street LIKE '%Leipzig%'
)
SELECT COUNT() * 100.0 / (SELECT COUNT() FROM cheapest_offers) AS percentage
FROM cheapest_offers
JOIN leipzig_offers ON cheapest_offers.product_id = leipzig_offers.product_id AND cheapest_offers.min_price = leipzig_offers.price;