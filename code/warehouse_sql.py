bcreate_staging_receipts = ("""
     DROP TABLE IF EXISTS staging_receipts;
     CREATE TABLE staging_receipts (
          customer VARCHAR(255),
          order_id VARCHAR(255),
          store VARCHAR(255),
          destination VARCHAR(255),
          time TIMESTAMP,
          payment_method VARCHAR(50),
          raw_cost INTEGER,
          shipping_cost INTEGER,
          service_cost INTEGER,
          customer_paid INTEGER
     );
""")

create_staging_items = ("""
     DROP TABLE IF EXISTS staging_items;
     CREATE TABLE staging_items (
          quantity INTEGER,
          product_name VARCHAR(255),
          price INTEGER,
          order_id VARCHAR(255),
          note VARCHAR(255)
     );
""")

create_staging_promotions = ("""
     DROP TABLE IF EXISTS staging_promotions;
     CREATE TABLE staging_promotions (
          code VARCHAR(255),
          value INTEGER,
          order_id VARCHAR(255)
     );
""")

create_facts = ("""
     DROP TABLE IF EXISTS grabfood_facts;
     CREATE TABLE grabfood_facts (
          order_id VARCHAR(14) PRIMARY KEY,
          time_id INTEGER,
          customer_id INTEGER,
          store_id INTEGER,
          destination_id INTEGER,
          payment_method_id INTEGER,
          raw_cost INTEGER,
          shipping_cost INTEGER,
          service_cost INTEGER,
          final_cost INTEGER
     );
""")

create_payment_method_dim = ("""
     DROP TABLE IF EXISTS payment_method_dim;
     CREATE TABLE payment_method_dim (
          payment_method_id INTEGER AUTO_INCREMENT PRIMARY KEY,
          payment_method VARCHAR(255)
     );
""")

create_time_dim = ("""
     DROP TABLE IF EXISTS time_dim;
     CREATE TABLE time_dim (
          time_id INTEGER AUTO_INCREMENT PRIMARY KEY,
          minute INTEGER,
          hour INTEGER,
          date INTEGER,
          date_of_week VARCHAR(255),
          month VARCHAR(255),
          year VARCHAR(255),
          full_time TIMESTAMP,
          is_holiday TINYINT(1) 
     );
""")

create_location_dim = ("""
     DROP TABLE IF EXISTS location_dim;
     CREATE TABLE location_dim (
          location_id INTEGER AUTO_INCREMENT PRIMARY KEY,
          address VARCHAR(255)
     );
""")

create_item_dim = ("""
     DROP TABLE IF EXISTS item_dim;
     CREATE TABLE item_dim (
          orderitem_id INTEGER AUTO_INCREMENT PRIMARY KEY,
          order_id VARCHAR(14),
          product_name VARCHAR(255),
          quantity INTEGER,
          cost INTEGER,
          note VARCHAR(1000)
     );
""")

create_promotion_dim = ("""
     DROP TABLE IF EXISTS promotion_dim;
     CREATE TABLE promotion_dim (
          promotion_id INTEGER AUTO_INCREMENT PRIMARY KEY,
          order_id VARCHAR(14),
          code VARCHAR(255),
          value INTEGER
     );
""")

create_customer_dim = ("""
     DROP TABLE IF EXISTS customer_dim;
     CREATE TABLE customer_dim (
          customer_id INTEGER AUTO_INCREMENT PRIMARY KEY,
          username VARCHAR(255)
     );
""")

create_store_dim = ("""
     DROP TABLE IF EXISTS store_dim;
     CREATE TABLE store_dim (
          store_id INTEGER AUTO_INCREMENT PRIMARY KEY,
          store_name VARCHAR(255),
          description VARCHAR(1000),
          location_id INTEGER
     );
""")

load_staging_receipts = ("""
     LOAD DATA LOCAL INFILE '/home/sky/Documents/prj/grab-tracking/data/data/all_receipt.csv' 
     INTO TABLE staging_receipts 
     FIELDS TERMINATED BY ',' 
     ENCLOSED BY '"' 
     LINES TERMINATED BY '\r\n' 
     IGNORE 1 ROWS;
""")

load_payment_method_dim = ("""
     INSERT INTO payment_method_dim(payment_method) (
          SELECT DISTINCT(payment_method) FROM staging_receipts
     );
""")

load_location_dim = ("""
     INSERT INTO location_dim(address) (
          SELECT DISTINCT(destination) FROM staging_receipts
     );
""")

load_item_dim = ("""
     INSERT INTO item_dim(order_id, product_name, quantity, cost, note) (
          SELECT DISTINCT
               order_id, 
               product_name, 
               quantity, 
               price, 
               note 
          FROM staging_items
     );
""")

load_promotion_dim = ("""
     INSERT INTO promotion_dim(order_id,code,value) (
          SELECT DISTINCT 
               order_id, 
               code, 
               value 
          FROM staging_promotions
     );
""")

load_customer_dim = ("""
     INSERT INTO customer_dim(customername) (
          SELECT DISTINCT(customer) FROM staging_receipts
     );
""")

load_store_dim = ("""
     INSERT INTO store_dim(store_name) (
          SELECT DISTINCT(store) FROM staging_receipts      
     );
""")

load_time_dim = ("""
     INSERT INTO time_dim(minute,hour,date,date_of_week,month,year,full_time) (
          SELECT DISTNCT 
               MINUTE(time),
               HOUR(time),
               DAY(time),
               DAYNAME(time),
               MONTH(time),
               YEAR(time),
               time 
          FROM staging_receipts   
     );
""")

load_facts = ("""
     INSERT INTO grabfood_facts(order_id,time_id,customer_id,store_id,destination_id,payment_method_id,raw_cost,shipping_cost,service_cost,final_cost) (
          SELECT DISTINCT 
               s.order_id,
               (SELECT time_id FROM time_dim WHERE s.time = full_time),
               (SELECT customer_id FROM customer_dim WHERE s.customer = customername),
               (SELECT store_id FROM store_dim WHERE s.store = store_name),
               (SELECT location_id FROM location_dim WHERE s.destination = address),
               (SELECT payment_method_id FROM payment_method_dim WHERE s.payment_method = payment_method),
               raw_cost,
               shipping_cost,
               service_cost,
               customer_paid
          FROM staging_receipts s
     );    
""")

alter_grabfood_fact_customerid_foreign = ("""
    ALTER TABLE grabfood_fact ADD CONSTRAINT grabfood_fact_customerid_foreign FOREIGN KEY (customer_id) REFERENCES customer_dim(customer_id);
""")

alter_promotion_dim_orderid_foreign = ("""
    ALTER TABLE promotion_dim ADD CONSTRAINT promotion_dim_orderid_foreign FOREIGN KEY (order_id) REFERENCES grabfood_fact(order_id);
""")

alter_grabfood_fact_destinationid_foreign = ("""
    ALTER TABLE grabfood_fact ADD CONSTRAINT grabfood_fact_destinationid_foreign FOREIGN KEY (destination_id) REFERENCES location_dim(location_id);
""")

alter_grabfood_fact_timeid_foreign = ("""
    ALTER TABLE grabfood_fact ADD CONSTRAINT grabfood_fact_timeid_foreign FOREIGN KEY (time_id) REFERENCES time_dim(time_id);
""")

alter_restaurant_dim_locationid_foreign = ("""
    ALTER TABLE restaurant_dim ADD CONSTRAINT restaurant_dim_locationid_foreign FOREIGN KEY (location_id) REFERENCES location_dim(location_id);
""")

alter_item_dimension_orderid_foreign = ("""
    ALTER TABLE item_dimension ADD CONSTRAINT item_dimension_orderid_foreign FOREIGN KEY (order_id) REFERENCES grabfood_fact(order_id);
""")

alter_grabfood_fact_paymentmethodid_foreign = ("""
    ALTER TABLE grabfood_fact ADD CONSTRAINT grabfood_fact_paymentmethodid_foreign FOREIGN KEY (payment_method_id) REFERENCES payment_method_dim(payment_method_id);
""")

alter_grabfood_fact_restaurantid_foreign = ("""
    ALTER TABLE grabfood_fact ADD CONSTRAINT grabfood_fact_restaurantid_foreign FOREIGN KEY (restaurant_id) REFERENCES restaurant_dim(restaurant_id);
""")
