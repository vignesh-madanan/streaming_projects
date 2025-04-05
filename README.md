# Streaming_projects
This projects goal is to create a simulated e-commerce websites backend.
Use these 3 DBs and create a high fidelity pipeline, to aggregate analysis of the simulation. 
The purpose is to learn each aspect of the data engineering pipeline.

### Higher level plan:
[ ] DB: Postgres SQL
    [ ] Setup
    [ ] DB creation
    [ ] Dummy/ Fake data generation
    
[ ] Kafka: messaging queue
    [ ] Setup

[ ] Airflow: orchestration
    [ ] Setup

[ ] Data warehouse: Big query
[ ] Data lake: Google cloud
[ ] VM: compute engine


### Requirements:
Setup steps involve installing packages/ libraries and spawning them using docker compose


#### Python requirements:
1. Faker
2. Postgres handler: psycopg2
3. Confluent kafka


### Data:
- user
id INT PRIMARY KEY
username VARCHAR
PASSWORD VARCHAR 

- ecommerce product
id int PRIMARY KEY
name VARCHAR(255)
description TEXT
price REAL

- click stream logs (logs of application clicks or webpage click)
checkout_id VARCHAR PRIMARY KEY
user_name VARCHAR
click_id VARCHAR
product_id VARCHAR
payment_method VARCHAR
total_amount DECIMAL(5, 2)
shipping_address VARCHAR
billing_address VARCHAR
user_agent VARCHAR
ip_address VARCHAR
checkout_time TIMESTAMP
click_time TIMESTAMP


### History:
#### Version 0.1:
- Created init.sql to create Table and schema
- Created docker compose to spawn the right services and netwrok management
