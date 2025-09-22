Accounts = """
  CREATE TABLE IF NOT EXISTS account (
    customer_id integer primary key,
    first_name varchar(64) not null,
    last_name varchar(64) not null,
    address_1 varchar(64),
    address_2 varchar(64),
    city varchar(64),
    state varchar(16),
    zip_code integer,
    join_date date
  )
"""

Products = """
  CREATE TABLE IF NOT EXISTS product (
    product_id integer primary key,
    product_code integer,
    product_description varchar(265)
  )
"""

Transactions = """
  CREATE TABLE  IF NOT EXISTS transaction (
    transaction_id varchar(32) primary key,
    transaction_date date,
    product_id integer REFERENCES product,
    product_code integer,
    product_description varchar(265),
    quantity integer,
    account_id integer REFERENCES account
  )
"""
