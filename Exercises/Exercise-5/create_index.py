account_id_index = """
  create unique index IF NOT EXISTS account_id_idx on account(customer_id)
"""

product_id_index = """
  create unique index IF NOT EXISTS product_id_idx on product(product_id)
"""

transaction_id_index = """
  create unique index IF NOT EXISTS transaction_id_idx on transaction(transaction_id)
"""
