import psycopg2
from create_tables import Accounts, Products, Transactions
from create_index import account_id_index, product_id_index, transaction_id_index

data_files = {
    'account': './data/accounts.csv',
    'product': './data/products.csv',
    'transaction': './data/transactions.csv'
}


def main():

    host = "localhost"
    database = "postgres"
    user = "postgres"
    pas = "postgres"
    try:
        # connect to PostgreSQL
        conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=pas
        )
        cur = conn.cursor()

        cur.execute(Accounts)
        cur.execute(Products)
        cur.execute(Transactions)

        cur.execute(account_id_index)
        cur.execute(product_id_index)
        cur.execute(transaction_id_index)
        for table, file in data_files.items():
            sql_command = f"""
                    COPY {table} from stdin
                    DELIMiTER AS ','
                    CSV HEADER
                """
            with open(file) as f:
                cur.copy_expert(sql_command, f)
        # commit changes
        conn.commit()

        print("Tables created successfully.")

    except Exception as e:
        print("Error:", e)

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


if __name__ == "__main__":
    main()
