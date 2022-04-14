import json
import psycopg2
# from sql_queries import proc
import os 

cwd = os.getcwd()
keys_file = os.path.join(cwd, 'keys.json')
keys = json.load(open(keys_file))
    
conn_params = (keys['Redshift']['Endpoint'],
                    keys['DWH']['DWH_DB'],
                    keys['DWH']['DWH_DB_USER'],
                    keys['DWH']['DWH_DB_PASSWORD'],
                    keys['DWH']['DWH_PORT']
                    )
conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*conn_params))
#Set autocommit to true so we can do DDL in sequence
conn.autocommit = True
print("Connected.")
cur = conn.cursor()

def format_rows(num):
    """
    This function formats large numbers into more readable formats:
    This function is to be used with the check_data() function.
    Input: 12173987 => Output: '12.17M Rows'
    """
    if num > 10**9:
        formatted_str = round(num/10**9,2)
        tens = 'B'

    elif num > 10**6:
        formatted_str = round(num/10**6,2)
        tens  = 'M'

    elif num > 10**3:
        formatted_str = round(num/10**3,2)
        tens = 'K'

    else:
        formatted_str = num
        tens = ''

    return f"{formatted_str}{tens} Rows"


def check_data(cur, table):
    try:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        nrow =  cur.fetchone()[0]
        if nrow == 0:
            raise ValueError("No Data in Table")
        else:
            nrow = format_rows(nrow)
            message = f"TABLE {table} EXISTS. {nrow} rows in Table"
            print(message)

    except:
        raise AttributeError(f"{table} Does Not Exist")


check_tables = ["STAGING_NYC_PROPERTY_SALES","STAGING_NYC_CRIME", "CRIMES","PROPERTY","NYC_PROPERTY_AND_CRIME"]


def main():
    # Check each table in database.
    for table in check_tables:
        check_data(cur, table)

    conn.close()


if __name__ == "__main__":
    main()
