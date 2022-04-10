import json
import psycopg2
from sql_queries import proc
import os 

cwd = os.getcwd()
keys_file = os.path.join(cwd, 'keys.json')
keys = json.load(open(keys_file))


def fill_stage(cur, conn):
    """
        Function Purpose: 
            This function executes SQL statements imported from the sql_queries.py script. 
            This is done in order of operation to reset the stage tables by dropping the old tables, creating the new tables,
            and finnaly copying of raw data from S3 to each staginng table. 
            This function used the imported dictionary of DML/DDL Statements and loops thorugh the SQL using keys to index them.
            proc['Staging']['Drop Old'] will give us the drop statements for the staging tables as list: (shown below) 
            ['DROP TABLE STAGING_NYC_CRIME;', 'DROP TABLE STAGING_NYC_PROPERTY_SALES;']
            
        Args:
            cur (psycopg2.connection): A psycopg2 PostgreSQL connection cursor.
            conn (psycopg2.connection.cursor): A psycopg2 PostgreSQL connection cursor.

        Returns:
            None
    """
    sql = proc['Staging']

    for drop_old in sql['Drop Old']:
        print(drop_old)
        cur.execute(drop_old)
        conn.commit()
        print('Success.')

    for create_new in sql['Create New']:
        print(create_new.split("\n")[0]) # Only print first line that has table name
        cur.execute(create_new)
        conn.commit()
        print('Success.')

    for copy_into in sql['Copy Into']:
        print(copy_into.split("\n")[0]) # Only print first line that has table name
        cur.execute(copy_into)
        conn.commit()
        print('Success.')

def create_analysis_tables_from_stage(cur, conn):
    """
        Function Purpose: 
            This function executes SQL statements imported from the sql_queries.py script. 
            This is done in order of operation to reset the data tables by dropping the old tables, creating the new tables,
            and finnaly copying of raw data from S3 to each staginng table. 
            This function will call SQL to select columns from the stage(raw) data tables and add additional fields. 
            
        Args:
            cur (psycopg2.connection): A psycopg2 PostgreSQL connection cursor.
            conn (psycopg2.connection.cursor): A psycopg2 PostgreSQL connection cursor.

        Returns:
            None
    """
    sql = proc['Analysis']

    for drop_old in sql['Drop Old']:
        print(drop_old)
        cur.execute(drop_old)
        conn.commit()
        print('Success.')

    for create_as in sql['Create Table As']:
        print(create_as.split("\n")[0]) # Only print first line that has table name
        cur.execute(create_as)
        conn.commit()
        print('Success.')

def main():
    """
        Function Purpose: 
            The main function in this script will connect to the Redshift PostgreSQL data warehouse 
            and then call the drop_tables and create_tables to set up the database for ETL.
            Then this function will close the connection to the database.
        Args:
            None

        Returns:
            None
    """
    

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
    

    # Do ETL
    fill_stage(cur,conn)
    create_analysis_tables_from_stage(cur, conn)


    conn.close()


if __name__ == "__main__":
    main()
