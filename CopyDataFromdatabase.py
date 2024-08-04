import psycopg2
from psycopg2 import sql
from datetime import datetime
import re

import pandas as pd
# Connection parameters for source database
source_db_config = {
    'user': 'tomer',
    'password': 't1',
    'host': 'localhost',
    'port': '5433',
    'database': 'appdb1'
}

# Connection parameters for destination database
destination_db_config = {
    'user': 'tomer',
    'password': 't1',
    'host': 'localhost',
    'port': '5433',
    'database': 'appData'
}

def get_columns(table_name, cursor):
    query_command = f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = '{table_name}'
        ORDER BY ordinal_position
    """
    cursor.execute(query_command)
    columns = [row[0] for row in cursor.fetchall()]
    return columns

def fetch_data_from_source(tablename, exclude_columns):
    try:
        # Connect to the source database
        source_conn = psycopg2.connect(**source_db_config)
        source_cursor = source_conn.cursor()

        # Get all column names
        all_columns = get_columns(tablename, source_cursor)

        # Exclude specified columns
        colnames = [col for col in all_columns if col not in exclude_columns]

        # Fetch data from source database
        source_cursor.execute(sql.SQL("SELECT {} FROM {}").format(
            sql.SQL(', ').join(map(sql.Identifier, colnames)),
            sql.Identifier(tablename)
        ))
        data = source_cursor.fetchall()

        # Close connection
        source_cursor.close()
        source_conn.close()

        return data, colnames
    except Exception as e:
        print(f"Error fetching data from source: {e}")
        return None, None

import psycopg2

def insert_data_into_destination(tablename, data, colnames):
    try:
        # Connect to the destination database
        dest_conn = psycopg2.connect(**destination_db_config)
        dest_cursor = dest_conn.cursor()

        # Iterate over each row in the data
        for row in data:
            # Build the insert query with actual values for each row
            try:
             insert_query = build_query_with_values(tablename, colnames, row)
             # Print the query before execution
             print(f"Executing query: {insert_query}")
             dest_cursor.execute(insert_query)
             dest_conn.commit()
            except Exception as e:
             print(f"Error inserting data into destination: {e}")
             dest_conn.rollback()
        # Commit changes
        dest_conn.commit()

        # Close connection
        dest_cursor.close()
        dest_conn.close()
    except Exception as e:
        print(f"Error inserting data into destination: {e}")

def build_query_with_values(tablename, colnames, row):
    # Format column names and values for the insert query
    colnames_str = ", ".join(colnames)
    values_str = ", ".join([f"'{str(value)}'" for value in row])
    query = f"INSERT INTO {tablename} ({colnames_str}) VALUES ({values_str})"
    return query

def calculate_age(birthdate, reference_date):
    age = reference_date.year - birthdate.year
    return age

def process_crf_table():
        tablename = 'crf'
        exclude_columns = ['id', 'phonenumber', 'email', 'firstname', 'lastname']

        try:
            # Connect to the source database
            source_conn = psycopg2.connect(**source_db_config)
            source_cursor = source_conn.cursor()
            dest_conn = psycopg2.connect(**destination_db_config)
            dest_cursor = dest_conn.cursor()
            # Get all column names
            all_columns = get_columns(tablename, source_cursor)

            # Exclude specified columns
            colnames = [col for col in all_columns if col not in exclude_columns]
            colnames = ['age' if col == 'dob' else col for col in colnames]

            # Fetch data from source database
            source_cursor.execute(sql.SQL("SELECT {} FROM {}").format(
                sql.SQL(', ').join(map(sql.Identifier, [col for col in all_columns if col not in exclude_columns])),
                sql.Identifier(tablename)
            ))
            data = source_cursor.fetchall()
            for row_index, row in enumerate(data):
                # Convert string to timestamp
                birthdate_str = row[3].split('.')[0] if '.' in row[3] else row[3]
                if birthdate_str!='NaT':
                   if re.match(r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$', birthdate_str):
                      birthdate = datetime.strptime(birthdate_str, '%Y-%m-%d %H:%M:%S')  # Adjust format if necessary
                      reference_date = row[0]
                      age = reference_date.year - birthdate.year
                   elif  re.match(r'^\d{2}/\d{2}/\d{4}$', birthdate_str):
                       birthdate = datetime.strptime(birthdate_str, '%d/%m/%Y')                         # Adjust format if necessary
                       reference_date = row[0]
                       age = reference_date.year - birthdate.year
                else:
                   age='NaT'

                row = list(row)
                row[3] = age
                data[row_index] = tuple(row)
            # Calculate age and prepare data for insertion
        except Exception as e:
            print(f"Error processing CRF table: {e}")
        insert_data_into_destination(tablename, data, colnames)

def main():
        tablenames = ['subjects', 'questionaire', 'questiones', 'crf', 'sharedscans', 'answers']
        exclude_columns = ['id', 'phonenumber', 'email', 'firstname', 'lastname']

        for tablename in tablenames:
            if tablename == 'crf':
                process_crf_table()
            else:
                # Fetch data from source database
                data, colnames = fetch_data_from_source(tablename, exclude_columns)
                if data and colnames:
                    # Insert data into destination database
                    insert_data_into_destination(tablename, data, colnames)
                else:
                    print(f"No data fetched from source for table {tablename}.")

if __name__ == "__main__":
     main()
