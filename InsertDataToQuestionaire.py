import pandas as pd
import psycopg2
from psycopg2 import Error
from datetime import datetime, timedelta
import gspread
from google.oauth2.service_account import Credentials

# Function to connect to PostgreSQL database
def connect_to_db():
    try:
        connection = psycopg2.connect(
            user="tomer",
            password="t1",
            host="localhost",
            port="5433",  # Assuming default PostgreSQL port
            database="postgres5"
        )
        connection.autocommit = True
        return connection
    except Error as e:
        print("Error while connecting to PostgreSQL:", e)
        return None


# Function to retrieve column names from a specified table

def open_and_read_google(sheet_id):
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file("credentials.json", scopes=scopes)
    client = gspread.authorize(creds)
    workbook = client.open_by_key(sheet_id)
    # Get all values from the worksheet
    worksheet = workbook.get_worksheet(0)
    data=worksheet.get_all_values()
    data=pd.DataFrame(data[1:], columns=data[0])
    return data




def get_columns(table_name, cursor):
    query_command = f"""SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'"""
    cursor.execute(query_command)
    columns = [row[0] for row in cursor.fetchall()]
    return columns


# Function to insert data into the questionaire table
def INSERT_INTO_QUESTIONAIRE_TABLE(questionaireVersion):
    connection = connect_to_db()
    if connection is None:
        return
    cursor = connection.cursor()
    for index, row in questionaireVersion.iterrows():
        try:
            # Format the timestamp correctly
            if isinstance(row['time.stamp'], datetime):
                if row['time.stamp'].microsecond > 0:
                    row['time.stamp'] = row['time.stamp'].strftime('%Y-%m-%d %H:%M:%S.%f')
                else:
                    row['time.stamp'] = row['time.stamp'].strftime('%Y-%m-%d %H:%M:%S')

            if '.' in row['time.stamp']:
                timestamp = datetime.strptime(row['time.stamp'], '%Y-%m-%d %H:%M:%S.%f')
                if timestamp.microsecond >= 500000:
                    # Round up if microseconds are 500000 or more
                    rounded_timestamp = timestamp + timedelta(seconds=1)
                else:
                    # Otherwise, keep the same second
                    rounded_timestamp = timestamp

                # Set microseconds to zero
                rounded_timestamp = rounded_timestamp.replace(microsecond=0)

                # Format the timestamp back to string
                row['time.stamp'] = rounded_timestamp.strftime('%Y-%m-%d %H:%M:%S')

            # Insert the formatted data into the questionaire table
            insert_query = f"INSERT INTO questionaire (QuestionaireVersion, dateTime) VALUES (N'{row['גרסת שאלון']}', N'{str(row['time.stamp'])}');"
            cursor.execute(insert_query)
            connection.commit()
        except Error as e:
            print("Error inserting data:", e)


# Function to insert data into the questiones table
def INSERT_INTO_QUESTIONES_TABLE(Questions):
    connection = connect_to_db()
    if connection is None:
        return
    cursor = connection.cursor()
    columns = get_columns('questiones', cursor)

    # Retrieve existing questions from the questiones table
    select_query = "SELECT DISTINCT question FROM questiones"
    cursor.execute(select_query)
    results = cursor.fetchall()
    QuestionsfromTable = [row[0] for row in results]

    for value in Questions:
        if value not in QuestionsfromTable:
            value = value.replace("'", "''")
            try:
                insert_query = f"INSERT INTO questiones (Question) VALUES ('{value}');"
                cursor.execute(insert_query)
                connection.commit()
            except Error as e:
                print("Error inserting data:", e)


# Function to insert data into the answers table
def INSERT_INTO_ANSWERS_TABLE(Answers):
    connection = connect_to_db()
    if connection is None:
        return
    cursor = connection.cursor()

    for AnswerColumn in Answers.columns:
        if AnswerColumn != 'subject.code' and AnswerColumn != 'time.stamp':
            for index, row in Answers.iterrows():
                try:
                    # Sanitize the question column name
                    sanitized_column = AnswerColumn.replace("'", "''")

                    # Retrieve the QuestioneId from the questiones table
                    select_query = f"SELECT QuestioneId FROM questiones WHERE question='{sanitized_column}'"
                    cursor.execute(select_query)
                    QuestioneId = cursor.fetchone()[0]

                    # Prepare the answer and timestamp
                    Answer = row[AnswerColumn]
                    Answer = str(Answer).replace("'", "''")
                    date_time_str = row['time.stamp']

                    if isinstance(row['time.stamp'], datetime):
                        if row['time.stamp'].microsecond > 0:
                            row['time.stamp'] = row['time.stamp'].strftime('%Y-%m-%d %H:%M:%S.%f')
                        else:
                            row['time.stamp'] = row['time.stamp'].strftime('%Y-%m-%d %H:%M:%S')

                    if '.' in row['time.stamp']:
                        timestamp = datetime.strptime(row['time.stamp'], '%Y-%m-%d %H:%M:%S.%f')
                        if timestamp.microsecond >= 500000:
                            # Round up if microseconds are 500000 or more
                            rounded_timestamp = timestamp + timedelta(seconds=1)
                        else:
                            # Otherwise, keep the same second
                            rounded_timestamp = timestamp

                        # Set microseconds to zero
                        rounded_timestamp = rounded_timestamp.replace(microsecond=0)

                        # Format the timestamp back to string
                        row['time.stamp'] = rounded_timestamp.strftime('%Y-%m-%d %H:%M:%S')

                    # Retrieve the QuestionaireID from the questionaire table
                    select_query = f"SELECT QuestionaireID FROM questionaire WHERE dateTime='{row['time.stamp']}'"
                    cursor.execute(select_query)
                    QuestionaireID = cursor.fetchone()[0]
                    select_query=F"SELECT GuId FROM SUBJECTS WHERE QuestionaireCode='{row['subject.code']}'"
                    cursor.execute(select_query)
                    GuId = cursor.fetchone()
                    if GuId:
                      insert_query = f"INSERT INTO answers (GuId,QuestionaireCode, QuestioneId, QuestionaireID, Answer) VALUES ('{GuId[0]}','{row['subject.code']}', {QuestioneId}, {QuestionaireID}, N'{Answer}');"
                      cursor.execute(insert_query)
                      connection.commit()
                except Error as e:
                    if "syntax error" in str(e).lower():
                        print(f"Failed query: {insert_query}")
                    else:
                        continue





# Function to read the Excel file and insert data into the PostgreSQL database
def read_excel_and_insert_data(file_path):
    excel_data = open_and_read_google(file_path)

    # Strip whitespace from all columns
    for column in excel_data.columns:
        excel_data[column] = [str(item).strip() for item in excel_data[column]]

    excluded_columns = [excel_data.columns[0]]
    Answers = excel_data

    # Insert data into the corresponding tables
    INSERT_INTO_QUESTIONAIRE_TABLE(excel_data)
    INSERT_INTO_QUESTIONES_TABLE(Answers.columns.tolist())
    INSERT_INTO_ANSWERS_TABLE(Answers)

    print("done")


# Example usage
read_excel_and_insert_data('1wyOBqgKe6mrSBQV32OAICFJIuWPqER_49cGaTvw41QM')
