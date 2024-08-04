import numpy as np
import pandas as pd
import psycopg2
from psycopg2 import Error
import re
import os

# Function to connect to the PostgreSQL database
def connect_to_db():
    try:
        connection = psycopg2.connect(
            user="tomer",
            password="t1",
            host="localhost",
            port="5433",  # Assuming default PostgreSQL port
            database="appdb1",
        )
        connection.autocommit = True
        return connection
    except Error as e:
        print("Error while connecting to PostgreSQL:", e)

# Function to process and insert data from CrfYaData DataFrame into the Patients table
def process_CrfYaData(CrfYaData, cursor, connection):
    CrfYaData[['FirstName', 'LastName']] = CrfYaData['Name'].str.split(' ', n=1, expand=True)
    columns_to_exclude = ['Unnamed: 18', 'Unnamed: 19', 'Unnamed: 20', 'Unnamed: 21', 'Unnamed: 22']
    CrfYaData = CrfYaData.drop(columns=columns_to_exclude, errors='ignore')

    for index, row in CrfYaData.iterrows():
        try:
            value = row['Questionnaire']
            if str(value) != 'nan':
                insert_query = f"INSERT INTO Subjects(SubjectID) VALUES ('{value}');"
                cursor.execute(insert_query)
                connection.commit()
        except psycopg2.Error as e:
            print(e)
            connection.rollback()

# Function to process and insert data from CrfData DataFrame into the Patients table
def process_CrfData(CrfData, cursor, connection):
    for index, row in CrfData.iterrows():
        try:
            value = str(row['Number'])
            if value != 'nan':
                insert_query = f"INSERT INTO Subjects(SubjectID) VALUES ('{value}');"
                cursor.execute(insert_query)
                connection.commit()
        except psycopg2.Error as e:
            print(e)
            connection.rollback()

# Function to process and insert data from QuestionaireData DataFrame into the Patients table
def process_QuestionaireData(QuestionaireData, cursor, connection):
    for index, row in QuestionaireData.iterrows():
        try:
            value = str(row['subject.code'])
            if value != 'nan':
                insert_query = f"INSERT INTO Subjects(SubjectID) VALUES ('{value}');"
                cursor.execute(insert_query)
                connection.commit()
        except psycopg2.Error as e:
            print(e)
            connection.rollback()

# Function to transform and insert data from CrfYaData DataFrame into the CRF table
def transform_and_insert_CrfYaData(CrfYaData, cursor, connection):
    CrfYaData['DateTimeScan'] = CrfYaData['Scan File'].str.extract(r'(\d{8}_\d{4})')
    CrfYaData['DateTimeScan'] = pd.to_datetime(CrfYaData['DateTimeScan'], format='%Y%m%d_%H%M')
    CrfYaData['Sex'] = CrfYaData['Sex'].apply(lambda x: 'Male' if x == 'M' else ('Female' if x == 'F' else x))
    new_order = [
        'DateTimeScan', 'Questionnaire', 'Group', 'FirstName', 'LastName', 'ID', 'Date of Birth', 'Phone', 'Email',
        'Notes', 'Sex', 'Unnamed: 0', 'Scan no. (per this questionnaire no)', 'Scan File', 'Hour of Scan', 'Height (cm)',
        'Weight (kg)', 'Study', 'Condition', 'Done'
    ]
    CrfYaData = CrfYaData.reindex(columns=new_order)

    for index, row in CrfYaData.iterrows():
        if str(row['DateTimeScan']) != 'NaT':
            first_10_values = [str(value) for value in row[:10]]
            extended_values = first_10_values + ['NULL'] * 6 + [str(row['Sex'])] + [str(row['Done'])] + [str(value) for value in row[11:19]] + ["CRF YA"]
            values = ', '.join(f"'{value}'" for value in extended_values)
            try:
                insert_query = f"""
                    INSERT INTO CRF (
                        DateTimeScan, SubjectID, groupname, firstname, lastname, id, dob, phonenumber, email, notes,
                        status, recruiter, fillq, scandate, invitehour, scanhour, gender, approved, date, scanno,
                        scanfile, hourofscan, heightcm, weightkg, study, Condition, CrfName
                    ) VALUES ({values});
                """
                cursor.execute(insert_query)
                connection.commit()
            except psycopg2.Error as e:
                print(e)
                connection.rollback()

# Function to transform and insert data from CrfData DataFrame into the CRF table
def transform_and_insert_CrfData(CrfData, cursor, connection):
    for index, row in CrfData.iterrows():
        try:
            scan_date = str(row['scan date'])
            scan_hour = str(row['scan hour'])

            if not re.match(r'\d{2}:\d{2}', scan_hour):
                raise ValueError(f"Scan hour not in format HH:MM for row {index}")

            hours_minutes = scan_hour[:5].replace(':', '')
            date_parts = scan_date.split(' ')[0]

            if re.match(r'\d{2}/\d{2}/\d{4}', scan_date) or re.match(r'\d{2}\.\d{2}\.\d{4}', scan_date):
                day, month, year = map(int, scan_date.split('/'))
                formatted_date = f'{year}{month:02d}{day:02d}'
            else:
                formatted_date = scan_date

            formatted_date = formatted_date.replace('-', '')
            if formatted_date.endswith('00:00:00'):
                formatted_date = formatted_date.split(' ')[0]
            date_time_scan = formatted_date + '_' + hours_minutes

            CrfData.at[index, 'DateTimeScan'] = date_time_scan
        except Exception as e:
            print(e)
            continue

    new_order = [
        'DateTimeScan', 'Number', 'group', 'First Name', 'Last Name', 'ID', 'D.O.B', 'Phone Number', 'Email', 'notes',
        'status', 'recruiter', 'fill Q?', 'scan date', 'invite hour', 'scan hour', 'gender', 'approved?'
    ]
    CrfData = CrfData.reindex(columns=new_order)
    CrfData['DateTimeScan'] = CrfData['DateTimeScan'].str.extract(r'(\d{8}_\d{4})')
    CrfData['DateTimeScan'] = pd.to_datetime(CrfData['DateTimeScan'], format='%Y%m%d_%H%M')
    CrfData['gender'] = CrfData['gender'].apply(lambda x: 'Male' if x == 'M' else ('Female' if x == 'F' else x))

    for index, row in CrfData.iterrows():
        if str(row['DateTimeScan']) != 'NaT':
            first_values = [str(value) for value in row]
            extended_values = first_values + ['NULL'] * 8 + ["CRF"]
            values = ', '.join(f"'{value}'" for value in extended_values)
            try:
                insert_query = f"""
                    INSERT INTO CRF (
                        DateTimeScan, SubjectID, groupname, firstname, lastname, id, dob, phonenumber, email, notes,
                        status, recruiter, fillq, scandate, invitehour, scanhour, gender, approved, date, scanno,
                        scanfile, hourofscan, heightcm, weightkg, study, Condition, CrfName
                    ) VALUES ({values});
                """
                cursor.execute(insert_query)
                connection.commit()
            except psycopg2.Error as e:
                print(e)
                connection.rollback()

# Function to process and insert data from SnBBData DataFrame into the SharedScans table
def process_and_insert_SnBBData(SnBBData, cursor, connection):
    SnBBData['DateTimeScan'] = SnBBData['Path'].str.extract(r'(\d{8}_\d{4})')
    SnBBData['DateTimeScan'] = pd.to_datetime(SnBBData['DateTimeScan'], format='%Y%m%d_%H%M')
    cols = SnBBData.columns.tolist()
    cols.insert(0, cols.pop(cols.index('DateTimeScan')))
    SnBBData = SnBBData.reindex(columns=cols)

    for index, row in SnBBData.iterrows():
        if row['DateTimeScan'] != 'NaT':
            values = ', '.join(f"'{value}'" for value in row)
            values = values + ',' + "'SNBB'"
            try:
                insert_query = f"""
                    INSERT INTO Scans (
                        DateTimeScan, Path, T1w, T2w, Flair, RestFmri, TaskFmri, TaskNames, DmriAp, DmriPa, Mrtrix, AxSi, Qsi,
                        HcpFreesurfer, HcpRestFMRI, HcpTaskFmri, HcpDiffusion, DataLocation
                    ) VALUES ({values});
                """
                cursor.execute(insert_query)
                connection.commit()
            except psycopg2.Error as e:
                print(e)
                connection.rollback()

# Function to process and insert data from YaSharedScansData DataFrame into the SharedScans table
def process_and_insert_YaSharedScansData(YaSharedScansData, cursor, connection):
    YaSharedScansData['DateTimeScan'] = YaSharedScansData['Path'].str.extract(r'(\d{8}_\d{4})')
    YaSharedScansData['DateTimeScan'] = pd.to_datetime(YaSharedScansData['DateTimeScan'], format='%Y%m%d_%H%M')
    cols = YaSharedScansData.columns.tolist()
    cols.insert(0, cols.pop(cols.index('DateTimeScan')))
    YaSharedScansData = YaSharedScansData.reindex(columns=cols)

    for index, row in YaSharedScansData.iterrows():
        if row['DateTimeScan'] != 'NaT':
            values = ', '.join(f"'{value}'" for value in row)
            values = values + ',' + "'ya_shared_scans'"
            try:
                insert_query = f"""
                    INSERT INTO Scans (
                        DateTimeScan, Path, T1w, T2w, Flair, RestFmri, TaskFmri, TaskNames, DmriAp, DmriPa, Mrtrix, AxSi, Qsi,
                        HcpFreesurfer, HcpRestFMRI, HcpTaskFmri, HcpDiffusion, DataLocation
                    ) VALUES ({values});
                """
                cursor.execute(insert_query)
                connection.commit()
            except psycopg2.Error as e:
                print(e)
                connection.rollback()

# Function to delete old data from sharedscans and crf tables
def delete_old_data(cursor, connection):
    try:
        delete_query = "DELETE FROM Scans WHERE datetimescan < '2019-02-25';"
        cursor.execute(delete_query)
        connection.commit()

        delete_query = "DELETE FROM crf WHERE datetimescan < '2019-02-25';"
        cursor.execute(delete_query)
        connection.commit()
    except psycopg2.Error as e:
        print(e)
        connection.rollback()

# Function to read the Excel and CSV files and insert data into the PostgreSQL database
def read_excel_and_insert_data(CrFYaPath, YaSharedScansPath, CrFPath, QuestionairePath, SnBBPath):
    try:
        # Connect to the PostgreSQL database
        connection = connect_to_db()
        cursor = connection.cursor()

        # Read data from the Excel and CSV files
        CrfYaData = pd.read_excel(CrFYaPath)
        CrfData = pd.read_excel(CrFPath)
        QuestionaireData = pd.read_excel(QuestionairePath)
        SnBBData = pd.read_csv(SnBBPath)
        YaSharedScansData = pd.read_csv(YaSharedScansPath)

        # Strip whitespace from all columns in the dataframes
        for column in CrfYaData.columns:
            CrfYaData[column] = CrfYaData[column].apply(lambda x: str(x).strip())
        for column in CrfData.columns:
            CrfData[column] = CrfData[column].apply(lambda x: str(x).strip())
        for column in SnBBData.columns:
            SnBBData[column] = SnBBData[column].apply(lambda x: str(x).strip())
        for column in YaSharedScansData.columns:
            YaSharedScansData[column] = YaSharedScansData[column].apply(lambda x: str(x).strip())

        # Process and insert data into the PostgreSQL database
        process_CrfYaData(CrfYaData, cursor, connection)
        process_CrfData(CrfData, cursor, connection)
        process_QuestionaireData(QuestionaireData, cursor, connection)
        transform_and_insert_CrfYaData(CrfYaData, cursor, connection)
        transform_and_insert_CrfData(CrfData, cursor, connection)
        process_and_insert_SnBBData(SnBBData, cursor, connection)
        process_and_insert_YaSharedScansData(YaSharedScansData, cursor, connection)
        delete_old_data(cursor, connection)

        print("Data insertion complete!")
    except Error as e:
        print("Error while inserting data to PostgreSQL:", e)

# Paths to the files
CRF_YA_path = 'https://docs.google.com/spreadsheets/d/18ITYqHSUnaabxq9vxmrW6W5uuSu7XGJ48keYpS-jMVc/export?format=xlsx'
Ya_Shared_Scans_path = 'ya_shared_scans.csv'
CRF_path = 'CRF.xlsx'
Questionaire_path = '2024 full data and coded categories.xlsx'
SNBB_path = 'SNBB_Table.csv'

# Run the data pipeline
read_excel_and_insert_data(CRF_YA_path, Ya_Shared_Scans_path, CRF_path, Questionaire_path, SNBB_path)
