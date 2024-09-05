import numpy as np
import pandas as pd
import psycopg2
from openpyxl.workbook import Workbook
from psycopg2 import Error
import re
import os
import openpyxl
import create_scan_type_table
from create_scan_type_table import get_df
# Function to connect to the PostgreSQL database
import random
import string
def connect_to_db():
    try:
        connection = psycopg2.connect(
            user="tomer",
            password="t1",
            host="localhost",
            port="5433",  # Assuming default PostgreSQL port
            database="postgres5",
        )
        connection.autocommit = True
        return connection
    except Error as e:
        print("Error while connecting to PostgreSQL:", e)


def generate_random_code(length=10):
    """Generate a random alphanumeric code of specified length."""
    characters = string.ascii_letters + string.digits  # Includes letters and digits
    code = ''.join(random.choice(characters) for _ in range(length))
    return code

def findGuid(ID_or_phone,cursor,connection):
    try:
     inser_query=f"""select guid from crf where id='{ID_or_phone}'"""
     cursor.execute(inser_query)
     guid=cursor.fetchall()
    except psycopg2.Error as e:
      print(e)
      connection.rollback()
    return guid


# Function to process and insert data from CrfYaData DataFrame into the Patients table
def Crf_to_subjet_table(Crf_Data,cursor, connection):
    Crf_Data = Crf_Data.sort_values(by='ID', na_position='last')
    for index, row in Crf_Data.iterrows():
        try:
            questionaire_code = row['Qcode']
            if str(questionaire_code) != 'nan':
              if row['ID']!='nan':
                  select_query = f"""select guid from subjects where id='{row['ID']}'"""
                  cursor.execute(select_query)
                  guid = cursor.fetchone()
                  if not guid:
                      while True:
                          guid = generate_random_code(length=9)
                          select_query = f"""select guid from subjects where guid='{guid}'"""
                          cursor.execute(select_query)
                          already_exsisting_guid = cursor.fetchall()
                          if not already_exsisting_guid:
                              insert_query = f"INSERT INTO subjects(GuId,QuestionaireCode,id,email) VALUES ('{guid}','{questionaire_code}','{row['ID']}','{row['Email']}');"
                              cursor.execute(insert_query)
                              connection.commit()
                              break

                  else:
                      insert_query = f"INSERT INTO subjects(GuId,QuestionaireCode,id,email) VALUES ('{guid[0]}','{questionaire_code}','{row['ID']}','{row['Email']}');"
                      cursor.execute(insert_query)
                      connection.commit()
              elif row['Email']!='nan':
                  select_query = f"""select guid from subjects where email='{row['Email']}'"""
                  cursor.execute(select_query)
                  guid = cursor.fetchone()
                  if guid:
                      insert_query = f"INSERT INTO subjects(GuId,QuestionaireCode,id,email) VALUES ('{guid}','{questionaire_code}','{row['ID']}','{row['Email']}');"
                      cursor.execute(insert_query)
                      connection.commit()
        except psycopg2.Error as e:
            print(e)
            connection.rollback()


# Function to transform and insert data from CrfYaData DataFrame into the CRF table
def Crf_Data_crf_table(Crf_Data, cursor, connection):
    Crf_Data = Crf_Data.sort_values(by='ID', na_position='last')
    columns_to_exclude = ['Unnamed: 16', 'Unnamed: 19']
    # Find the index of the 'Name' column
    name_index = Crf_Data.columns.get_loc('Name')
    # Split the 'Name' column into 'FirstName' and 'LastName'
    Crf_Data[['FirstName', 'LastName']] = Crf_Data['Name'].str.split(' ', n=1, expand=True)
    # Drop the original 'Name' column
    Crf_Data.drop(columns=['Name'], inplace=True)
    # Insert 'FirstName' and 'LastName' columns at the same location as the original 'Name' column
    Crf_Data.insert(name_index, 'FirstName', Crf_Data.pop('FirstName'))
    Crf_Data.insert(name_index + 1, 'LastName', Crf_Data.pop('LastName'))
    Crf_Data=Crf_Data.drop(columns=columns_to_exclude, errors='ignore')
    for index, row in Crf_Data.iterrows():
        if str(row['ScanID']) != 'NaT' and str(row['ScanID']) != 'nan':
            if row['ID']!='nan':
                  select_query = f"""select guid from subjects where ID='{row['ID']}'"""
                  cursor.execute(select_query)
                  guid = cursor.fetchone()
                  if guid:
                     values = [guid[0]] + [str(value) for value in row]
                     values = ', '.join(f"'{value}'" for value in values)
                     try:
                        insert_query = f"""
                           INSERT INTO CRF (
                             guid,datetimescan,status,lab,firstname,lastname,id,cellularno,email,gender,dob,scandate,ageofscan
                             ,weight,height,protocol,study,groupname,scantag,questionairecode,noscan,scandayqday) VALUES ({values});
                           """
                        cursor.execute(insert_query)
                        connection.commit()
                     except psycopg2.Error as e:
                        print(e)
            elif row['Email']:
              try:
                  select_query = f"""select guid from subjects where email='{row['Email']}'"""
                  cursor.execute(select_query)
                  guid = cursor.fetchone()
                  if guid:
                     insert_query = f"""
                                  INSERT INTO CRF (
                                    guid,scanid,status,lab,firstname,lastname,id,cellularno,email,gender,dob,scandate,ageofscan
                                    ,weight,height,protocol,study,groupname,scantag,questionairecode,noscan,scandayqday,datetimescan) VALUES ({values});
                              """
                     cursor.execute(insert_query)
                     connection.commit()
              except psycopg2.Error as e:
                print(e)
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
            parts = values.split(', ', 1)
            values = f"{parts[0]},NULL,NULL,{parts[1]},'SNBB'"
            try:
                insert_query = f"""
                    INSERT INTO Scans (
                        DateTimeScan,bidspath,resultspath,rawdatapath, T1w, T2w, Flair, RestFmri, TaskFmri, TaskNames, DmriAp, DmriPa, Mrtrix, AxSi, Qsi,
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
        if row['DateTimeScan'] != pd.NaT:
            values = ', '.join(f"'{value}'" for value in row)
            parts = values.split(', ', 1)
            values = f"{parts[0]},NULL,NULL,{parts[1]},'ya_shared_scans'"
            try:
                insert_query = f"""
                    INSERT INTO Scans (
                        DateTimeScan,bidspath, resultspath,rawdatapath,T1w, T2w, Flair, RestFmri, TaskFmri, TaskNames, DmriAp, DmriPa, Mrtrix, AxSi, Qsi,
                        HcpFreesurfer, HcpRestFMRI, HcpTaskFmri, HcpDiffusion, DataLocation
                    ) VALUES ({values});
                """
                cursor.execute(insert_query)
                connection.commit()
            except psycopg2.Error as e:
                print(e)
                connection.rollback()



# Function to read the Excel and CSV files and insert data into the PostgreSQL database
def read_excel_and_insert_data(Crf_Path,Ya_Shared_Scans_path,Questionaire_path,SNBB_path):
    try:
        # Connect to the PostgreSQL database
        connection = connect_to_db()
        cursor = connection.cursor()

        # Read data from the Excel and CSV files
        Crf_Data = pd.read_excel(Crf_Path)
        Questionaire_Data = pd.read_excel(Questionaire_path)
        SnBB_Scans_Data=pd.read_excel(SNBB_path)
        YaShared_Scans_Data=pd.read_excel(Ya_Shared_Scans_path)

        # SnBBData = create_scan_type_table.get_df(SnBBPath)
        # YaSharedScansData = create_scan_type_table.get_df(YaSharedScansPath)
        # snbb_excel_file_path = 'SnBBData.xlsx'
        # ya_shared_scans_excel_file_path = 'YaSharedScansData.xlsx'
        # SnBBData.to_excel(snbb_excel_file_path, sheet_name='SnBBData', index=False)
        # # Write the YaSharedScansData DataFrame to its own Excel file
        # YaSharedScansData.to_excel(ya_shared_scans_excel_file_path, sheet_name='YaSharedScansData', index=False)
        # print(f'Data successfully written to {snbb_excel_file_path} and {ya_shared_scans_excel_file_path}')
        # Strip whitespace from all columns in the dataframes
        for column in Crf_Data.columns:
            Crf_Data[column] = Crf_Data[column].apply(lambda x: str(x).strip())
        for column in Questionaire_Data.columns:
            Questionaire_Data[column] = Questionaire_Data[column].apply(lambda x: str(x).strip())

        for column in SnBB_Scans_Data.columns:
            SnBB_Scans_Data[column] = SnBB_Scans_Data[column].apply(lambda x: str(x).strip())
        for column in YaShared_Scans_Data.columns:
           YaShared_Scans_Data[column] = YaShared_Scans_Data[column].apply(lambda x: str(x).strip())

        # Process and insert data into the PostgreSQL database
        Crf_to_subjet_table(Crf_Data,cursor, connection)
        Crf_Data_crf_table(Crf_Data, cursor, connection)
        process_and_insert_SnBBData(SnBB_Scans_Data, cursor, connection)
        process_and_insert_YaSharedScansData(YaShared_Scans_Data, cursor, connection)

        print("Data insertion complete!")
    except Error as e:
        print("Error while inserting data to PostgreSQL:", e)

def check_for_subject_with_missing_details(Questionaire_path):
    connection = connect_to_db()
    cursor = connection.cursor()
    selcect_query=f"""select subjectid from crf where id='nan'"""
    cursor.execute(selcect_query)
    # Fetch all the results
    Subjects_with_missing_data = cursor.fetchall()
    # Specify the path where the Excel file will be saved
    excel_path = "Subjects_with_missing_id.xlsx"
    # Save the Excel file
    workbook = openpyxl.Workbook()
    sheet = workbook.active

    # Set the title for the sheet
    sheet.title = "Subjects rows with missing ID "
    row = 1
    # Add header (optional)
    sheet.cell(row=row, column=1).value = "Subject Codes"
    row += 1
    # Iterate through subject codes and add those not in subjectids to the Excel sheet
    for subject in subject_ids:
        sheet.cell(row=row, column=1).value = subject
        row += 1
    # Save the workbook
    workbook.save(excel_path)

    delete_query = f"DELETE FROM subjects WHERE subjectid IN ({subject_ids_str})"
    cursor.execute(delete_query)
    connection.commit()
    # Create a new workbook and get the active sheet
    print(f"Data has been successfully saved to {excel_path}")
    workbook = openpyxl.Workbook()
    sheet = workbook.active

    # Set the title for the sheet
    sheet.title = "Questionaire Codes Not in CRF"

    # Fetch the subject IDs from the database
    select_query = "SELECT subjectid FROM subjects"
    cursor.execute(select_query)
    subjectids = [item[0] for item in cursor.fetchall()]  # Convert fetched tuples to a list of subject IDs
    # Read the Questionnaire data from Excel
    questionaire_data = pd.read_excel(Questionaire_path)
    subject_codes = questionaire_data['subject.code']
    # Initialize row counter for the Excel sheet
    row = 1
    # Add header (optional)
    sheet.cell(row=row, column=1).value = "Subject Codes"
    row += 1
    # Iterate through subject codes and add those not in subjectids to the Excel sheet
    for subject in subject_codes:
        if subject not in subjectids:
            sheet.cell(row=row, column=1).value = subject
            row += 1
    # Save the workbook
    workbook.save(excel_path)


# Paths to the files
CrfPath='AllSubjectData.xlsx'
#Ya_Shared_Scans_path = '//132.66.46.62/Raw_Data'
Questionaire_path = '2024 full data and coded categories.xlsx'
#SNBB_path = '//132.66.46.165/snbb'
Ya_Shared_Scans_path ='YaSharedScansData.xlsx'
SNBB_path = 'SnBBData.xlsx'

# Run the data pipeline
read_excel_and_insert_data(CrfPath,Ya_Shared_Scans_path,Questionaire_path,SNBB_path)
###check_for_subject_with_missing_details(Questionaire_path)
