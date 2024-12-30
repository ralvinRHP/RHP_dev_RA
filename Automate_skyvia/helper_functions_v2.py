import pandas as pd
from simple_salesforce import Salesforce, SalesforceLogin
import pyodbc, sys, time 
import requests
import numpy as np
from datetime import datetime
from datetime import datetime
import pandas as pd
from typing import List
import io
import paramiko





def salesforce_connection(sandbox=True):

    session = requests.Session()

    if sandbox != True:
        inst = 'Prod'
        security_token = 'JytX4FFl2pBPdRIaGLjd3aCwT'
        username = 'ralvin@relianthp.com'
        password = 'Flash363!'
        domain = 'login'
    else:
        inst = 'sandbox'
        security_token = 'pIjwc8P0OB2ZisbQYhlKxfLd'
        username = 'ralvin@relianthp.com.tctdev'
        password = 'Flash363!'
        domain = 'test'

    session_id, instance = SalesforceLogin(username=username, password=password, security_token=security_token, domain=domain)
    sf = Salesforce(instance=instance, session_id=session_id, session=session)
    print(f'Connected to Salesforce {inst}')
    return sf



def read_sftp_data(remote_path, host, port, username, password):
    try:
        # Step 3: Connect to the SFTP server using Paramiko
        transport = paramiko.Transport((host, port))
        transport.connect(username=username, password=password)

        with paramiko.SFTPClient.from_transport(transport) as sftp:
            print("Connection established successfully!")


            if remote_path:
                with sftp.file(remote_path, "r") as remote_file_obj:
                    file_content = remote_file_obj.read().decode("utf-8")
                    df = pd.read_csv(io.StringIO(file_content))
                    print("File read into DataFrame successfully")
            
            else:
                print("No file specified to read. Please set 'remote_file' in the configuration.")

    except paramiko.AuthenticationException:
        print("Authentication failed. Please check your credentials.")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if 'transport' in locals() and transport.is_active():
            transport.close()
            print("Connection closed.")

    return df






def updated_data_pull(object_names: List[str], sf):
    store = {}
    for i in object_names:
        sf_object = getattr(sf, i)

        # Retrieve and print field names
        metadata = sf_object.describe()
        field_names = [field['name'] for field in metadata['fields']]
        # print(f"Field names for {i}: {field_names}")

        # Retrieve and print record data
        query = f"SELECT {', '.join(field_names)} FROM {i}"  # Fetch all fields
        records = sf.query(query)
        all_records = records['records']

        # Handle pagination
        while not records['done']:
            records = sf.query_more(records['nextRecordsUrl'], True)
            all_records.extend(records['records'])

        # Convert to DataFrame
        df = pd.DataFrame(all_records)

        store[i] = df
        

    if len(store) == 1:
        print(f'pulled {i}')
        return store[i]
    else:
        return store


def create_df(source_dict, Source_data, sf):
    new_columns = [col for mapped_cols in source_dict['MAPPINGS'].values() for col in mapped_cols]
    new_df = pd.DataFrame(columns=new_columns)
    keys = source_dict['KEYS']
    try:
        Table_convert_cols = list(source_dict['TABLE_CONVERT'].keys())
    except:
        Table_convert_cols = []
    for source_col, target_cols in source_dict['MAPPINGS'].items():
        if source_col in Table_convert_cols:
            map_table = source_dict['TABLE_CONVERT'][source_col]
            try:
                string_ids = [
                        str(int(float(id))) if pd.notna(id) and id != 'nan' else np.nan
                        for id in Source_data[source_col]                               #### handeling DRG col
                    ]
            except:
                string_ids = [str(id) for id in Source_data[source_col]]
            Source_data[source_col] = string_ids #### col update to match same type in foreign table
            foreign_table = list(map_table.keys())[0]
            print(f'pulling updated {foreign_table} table')
            sf_object = updated_data_pull([foreign_table], sf) #### need logic to pull only the records I need rather than the entire table
    


            merged = Source_data.merge(sf_object.loc[:, map_table[foreign_table]], left_on=source_col, right_on=map_table[foreign_table][0], how='left')

            for i, target_col in enumerate(target_cols):
                foreign_col = map_table[foreign_table][i + 1]
                new_df[target_col] = merged[foreign_col]
            


        if source_col not in Table_convert_cols:
            if source_col in Source_data.columns:
                for target_col in target_cols:
                    new_df[target_col] = Source_data[source_col]
        



    return new_df, keys





from datetime import datetime


def format_date(value):
    try:
        # Attempt to parse and format the date
        return datetime.strptime(value, '%m/%d/%Y').strftime('%Y-%m-%d')
    except (ValueError, TypeError):
        # Return the value as-is if it’s not a valid date
        return value



def upsert(new_df, keys, sf):

    object_name = list(keys.keys())[0]
    sf_object = getattr(sf, object_name)
    object_key = list(keys.values())[0]

    error_logs = []
    new_ids = []

    # print(object_key)

    for i in range(len(new_df)):
        # print(i)
        record = new_df.iloc[i, :].to_dict()
        # print(record)
        record_upsert = {k: v for k, v in record.items() if k != object_key}
        # Replace NaN with None
        record_upsert = {k: (None if pd.isna(v) else v) for k, v in record_upsert.items()}
        # Format date fields
        record_upsert = {k: format_date(v) if 'Date' in k or 'DOS' in k else v for k, v in record_upsert.items()}
    
        # print(record_upsert)
        
        try:
            new_id = sf_object.upsert(f"{object_key}/{record[object_key]}", record_upsert)
            new_ids.append(new_id)
            # pass
        except Exception as e:
            print(f"Error during upsert: {e}")
            error_logs.append(e)

    return new_ids, error_logs

            

def format_date(value):
    try:
        # Attempt to parse and format the date
        return datetime.strptime(value, '%m/%d/%Y').strftime('%Y-%m-%d')
    except (ValueError, TypeError):
        # Return the value as-is if it’s not a valid date
        return value


def insert_records(new_df, keys, sf):
    object_name = list(keys.keys())[0]
    sf_object = getattr(sf, object_name)
    object_key = list(keys.values())[0]

    error_logs = []
    new_ids = []

    try:
        for i in range(len(new_df)):  # Adjust index appropriately
            record = new_df.iloc[i, :].to_dict()
            print(f"Processing record {i}: {record}")

            # Ensure NPI__c is properly formatted as a string
            if "NPI__c" in record:
                record["NPI__c"] = str(int(record["NPI__c"])) if pd.notnull(record["NPI__c"]) else None

            # Exclude the key field and replace NaN with None
            record_insert = {k: (None if pd.isna(v) else v) for k, v in record.items()}
            # Format date fields
            record_insert = {k: format_date(v) if 'Date' in k or 'DOS' in k else v for k, v in record_insert.items()}

            try:
                # Perform insert
                new_id = sf_object.create(record_insert)
                new_ids.append(new_id['id'])
            except Exception as e:
                print(f"Error during insert: {e}")
                error_logs.append(e)

    except Exception as e:
        print(f"Unexpected error during processing: {e}")
        error_logs.append(e)

    print(f"Total records inserted: {len(new_ids)}")
    print(f"Total errors logged: {len(error_logs)}")
    return new_ids, error_logs



def delete_record(Ids, keys, sf):
    # Extract Salesforce object name and key field
    object_name = next(iter(keys.keys()))
    object_key = keys[object_name]
    # Get the Salesforce object reference
    sf_object = getattr(sf, object_name)
    print(f"Object: {object_name}, Records to process: {len(Ids)}")
    
    errors = []
    
    # Delete records
    for record_id in Ids:
        try:
            sf_object.delete(record_id)
        except Exception as e:
            errors.append((record_id, str(e)))
    
    if errors:
        print(f"Errors occurred during deletion:")
        for record_id, error_message in errors:
            print(f"Record ID {record_id}: {error_message}")
    else:
        print("All records deleted successfully.")
        
