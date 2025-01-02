import pandas as pd
from simple_salesforce import Salesforce, SalesforceLogin
import pyodbc, sys, time 
import requests
import numpy as np
from datetime import datetime








def updated_data_pull(object_name, needed_objects, sf):

    object_fields = needed_objects[object_name]
    fields_str = ", ".join(object_fields)
    query = f"SELECT {fields_str} FROM {object_name}"
    results = sf.query_all(query)

    records = results['records']
    df = pd.DataFrame(records).drop(columns='attributes')  # Remove unwanted attributes column
    
    return df





#### instead of passing tablestore, have an uatomated data pull?
# def create_df(source_dict, Source_data, table_store):
#     new_columns = [col for mapped_cols in source_dict['MAPPINGS'].values() for col in mapped_cols]
#     new_df = pd.DataFrame(columns=new_columns)

#     try:
#         Table_convert_cols = list(source_dict['TABLE_CONVERT'].keys())
#     except:
#         Table_convert_cols = []

#     for source_col, target_cols in source_dict['MAPPINGS'].items():
#         if source_col in Table_convert_cols:
#             map_table = source_dict['TABLE_CONVERT'][source_col]
#             print(map_table)
            
#             try:
#                 string_ids = [
#                         str(int(float(id))) if pd.notna(id) and id != 'nan' else np.nan
#                         for id in Source_data[source_col]                               #### handeling DRG col
#                     ]
#             except:
#                 string_ids = [str(id) for id in Source_data[source_col]]
         
#             Source_data[source_col] = string_ids #### col update to match same type in foreign table
#             foreign_table = list(map_table.keys())[0]
#             print(foreign_table)

#             merged = Source_data.merge(table_store[foreign_table].loc[:, map_table[foreign_table]], left_on=source_col, right_on=map_table[foreign_table][0], how='left')

#             for idx, col in enumerate(map_table[foreign_table], start=0):  # Start from the first column
#                 if len(map_table[foreign_table]) == 1:
#                     # Handle the edge case where only one column exists
#                     target_col = target_cols[0]  # Use the first target column
#                     print(f"Single column mapping: {col} to {target_col}")
#                     new_df[target_col] = merged[col]
#                     break  # Exit the loop since there is only one column to map
#                 elif idx > 0 and idx - 1 < len(target_cols):  # For columns after the first
#                     target_col = target_cols[idx - 1]  # Adjust indexing to match target columns
#                     print(f"Mapping {col} to {target_col}")
#                     new_df[target_col] = merged[col]
#                 else:
#                     print(f"Skipping column {col} or no target column available.")


#         if source_col not in Table_convert_cols:
#             if source_col in Source_data.columns:
#                 for target_col in target_cols:
#                     new_df[target_col] = Source_data[source_col]
        

#     keys = source_dict['KEYS']


#     return new_df, keys


#### instead of passing tablestore, have an uatomated data pull?
def create_df(source_dict, Source_data, needed_objects, sf):

    
    new_columns = [col for mapped_cols in source_dict['MAPPINGS'].values() for col in mapped_cols]
    new_df = pd.DataFrame(columns=new_columns)

    try:
        Table_convert_cols = list(source_dict['TABLE_CONVERT'].keys())
    except:
        Table_convert_cols = []

    for source_col, target_cols in source_dict['MAPPINGS'].items():
        if source_col in Table_convert_cols:
            map_table = source_dict['TABLE_CONVERT'][source_col]
            print(map_table)
            
            try:
                string_ids = [
                        str(int(float(id))) if pd.notna(id) and id != 'nan' else np.nan
                        for id in Source_data[source_col]                               #### handeling DRG col
                    ]
            except:
                string_ids = [str(id) for id in Source_data[source_col]]
         
            Source_data[source_col] = string_ids #### col update to match same type in foreign table
            foreign_table = list(map_table.keys())[0]
            # print(foreign_table)


            ### pull needed table
            print(f'pulling updated {foreign_table} table')
            sf_object = updated_data_pull(foreign_table, needed_objects, sf)


            merged = Source_data.merge(sf_object.loc[:, map_table[foreign_table]], left_on=source_col, right_on=map_table[foreign_table][0], how='left')

            for idx, col in enumerate(map_table[foreign_table], start=0):  # Start from the first column
                if len(map_table[foreign_table]) == 1:
                    # Handle the edge case where only one column exists
                    target_col = target_cols[0]  # Use the first target column
                    print(f"Single column mapping: {col} to {target_col}")
                    new_df[target_col] = merged[col]
                    break  # Exit the loop since there is only one column to map
                elif idx > 0 and idx - 1 < len(target_cols):  # For columns after the first
                    target_col = target_cols[idx - 1]  # Adjust indexing to match target columns
                    print(f"Mapping {col} to {target_col}")
                    new_df[target_col] = merged[col]
                else:
                    print(f"Skipping column {col} or no target column available.")


        if source_col not in Table_convert_cols:
            if source_col in Source_data.columns:
                for target_col in target_cols:
                    new_df[target_col] = Source_data[source_col]
        

    keys = source_dict['KEYS']


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
            sf_object.upsert(f"{object_key}/{record[object_key]}", record_upsert)
            # pass
        except Exception as e:
            print(f"Error during upsert: {e}")
            error_logs.append(e)

    return error_logs

            

from datetime import datetime
import pandas as pd

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



### how to prevent claims records from deleting that were in the past


# def delete_record(new_df, keys, table_store, sf):
#     # Extract Salesforce object name and key field
#     object_name = next(iter(keys.keys()))
#     object_key = keys[object_name]
    
#     # Get the Salesforce object reference
#     sf_object = getattr(sf, object_name)
    
#     # Retrieve pulled data for comparison
#     pulled_data = table_store[object_name]
#     print(f"Object: {object_name}, Records to process: {new_df.shape[0]}")
    
#     # Find matching record IDs to delete
#     matching_ids = pulled_data[pulled_data['Claim_ID__c'].isin(new_df[object_key])]['Id'].tolist()
#     print(f"Matching record IDs to delete: {len(matching_ids)}")
    
#     errors = []
    
#     # Delete records
#     for record_id in matching_ids:
#         try:
#             sf_object.delete(record_id)
#         except Exception as e:
#             errors.append((record_id, str(e)))
    
#     if errors:
#         print(f"Errors occurred during deletion:")
#         for record_id, error_message in errors:
#             print(f"Record ID {record_id}: {error_message}")
#     else:
#         print("All records deleted successfully.")


def delete_record(new_df, keys, needed_objects, sf):
    # Extract Salesforce object name and key field
    object_name = next(iter(keys.keys()))
    object_key = keys[object_name]
    
    # Get the Salesforce object reference
    sf_object = getattr(sf, object_name)
    
    # Retrieve pulled data for comparison
    print(f'pulling updated {object_name} table')
    pulled_data = updated_data_pull(object_name, needed_objects, sf)
    print(f"Object: {object_name}, Records to process: {new_df.shape[0]}")
    
    # Find matching record IDs to delete
    matching_ids = pulled_data[pulled_data[object_key].isin(new_df[object_key])]['Id'].tolist()
    print(f"Matching record IDs to delete: {len(matching_ids)}")
    
    errors = []
    
    # Delete records
    for record_id in matching_ids:
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
        
