#### fun defs
from simple_salesforce import Salesforce, SalesforceLogin
from my_functions.configs import desktop_configs as desk
from quickbooks.objects.customer import Customer
from intuitlib.client import AuthClient
from quickbooks import QuickBooks
import os, pyodbc, sys, math, pytz, logging, time, urllib, sqlalchemy, textwrap
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta
from collections import OrderedDict
from sqlalchemy import event
from pathlib import Path
import pandas as pd
import numpy as np



def salesforce_insert(import_config, cur, conn, filename, debug=False):
    sf = import_config['Salesforce_Connection']

## Import Data into Salesforce ##
    # Upsert Provider_TIN__c
    provider_tin_query = f"SELECT DISTINCT * FROM {import_config['azure_schema']}.import837_insert_ProviderTIN"
    df_provider_tin = pd.read_sql(provider_tin_query, conn)
    df_provider_tin_insert = df_provider_tin.drop(columns=['Unique_TIN__c'])

    for i in range(len(df_provider_tin_insert)):
        tax_id = df_provider_tin['Unique_TIN__c'][i]
        provider_tin_data = df_provider_tin_insert.iloc[i, :].to_dict()
        try:
            sf.Provider_TIN__c.upsert(f"Unique_TIN__c/{tax_id}", provider_tin_data)
        except:
            logging.error(f"Error upserting into Provider_TIN__c for {tax_id}")

    # Get Claims Data
    claims_query = f"SELECT * FROM {import_config['azure_schema']}.import837_insert_Claims"
    df_claims = pd.read_sql(claims_query, conn)

    # Get Line Item Data
    line_items_query = f"SELECT * FROM {import_config['azure_schema']}.import837_insert_Lines"
    df_line_items = pd.read_sql(line_items_query, conn)

    if debug == True:
        print(df_claims)
        print(df_line_items)

    for j in range(len(df_claims)):
# Handle Duplicate Claim_IDs
    ## ** Handle duplicate Claim IDs in the same file (?) ##

    ## Duplicate Claim IDs in Salesforce ##
        duplicate_claim_counter = 1
        check_claim_id_query = f"SELECT Id FROM Claims__c WHERE Claim_ID__c = '{df_claims['Claim_ID__c'][j]}' AND Client__c = '{import_config['Client_Id']}'"
        df_check_claim_id = fun.resp_to_dataframe(sf.query_all(check_claim_id_query))
        if len(df_check_claim_id) > 0:
            df_claims.loc[j, 'Claim_ID__c'] = df_claims['Claim_ID__c'][j] + '-01'
            duplicate_claim_counter += 1
            duplicate = True
            while duplicate == True:
                check_claim_id_query = f"SELECT Id FROM Claims__c WHERE Claim_ID__c = '{df_claims['Claim_ID__c'][j]}' AND Client__c = '{import_config['Client_Id']}'"
                df_check_claim_id = fun.resp_to_dataframe(sf.query_all(check_claim_id_query))
                if len(df_check_claim_id) > 0:
                    df_claims.loc[j, 'Claim_ID__c'] = df_claims['Claim_ID__c'][j][:-2] + str(duplicate_claim_counter).zfill(2)
                    duplicate_claim_counter += 1
                else:
                    duplicate = False
            logging.info(f"Duplicate found for Client/ClaimID {import_config['Client_Id']}/{df_claims['Original_Claim_ID__c'][j]} - Updated ClaimID to {df_claims['Claim_ID__c'][j]}")
            df_line_items.loc[df_line_items['Claim_ID__c'] == df_claims['Original_Claim_ID__c'][j], 'Claim_ID__c'] = df_claims['Claim_ID__c'][j]
            cur.execute(f"UPDATE claim_log SET Updated_Claim_ID = '{df_claims['Claim_ID__c'][j]}' FROM {import_config['azure_schema']}.[837_Claim_Log] claim_log WHERE Original_Claim_ID = '{df_claims['Original_Claim_ID__c'][j]}' AND Client__c = '{import_config['Client_Id']}' AND ImportDate = '{df_claims['ImportDate'][j]}'")
            cur.execute(f"UPDATE claim_detail SET Updated_Claim_ID = '{df_claims['Claim_ID__c'][j]}' FROM {import_config['azure_schema']}.export837_Claims_Detail claim_detail WHERE Original_Claim_ID = '{df_claims['Original_Claim_ID__c'][j]}' AND Client__c = '{import_config['Client_Id']}' AND ImportDate = '{df_claims['ImportDate'][j]}'")
            cur.commit()
        ## Get Related Object Id Values (Claims)
        try:
            provider_tin__c = sf.query_all(f"SELECT Id FROM Provider_TIN__c WHERE Unique_TIN__c = '{df_claims['Provider_TIN'][j]}'").get('records')[0].get('Id')
            df_claims.loc[j, 'Provider_TIN__c'] = provider_tin__c
        except:
            logging.error(f"Error querying Provider_TIN__c for {df_claims['Provider_TIN'][j]} - Claim Id {df_claims['Claim_ID__c'][j]}")

        try:
            jurisdiction__c = sf.query_all(f"SELECT Id FROM Jurisdiction__c WHERE Name = '{df_claims['Jurisdiction'][j]}'").get('records')[0].get('Id')
            df_claims.loc[j, 'Jurisdiction__c'] = jurisdiction__c
        except:
            logging.error(f"Error querying Jurisdiction__c for {df_claims['Jurisdiction'][j]} - Claim Id {df_claims['Claim_ID__c'][j]}")

        if df_claims.loc[j, 'Provider_Specialty'] is None or df_claims.loc[j, 'Provider_Specialty'] == 'None':
            logging.info(f"No provider specialty found for Claim Id {df_claims['Claim_ID__c'][j]}")
        else:
            try:
                provider_specialty__c = sf.query_all(f"SELECT Id FROM Provider_Specialty__c WHERE Name = '{df_claims['Provider_Specialty'][j]}'").get('records')[0].get('Id')
                df_claims.loc[j, 'Provider_Specialty__c'] = provider_specialty__c
            except:
                try:
                    spec_result = sf.Provider_Specialty__c.create({'Name': df_claims['Provider_Specialty'][j]})
                    logging.info(f"Provider Specialty {df_claims['Provider_Specialty'][j]} created")
                    df_claims.loc[j, 'Provider_Specialty__c'] = spec_result.get('id')
                except:
                    logging.error(f"Error querying Provider_Specialty__c for {df_claims['Provider_Specialty'][j]} - Claim Id {df_claims['Claim_ID__c'][j]}")

        try:
            drg__c = sf.query_all(f"SELECT Id FROM DRG__c WHERE Name = '{df_claims['DRG'][j]}'").get('records')[0].get('Id')
            df_claims.loc[j, 'DRG__c'] = drg__c
        except:
            logging.error(f"Error querying DRG__c for {df_claims['DRG'][j]} - Claim Id {df_claims['Claim_ID__c'][j]}")

        try:
            original_claim_for_reconsideration__c = sf.query_all(f"SELECT Id FROM Claims__c WHERE Name = '{df_claims['Original_Claim_for_Reconsideration'][j]}' AND Group_Client__c = '{df_claims['Group_Client__c'][j]}'").get('records')[0].get('Id')
            df_claims.loc[j, 'Original_Claim_for_Reconsideration__c'] = original_claim_for_reconsideration__c
        except:
            logging.error(f"Error querying Original_Claim_for_Reconsideration for {df_claims['Original_Claim_for_Reconsideration'][j]} - Group_Client {df_claims['Group_Client__c'][j]}")

    df_claims_insert = df_claims.drop(columns=['Original_Claim_ID__c','Jurisdiction','Provider_TIN','Provider_Specialty','DRG','Original_Claim_for_Reconsideration','RHP_ID__c','ImportDate'])

    ## Get Related Object Id Values (LineItems)
        # ** Pull in HCPCS CPT Code description for record creation?
    for k in range(len(df_line_items)):
        if df_line_items.loc[k, 'HCPCS_CPT_Code'] is None or df_line_items.loc[k, 'HCPCS_CPT_Code'] == 'None':
            logging.info(f"No HCPCS_CPT_Code found for Claim Id {df_line_items['Claim_ID__c'][k]}, Line {k}")
        else:
            try:
                hcpcs_cpt_code__c = sf.query_all(f"SELECT Id FROM HCPCS_CPT_Code__c WHERE Name = '{df_line_items['HCPCS_CPT_Code'][k]}'").get('records')[0].get('Id')
                df_line_items.loc[k, 'HCPCS_CPT_Code__c'] = hcpcs_cpt_code__c
            except:
                try:
                    code_result = sf.HCPCS_CPT_Code__c.create({'Name': df_line_items['HCPCS_CPT_Code'][k]})
                    logging.info(f"HCPCS_CPT_Code {df_line_items['HCPCS_CPT_Code'][k]} created")
                    df_line_items.loc[k, 'HCPCS_CPT_Code__c'] = code_result.get('id')
                except:
                    logging.error(f"Error querying HCPCS_CPT_Code__c for {df_line_items['HCPCS_CPT_Code'][k]} - Claim Id {df_line_items['Claim_ID__c'][k]}")

    ## Insert Claims/LineItems into Salesforce ##
    for i in range(len(df_claims_insert)):
        claim_data = df_claims_insert.iloc[i, :].to_dict()
        claim_data['Date_of_Birth__c'] = claim_data['Date_of_Birth__c'].strftime('%Y-%m-%d') if claim_data['Date_of_Birth__c'] is not None else None
        result = sf.Claims__c.create(claim_data)
        df_claims.loc[i, 'RHP_ID__c'] = result.get('id')
        logging.info(f"Claim {df_claims['Claim_ID__c'][i]} created - RHP_Id: {result.get('id')}")

    for m in range(len(df_claims)):
        claim_id = df_claims.loc[m, 'Claim_ID__c']
        claim_number = df_claims.loc[m, 'Claim_Number__c']
        df_line_items.loc[(df_line_items['Claim_ID__c'] == claim_id) & (df_line_items['Claim_Number__c'] == claim_number), 'RHP_ID__c'] = df_claims.loc[m, 'RHP_ID__c']

    df_line_items_insert = df_line_items.drop(columns=['Client_ID__c', 'Original_Claim_ID__c', 'Claim_ID__c', 'Claim_Number__c', 'HCPCS_CPT_Code'])

    for j in range(len(df_line_items_insert)):
        line_data = df_line_items_insert.iloc[j, :].to_dict()
        line_data['DOS__c'] = line_data['DOS__c'].strftime('%Y-%m-%d') if line_data['DOS__c'] is not None else None
        line_result = sf.Line_Items__c.create(line_data)
        logging.info(f"Line Item {line_result.get('id')} created - RHP_Id__c: {df_line_items['RHP_ID__c'][j]}")

    ## Update RHP_ID__c in Azure tables ##
    for k in range(len(df_claims)):
        cur.execute(f"UPDATE claim_log SET RHP_ID__c = '{df_claims.iloc[k]['RHP_ID__c']}' FROM {import_config['azure_schema']}.[837_Claim_Log] claim_log \
                    WHERE Original_Claim_ID = '{df_claims.iloc[k]['Original_Claim_ID__c']}' AND Claim_Number__c = '{df_claims.iloc[k]['Claim_Number__c']}' AND Client__c = '{df_claims.iloc[k]['Client__c']}' AND ImportDate = '{df_claims['ImportDate'][k]}'")
        cur.execute(f"UPDATE claim_detail SET RHP_ID__c = '{df_claims.iloc[k]['RHP_ID__c']}' FROM {import_config['azure_schema']}.export837_Claims_Detail claim_detail \
                    WHERE Original_Claim_ID = '{df_claims.iloc[k]['Original_Claim_ID__c']}' AND Claim_Number__c = '{df_claims.iloc[k]['Claim_Number__c']}' AND Client__c = '{df_claims.iloc[k]['Client__c']}' AND ImportDate = '{df_claims['ImportDate'][k]}'")
        cur.commit()

    logging.info(f"File {filename} imported into Salesforce")
    print(f"imported into Salesforce")

    return df_claims, df_line_items