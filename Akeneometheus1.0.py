from dotenv import load_dotenv
load_dotenv()
import time
import json
import os
import logging
import logzero
import numpy as np
import pandas as pd
import pyodbc
import subprocess
import requests
from subprocess import Popen
import json
import pickle
from datetime import datetime
from pandas.io.json import json_normalize

def makeWrikeTask (title = "New Pricing Task", description = "No Description Provided", status = "Active", assignees = "KUAAY4PZ", folderid = "IEAAJKV3I4JBAOZD"):
    url = "https://www.wrike.com/api/v4/folders/" + folderid + "/tasks"
    querystring = {
        'title':title,
        'description':description,
        'status':status,
        'responsibles':assignees
        } 
    headers = {
        'Authorization': 'bearer TOKEN'.replace('TOKEN',os.environ.get(r"WRIKE_TOKEN"))
        }        
    response = requests.request("POST", url, headers=headers, params=querystring)
    return response

def attachWrikeTask (attachmentpath, taskid):
    url = "https://www.wrike.com/api/v4/tasks/" + taskid + "/attachments"
    headers = {
                'Authorization': 'bearer TOKEN'.replace('TOKEN',os.environ.get(r"WRIKE_TOKEN"))

    }

    files = {
        'X-File-Name': (attachmentpath, open(attachmentpath, 'rb')),
    }

    response = requests.post(url, headers=headers, files=files)
    print(response)
    return response       


def flatten_json(nested_json, exclude=['']):
    out = {}
    def flatten(x, name='', exclude=exclude):
        if type(x) is dict:
            for a in x:
                if a not in exclude:
                    flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
                if '_products' or 'categories' in name:
                    out[name[:-1]] = x
                elif a not in exclude: 
                    flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x
    flatten(nested_json)
    return out    

def construct_qarl_sql(table, row, code, sql_type = 'update'):   
    no_quote_columns = ['ClearanceFlag','Weight','ShipWeight','ShipLength','ShipWidth','ShipHeight']
    row_dict = row.dropna().to_dict()
    if table == 'ProductInfo':
        row_dict['DateUpdated'] = datetime.today().strftime("%m/%d/%Y")
    if sql_type == 'update':
        del row_dict['ItemCode']
        #sql_set_data = ", ".join([k + " = '" + v + "'" for k,v in row_dict.items()])
        sql_set_data = ", ".join([k + " = " + str(v) if k in no_quote_columns else k + " = '" + v.replace("'","''") + "'" for k,v in row_dict.items()])
        sql = """UPDATE target_table 
                SET data_set
                WHERE ItemCode = 'row_ItemCode'""".replace('target_table',table).replace('row_ItemCode',code).replace('data_set',sql_set_data)
    elif sql_type =='add':
        row_keys = ",".join([k for k in row_dict.keys()])
        row_data = ",".join([str(row_dict[k]) if k in no_quote_columns else "'" + row_dict[k] + "'" for k in row_dict.keys()])
        sql = """INSERT INTO target_table (table_columns) 
                VALUES (table_values)""".replace('target_table',table).replace('table_columns',row_keys).replace('table_values',row_data)   
    return sql

def make_json_attribute_data_nest(row, column_name, unit, currency):
    if row[column_name] is None or row[column_name] is np.nan or str(row[column_name]) == 'nan':
        # or str(row[column_name]) == ''
        row[column_name] = np.nan  
    elif type(row[column_name]) != list:
        if isinstance(row[column_name], bool):
            d = row[column_name]
        elif not isinstance(row[column_name], str):
            d = str(row[column_name]).encode().decode()
        else:
            d = row[column_name].encode().decode()
        if unit is not None and currency is None:
            if row[column_name] == '':
                row[column_name] = np.nan
                return row
            else:
                d = np.array({"amount":d,"unit":unit}).tolist()
        elif unit is None and currency is not None:
            d = [np.array({"amount":d,"currency":currency}).tolist()]
        d = {"data":d,"locale":None,"scope":None}
        row[column_name] = [d]
    return row    

if __name__ == '__main__':

    logzero.loglevel(logging.WARN)

    try:
        from akeneo_api_client.client import Client
    except ModuleNotFoundError as e:
        import sys
        sys.path.append("..")
        from akeneo_api_client.client import Client

    AKENEO_CLIENT_ID = os.environ.get("AKENEO_CLIENT_ID")
    AKENEO_SECRET = os.environ.get("AKENEO_SECRET")
    AKENEO_USERNAME = os.environ.get("AKENEO_USERNAME")
    AKENEO_PASSWORD = os.environ.get("AKENEO_PASSWORD")
    AKENEO_BASE_URL = os.environ.get("AKENEO_BASE_URL")

    akeneo = Client(AKENEO_BASE_URL, AKENEO_CLIENT_ID,
                    AKENEO_SECRET, AKENEO_USERNAME, AKENEO_PASSWORD)

    searchparams = '{"search":{"parent":[{"operator":"=","value":"products"}]}}'

    #make JSON for API call
    aksearchparam = json.loads(searchparams)

    #Get API object to iternate through
    result = akeneo.categories.fetch_list(aksearchparam)

    #setting up the dataframe to be filled   
    pandaObject = pd.DataFrame(data=None)  

    only_wanted_certain_columns_list = ['code','parent','labels_en_US']

    #loopy toogles
    go_on = True
    count = 0
    #for i in range(1,2):  #this is for testing ;)  
    while go_on:
        count += 1
        try:                
            page = result.get_page_items()

            #flatten a page JSON response into a datafarme (excludes the JSON fields that are contained in the list below)
            pagedf = pd.DataFrame([flatten_json(x,[]) for x in page])

            #This code would be used if you only wanted certain columns...since we defined which attributes to grab, we don't need this
            pagedf.drop(pagedf.columns.difference(only_wanted_certain_columns_list), 1, inplace=True)
            
            #This appends each 'Page' from the the API to the              
            pandaObject = pandaObject.append(pagedf, sort=False)
        except:
            #...means we reached the end of the API pagination
            go_on = False
            break
        go_on = result.fetch_next_page()

    pandaObject = pandaObject.reset_index(drop=True)

    parentlist = ['products']
    L1CatDF = pandaObject[pandaObject['parent'].isin(parentlist)]
    parentlist = list(set(L1CatDF['code']))
    L1CatDF = L1CatDF.rename(columns={'labels_en_US':'Category1'})
    L1CatDF = L1CatDF.set_index('code')
    
    L2CatDF = pandaObject[pandaObject['parent'].isin(parentlist)]
    parentlist = list(set(L2CatDF['code']))
    L2CatDF = L2CatDF.rename(columns={'labels_en_US':'Category2'})
    L2CatDF = L2CatDF.reindex(columns = L2CatDF.columns.tolist() + ["Category1"]).set_index('parent')
    L2CatDF.update(L1CatDF)
    L2CatDF = L2CatDF.set_index('code')

    L3CatDF = pandaObject[pandaObject['parent'].isin(parentlist)]
    parentlist = list(set(L3CatDF['code']))
    L3CatDF = L3CatDF.rename(columns={'labels_en_US':'Category3'})
    L3CatDF = L3CatDF.reindex(columns = L3CatDF.columns.tolist() + ["Category1", "Category2"]).set_index('parent')
    L3CatDF.update(L2CatDF)
    L3CatDF = L3CatDF.set_index('code')

    L4CatDF = pandaObject[pandaObject['parent'].isin(parentlist)]
    parentlist = list(set(L4CatDF['code']))
    L4CatDF = L4CatDF.rename(columns={'labels_en_US':'Category4'})
    L4CatDF = L4CatDF.reindex(columns = L4CatDF.columns.tolist() + ["Category1", "Category2","Category3"]).set_index('parent')
    L4CatDF.update(L3CatDF)
    L4CatDF = L4CatDF.set_index('code')    

    try:
        L5CatDF = pandaObject[pandaObject['parent'].isin(parentlist)]
        parentlist = list(set(L5CatDF['code']))
        L5CatDF = L5CatDF.rename(columns={'labels_en_US':'Category5'})
        L5CatDF = L5CatDF.reindex(columns = L5CatDF.columns.tolist() + ["Category1", "Category2","Category3","Category4"]).set_index('parent')
        L5CatDF.update(L4CatDF)
        L5CatDF = L5CatDF.set_index('code')    
    except:
        pass

    pandaObject = pandaObject.reindex(columns = pandaObject.columns.tolist() + ["Category1", "Category2","Category3","Category4","Category5"]).set_index('code',drop=True)    
    
    pandaObject.update(L1CatDF)
    pandaObject.update(L2CatDF)
    pandaObject.update(L3CatDF)
    pandaObject.update(L4CatDF)
    pandaObject.update(L5CatDF)

    print(pandaObject)

    pandaObject = pandaObject.reset_index()

    akeneo_att_string = ','.join(['webCategory1','webCategory2','webCategory3']) #these fellas toggle whether or not data needs to be synced back to systems

    #Now Query for just the items that have TED akeneo cateogory codes
    searchparams = """
    {
    "limit":100,
    "scope":"ecommerce",
    "attributes":"search_atts",
    "with_count":true,
    "search":{
        "categories":[
            {
                "operator":"IN CHILDREN",
                "value":[
                "products"
                ]
            }
        ]
    }
    }
    """.replace('search_atts',akeneo_att_string)    

    #make JSON for API call
    aksearchparam = json.loads(searchparams)

    #Get API object to iternate through
    result = akeneo.products.fetch_list(aksearchparam)

    #setting up the dataframe to be filled   
    productdf = pd.DataFrame(data=None)  

    #loopy toogles
    go_on = True
    count = 0
    #for i in range(1,1175):  #this is for testing ;)  
    while go_on:
        count += 1
        try:                
            page = result.get_page_items()
            print(str(count) + ": normalizing")  

            #flatten a page JSON response into a datafarme (excludes the JSON fields that are contained in the list below)
            pagedf = pd.DataFrame([flatten_json(x,[]) for x in page])

            #This code would be used if you only wanted certain columns...since we defined which attributes to grab, we don't need this
            #pagedf.drop(pagedf.columns.difference(only_wanted_certain_columns_list), 1, inplace=True)
            
            #This appends each 'Page' from the the API to the              
            productdf = productdf.append(pagedf, sort=False)
        except:
            #...means we reached the end of the API pagination
            go_on = False
            break
        go_on = result.fetch_next_page()


    catlist = pandaObject['code'].tolist()
    df2 = productdf.reindex().explode('categories')
    df2 = df2[df2['categories'].isin(catlist)]
    
    #df2.drop_duplicates(subset=['identifier'])
    df2 = df2.merge(pandaObject, left_on='categories', right_on='code')

    sage_conn_str = os.environ.get(r"sage_conn_str").replace("UID=;","UID=" + os.environ.get(r"sage_login") + ";").replace("PWD=;","PWD=" + os.environ.get(r"sage_pw") + ";")             
            
    #This makes the connection to Sage based on the string above.
    cnxn = pyodbc.connect(sage_conn_str, autocommit=True)

    #This is responsible for selecting what data to pull from Sage.
    sql = """SELECT 
                CI_Item.ItemCode, 
                CI_Item.UDF_CATEGORY1, 
                CI_Item.UDF_CATEGORY2, 
                CI_Item.UDF_CATEGORY3, 
                CI_Item.UDF_CATEGORY4, 
                CI_Item.UDF_CATEGORY5,
                CI_Item.UDF_CATEGORY_ID
            FROM 
                CI_Item CI_Item
    """
    sagedf = pd.read_sql(sql,cnxn)    
    pandaObject = df2.merge(sagedf, left_on='identifier', right_on='ItemCode')

    #Get the deepest node
    pandaObject.fillna('', inplace=True) #Line below breaks if there are nulls present...this fixes
    pandaObject['FullString'] = pandaObject[['Category1', 'Category2', 'Category3', 'Category4', 'Category5']].agg('-'.join, axis=1)

    #Sort deepest to top and drop duplicates...likely the least prescise category is removed..on a tie you have whomever is alphabetically last
    pandaObject.sort_values(ascending=False, by='FullString', inplace= True)
    pandaObject.drop_duplicates(subset=['identifier'], inplace=True)

    #pandaObject.to_excel(r'\\FOT00WEB\Alt Team\Andrew\Andrews_Code\product-cat-dumps.xlsx', index=True)
    pandaObject.to_csv(r'\\FOT00WEB\Alt Team\Andrew\Andrews_Code\product-cat-dumps.csv', index=True, sep = '|', header=False )

    #Only stuff that's changing
    pandaObject.loc[(pandaObject['code'] != pandaObject['UDF_CATEGORY_ID']), 'syncme'] = 'Y' 
    pandaObject = pandaObject.loc[pandaObject['syncme'] == 'Y']

    pandaObject.drop(pandaObject.columns.difference(['ItemCode','Category1','Category2','Category3','Category4','Category5','code']), 1, inplace=True)

    if pandaObject.shape[0] > 0:            
        #sage data batch file
        print('syncing: ' + str(pandaObject.shape[0]))
        pandaObject.to_csv('\\\\FOT00WEB\\Alt Team\\Qarl\\Automatic VI Jobs\\AkeneoSync\\from_akeneo_cat_sync_VIWI7A.csv', columns=['ItemCode','Category1','Category2','Category3','Category4','Category5','code'], header=False, sep='|', index=False) 
        print('to csv')
        time.sleep(6) 
        p = subprocess.Popen('Auto_SyncAkeneoCats_VIWI7A.bat', cwd= 'Y:\\Qarl\\Automatic VI Jobs\\AkeneoSync', shell = True)
        stdout, stderr = p.communicate()   
        print('to sage done')
    else:
        print('nothing to sync')
    print('done!')    
