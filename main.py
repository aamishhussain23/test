from typing import Any, Dict, List, Union
from fastapi import FastAPI, HTTPException, Request, Response
from pydantic import BaseModel
from datetime import datetime
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
import json
import psycopg2 
import uuid
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

# CORS configuration to allow all origins
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class TokenData(BaseModel):
    token: str
    sheetId: str
    tabId: int
    email: str

class CreateEndpointRequest(BaseModel):
    sheetId: str
    tabId: int
    sheetName : str

def collect_keys(data, level=0, keys_dict=[[]], prevkey="", colchanges=[]):
    if level>(len(keys_dict)-1):
        if level>0:
            keys_dict.append(['']*len(keys_dict[level-1]))
    elif level>0:
        if len(keys_dict[level])<len(keys_dict[level-1]):
            y = len(keys_dict[level])
            while y < len(keys_dict[level-1]):
                keys_dict[level].append('')
                y = y + 1
    if isinstance(data, dict):
        for key, value in data.items():
            if prevkey!= "":
                key = prevkey+"char$tGPT"+key
            if level>0:    
                if key not in keys_dict[level]:
                    rev_index = keys_dict[level-1][::-1].index(prevkey)
                    y = len(keys_dict[level-1])-rev_index-1
                    if 'char$tGPT'.join(keys_dict[level][y].split('char$tGPT')[:-1])==prevkey:
                        y = y + 1
                    keys_dict[level].insert(y,key)
                    if not isinstance(value,dict) or isinstance(value,list):
                        colchanges.append(y)
                    if len(keys_dict[level])>y:
                        level2=level
                        oldkey = key
                        while level2>0:                                
                            oldkey = 'char$tGPT'.join(oldkey.split('char$tGPT')[:-1])
                            rev_index =  keys_dict[level2-1][::-1].index(oldkey)
                            index = len(keys_dict[level2-1])-rev_index-1
                            m=0
                            times=0
                            while m<len(keys_dict[level2]):
                                if 'char$tGPT'.join(keys_dict[level2][m].split('char$tGPT')[:-1])==oldkey:
                                    times = times + 1
                                m = m+1
                            if times>keys_dict[level2-1].count(oldkey):
                                keys_dict[level2-1].insert(index,oldkey)      
                            level2 = level2-1
                        
                        level2=level
                        if not isinstance(value,dict) and not isinstance(value,list):
                            while level2<len(keys_dict)-1:
                                keys_dict[level2+1].insert(y,'')
                                level2 = level2+1
                if isinstance(value,dict):
                    collect_keys(value,level+1,keys_dict,key,colchanges)
                elif isinstance(value,list):
                    for j in range(0,len(value)):
                        if isinstance(value[j],dict):
                            collect_keys(value[j],level+1,keys_dict,key,colchanges)
            else: 
                if key not in keys_dict[level]:
                    keys_dict[level].append(key)
                if isinstance(value,dict):
                    collect_keys(value,level+1,keys_dict,key,colchanges)
                elif isinstance(value,list):
                    for j in range(0,len(value)):
                        if isinstance(value[j],dict):
                            collect_keys(value[j],level+1,keys_dict,key,colchanges)
    return [keys_dict,colchanges]

def fill_rows(data, level=0, keys_dict=[],row=[],rowlevel=0,prevkey="",pos={}):
    if row == []:
        row.append(['']*len(keys_dict[0]))
    if rowlevel>(len(row)-1):
        row.append(['']*len(keys_dict[0]))
    if isinstance(data, dict):
        for key, value in data.items():
            if prevkey!= "":
                key = prevkey+"char$tGPT"+key
            if key in keys_dict[level]:
                if isinstance(value,dict):
                    fill_rows(value,level+1,keys_dict,row,rowlevel,key,pos)
                elif isinstance(value,list):                                                       
                    z=0
                    if 'char$tGPT'.join(key.split('char$tGPT')[:-1]) in pos:
                        if pos['char$tGPT'.join(key.split('char$tGPT')[:-1])]>1:
                            rowlevel = pos['char$tGPT'.join(key.split('char$tGPT')[:-1])]
                            z = 1
                    poslevel = rowlevel
                    starter = rowlevel
                    if key not in pos:
                        pos[key]=1
                    for j in range(0,len(value)):
                        if isinstance(value[j],dict):                        
                            fill_rows(value[j],level+1,keys_dict,row,poslevel,key,pos)
                            poskey = key
                            while 'char$tGPT' in poskey:  
                                if len(poskey.split('char$tGPT')[:-1])==1:
                                    poskey = poskey.split('char$tGPT')[0]
                                else:    
                                    poskey = 'char$tGPT'.join(poskey.split('char$tGPT')[:-1])
                                if poskey in pos:
                                    if poslevel != rowlevel:
                                        pos[poskey] = pos[poskey] + 1
                                    elif poslevel == rowlevel and z==1:
                                        pos[poskey] = pos[poskey] + 1
                                else:
                                    pos[poskey] = 1                            
                            poslevel = starter + pos[key]
                        else:
                            index = keys_dict[level].index(key)
                            row[rowlevel][index] = repr(value)
                            break             
                else:
                    index = keys_dict[level].index(key)
                    row[rowlevel][index] = str(value)
    else:
        index = keys_dict[level].index(key)
        row[rowlevel][index] = str(value)
    return row

def format_keys(keys_dict):
    max_len = max(len(keys) for keys in keys_dict)
    formatted_keys = []
    for keys in keys_dict:
        while len(keys) < max_len:
            keys.append('')
        formatted_keys.append(keys)
    keys = formatted_keys
    y1 = 0
    while y1<len(keys[0]):
        y2 = 0
        counter = 0
        while y2<len(keys):
            if keys[y2][y1]=='':
                counter = counter+1
            y2 = y2 + 1
        if counter==len(keys):
            keys = [[row[i] for i in range(len(row)) if i != y1] for row in keys]
        else:
            y1 = y1 + 1
    return keys 

def getback(keys):
    y1 = 0
    while y1<len(keys):
        y2 = 0
        while y2<len(keys[y1]):
            if keys[y1][y2]!='':
                keys[y1][y2] = keys[y1][y2].split('char$tGPT')[-1]
            y2 = y2 + 1
        y1 = y1 + 1
    return keys    

def merge(keys,keys1):
    requests = []
    requests.append({
        "updateCells": {
            "range": {
                "sheetId": 0,
                "startRowIndex": 0,
                "endRowIndex": len(keys1),
                "startColumnIndex": 0,
                "endColumnIndex": len(keys1[0])
            },
            "rows": [{
                "values": [
                {"userEnteredValue": {"stringValue": str(cell)}} for cell in row
            ]} for row in keys1
        ],
        "fields": "userEnteredValue"
        }
    })
    y1 = 0
    while y1<len(keys):
        y2 = 0
        count=1
        while y2<len(keys[y1]):
            if y2>0:
                if keys[y1][y2]==keys[y1][y2-1] and keys[y1][y2]!='':
                    count = count+1
                else:
                    if count>1:
                        requests.append({
                            'mergeCells': {
                                'range': {
                                    'sheetId': 0,
                                    'startRowIndex': y1,
                                    'endRowIndex': y1+1,
                                    'startColumnIndex': y2-count,
                                    'endColumnIndex': y2
                                    },
                                'mergeType': 'MERGE_ALL'
                                }
                            })
                    count = 1
            y2 = y2 + 1
        if count>1:
            requests.append({
                'mergeCells': {
                    'range': {
                        'sheetId': 0,
                        'startRowIndex': y1,
                        'endRowIndex': y1+1,
                        'startColumnIndex': y2-count,
                        'endColumnIndex': y2
                        },
                    'mergeType': 'MERGE_ALL'
                    }
                })
        y1 = y1 + 1
    y1 = 0
    while y1<len(keys[0]):
        count = 1
        y2 = 0
        while y2<len(keys):
            if y2>0:
                if keys[y2][y1]=='':
                    count = count+1
                else:
                    if count>1:
                        requests.append({
                            'mergeCells': {
                                'range': {
                                    'sheetId': 0,
                                    'startRowIndex': y2-count,
                                    'endRowIndex': y2,
                                    'startColumnIndex': y1,
                                    'endColumnIndex': y1+1
                                    },
                                'mergeType': 'MERGE_ALL'
                                }
                            })
                    count = 1
            y2 = y2 + 1
        if count>1:
            requests.append({
                'mergeCells': {
                    'range': {
                        'sheetId': 0,
                        'startRowIndex': y2-count,
                        'endRowIndex': y2,
                        'startColumnIndex': y1,
                        'endColumnIndex': y1+1
                        },
                    'mergeType': 'MERGE_ALL'
                    }
                })
        y1 = y1 + 1
    return requests            

def value_merge(rows,pos):
    requests = []
    requests.append({
        "updateCells": {
            "range": {
                "sheetId": 0,
                "startRowIndex": pos,
                "endRowIndex": pos+len(rows),
                "startColumnIndex": 0,
                "endColumnIndex": len(rows[0])
            },
            "rows": [{
                "values": [
                {"userEnteredValue": {"stringValue": str(cell)}} for cell in row
            ]} for row in rows
        ],  
        "fields": "userEnteredValue"
        }
    })
    return requests   

@app.post("/sendtoken", response_model=None)
async def receive_token(data: TokenData):
    print("hello")
    insert_query = """INSERT INTO oauth_token ("token","sheetId","utcTime") VALUES (%s, %s, %s) ON CONFLICT ("sheetId")  DO UPDATE SET "token" = EXCLUDED."token", "utcTime" = EXCLUDED."utcTime";"""
    data_query = (data.token, data.sheetId, datetime.now())
    conn_sq = psycopg2.connect("postgresql://retool:yosc9BrPx5Lw@ep-silent-hill-00541089.us-west-2.retooldb.com/retool?sslmode=require")
    cur_sq = conn_sq.cursor()
    cur_sq.execute(insert_query, data_query)
    conn_sq.commit()
    
    # Check if 'Hits' sheet exists, if not, create it
    creds = Credentials(token=data.token)
    service = build('sheets', 'v4', credentials=creds)
    sheet_metadata = service.spreadsheets().get(spreadsheetId=data.sheetId).execute()
    sheets = sheet_metadata.get('sheets', '')
    sheet_names = [sheet['properties']['title'] for sheet in sheets]

    if 'Hits' not in sheet_names:
        requests = [{
            'addSheet': {
                'properties': {
                    'title': 'Hits',
                    'gridProperties': {
                        'rowCount': 100,
                        'columnCount': 5  # Decrease column count to 5
                    }
                }
            }
        }]
        body = {
            'requests': requests
        }
        service.spreadsheets().batchUpdate(spreadsheetId=data.sheetId, body=body).execute()

        # Add headers to the 'Hits' sheet without 'Data' column
        headers = [['URL', 'Hostname', 'User Agent', 'Date/Time', 'Method']]
        range_name = 'Hits!A1'
        body = {
            'values': headers
        }
        service.spreadsheets().values().update(
            spreadsheetId=data.sheetId,
            range=range_name,
            valueInputOption='RAW',
            body=body
        ).execute()

    return {"status": "token received and Hits sheet ensured"}


@app.post("/datasink/{param:path}")
async def receive_token(param: str, data: Dict):
    return await process_data_sink(param, data)

async def process_data_sink(param: str, data: Dict):
    conn = psycopg2.connect("postgresql://retool:yosc9BrPx5Lw@ep-silent-hill-00541089.us-west-2.retooldb.com/retool?sslmode=require")
    cur = conn.cursor()
    query = """SELECT "sheetId", "tabId", "rows" FROM header_structure WHERE "param" = %s;"""
    cur.execute(query, (param,))
    row = cur.fetchone()

    if row:
        query = """SELECT "token" FROM oauth_token WHERE "sheetId" = %s;"""
        cur.execute(query, (row[0],))
        token = cur.fetchone()
        access_token = token[0]
        creds = Credentials(token=access_token)
        service = build('sheets', 'v4', credentials=creds)

        raw_feed = getdata(token[0], row[0], row[1], row[2])

        # existing data
        header = raw_feed[0]
        lastrow = raw_feed[1]

        # new keys cleaning
        results = collect_keys(data, 0, header, "", [])

        cleaned = format_keys(results[0])

        # new data cleaning
        pos = {}
        datarow = fill_rows(data, 0, cleaned, [], 0, "", pos)

        cleaned_2 = getback(cleaned.copy())

        if len(cleaned) > int(row[2]):
            requests = [
                {
                    "insertDimension": {
                        "range": {
                            "sheetId": row[1],
                            "dimension": "ROWS",
                            "startIndex": int(row[2]),
                            "endIndex": len(cleaned)
                        },
                        "inheritFromBefore": False  # or True depending on context
                    }
                }
            ]

            body = {
                'requests': requests
            }

            service.spreadsheets().batchUpdate(spreadsheetId=row[0], body=body).execute()

        requests = []
        if len(results[1]) > 0:
            for j in range(len(results[1])):
                requests.append({
                    "insertRange": {
                        "range": {
                            "sheetId": row[1],
                            "startRowIndex": len(cleaned),
                            "endRowIndex": lastrow + len(cleaned) - int(row[2]) + 1,
                            "startColumnIndex": results[1][j],
                            "endColumnIndex": results[1][j] + 1,
                        },
                        "shiftDimension": "COLUMNS"
                    }})

            body = {
                'requests': requests
            }
            service.spreadsheets().batchUpdate(spreadsheetId=row[0], body=body).execute()

        # write the header
        requests = []
        requests = merge(cleaned, cleaned_2)

        # write the row
        requests.append(value_merge(datarow, lastrow + len(cleaned) - int(row[2])))

        clear_formatting_request = {
            'requests': [{
                'unmergeCells': {
                    'range': {
                        'sheetId': 0,
                        "startRowIndex": 0,
                        "endRowIndex": len(cleaned),
                        "startColumnIndex": 0,
                        "endColumnIndex": len(cleaned[0])
                    },
                }
            }]}

        clear_values_request = {
            'requests': [{
                'updateCells': {
                    'range': {
                        'sheetId': 0,
                        "startRowIndex": 0,
                        "endRowIndex": len(cleaned),
                        "startColumnIndex": 0,
                        "endColumnIndex": len(cleaned[0])
                    },
                    'fields': 'userEnteredValue'
                }
            }]}

        service.spreadsheets().batchUpdate(spreadsheetId=row[0], body=clear_values_request).execute()
        service.spreadsheets().batchUpdate(spreadsheetId=row[0], body=clear_formatting_request).execute()

        body = {
            'requests': requests
        }
        service.spreadsheets().batchUpdate(spreadsheetId=row[0], body=body).execute()

        if len(results[0]) > int(row[2]):
            query = """UPDATE header_structure SET "rows" = %s WHERE "sheetId" = %s AND "tabId" = %s;"""
            cur.execute(query, (len(results[0]), row[0], row[1]))
            conn.commit()
    conn.close()
    return 0


def getdata(token,sheetId,tabId,rows):
    access_token = token
    spreadsheet_id = sheetId
    creds = Credentials(token=access_token)
    service = build('sheets', 'v4', credentials=creds)

    sheet_metadata = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    sheets = sheet_metadata.get('sheets', '')
    sheet_name = None
    for sheet in sheets:
        if sheet['properties']['sheetId'] == int(tabId):
            sheet_name = sheet['properties']['title']
            break
    
    if int(rows)!=0:
        range_name = f'{sheet_name}!1:{rows}' 
        range_all = f'{sheet_name}'
        result = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=range_name).execute()
        result_all = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=range_all).execute()
        values = result.get('values', [])
        values_all = result_all.get('values', [])

        if values!=[]:
            max_len = max(len(keys) for keys in values)
            formatted_keys = []

            for keys in values:
                while len(keys) < max_len:
                    keys.append('')
                formatted_keys.append(keys)
        
            values = formatted_keys

        y1 = 0
        while y1<len(values):
            if y1==0:
                y2 = 0
                while y2<len(values[y1]):   
                    if values[y1][y2]=='' and y2>0:
                        values[y1][y2] = values[y1][y2-1]
                    y2 = y2+1
            else:
                y2 = 0
                flag = values[y1-1][0]
                detect = 0
                while y2<len(values[y1]):
                    if values[y1][y2]!='':
                        detect = 1 
                    if values[y1-1][y2]==flag and values[y1][y2]=='' and y2>0 and detect==1:
                        values[y1][y2] = values[y1][y2-1]
                    else:
                        flag = values[y1-1][y2]
                    y2 = y2+1
            y1 = y1 + 1

        y1 = 0
        while y1<len(values):
            y2 = 0
            while y2<len(values[y1]):   
                if y1>0 and values[y1][y2]!='':
                    values[y1][y2] = values[y1-1][y2]+"char$tGPT"+values[y1][y2]
                y2 = y2 + 1
            y1 = y1 + 1
        
    else:
        values = [[]]
        values_all = []

    return [values,len(values_all)]

def generate_unique_url(cur):
    while True:
        endpoint_url = f"{str(uuid.uuid4())}"
        check_query = "SELECT COUNT(*) FROM header_structure WHERE param = %s;"
        cur.execute(check_query, (endpoint_url,))
        if cur.fetchone()[0] == 0:
            return endpoint_url

@app.post("/create-endpoint")
async def create_endpoint(request: CreateEndpointRequest):
    print(request)
    conn = psycopg2.connect("postgresql://retool:yosc9BrPx5Lw@ep-silent-hill-00541089.us-west-2.retooldb.com/retool?sslmode=require")
    cur = conn.cursor()

    # Generate a unique endpoint URL
    endpoint_url = generate_unique_url(cur)
    print(endpoint_url)
    # Insert new endpoint information into header_structure
    insert_query = """INSERT INTO header_structure ("param", "sheetId", "tabId", "rows") VALUES (%s, %s, %s, %s);"""
    rows = 0  # Initial rows set to 0, this will be updated later
    tab_id = request.tabId  
    data_query = (endpoint_url, request.sheetId, tab_id, rows)
    cur.execute(insert_query, data_query)
    conn.commit()

    cur.close()
    conn.close()

    return {"url": endpoint_url, "sheetId": request.sheetId, "sheetName" : request.sheetName}

@app.api_route("/{endpoint_id}", methods=["GET", "POST", "PUT", "PATCH", "DELETE"], response_model=None)
async def handle_webhook(endpoint_id: str, request: Request):
    # Fetch JSON data if present
    if request.method in ["POST", "PUT", "PATCH"]:
        try:
            data = await request.json()
        except:
            data = {}
        
        # Call the process_data_sink function
        await process_data_sink(endpoint_id, data)
    
    user_agent = request.headers.get('user-agent')
    method = request.method
    hostname = request.client.host
    url = str(request.url)
    hit_time = datetime.now().isoformat()

    # Fetch the token and sheet information based on the endpoint_id
    conn = psycopg2.connect("postgresql://retool:yosc9BrPx5Lw@ep-silent-hill-00541089.us-west-2.retooldb.com/retool?sslmode=require")
    cur = conn.cursor()
    query = """SELECT "sheetId", "tabId" FROM header_structure WHERE "param" = %s;"""
    cur.execute(query, (endpoint_id,))
    row = cur.fetchone()
    
    if not row:
        raise HTTPException(status_code=404, detail="Endpoint not found")

    sheet_id, tab_id = row
    query = """SELECT "token" FROM oauth_token WHERE "sheetId" = %s;"""
    cur.execute(query, (sheet_id,))
    token = cur.fetchone()
    
    if not token:
        raise HTTPException(status_code=404, detail="Token not found")

    access_token = token[0]
    creds = Credentials(token=access_token)
    service = build('sheets', 'v4', credentials=creds)

    # Prepare the hit data without 'data' column
    hit_data = [
        [url, hostname, user_agent, hit_time, method]
    ]

    # Append the data to the 'Hits' tab
    sheet_name = 'Hits'
    range_name = f'{sheet_name}!A1'
    body = {
        'values': hit_data
    }
    
    service.spreadsheets().values().append(
        spreadsheetId=sheet_id,
        range=range_name,
        valueInputOption='USER_ENTERED',
        body=body
    ).execute()

    return {"status": "success", "data": hit_data}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)