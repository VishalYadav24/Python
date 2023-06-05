import urllib3
import json
import gzip
import sqlite3
http = urllib3.PoolManager()

def fetchPagesResponse():
    # resp = http.request("GET","https://dumps.wikimedia.org/other/pageviews/2023/2023-02/pageviews-20230201-010000.gz");
    
    with gzip.open('pageviews-20230201-000000.gz', 'rb') as f:
        file_content = f.read()
        string_response = (file_content.decode())
        keys = ["domain_code", "page_title", "view_count", "response_size"]
        arr = []
        c = list(map(lambda x: x.split(),string_response.split('\n')))
        for item in c:

            arr.append(dict(zip(keys,item)))

        return arr

def createDatabase():
    conn = sqlite3.connect('pageview.db')
    c = conn.cursor()
    c.execute(
        '''
        CREATE TABLE IF NOT EXISTS page_data (
        domain_code TEXT,
        page_title TEXT,
        view_count INTEGER,
        response_size INTEGER
        )
        '''
    )
    conn.commit()  
    return conn

def closeConnection(connectionObject):
    connectionObject.close()


def insertData(cursorObject,data):
    insert_query= "INSERT INTO page_data (domain_code, page_title, view_count,response_size) VALUES(?, ?,?,?)"
    cursorObject.execute(insert_query,data)
def getAllRecords(cursorObject):
    select_query= "SELECT * FROM page_data where view_count = 0"
    d = cursorObject.execute(select_query)
    print(list(d))

def wikipedia_page_views_api():
    # fetch the data
    data = fetchPagesResponse()
    # create a connection to database
    conn = createDatabase()
    # insert the data
    for item in data:
        a = (
            item["domain_code"],
            item["page_title"],
            item["view_count"],
            item["response_size"]
        )
        #print(a)
        insertData(conn, a)
        conn.commit()
        

    # select the data from table
    getAllRecords(conn)

    #close the connection
    closeConnection(conn)

wikipedia_page_views_api()    