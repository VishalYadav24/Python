import urllib3
import json
import gzip
import sqlite3
from tabulate import tabulate
from datetime import date, datetime
http = urllib3.PoolManager()

def pretty_print_data(data):
    print(tabulate(data, headers=["Domain Code","Page Title", "View Count","Response size (bytes)"]))

def format_number(number):
    if number < 9 :
        return str("0"+ str(number))
def create_Url():
    year = int(input('Enter a year: '))
    month = format_number(int(input('Enter a month: ')))
    day = format_number(int(input('Enter a day: ')))
    hour = format_number(int(input('Enter the hour: ')))
    newUrl = "https://dumps.wikimedia.org/other/pageviews/{year}/{year}-{month}/pageviews-{year}{month}{day}-{hour}0000.gz".format(year=year, month=month, day=day, hour=hour)
    return newUrl

def fetchPagesResponse():
    # resp = http.request("GET","https://dumps.wikimedia.org/other/pageviews/2023/2023-02/pageviews-20230201-010000.gz");
    
    with gzip.open('pageviews-20230201-000000.gz', 'rb') as f:
        file_content = f.read()
        string_response = (file_content.decode())
        keys = ["domain_code", "page_title", "view_count", "response_size"]
        arr = []
        c = list(map(lambda x: x.split(),string_response.split('\n')))
        for item in c[0:20]:

            arr.append(dict(zip(keys,item)))

        return arr

def createDatabase():
    connection = sqlite3.connect('pageview.db')
    c = connection.cursor()
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
    connection.commit()  
    return connection

def closeConnection(connectionObject):
    connectionObject.close()


def insertData(cursorObject,data):
    insert_query= "INSERT INTO page_data (domain_code, page_title, view_count,response_size) VALUES(?, ?,?,?)"
    cursorObject.execute(insert_query,data)
def getAllRecords(cursorObject):
    select_query= "SELECT * FROM page_data"
    d = cursorObject.execute(select_query)
    pretty_print_data(d)

def wikipedia_page_views_api():
    # fetch the data
    data = fetchPagesResponse()
    # create a connection to database
    connection = createDatabase()
    # insert the data
    for item in data:
        a = (
            item["domain_code"],
            item["page_title"],
            item["view_count"],
            item["response_size"]
        )
        #print(a)
        insertData(connection, a)
        connection.commit()
        

    # select the data from table
    getAllRecords(connection)

    #close the connection
    closeConnection(connection)

#wikipedia_page_views_api()    
create_Url()