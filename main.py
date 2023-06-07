import urllib3
import json
import gzip
import sqlite3
from tabulate import tabulate
from datetime import date, datetime
http = urllib3.PoolManager()
import requests



def pretty_print_data(data):
    '''Print data retrieved from database in tabular format'''
    print(tabulate(data, headers=["Domain Code","Page Title", "View Count","Response size (bytes)"]))

def format_number(number):
    '''Return string representation of a number also if no less than 9 add leading zeros'''
    if number < 9 :
        return str("0"+ str(number))
    else:
        return str(number)
def create_Url():
    '''Ask user for date and time of which user wants to see page view data and return file downloaded data'''
    year = format_number(int(input('Enter a year: ')))
    month = format_number(int(input('Enter a month: ')))
    day = format_number(int(input('Enter a day: ')))
    hour = format_number(int(input('Enter the hour: ')))
    newUrl = "https://dumps.wikimedia.org/other/pageviews/{year}/{year}-{month}/pageviews-{year}{month}{day}-{hour}0000.gz".format(year=year, month=month, day=day, hour=hour)
    return makeRequest(newUrl,str(year+month+day))
def makeRequest(url,filename):
    '''Download file and rename and save it in local directory''' 
    c = filename
    print("Requesting "+url)
    response = requests.get(url)
    open(filename, 'wb').write(response.content)
    return filename
def fetchPagesResponse(filename):
    '''Extract zip file data and create list containing relevant information''' 
    with gzip.open(filename, 'rb') as f:
        file_content = f.read()
        string_response = (file_content.decode())
        keys = ["domain_code", "page_title", "view_count", "response_size"]
        arr = []
        c = list(map(lambda x: x.split(),string_response.split('\n')))
        for item in c[0:20]:

            arr.append(dict(zip(keys,item)))

        return arr

def createDatabase():
    '''Creates a new database and return connection to database''' 
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
    '''Close connection with the given connection object''' 
    connectionObject.close()


def insertData(cursorObject,data):
    insert_query= "INSERT INTO page_data (domain_code, page_title, view_count,response_size) VALUES(?, ?,?,?)"
    cursorObject.execute(insert_query,data)
def getAllRecords(cursorObject):
    select_query= "SELECT * FROM page_data"
    d = cursorObject.execute(select_query)
    pretty_print_data(d)

def wikipedia_page_views_api():
    '''Parent method which performs all the necessary actions''' 
    #Ask user to give date and time range
    file_name = create_Url()
    print(file_name)
    # fetch the data
    data = fetchPagesResponse(file_name)
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

# Invoking the parent function
wikipedia_page_views_api()    

