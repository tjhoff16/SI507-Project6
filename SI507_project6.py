# Import statements
import psycopg2
import psycopg2
import psycopg2.extras
from psycopg2 import sql
import requests
from config import *
import sys
import json
import csv


# Write code / functions to set up database connection and cursor here.

db_connection = None
db_cursor = None
def get_connection_and_cursor():
    global db_connection, db_cursor
    if not db_connection:
        try:
            db_connection = psycopg2.connect("dbname='{0}' user='{1}' password='{2}'".format(db_name, db_user, db_password))
            print("Success connecting to database")
        except:
            print("Unable to connect to the database. Check server and credentials.")
            sys.exit(1) # Stop running program if there's no db connection.

    if not db_cursor:
        db_cursor = db_connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    return db_connection, db_cursor

def setup_database():
    # Invovles DDL commands
    # DDL --> Data Definition Language
    # CREATE, DROP, ALTER, RENAME, TRUNCATE

    conn, cur = get_connection_and_cursor()
    cur.execute ("""
    CREATE TABLE States (
    ID SERIAL PRIMARY KEY,
    Name VARCHAR(40) UNIQUE
    )""")

    cur.execute("""
    CREATE TABLE Sites (
      ID SERIAL PRIMARY KEY,
      Name VARCHAR(128) UNIQUE,
      Type VARCHAR(128),
      State_ID INTEGER REFERENCES States (ID),
      Location VARCHAR(255),
      DESCRIPTION TEXT)""")


    conn.commit()
    print('Setup database complete')

# Write code / functions to create tables with the columns you want and all database setup here.
# Write code / functions to deal with CSV files and insert data into the database here.

class NationalSite(object):
    def __init__(self, data, state):
        self.location = data[1]
        self.name = data[0]
        self.type = data[2]
        self.description = data[4].strip()
        self.addr = data[3]
        self.state = state

    def __str__(self):
        return "{} | {}".format(self.name, self.location)
conn, cur = get_connection_and_cursor()

def process_csv(fl):
    f = open(fl+".csv", 'r', encoding="utf-8")
    rd=csv.reader(f)
    data = []
    for e in rd:
        if e[0] != "NAME":
            site = NationalSite(e, fl)
            data.append(site)
    return data
def insert_sites(sites, state):
    cur.execute("""
    INSERT INTO States (Name)
    values (%s) ON CONFLICT DO NOTHING RETURNING ID""",
    (state,))
    conn.commit()
    try :
        stateid = cur.fetchone()
        stateid = stateid['id']
        print ("stateid",type(stateid))
    except:
        cur.execute("SELECT States.id FROM States WHERE States.Name=%s", (state,))
        stateid=cur.fetchone()
        stateid = stateid['id']
    conn.commit()
    for site in sites:
        try:
            cur.execute("""
            INSERT INTO Sites (Name, Type, Location, Description, State_ID)
            values (%s, %s, %s, %s, %s)""",
            (site.name, site.type, site.location, site.description, stateid))
        except:
            print (site.name)
        conn.commit()
def query_db():
    cur.execute("SELECT Sites.location FROM Sites")
    all_locations = cur.fetchall()
    cur.execute("SELECT Sites.name FROM Sites WHERE Sites.description LIKE '%beautiful%'")
    beautiful_sites = cur.fetchall()
    cur.execute("SELECT count(Sites.name) FROM Sites WHERE Sites.type = 'National Lakeshore'")
    natl_lakeshores = cur.fetchall()
    cur.execute("SELECT Sites.name FROM Sites INNER JOIN States ON (Sites.state_id = States.id) WHERE States.name ='michigan'")
    michigan_names = cur.fetchall()
    cur.execute("SELECT count(Sites.name) FROM Sites WHERE Sites.state_id = (SELECT States.id FROM States WHERE States.name='arkansas')")
    total_number_arkansas = cur.fetchall()
    return all_locations, beautiful_sites, natl_lakeshores, michigan_names, total_number_arkansas


if __name__ == '__main__':
    command = None
    command = sys.argv[1]

    if command == 'setup':
        print('setting up database')
        setup_database()
    elif command == 'insert':
        arkdata = process_csv("arkansas")
        midata = process_csv("michigan")
        cadata = process_csv("california")
        insert_sites(arkdata, "arkansas")
        insert_sites(midata, "michigan")
        insert_sites(cadata, "california")
    elif command == "search":
        all_locations, beautiful_sites, natl_lakeshores, michigan_names, total_number_arkansas = query_db()
        print (all_locations)
        print (beautiful_sites)
        print (natl_lakeshores)
        print (michigan_names)
        print (total_number_arkansas)
# Make sure to commit your database changes with .commit() on the database connection.



# Write code to be invoked here (e.g. invoking any functions you wrote above)



# Write code to make queries and save data in variables here.






# We have not provided any tests, but you could write your own in this file or another file, if you want.
