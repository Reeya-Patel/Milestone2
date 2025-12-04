#import pymysql,json,os
import pymysql
import json
import os


# Set this to "GCP" or "AWS" (or use an env var: DB_PLATFORM=GCP/AWS)
PLATFORM = os.getenv("DB_PLATFORM", "AWS").upper()

def getconn():
        # ----- AWS ONLY -----
        # install: pip install PyMySQL
        return pymysql.connect(
            host="database-termproject.cfqmw8c6u8ay.us-east-2.rds.amazonaws.com",  # RDS endpoint = host
            port=3306,                                       # MySQL default port
            user="admin",
            password="ReeyaPatel111.",

            #database=None                            # or None if you CREATE first
        )
    
def setup_db(cur):
    # Create & select a DB for this project
    cur.execute('CREATE DATABASE IF NOT EXISTS house_sales_db')
    cur.execute('USE house_sales_db')

    # Drop in dependency order
    cur.execute('DROP TABLE IF EXISTS Member;')
    cur.execute('DROP TABLE IF EXISTS Course;')
    cur.execute('DROP TABLE IF EXISTS User;')

    # Widen columns so real data fits (addresses & MLS ids can be long)
    cur.execute('''
        CREATE TABLE User (
            id   INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
            name VARCHAR(255) UNIQUE
        );
    ''')

    cur.execute('''
        CREATE TABLE Course (
            id    INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
            title VARCHAR(100) UNIQUE
        );
    ''')

    cur.execute('''
        CREATE TABLE Member (
            user_id   INT,
            course_id INT,
            role      INT,
            FOREIGN KEY(user_id)  REFERENCES User(id),
            FOREIGN KEY(course_id) REFERENCES Course(id),
            PRIMARY KEY (user_id, course_id)
        );
    ''')


def insert_data(cur):
    # use the DB we created in Step 1
    cur.execute('USE house_sales_db')

    fname = 'house_sales_data.json'   # your generated JSON (array-of-arrays)

    # open and load
    with open(fname, 'r', encoding='utf-8') as f:
        json_data = json.load(f)

    # HouseSales JSON order (for reference):
    # 0 Price, 1 Address, 2 City, 3 Zipcode, 4 State, 5 Bedrooms, 6 Bathrooms,
    # 7 Area (sqft), 8 Lot Size, 9 Year Built, 10 Days on Market,
    # 11 Property Type, 12 MLS ID, 13 Listing Agent, 14 Status, 15 Listing URL

    # Map common Status strings to integers for Member.role
    status_map = {
        "Active": 1,
        "Pending": 2,
        "Sold": 3,
        "Inactive": 0
    }

    for entry in json_data:
        # pull the three fields we need
        address  = str(entry[1]).strip()   # -> User.name
        mls_id   = str(entry[12]).strip()  # -> Course.title
        status_s = str(entry[14]).strip()  # -> Member.role (int)

        # coerce status to int (try numeric, else map, else 0)
        try:
            role_int = int(float(status_s))
        except Exception:
            role_int = status_map.get(status_s, 0)

        # visibility while loading
        print(address, " | ", mls_id, " | role:", role_int)

        # INSERT IGNORE keeps uniqueness semantics like the original
        cur.execute('''INSERT IGNORE INTO User (name)
                       VALUES (%s)''', (address, ))   # NOTE: tuple (address,)

        # fetch id
        cur.execute('SELECT id FROM User WHERE name = %s', (address, ))
        user_id = cur.fetchone()[0]

        # same for "Course" (we store MLS ID as the unique title)
        cur.execute('''INSERT IGNORE INTO Course (title)
                       VALUES (%s)''', (mls_id, ))    # tuple (mls_id,)

        cur.execute('SELECT id FROM Course WHERE title = %s', (mls_id, ))
        course_id = cur.fetchone()[0]

        # link table with role = status code
        cur.execute('''INSERT IGNORE INTO Member (user_id, course_id, role)
                       VALUES (%s, %s, %s)''',
                    (user_id, course_id, role_int))

cnx = getconn()
cur = cnx.cursor()
print("Starting Setup...")
setup_db(cur)
print("Finished Setup.")
print("Starting Insert...")
insert_data(cur)
print("Finished Insert.")
cur.close()
cnx.commit()
cnx.close()
print("FINISHED")
