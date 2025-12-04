# CSC/SER 325 – Milestone 3 Data Loader
# Reeya Patel & Megan Mohr

import json
import pymysql
import re


def get_connection():
    return pymysql.connect(
        host="database-termproject.cfqmw8c6u8ay.us-east-2.rds.amazonaws.com",
        port=3306,
        user="admin",
        password="ReeyaPatel111.",   
        database="house_sales",
        autocommit=False # manual commit for control
    )

def clean_price(s):
    # Convert "$1,234,567" to float
    if not s:
        return None
    s = s.replace("$", "").replace(",", "").strip()
    try:
        return float(s)
    except:
        return None

def clean_beds(s):
    # Extract the integer number of bedrooms
    if not s:
        return None
    m = re.match(r"(\d+)", s)
    return int(m.group(1)) if m else None

def clean_baths(s):
    # Extract float number of bathrooms
    if not s:
        return None
    m = re.match(r"(\d+(\.\d+)?)", s)
    return float(m.group(1)) if m else None

def clean_sqft(s):
    # Extract square footage as integer
    if not s:
        return None
    m = re.match(r"(\d+)", s.replace(",", ""))
    return int(m.group(1)) if m else None

def to_int(s):
    # Safely convert to int
    try:
        return int(s)
    except:
        return None

def to_float(s):
    # Safely convert to float
    try:
        return float(s)
    except:
        return None

def get_or_create_city(cur, city, state, zipcode):
    # Look up existing city or insert a new one
    cur.execute(
        "SELECT city_id FROM City WHERE city=%s AND state=%s AND zipcode=%s",
        (city, state, zipcode)
    )
    row = cur.fetchone()
    if row:
        return row[0]

    cur.execute(
        "INSERT INTO City (city, state, zipcode) VALUES (%s,%s,%s)",
        (city, state, zipcode)
    )
    return cur.lastrowid

def get_or_create_agent(cur, agent):
    # Insert agent only if they don't already exist
    if not agent:
        agent = "Unknown"
    cur.execute("SELECT agent_id FROM Agent WHERE listing_agent=%s", (agent,))
    row = cur.fetchone()
    if row:
        return row[0]

    cur.execute(
        "INSERT INTO Agent (listing_agent) VALUES (%s)",
        (agent,)
    )
    return cur.lastrowid

def get_or_create_status(cur, status):
    # Insert status only if it doesn't already exist
    if not status:
        status = "Unknown"
    cur.execute("SELECT status_id FROM Status WHERE status=%s", (status,))
    row = cur.fetchone()
    if row:
        return row[0]

    cur.execute(
        "INSERT INTO Status (status) VALUES (%s)",
        (status,)
    )
    return cur.lastrowid

def get_or_create_property(cur, addr, city_id, price, beds, baths,
                           sqft, lot, year, dom, ptype):

    # Check if property already exists (address + city)
    cur.execute(
        "SELECT property_id FROM Property WHERE address=%s AND city_id=%s",
        (addr, city_id)
    )
    row = cur.fetchone()
    if row:
        return row[0]

    # Insert new property row
    cur.execute("""
        INSERT INTO Property (
            address, city_id, price, bedrooms, bathrooms,
            area_sqft, lot_size, year_built, days_on_market, property_type
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, (addr, city_id, price, beds, baths, sqft, lot, year, dom, ptype))

    return cur.lastrowid

def create_listing(cur, property_id, agent_id, status_id, url):
    # Insert listing only if unique (property_id + agent_id)
    cur.execute(
        "SELECT listing_id FROM Listing WHERE property_id=%s AND agent_id=%s",
        (property_id, agent_id)
    )
    row = cur.fetchone()
    if row:
        return row[0]

    # Insert new listing row
    cur.execute("""
        INSERT INTO Listing (
            property_id, agent_id, status_id, listing_url,
            list_date, close_date
        )
        VALUES (%s,%s,%s,%s,NULL,NULL)
    """, (property_id, agent_id, status_id, url))

    return cur.lastrowid

# ---------- MAIN LOADER ----------

def load_data():
    print("Loading house_sales_subset.json ...")

    # Read the JSON file into Python list
    with open("house_sales_subset.json", "r") as f:
        rows = json.load(f)

    conn = get_connection()
    cur = conn.cursor()

    try:
        # Ensure correct schema is active
        cur.execute("USE house_sales;")

        for i, row in enumerate(rows, start=1):
            # Extract and clean each data field
            price        = clean_price(row[0])
            address      = row[1]
            city         = row[2]
            zipcode      = row[3]
            state        = row[4]
            bedrooms     = clean_beds(row[5])
            bathrooms    = clean_baths(row[6])
            area_sqft    = clean_sqft(row[8])
            year_built   = to_int(row[9])
            days_on_mkt  = to_int(row[10])
            prop_type    = row[11]
            agent        = row[13]
            status       = row[14]
            listing_url  = row[15]

            # Insert or get IDs from lookup tables
            city_id = get_or_create_city(cur, city, state, zipcode)
            agent_id = get_or_create_agent(cur, agent)
            status_id = get_or_create_status(cur, status)

            # Insert or get property_id
            property_id = get_or_create_property(
                cur, address, city_id, price, bedrooms, bathrooms,
                area_sqft, None, year_built, days_on_mkt, prop_type
            )

            # Insert listing row
            create_listing(cur, property_id, agent_id, status_id, listing_url)

            # Progress output every 5 rows
            if i % 5 == 0:
                print(f"Inserted {i} rows...")

        # Commit all inserts
        conn.commit()
        print("DONE! Loaded all data successfully.")

        # Quick count check
        cur.execute("SELECT COUNT(*) FROM Property;")
        print("Property rows after load:", cur.fetchone()[0])

    except Exception as e:
        conn.rollback()  # undo changes on error
        print("ERROR:", e)
        raise

    finally:
        cur.close()  # close cursor
        conn.close() # close connection


if __name__ == "__main__":
    load_data()  # run the loader