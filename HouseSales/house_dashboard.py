# CSC/SER 325 – Milestone 3 Visualization Dashboard
# Reeya Patel & Megan Mohr

import matplotlib
matplotlib.use("TkAgg")  # ensures plots pop up in a window

import pymysql
import pymysql.cursors
import pandas as pd
import matplotlib.pyplot as plt

def get_connection():
    return pymysql.connect(
        host="database-termproject.cfqmw8c6u8ay.us-east-2.rds.amazonaws.com",
        port=3306,
        user="admin",
        password="ReeyaPatel111.",  
        database="house_sales",
        cursorclass=pymysql.cursors.DictCursor 
    )


def run_query(sql, params=None):
  
    #Run a SELECT query against the house_sales schema and return a pandas DataFrame.

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("USE house_sales;") # ensure correct schema
            cur.execute(sql, params or ())
            rows = cur.fetchall()
        return pd.DataFrame(rows)           # convert to DataFrame
    finally:
        conn.close()    # close connection after use


def debug_counts():
   # show how many rows are in each table. This will print to the terminal when you run the dashboard.

    sql = """
        SELECT 'Property' AS table_name, COUNT(*) AS cnt FROM Property
        UNION ALL
        SELECT 'Listing'  AS table_name, COUNT(*) AS cnt FROM Listing
        UNION ALL
        SELECT 'City'     AS table_name, COUNT(*) AS cnt FROM City
        UNION ALL
        SELECT 'Agent'    AS table_name, COUNT(*) AS cnt FROM Agent
        UNION ALL
        SELECT 'Status'   AS table_name, COUNT(*) AS cnt FROM Status;
    """
    df = run_query(sql)   # run count query
    print("\nRow counts by table:")
    print(df.to_string(index=False))
    print()


def viz_avg_price_by_city(): 
    # Top 10 cities by average listing price.
    sql = """
        SELECT c.city, c.state, AVG(p.price) AS avg_price
        FROM Property p
        JOIN City c ON p.city_id = c.city_id
        GROUP BY c.city, c.state
        ORDER BY avg_price DESC
        LIMIT 10;
    """
    df = run_query(sql)   # run query
    if df.empty:
        print("No data for avg price by city.")
        return

    df["label"] = df["city"] + ", " + df["state"]   # create label column

    plt.figure()  # create new figure
    plt.bar(df["label"], df["avg_price"])  # bar chart
    plt.xticks(rotation=45, ha="right")    # rotate x labels
    plt.ylabel("Average Price ($)")
    plt.title("Top 10 Cities by Average Listing Price")
    plt.tight_layout()  # adjust layout


def viz_listing_count_by_status():
    # Number of listings in each status (For Sale / Pending / Sold / etc).
    sql = """
        SELECT s.status, COUNT(*) AS cnt
        FROM Listing l
        JOIN Status s ON l.status_id = s.status_id
        GROUP BY s.status
        ORDER BY cnt DESC;
    """
    df = run_query(sql)  # run query
    if df.empty:
        print("No data for status counts.")
        return

    plt.figure()  # new figure
    plt.bar(df["status"], df["cnt"])  # bar chart
    plt.xlabel("Status")
    plt.ylabel("Number of Listings")
    plt.title("Listing Count by Status")
    plt.tight_layout()

def viz_property_type_distribution():
    # Distribution of property types (Townhouse, Apartment, etc).
    
    sql = """
        SELECT property_type, COUNT(*) AS cnt
        FROM Property
        GROUP BY property_type
        ORDER BY cnt DESC;
    """
    df = run_query(sql)  # run query
    if df.empty:
        print("No data for property types.")
        return

    plt.figure()  # new figure
    plt.pie(df["cnt"], labels=df["property_type"], autopct="%1.1f%%")  # pie chart
    plt.title("Property Type Distribution")
    plt.tight_layout()


def viz_avg_days_on_market_by_status():
    # Average days on market for each status.
    sql = """
        SELECT s.status, AVG(p.days_on_market) AS avg_dom
        FROM Listing l
        JOIN Property p ON l.property_id = p.property_id
        JOIN Status s ON l.status_id = s.status_id
        GROUP BY s.status
        ORDER BY avg_dom DESC;
    """
    df = run_query(sql)  # run query
    if df.empty:
        print("No data for days on market.")
        return

    plt.figure()  # new figure
    plt.bar(df["status"], df["avg_dom"])  # bar chart
    plt.xlabel("Status")
    plt.ylabel("Average Days on Market")
    plt.title("Average Days on Market by Status")
    plt.tight_layout()


def viz_bedrooms_vs_avg_price():
    # Average price for each bedroom count.
    sql = """
        SELECT bedrooms, AVG(price) AS avg_price
        FROM Property
        GROUP BY bedrooms
        HAVING bedrooms IS NOT NULL
        ORDER BY bedrooms;
    """
    df = run_query(sql)  # run query
    if df.empty:
        print("No data for bedrooms vs price.")
        return

    plt.figure()  # new figure
    plt.plot(df["bedrooms"], df["avg_price"], marker="o")  # line plot
    plt.xlabel("Bedrooms")
    plt.ylabel("Average Price ($)")
    plt.title("Average Price by Bedroom Count")
    plt.grid(True)  # add grid lines
    plt.tight_layout()

def main():
    print("Running house sales dashboard...")

    debug_counts()  # show row counts

    # generate all visualizations
    viz_avg_price_by_city()
    viz_listing_count_by_status()
    viz_property_type_distribution()
    viz_avg_days_on_market_by_status()
    viz_bedrooms_vs_avg_price()

    plt.show()  # display all plots

    print("Done.")  # finished running


if __name__ == "__main__":
    main()  # run the script