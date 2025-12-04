# https://plotly.com/python/v3/graph-data-from-mysql-database-in-python/

import pandas as pd
import plotly.express as px
import pymysql

# -----------------------------
# 1 & 2. Connect directly to AWS RDS MySQL
# -----------------------------
def make_connection():
    return pymysql.connect(
        host="database-termproject.cfqmw8c6u8ay.us-east-2.rds.amazonaws.com",
        port=3306,
        user="admin",
        password="ReeyaPatel111.",   # your RDS password
        database="house_sales"
    )

# -----------------------------
# 3. Fetch data from MySQL
# -----------------------------
conn = make_connection()

query = """
    SELECT 
        p.price,
        p.area_sqft,
        p.bedrooms,
        p.days_on_market,
        p.property_type,
        c.city,
        c.state,
        s.status
    FROM Property p
    JOIN City c    ON p.city_id = c.city_id
    JOIN Listing l ON p.property_id = l.property_id
    JOIN Status s  ON l.status_id = s.status_id;
"""

df = pd.read_sql(query, conn)
conn.close()

print(df.head())   # optional sanity check

# -----------------------------
# 4. Plotly Express Charts (5 total)
# -----------------------------

# 1. Scatter Plot: Price vs Area
fig1 = px.scatter(
    df,
    x='area_sqft',
    y='price',
    hover_data=['city', 'state', 'property_type', 'status'],
    title='Price vs Area (sqft)',
    color='property_type'
)
fig1.show()

# 2. Bar Chart: Number of Listings by Status
status_counts = df.groupby('status').size().reset_index(name='num_listings')
fig2 = px.bar(
    status_counts,
    x='status',
    y='num_listings',
    title='Number of Listings by Status',
    color='status'
)
fig2.show()

# 3. Histogram: Price Distribution
fig3 = px.histogram(
    df,
    x='price',
    nbins=20,
    title='Listing Price Distribution',
    color='property_type'
)
fig3.show()

# 4. Bar Chart: Average Days on Market by Status
dom = df.groupby('status')['days_on_market'].mean().reset_index()
fig4 = px.bar(
    dom,
    x='status',
    y='days_on_market',
    title='Average Days on Market by Status',
    color='status'
)
fig4.show()

# 5. Line Chart: Average Price by Bedroom Count
bed_price = df.groupby('bedrooms')['price'].mean().reset_index()
fig5 = px.line(
    bed_price,
    x='bedrooms',
    y='price',
    markers=True,
    title='Average Price by Bedroom Count'
)
fig5.show()
