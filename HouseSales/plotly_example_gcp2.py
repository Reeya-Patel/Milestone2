# https://plotly.com/python/v3/graph-data-from-mysql-database-in-python/

# CSC/SER 325 – Milestone 3 Visualization Dashboard
# Reeya Patel & Megan Mohr

import pandas as pd
import plotly.express as px
import pymysql
import plotly.io as pio

# make Plotly open directly in the browser
pio.renderers.default = "browser"

# -----------------------------
# 1 & 2. Connect directly to AWS RDS MySQL
# -----------------------------
def make_connection():
    return pymysql.connect(
        host="database-termproject.cfqmw8c6u8ay.us-east-2.rds.amazonaws.com",
        port=3306,
        user="admin",
        password="ReeyaPatel111.",   # put your real password here
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
print(df.head())
conn.close()

# -----------------------------
# 4. Plotly Express Charts
# -----------------------------

# 1. Avg Price by City (Top 10) – Bar Chart
city_price = (
    df.groupby(["city", "state"])["price"]
      .mean()
      .reset_index()
      .sort_values("price", ascending=False)
      .head(10)
)
city_price["label"] = city_price["city"] + ", " + city_price["state"]

fig1 = px.bar(
    city_price,
    x="label",
    y="price",
    title="Top 10 Cities by Average Listing Price"
)
fig1.update_layout(
    xaxis_title="City",
    yaxis_title="Average Price ($)"
)
fig1.show()

# 2. Listing Count by Status – Bar Chart
status_counts = (
    df.groupby("status")
      .size()
      .reset_index(name="num_listings")
      .sort_values("num_listings", ascending=False)
)

fig2 = px.bar(
    status_counts,
    x="status",
    y="num_listings",
    title="Number of Listings by Status"
)
fig2.update_layout(
    xaxis_title="Status",
    yaxis_title="Number of Listings"
)
fig2.show()

# 3. Property Type Distribution – Pie Chart
type_counts = (
    df.groupby("property_type")
      .size()
      .reset_index(name="count")
      .sort_values("count", ascending=False)
)

fig3 = px.pie(
    type_counts,
    names="property_type",
    values="count",
    title="Property Type Distribution"
)
fig3.show()

# 4. Average Days on Market by Status – Bar Chart
dom_status = (
    df.groupby("status")["days_on_market"]
      .mean()
      .reset_index()
      .sort_values("days_on_market", ascending=False)
)

fig4 = px.bar(
    dom_status,
    x="status",
    y="days_on_market",
    title="Average Days on Market by Status"
)
fig4.update_layout(
    xaxis_title="Status",
    yaxis_title="Average Days on Market"
)
fig4.show()

# 5. Average Price by Bedroom Count – Line Chart
bed_price = (
    df.groupby("bedrooms")["price"]
      .mean()
      .reset_index()
      .sort_values("bedrooms")
)

fig5 = px.line(
    bed_price,
    x="bedrooms",
    y="price",
    markers=True,
    title="Average Price by Bedroom Count"
)
fig5.update_layout(
    xaxis_title="Bedrooms",
    yaxis_title="Average Price ($)"
)
fig5.show()
