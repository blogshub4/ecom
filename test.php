import streamlit as st
import pandas as pd
import plotly.express as px
import psycopg2
from datetime import datetime

# --- Configuration ---
st.set_page_config(page_title="IP Change Tracker", layout="wide")

# --- Custom Header ---
def render_custom_header():
    st.markdown("""
        <style>
        .custom-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            background-color: #003366;
            color: white;
            padding: 0.8rem 1.2rem;
            border-radius: 5px;
            margin-bottom: 1rem;
        }
        .custom-header h1 {
            margin: 0;
            font-size: 1.4rem;
        }
        .dropdown {
            position: relative;
            display: inline-block;
        }
        .dropdown-content {
            display: none;
            position: absolute;
            right: 0;
            background-color: white;
            min-width: 120px;
            box-shadow: 0px 8px 16px rgba(0,0,0,0.2);
            z-index: 1;
        }
        .dropdown-content a {
            color: black;
            padding: 10px 14px;
            text-decoration: none;
            display: block;
        }
        .dropdown:hover .dropdown-content {
            display: block;
        }
        .dropdown-content a:hover {
            background-color: #ddd;
        }
        </style>

        <div class="custom-header">
            <h1>📡 IP Change Dashboard</h1>
            <div class="dropdown">
                <span style="cursor:pointer;">☰ Menu</span>
                <div class="dropdown-content">
                    <a href="#">Refresh</a>
                    <a href="#">Settings</a>
                    <a href="#">Logout</a>
                </div>
            </div>
        </div>
    """, unsafe_allow_html=True)

render_custom_header()

# --- Hide Streamlit Default UI Elements ---
hide_st_style = """
    <style>
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
    </style>
"""
st.markdown(hide_st_style, unsafe_allow_html=True)

# --- DB Config (Replace with real credentials) ---
DB_HOST = "your_host"
DB_PORT = "5432"
DB_NAME = "your_db"
DB_USER = "your_user"
DB_PASS = "your_password"

# --- Database Connection ---
@st.cache_resource
def get_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )

# --- Load IP History Data ---
@st.cache_data(ttl=600)
def load_data():
    query = """
        SELECT *, lower(systime) AS change_time
        FROM qu.ip_history_test
        ORDER BY change_time DESC
    """
    return pd.read_sql(query, get_connection())

df = load_data()

# --- Sidebar Filters ---
st.sidebar.header("🔍 Filter Options")

min_date = df["change_time"].min().date()
max_date = df["change_time"].max().date()

date_range = st.sidebar.date_input("Date Range", [min_date, max_date])
action_filter = st.sidebar.multiselect("Action Type", df["action"].unique(), default=df["action"].unique())
country_filter = st.sidebar.multiselect("Country", sorted(df["country"].dropna().unique()))

# --- Apply Filters ---
filtered_df = df[
    (df["change_time"].dt.date >= date_range[0]) &
    (df["change_time"].dt.date <= date_range[1]) &
    (df["action"].isin(action_filter))
]
if country_filter:
    filtered_df = filtered_df[filtered_df["country"].isin(country_filter)]

# --- Dashboard Header ---
st.markdown(f"### Showing **{len(filtered_df)}** records from **{min_date}** to **{max_date}**")

# --- Summary Cards ---
col1, col2, col3 = st.columns(3)
col1.metric("📥 Inserted", (df["action"] == "insert").sum())
col2.metric("✏️ Updated", (df["action"] == "update").sum())
col3.metric("🗑️ Deleted", (df["action"] == "delete").sum())

# --- Bar Chart: Events Over Time ---
st.subheader("📈 Change Events Over Time")
daily_counts = filtered_df.groupby([filtered_df["change_time"].dt.date, "action"]).size().reset_index(name="count")
fig = px.bar(
    daily_counts,
    x="change_time",
    y="count",
    color="action",
    barmode="group",
    title="Insert / Update / Delete Trends"
)
st.plotly_chart(fig, use_container_width=True)

# --- Data Table ---
st.subheader("📋 Detailed Change History")
st.dataframe(filtered_df.drop(columns=["systime"]), use_container_width=True)

# --- Download Button ---
csv = filtered_df.to_csv(index=False).encode("utf-8")
st.download_button("⬇️ Download CSV", csv, "ip_history_export.csv", "text/csv")




///////////////////////////////////////////////////////////


pip install streamlit pandas psycopg2-binary plotly


import streamlit as st
import pandas as pd
import plotly.express as px
import psycopg2
from datetime import datetime

# --- DB Config ---
DB_HOST = "your_host"
DB_PORT = "5432"
DB_NAME = "your_db"
DB_USER = "your_user"
DB_PASS = "your_password"

# --- Connect to PostgreSQL ---
@st.cache_resource
def get_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )

@st.cache_data(ttl=600)
def load_data():
    query = """
        SELECT *, lower(systime) AS change_time
        FROM qu.ip_history_test
        ORDER BY change_time DESC
    """
    return pd.read_sql(query, get_connection())

# --- UI: Sidebar Filters ---
st.sidebar.header("🔍 Filter Options")

df = load_data()

min_date = df["change_time"].min().date()
max_date = df["change_time"].max().date()

date_range = st.sidebar.date_input("Date Range", [min_date, max_date])
action_filter = st.sidebar.multiselect("Action Type", df["action"].unique(), default=df["action"].unique())
country_filter = st.sidebar.multiselect("Country", sorted(df["country"].dropna().unique()))

# --- Filter Logic ---
filtered_df = df[
    (df["change_time"].dt.date >= date_range[0]) &
    (df["change_time"].dt.date <= date_range[1]) &
    (df["action"].isin(action_filter))
]

if country_filter:
    filtered_df = filtered_df[filtered_df["country"].isin(country_filter)]

# --- Dashboard Layout ---
st.title("📊 IP Change History Dashboard")

st.markdown(f"Showing **{len(filtered_df)}** records from **{min_date}** to **{max_date}**")

# --- Summary Cards ---
col1, col2, col3 = st.columns(3)
col1.metric("📥 Inserted", (df["action"] == "insert").sum())
col2.metric("✏️ Updated", (df["action"] == "update").sum())
col3.metric("🗑️ Deleted", (df["action"] == "delete").sum())

# --- Change Over Time Chart ---
st.subheader("📈 Change Events Over Time")
daily_counts = filtered_df.groupby([filtered_df["change_time"].dt.date, "action"]).size().reset_index(name="count")

fig = px.bar(
    daily_counts,
    x="change_time",
    y="count",
    color="action",
    barmode="group",
    title="Insert / Update / Delete Trends"
)
st.plotly_chart(fig, use_container_width=True)

# --- Table View ---
st.subheader("📋 Detailed Change History")
st.dataframe(filtered_df.drop(columns=["systime"]), use_container_width=True)

# Optional CSV download
csv = filtered_df.to_csv(index=False).encode("utf-8")
st.download_button("⬇️ Download CSV", csv, "ip_history_export.csv", "text/csv")


        streamlit run streamlit_app.py
        
ip_streamlit_dashboard/
├── streamlit_app.py              <-- Main dashboard
├── pages/
│   └── 1_Map_View.py             <-- Map page
    pages/1_Map_View.py


import streamlit as st
import pandas as pd
import plotly.express as px
import psycopg2

# --- DB Connection ---
@st.cache_resource
def get_connection():
    return psycopg2.connect(
        host="your_host",
        port="5432",
        dbname="your_db",
        user="your_user",
        password="your_password"
    )

@st.cache_data(ttl=600)
def load_data():
    query = """
        SELECT *,
               lower(systime) AS change_time
        FROM qu.ip_history_test
        WHERE longt IS NOT NULL AND langt IS NOT NULL
        ORDER BY change_time DESC
    """
    return pd.read_sql(query, get_connection())

# --- Load Data ---
df = load_data()

st.title("🌐 IP Change Map View")

# --- Filters ---
st.sidebar.subheader("🌍 Map Filters")
actions = st.sidebar.multiselect("Action Type", df["action"].unique(), default=df["action"].unique())
date_range = st.sidebar.date_input("Date Range", [df["change_time"].min().date(), df["change_time"].max().date()])

filtered_df = df[
    (df["action"].isin(actions)) &
    (df["change_time"].dt.date >= date_range[0]) &
    (df["change_time"].dt.date <= date_range[1])
]

# --- Map Plot ---
if filtered_df.empty:
    st.warning("No data for selected filters.")
else:
    st.markdown(f"Showing **{len(filtered_df)}** records on map.")

    fig = px.scatter_geo(
        filtered_df,
        lat="langt",
        lon="longt",
        color="action",
        hover_name="country",
        hover_data=["city", "region", "change_time"],
        title="IP Changes by Geolocation",
        projection="natural earth",
        opacity=0.7,
        height=600
    )
    st.plotly_chart(fig, use_container_width=True)


        pages/1_Map_View.py

     
        import streamlit as st
import pandas as pd
import plotly.express as px
import psycopg2

# --- DB Connection ---
@st.cache_resource
def get_connection():
    return psycopg2.connect(
        host="your_host",
        port="5432",
        dbname="your_db",
        user="your_user",
        password="your_password"
    )

@st.cache_data(ttl=600)
def load_data():
    query = """
        SELECT *,
               lower(systime) AS change_time
        FROM qu.ip_history_test
        WHERE longt IS NOT NULL AND langt IS NOT NULL
        ORDER BY change_time ASC
    """
    return pd.read_sql(query, get_connection())

# --- Load Data ---
df = load_data()

st.title("🌍 Animated IP Change Timeline")

# Format date column
df["change_date"] = df["change_time"].dt.date.astype(str)  # for animation_frame

# --- Filters ---
st.sidebar.header("📅 Filters")
actions = st.sidebar.multiselect("Action", df["action"].unique(), default=df["action"].unique())
date_range = st.sidebar.date_input("Date range", [df["change_time"].min().date(), df["change_time"].max().date()])

filtered_df = df[
    (df["action"].isin(actions)) &
    (df["change_time"].dt.date >= date_range[0]) &
    (df["change_time"].dt.date <= date_range[1])
]

# --- Display Map Animation ---
if filtered_df.empty:
    st.warning("No data found for selected filters.")
else:
    st.markdown(f"Showing **{len(filtered_df)}** records")

    fig = px.scatter_geo(
        filtered_df,
        lat="langt",
        lon="longt",
        color="action",
        animation_frame="change_date",
        hover_name="country",
        hover_data=["city", "region", "change_time"],
        projection="natural earth",
        title="📍 IP Changes Over Time (Animated)",
        opacity=0.7,
        height=650
    )

    fig.update_layout(margin={"r":0,"t":50,"l":0,"b":0})
    st.plotly_chart(fig, use_container_width=True)



=======================================================

import os
from flask import Flask, jsonify
from pyspark.sql import SparkSession

app = Flask(__name__)

# Setup Spark
os.environ["PYSPARK_SUBMIT_ARGS"] = "--driver-memory 2g pyspark-shell"
spark = SparkSession.builder \
    .appName("IP History Viewer") \
    .master("local[*]") \
    .config("spark.jars", "C:/path/to/postgresql-42.7.4.jar") \
    .getOrCreate()

# Replace with your real PostgreSQL connection info
POSTGRES_URL = "jdbc:postgresql://your-host:5432/your_db"
POSTGRES_PROPS = {
    "user": "your_user",
    "password": "your_password",
    "driver": "org.postgresql.Driver"
}

@app.route("/")
def home():
    # Read IP history data
    df = spark.read.jdbc(
        url=POSTGRES_URL,
        table="qu.ip_history_test",
        properties=POSTGRES_PROPS
    )

    # Show recent records only
    df = df.orderBy(df["systime"].desc()).limit(10)

    # Convert to JSON-friendly format
    records = df.toPandas().to_dict(orient="records")
    return jsonify(records)

if __name__ == "__main__":
    app.run(debug=True)


=-=-=-=-=



from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("TestApp") \
    .master("local[*]") \
    .getOrCreate()

# Create sample data
data = [("Alice", 1), ("Bob", 2), ("Charlie", 3)]
columns = ["name", "id"]

df = spark.createDataFrame(data, columns)
df.show()

spark.stop()



-----==
@echo off
set SPARK_HOME=C:\spark\spark-3.4.4-bin-hadoop3
set PATH=%SPARK_HOME%\bin;%PATH%

@echo off
set PATH=%PATH:%SPARK_HOME%\bin;=%
set SPARK_HOME=



setx SPARK_HOME "C:\spark"
setx PATH "%PATH%;C:\spark\bin"


mkdir %CONDA_PREFIX%\etc\conda\activate.d
mkdir %CONDA_PREFIX%\etc\conda\deactivate.d

@echo off
set "JAVA_HOME=C:\devhome\tools\oraclejdk17\current"
set "PATH=%JAVA_HOME%\bin;%PATH%"


%CONDA_PREFIX%\etc\conda\activate.d\env_vars.bat

@echo off
REM Remove Java bin folder from PATH
set "PATH=%PATH:C:\devhome\tools\oraclejdk17\current\bin;=%"
set "JAVA_HOME="

%CONDA_PREFIX%\etc\conda\deactivate.d\env_vars.bat

conda deactivate
conda activate spark_test


/////////////11111111111111111111


Download here:
https://archive.apache.org/dist/spark/spark-3.3.2/

Choose:
spark-3.3.2-bin-hadoop3.tgz or .zip

Extract to: C:\spark\

Step 3: Set Environment Variables
Open CMD as Administrator, then run:

bash
Copy
Edit
setx JAVA_HOME "C:\Program Files\Eclipse Adoptium\jdk-11.0.X"
setx SPARK_HOME "C:\spark\spark-3.3.2-bin-hadoop3"
setx PATH "%JAVA_HOME%\bin;%SPARK_HOME%\bin;%PATH%"
🔁 Restart your terminal to apply changes.

Test:

bash
Copy
Edit
java -version
spark-submit --version





wget https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.7/postgresql-42.7.7.jar
spark = SparkSession.builder \
    .appName("IP History Analysis") \
    .config("spark.jars", "/home/you/jars/postgresql-42.7.7.jar") \
    .getOrCreate()

df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/your_db") \
    .option("dbtable", "qu.ip_history_test") \
    .option("user", "your_user") \
    .option("password", "your_pass") \
    .option("driver", "org.postgresql.Driver") \
    .load()

df.show()

-0000-
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Azure Postgres Connect") \
    .config("spark.jars", "/path/to/postgresql-42.7.7.jar") \
    .getOrCreate()

df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://<your-server>.postgres.database.azure.com:5432/<your-db>?sslmode=require") \
    .option("dbtable", "qu.ip_history_test") \
    .option("user", "your_user@<your-server>") \
    .option("password", "your_password") \
    .option("driver", "org.postgresql.Driver") \
    .load()

df.show()


        .option("ssl", "true") \
.option("sslmode", "verify-ca") \
.option("sslrootcert", "/path/to/BaltimoreCyberTrustRoot.crt.pem")



Setup Spark Env
pip install pyspark psycopg2-binary

Ensure your Spark session includes the Postgres JDBC driver:
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("IP History Analysis") \
    .config("spark.jars", "/path/postgresql-42.7.4.jar") \
    .getOrCreate()

Load the ip_history_test Table via JDBC
df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://HOST:5432/your_db") \
    .option("dbtable", "qu.ip_history_test") \
    .option("user", "your_user") \
    .option("password", "your_pass") \
    .load()
df.show(5)
df.printSchema()

=-==============--------------
CREATE OR REPLACE FUNCTION qu.sync_ip_with_history()
RETURNS void AS $$
DECLARE
    now_ts timestamptz := now();
BEGIN
    -- 1. INSERT new rows
    WITH latest_hist AS (
      SELECT DISTINCT ON (start_ip_int, end_ip_int)
          *
      FROM qu.ip_history_test
      ORDER BY start_ip_int, end_ip_int, lower(systime) DESC
    )
    INSERT INTO qu.ip_history_test (
       history_id, systime, action,
       start_ip_int, end_ip_int, continent, country, city,
       longt, langt, region, phone, dma, msa, countryiso2
    )
    SELECT
        gen_random_uuid(),
        tstzrange(now_ts, NULL::timestamptz),
        'insert',
        CUR.start_ip_int, CUR.end_ip_int, CUR.continent, CUR.country, CUR.city,
        CUR.longt, CUR.langt, CUR.region, CUR.phone, CUR.dma, CUR.msa, CUR.countryiso2
    FROM qu.ip_test CUR
    LEFT JOIN latest_hist LH
      ON CUR.start_ip_int = LH.start_ip_int
     AND CUR.end_ip_int   = LH.end_ip_int
    WHERE LH.start_ip_int IS NULL;

    -- 2. UPDATE changed rows
    WITH latest_hist AS (
      SELECT DISTINCT ON (start_ip_int, end_ip_int)
          *
      FROM qu.ip_history_test
      ORDER BY start_ip_int, end_ip_int, lower(systime) DESC
    )
    INSERT INTO qu.ip_history_test (
       history_id, systime, action,
       start_ip_int, end_ip_int, continent, country, city,
       longt, langt, region, phone, dma, msa, countryiso2
    )
    SELECT
       gen_random_uuid(),
       tstzrange(now_ts, NULL::timestamptz),
       'update',
       CUR.start_ip_int, CUR.end_ip_int, CUR.continent, CUR.country, CUR.city,
       CUR.longt, CUR.langt, CUR.region, CUR.phone, CUR.dma, CUR.msa, CUR.countryiso2
    FROM qu.ip_test CUR
    JOIN latest_hist LH
      ON CUR.start_ip_int = LH.start_ip_int
     AND CUR.end_ip_int   = LH.end_ip_int
    WHERE
      (CUR.continent   IS DISTINCT FROM LH.continent) OR
      (CUR.country     IS DISTINCT FROM LH.country)   OR
      (CUR.city        IS DISTINCT FROM LH.city)      OR
      (CUR.longt       IS DISTINCT FROM LH.longt)     OR
      (CUR.langt       IS DISTINCT FROM LH.langt)     OR
      (CUR.region      IS DISTINCT FROM LH.region)    OR
      (CUR.phone       IS DISTINCT FROM LH.phone)     OR
      (CUR.dma         IS DISTINCT FROM LH.dma)       OR
      (CUR.msa         IS DISTINCT FROM LH.msa)       OR
      (CUR.countryiso2 IS DISTINCT FROM LH.countryiso2);

    -- 3. DELETE removed rows
    WITH latest_hist AS (
      SELECT DISTINCT ON (start_ip_int, end_ip_int)
          *
      FROM qu.ip_history_test
      ORDER BY start_ip_int, end_ip_int, lower(systime) DESC
    )
    INSERT INTO qu.ip_history_test (
       history_id, systime, action,
       start_ip_int, end_ip_int, continent, country, city,
       longt, langt, region, phone, dma, msa, countryiso2
    )
    SELECT
       gen_random_uuid(),
       tstzrange(now_ts, NULL::timestamptz),
       'delete',
       LH.start_ip_int, LH.end_ip_int, LH.continent, LH.country, LH.city,
       LH.longt, LH.langt, LH.region, LH.phone, LH.dma, LH.msa, LH.countryiso2
    FROM latest_hist LH
    LEFT JOIN qu.ip_test CUR
      ON CUR.start_ip_int = LH.start_ip_int
     AND CUR.end_ip_int   = LH.end_ip_int
    WHERE CUR.start_ip_int IS NULL;
END;
$$ LANGUAGE plpgsql;



=-=-=-=

CREATE OR REPLACE FUNCTION qu.trigger_ip_sync()
RETURNS TRIGGER AS $$
BEGIN
    PERFORM qu.sync_ip_with_history();
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_ip_sync ON qu.ip_test;

CREATE TRIGGER trg_ip_sync
AFTER INSERT ON qu.ip_test
FOR EACH STATEMENT
EXECUTE FUNCTION qu.trigger_ip_sync();














CREATE OR REPLACE FUNCTION qu.log_ip_test_change()
RETURNS TRIGGER AS $$
DECLARE
    current_time timestamptz := now();
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO qu.ip_history_test (
            history_id,
            systime,
            action,
            start_ip_int,
            end_ip_int,
            continent,
            country,
            city,
            longt,
            langt,
            region,
            phone,
            dma,
            msa,
            countryiso2
        ) VALUES (
            gen_random_uuid(),
            tstzrange(current_time, NULL::timestamptz),
            'insert',
            NEW.start_ip_int,
            NEW.end_ip_int,
            NEW.continent,
            NEW.country,
            NEW.city,
            NEW.longt,
            NEW.langt,
            NEW.region,
            NEW.phone,
            NEW.dma,
            NEW.msa,
            NEW.countryiso2
        );
    ELSIF TG_OP = 'UPDATE' THEN
        INSERT INTO qu.ip_history_test (
            history_id,
            systime,
            action,
            start_ip_int,
            end_ip_int,
            continent,
            country,
            city,
            longt,
            langt,
            region,
            phone,
            dma,
            msa,
            countryiso2
        ) VALUES (
            gen_random_uuid(),
            tstzrange(current_time, NULL::timestamptz),
            'update',
            NEW.start_ip_int,
            NEW.end_ip_int,
            NEW.continent,
            NEW.country,
            NEW.city,
            NEW.longt,
            NEW.langt,
            NEW.region,
            NEW.phone,
            NEW.dma,
            NEW.msa,
            NEW.countryiso2
        );
    ELSIF TG_OP = 'DELETE' THEN
        INSERT INTO qu.ip_history_test (
            history_id,
            systime,
            action,
            start_ip_int,
            end_ip_int,
            continent,
            country,
            city,
            longt,
            langt,
            region,
            phone,
            dma,
            msa,
            countryiso2
        ) VALUES (
            gen_random_uuid(),
            tstzrange(current_time, NULL::timestamptz),
            'delete',
            OLD.start_ip_int,
            OLD.end_ip_int,
            OLD.continent,
            OLD.country,
            OLD.city,
            OLD.longt,
            OLD.langt,
            OLD.region,
            OLD.phone,
            OLD.dma,
            OLD.msa,
            OLD.countryiso2
        );
    END IF;

    RETURN NULL;  -- AFTER trigger must return NULL
END;
$$ LANGUAGE plpgsql;

trg

DROP TRIGGER IF EXISTS trg_log_ip_test_change ON qu.ip_test;

CREATE TRIGGER trg_log_ip_test_change
AFTER INSERT OR UPDATE OR DELETE ON qu.ip_test
FOR EACH ROW
EXECUTE FUNCTION qu.log_ip_test_change();


=


CREATE OR REPLACE FUNCTION log_ip_change()
RETURNS TRIGGER AS $$
DECLARE
    current_time timestamptz := now();
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO ip_history (
            history_id, systime, action,
            start_ip_int, end_ip_int, continent, country, city,
            longt, langt, region, phone, dma, msa, countryiso2
            -- add any other fields from ip
        )
        VALUES (
            gen_random_uuid(),
            tstzrange(current_time, NULL::timestamptz),
            'insert'::record_action,
            NEW.start_ip_int, NEW.end_ip_int, NEW.continent, NEW.country, NEW.city,
            NEW.longt, NEW.langt, NEW.region, NEW.phone, NEW.dma, NEW.msa, NEW.countryiso2
            -- same order as above
        );

    ELSIF TG_OP = 'UPDATE' THEN
        IF NEW IS DISTINCT FROM OLD THEN
            INSERT INTO ip_history (
                history_id, systime, action,
                start_ip_int, end_ip_int, continent, country, city,
                longt, langt, region, phone, dma, msa, countryiso2
            )
            VALUES (
                gen_random_uuid(),
                tstzrange(current_time, NULL::timestamptz),
                'update'::record_action,
                NEW.start_ip_int, NEW.end_ip_int, NEW.continent, NEW.country, NEW.city,
                NEW.longt, NEW.langt, NEW.region, NEW.phone, NEW.dma, NEW.msa, NEW.countryiso2
            );
        END IF;

    ELSIF TG_OP = 'DELETE' THEN
        INSERT INTO ip_history (
            history_id, systime, action,
            start_ip_int, end_ip_int, continent, country, city,
            longt, langt, region, phone, dma, msa, countryiso2
        )
        VALUES (
            gen_random_uuid(),
            tstzrange(current_time, NULL::timestamptz),
            'delete'::record_action,
            OLD.start_ip_int, OLD.end_ip_int, OLD.continent, OLD.country, OLD.city,
            OLD.longt, OLD.langt, OLD.region, OLD.phone, OLD.dma, OLD.msa, OLD.countryiso2
        );
    END IF;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;


Trigger 
DROP TRIGGER IF EXISTS trg_log_ip_change ON ip;

CREATE TRIGGER trg_log_ip_change
AFTER INSERT OR UPDATE OR DELETE ON ip
FOR EACH ROW
EXECUTE FUNCTION log_ip_change();
 



=====----------------------------



CREATE OR REPLACE FUNCTION sync_ip_with_history()
RETURNS void AS $$
DECLARE
    current_time timestamptz := now();
BEGIN
    -- INSERTED: rows in ip but not in ip_snapshot
    INSERT INTO ip_history (
        history_id,
        system,
        action,
        start_ipint, end_ip_int, continent, country, city,
        longt, langt, region, phone, dma, msa
        -- Add all other columns from ip here in the same order
    )
    SELECT
        gen_random_uuid(),
        tstzrange(current_time, NULL::timestamptz),
        'inserted',
        ip.start_ipint, ip.end_ip_int, ip.continent, ip.country, ip.city,
        ip.longt, ip.langt, ip.region, ip.phone, ip.dma, ip.msa
        -- Add other columns here as needed
    FROM ip
    LEFT JOIN ip_snapshot snap
      ON ip.start_ipint = snap.start_ipint AND ip.end_ip_int = snap.end_ip_int
    WHERE snap.start_ipint IS NULL;

    -- UPDATED: same keys, but different values
    INSERT INTO ip_history (
        history_id,
        system,
        action,
        start_ipint, end_ip_int, continent, country, city,
        longt, langt, region, phone, dma, msa
        -- Add all other columns here too
    )
    SELECT
        gen_random_uuid(),
        tstzrange(current_time, NULL::timestamptz),
        'updated',
        ip.start_ipint, ip.end_ip_int, ip.continent, ip.country, ip.city,
        ip.longt, ip.langt, ip.region, ip.phone, ip.dma, ip.msa
        -- Add other columns here as needed
    FROM ip
    JOIN ip_snapshot snap
      ON ip.start_ipint = snap.start_ipint AND ip.end_ip_int = snap.end_ip_int
    WHERE
        ip.continent IS DISTINCT FROM snap.continent OR
        ip.country IS DISTINCT FROM snap.country OR
        ip.city IS DISTINCT FROM snap.city OR
        ip.longt IS DISTINCT FROM snap.longt OR
        ip.langt IS DISTINCT FROM snap.langt OR
        ip.region IS DISTINCT FROM snap.region OR
        ip.phone IS DISTINCT FROM snap.phone OR
        ip.dma IS DISTINCT FROM snap.dma OR
        ip.msa IS DISTINCT FROM snap.msa;
        -- Add other column comparisons as needed

    -- DELETED: rows in ip_snapshot but not in ip
    INSERT INTO ip_history (
        history_id,
        system,
        action,
        start_ipint, end_ip_int, continent, country, city,
        longt, langt, region, phone, dma, msa
        -- Add all other columns from ip here
    )
    SELECT
        gen_random_uuid(),
        tstzrange(current_time, NULL::timestamptz),
        'deleted',
        snap.start_ipint, snap.end_ip_int, snap.continent, snap.country, snap.city,
        snap.longt, snap.langt, snap.region, snap.phone, snap.dma, snap.msa
        -- Add other columns here as needed
    FROM ip_snapshot snap
    LEFT JOIN ip
      ON ip.start_ipint = snap.start_ipint AND ip.end_ip_int = snap.end_ip_int
    WHERE ip.start_ipint IS NULL;

    -- Refresh snapshot
    TRUNCATE ip_snapshot;
    INSERT INTO ip_snapshot SELECT * FROM ip;

END;
$$ LANGUAGE plpgsql;





=-=-=-=-=-=-=-=-=-=-=-=-=-=

Create ip_snapshot table
CREATE TABLE IF NOT EXISTS ip_snapshot AS TABLE ip WITH NO DATA;
Create the sync_ip_with_history() function
CREATE OR REPLACE FUNCTION sync_ip_with_history()
RETURNS VOID AS $$
DECLARE
    current_time TIMESTAMPTZ := now();
BEGIN
    -- INSERTED records
    INSERT INTO ip_history (
        system, action,
        start_ipint, end_ip_int, continent, country, city, longt, langt, region, phone, dma, msa
    )
    SELECT
        tstzrange(current_time, NULL), 'inserted',
        ip.start_ipint, ip.end_ip_int, ip.continent, ip.country, ip.city,
        ip.longt, ip.langt, ip.region, ip.phone, ip.dma, ip.msa
    FROM ip
    LEFT JOIN ip_snapshot snap
      ON ip.start_ipint = snap.start_ipint AND ip.end_ip_int = snap.end_ip_int
    WHERE snap.start_ipint IS NULL;

    -- UPDATED records
    INSERT INTO ip_history (
        system, action,
        start_ipint, end_ip_int, continent, country, city, longt, langt, region, phone, dma, msa
    )
    SELECT
        tstzrange(current_time, NULL), 'updated',
        ip.start_ipint, ip.end_ip_int, ip.continent, ip.country, ip.city,
        ip.longt, ip.langt, ip.region, ip.phone, ip.dma, ip.msa
    FROM ip
    JOIN ip_snapshot snap
      ON ip.start_ipint = snap.start_ipint AND ip.end_ip_int = snap.end_ip_int
    WHERE ip.continent IS DISTINCT FROM snap.continent
       OR ip.country IS DISTINCT FROM snap.country
       OR ip.city IS DISTINCT FROM snap.city
       OR ip.longt IS DISTINCT FROM snap.longt
       OR ip.langt IS DISTINCT FROM snap.langt
       OR ip.region IS DISTINCT FROM snap.region
       OR ip.phone IS DISTINCT FROM snap.phone
       OR ip.dma IS DISTINCT FROM snap.dma
       OR ip.msa IS DISTINCT FROM snap.msa;

    -- DELETED records
    INSERT INTO ip_history (
        system, action,
        start_ipint, end_ip_int, continent, country, city, longt, langt, region, phone, dma, msa
    )
    SELECT
        tstzrange(current_time, NULL), 'deleted',
        snap.start_ipint, snap.end_ip_int, snap.continent, snap.country, snap.city,
        snap.longt, snap.langt, snap.region, snap.phone, snap.dma, snap.msa
    FROM ip_snapshot snap
    LEFT JOIN ip
      ON ip.start_ipint = snap.start_ipint AND ip.end_ip_int = snap.end_ip_int
    WHERE ip.start_ipint IS NULL;

    -- Refresh snapshot
    TRUNCATE ip_snapshot;
    INSERT INTO ip_snapshot SELECT * FROM ip;

END;
$$ LANGUAGE plpgsql;

Create the trigger function to call sync
CREATE OR REPLACE FUNCTION trigger_sync_ip_history()
RETURNS TRIGGER AS $$
BEGIN
    PERFORM sync_ip_with_history();
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

Attach the trigger to ip
DROP TRIGGER IF EXISTS sync_ip_on_insert ON ip;

CREATE TRIGGER sync_ip_on_insert
AFTER INSERT ON ip
FOR EACH STATEMENT
EXECUTE FUNCTION trigger_sync_ip_history();





==============================================

CREATE OR REPLACE FUNCTION sync_ip_with_history()
RETURNS void AS $$
DECLARE
    current_time TIMESTAMPTZ := now();
BEGIN
    -- 1. Create a temporary backup of the current IP data
    DROP TABLE IF EXISTS ip_backup;
    CREATE TEMP TABLE ip_backup AS TABLE ip;

    -- 2. Truncate and reload the ip table (assumed handled outside this function)

    -- 3. Insert new records
    INSERT INTO ip_history (
        history_id, start_ipint, end_ip_int, continent, country, city, longt, langt, region, phone, dma, msa,
        -- all other fields...
        system, action
    )
    SELECT
        gen_random_uuid(),
        i.start_ipint, i.end_ip_int, i.continent, i.country, i.city, i.longt, i.langt, i.region, i.phone, i.dma, i.msa,
        -- other fields...
        tstzrange(current_time, NULL), 'inserted'
    FROM ip i
    LEFT JOIN ip_backup b ON i.start_ipint = b.start_ipint AND i.end_ip_int = b.end_ip_int
    WHERE b.start_ipint IS NULL;

    -- 4. Insert updated records (compare all columns)
    INSERT INTO ip_history (
        history_id, start_ipint, end_ip_int, continent, country, city, longt, langt, region, phone, dma, msa,
        -- all other fields...
        system, action
    )
    SELECT
        gen_random_uuid(),
        i.start_ipint, i.end_ip_int, i.continent, i.country, i.city, i.longt, i.langt, i.region, i.phone, i.dma, i.msa,
        -- other fields...
        tstzrange(current_time, NULL), 'updated'
    FROM ip i
    JOIN ip_backup b ON i.start_ipint = b.start_ipint AND i.end_ip_int = b.end_ip_int
    WHERE (
        i.continent IS DISTINCT FROM b.continent OR
        i.country IS DISTINCT FROM b.country OR
        i.city IS DISTINCT FROM b.city OR
        i.longt IS DISTINCT FROM b.longt OR
        i.langt IS DISTINCT FROM b.langt OR
        i.region IS DISTINCT FROM b.region OR
        i.phone IS DISTINCT FROM b.phone OR
        i.dma IS DISTINCT FROM b.dma OR
        i.msa IS DISTINCT FROM b.msa
        -- Add IS DISTINCT FROM for all other 30 columns
    );

    -- 5. Insert deleted records
    INSERT INTO ip_history (
        history_id, start_ipint, end_ip_int, continent, country, city, longt, langt, region, phone, dma, msa,
        -- all other fields...
        system, action
    )
    SELECT
        gen_random_uuid(),
        b.start_ipint, b.end_ip_int, b.continent, b.country, b.city, b.longt, b.langt, b.region, b.phone, b.dma, b.msa,
        -- other fields...
        tstzrange(current_time, NULL), 'deleted'
    FROM ip_backup b
    LEFT JOIN ip i ON i.start_ipint = b.start_ipint AND i.end_ip_int = b.end_ip_int
    WHERE i.start_ipint IS NULL;
END;
$$ LANGUAGE plpgsql;









<?php
$uid = posix_getuid();
putenv("KRB5CCNAME=FILE:/tmp/krb5cc_$uid");

$keytab = '/path/to/your.keytab';
$principal = 'your_user@YOUR.REALM.COM';

// Run kinit
exec("kinit -k -t $keytab $principal 2>&1", $output, $status);

if ($status !== 0) {
    echo "kinit failed:\n";
    echo implode("\n", $output);
    exit;
}

echo "kinit successful\n";

// List the ticket
exec("klist", $klistOutput);
echo implode("\n", $klistOutput);



======================================
1
<?php
$principal = "youruser@YOUR.REALM.COM"; // Replace with your principal
$keytab = "/path/to/your.keytab";       // Replace with the actual keytab path

putenv("KRB5CCNAME=FILE:/tmp/krb5cc_" . posix_getuid());

// Run kinit using the keytab
exec("kinit -k -t $keytab $principal", $output, $return_var);

if ($return_var !== 0) {
    echo "Kerberos ticket initialization failed.\n";
    print_r($output);
    exit;
}
?>
<?php
$dbHost = 'your-db-host.example.com'; // DNS name, not IP
$dbPort = '5432';
$dbName = 'your_database';
$krbUser = 'appuser'; // This maps from Kerberos to DB role via pg_ident.conf

$dsn = "pgsql:host=$dbHost;port=$dbPort;dbname=$dbName";

try {
    $pdo = new PDO($dsn, $krbUser, null, [
        PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION
    ]);
    echo "✅ Kerberos-authenticated DB connection successful!";
} catch (PDOException $e) {
    echo "❌ Connection failed: " . $e->getMessage();
}
?>


2====================

putenv("KRB5CCNAME=FILE:/tmp/krb5cc_" . posix_getuid());

// Run kinit using the keytab
$principal = 'youruser@INT.BAR.COM';  // Replace with your principal
$keytab = '/path/to/your.keytab';     // Make sure this is readable

exec("kinit -k -t $keytab $principal", $output, $status);

if ($status !== 0) {
    die("❌ Kerberos kinit failed: " . implode("\n", $output));
}


<?php
ini_set('display_errors', 1);
error_reporting(E_ALL);

putenv("KRB5CCNAME=FILE:/tmp/krb5cc_" . posix_getuid());

// Step 1: Authenticate via keytab
$principal = 'youruser@INT.BAR.COM';         // Your Kerberos principal
$keytab = '/path/to/your.keytab';            // Your keytab file
exec("kinit -k -t $keytab $principal", $output, $status);

if ($status !== 0) {
    die("Kerberos auth failed:\n" . implode("\n", $output));
}

// Step 2: Connect to PostgreSQL using Kerberos (GSSAPI)
$dsn = "pgsql:host=dbserver.int.bar.com;port=5432;dbname=yourdb";
$user = $principal; // or a mapped DB role like 'appuser'
$password = null;

try {
    $pdo = new PDO($dsn, $user, $password, [
        PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION
    ]);
    echo "✅ Connected to database successfully using Kerberos.";
} catch (PDOException $e) {
    die("❌ Connection failed: " . $e->getMessage());
}
?>



==============

<?php

// Configuration (⚠️ DO NOT hardcode password in production)
$principal = "youruser@INT.BAR.COM";
$password  = "yourpassword"; // Insecure! Use only in test env

// Create temp file for password input
$tmpPasswordFile = tempnam("/tmp", "krb_pass");
file_put_contents($tmpPasswordFile, $password . "\n");

// Run kinit with the password file
$cmd = "kinit {$principal} < {$tmpPasswordFile}";
exec($cmd, $output, $returnVar);

// Cleanup
unlink($tmpPasswordFile);

if ($returnVar !== 0) {
    echo "❌ kinit failed.\n";
    exit;
}

// Set Kerberos ticket cache environment
$uid = function_exists('posix_getuid') ? posix_getuid() : getmyuid();
putenv("KRB5CCNAME=FILE:/tmp/krb5cc_" . $uid);

// Now try connecting to PostgreSQL using Kerberos
$dsn = "pgsql:host=grdsrv001234.INT.BAR.COM;port=5432;dbname=your_db";
$username = $principal;  // youruser@INT.BAR.COM
$password = ""; // No password needed, Kerberos ticket used

try {
    $pdo = new PDO($dsn, $username, $password);
    echo "✅ Connected to PostgreSQL using Kerberos.\n";
} catch (PDOException $e) {
    echo "❌ Connection failed: " . $e->getMessage() . "\n";
}



<?php
putenv("KRB5CCNAME=/tmp/krb5cc_" . posix_getuid());

$output = [];
exec("klist", $output);

$principal = null;

foreach ($output as $line) {
    if (stripos($line, 'Default principal:') !== false) {
        $parts = explode(':', $line);
        $principal = trim($parts[1]);
        break;
    }
}

if ($principal) {
    echo "Kerberos principal is: $principal\n";
} else {
    echo "Principal not found or ticket is missing.\n";
}
?>


pg_ident.conf
# MAPNAME    SYSTEM-USERNAME        PG-USERNAME
krbmap       /^[a-zA-Z0-9]+$/       appuser

pg_hba.conf #Add this line before any other generic host entries
# TYPE       DATABASE   USER     ADDRESS          METHOD
hostgssenc   all        all      0.0.0.0/0        gss map=krbmap

Test the Mapping
export KRB5CCNAME=FILE:/tmp/krb5cc_$(id -u)
kinit -k -t /etc/app.keytab your_user@INT.BAR.COM
psql -h your-db-host -U your_user -d your_db
SELECT current_user;






<?php
try {
    $dsn = 'pgsql:host=db.example.com;port=5432;dbname=mydatabase';
    
    // DO NOT pass username/password — Kerberos will handle it via ticket cache
    $pdo = new PDO($dsn, null, null, [
        PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION
    ]);

    echo "✅ Kerberos-authenticated connection successful!";
} catch (PDOException $e) {
    echo "❌ Connection failed: " . $e->getMessage();
}


<?php
class KerberosPostgresPDO {
    private $host;
    private $port;
    private $database;
    private $pdo;

    public function __construct($host, $port, $database) {
        $this->host = $host;
        $this->port = $port;
        $this->database = $database;
    }

    /**
     * Authenticate and establish Kerberos PDO connection
     * @return PDO
     * @throws Exception
     */
    public function connectWithKerberos() {
        // Prerequisite: Ensure Kerberos libraries are installed
        // Install: 
        // - libkrb5-dev 
        // - php-gssapi 
        // - php-pdo-pgsql

        // Step 1: Validate Kerberos ticket
        if (!$this->validateKerberosTicket()) {
            throw new Exception("Kerberos authentication failed");
        }

        // Step 2: Construct PDO connection string with GSSAPI
        $dsn = "pgsql:host={$this->host};port={$this->port};dbname={$this->database};";
        $options = [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
            PDO::ATTR_TIMEOUT => 10,
            PDO::PGSQL_ATTR_USE_GSSAPI => true  // Critical for Kerberos
        ];

        try {
            // Establish PDO connection using Kerberos
            $this->pdo = new PDO($dsn, '', '', $options);
            return $this->pdo;
        } catch (PDOException $e) {
            error_log("Kerberos PDO Connection Error: " . $e->getMessage());
            throw $e;
        }
    }

    /**
     * Validate existing Kerberos ticket
     * @return bool
     */
    private function validateKerberosTicket(): bool {
        // Check if Kerberos ticket is valid
        $klist = shell_exec('klist -s');
        
        // If no ticket exists or is invalid
        if ($klist === null) {
            // Attempt to renew or acquire ticket
            $kinit = shell_exec('kinit -R');
            
            // If renewal fails, prompt for manual ticket acquisition
            if ($kinit === null) {
                error_log("No valid Kerberos ticket. Please run 'kinit'");
                return false;
            }
        }

        return true;
    }

    /**
     * Execute a prepared statement with Kerberos authentication
     * @param string $query
     * @param array $params
     * @return PDOStatement
     */
    public function executeQuery(string $query, array $params = []) {
        try {
            // Ensure connection is established
            if (!$this->pdo) {
                $this->connectWithKerberos();
            }

            $stmt = $this->pdo->prepare($query);
            $stmt->execute($params);
            return $stmt;
        } catch (PDOException $e) {
            error_log("Kerberos Query Execution Error: " . $e->getMessage());
            throw $e;
        }
    }

    /**
     * Retrieve user principal from Kerberos ticket
     * @return string|null
     */
    public function getCurrentUserPrincipal(): ?string {
        $principal = shell_exec('klist | grep "Principal:" | awk \'{print $2}\'');
        return trim($principal);
    }
}

// Usage Example
try {
    $kerberosDB = new KerberosPostgresPDO(
        'your_postgres_host', 
        5432, 
        'your_database_name'
    );

    // Establish Kerberos-authenticated connection
    $pdo = $kerberosDB->connectWithKerberos();

    // Get current Kerberos user
    $currentUser = $kerberosDB->getCurrentUserPrincipal();
    echo "Authenticated as: " . $currentUser;

    // Execute a sample query
    $stmt = $kerberosDB->executeQuery(
        "SELECT * FROM users WHERE username = :username", 
        ['username' => $currentUser]
    );
    $result = $stmt->fetchAll(PDO::FETCH_ASSOC);

} catch (Exception $e) {
    // Handle authentication or connection errors
    die("Authentication Failed: " . $e->getMessage());
}
