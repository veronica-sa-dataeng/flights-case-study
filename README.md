# Flights Case Study

In this project, I am working with a compressed flat file (flights.gz) containing data about domestic flights in the United States. The data is stored as a stage file in S3 and will serve as the foundation for the ETL process. I am using Airflow for workflow orchestration, Python and Pandas for data processing, and Snowflake for data storage and querying. The goal is to load the data into Snowflake, model it appropriately, and make it ready for business intelligence analysis with Tableau.

Flat File:![alt text](https://github.com/veronica-sa-dataeng/flights-case-study/blob/dev/images/flat%20file.png?raw=true "Flat File")

## **1. File Format Creation**
I created a file format in Snowflake to properly load the compressed flat file (flights.gz) and handle its format and compression.

```sql
CREATE OR REPLACE FILE FORMAT infer_schema_format
    TYPE = 'CSV'
    COMPRESSION = 'GZIP'
    ENCODING = 'UTF8'
    PARSE_HEADER = true;
```
```
CREATE OR REPLACE FILE FORMAT csv_format
    TYPE = 'CSV'
    COMPRESSION = 'GZIP'
    ENCODING = 'UTF8'
    FIELD_DELIMITER = '|'
    SKIP_HEADER = 1;
```

## **2. Table Creation**

### **DIM_AIRLINE Table**

```sql
CREATE OR REPLACE TABLE RECRUITMENT_DB.CANDIDATE_00184.DIM_AIRLINE (
    AIRLINE_ID NUMBER(38,0) NOT NULL AUTOINCREMENT START 1 INCREMENT 1,
    AIRLINECODE VARCHAR(100),
    AIRLINENAME VARCHAR(200),
    PRIMARY KEY (AIRLINE_ID)
);
```

### **DIM_AIRPORT Table**

```sql
CREATE OR REPLACE TABLE RECRUITMENT_DB.CANDIDATE_00184.DIM_AIRPORT (
    AIRPORT_ID NUMBER(38,0) NOT NULL AUTOINCREMENT START 1 INCREMENT 1,
    AIRPORTCODE VARCHAR(100),
    AIRPORTNAME VARCHAR(200),
    CITYNAME VARCHAR(200),
    STATECODE VARCHAR(100),
    STATENAME VARCHAR(200),
    PRIMARY KEY (AIRPORT_ID)
);
```

### **FACT_FLIGHTS Table**

```sql
CREATE OR REPLACE TABLE RECRUITMENT_DB.CANDIDATE_00184.FACT_FLIGHTS (
    FLIGHT_ID NUMBER(38,0) NOT NULL AUTOINCREMENT START 1 INCREMENT 1 NOORDER,
    TRANSACTIONID VARCHAR(200),
    FLIGHTDATE DATE,
    TAILNUM VARCHAR(100),
    FLIGHTNUM VARCHAR(100),
    CANCELLED BOOLEAN,
    DEPDELAY VARCHAR(100),
    ARRDELAY VARCHAR(100),
    CRSDEPTIME DATE,
    DEPTIME TIMESTAMP_NTZ(9),
    CRSARRTIME DATE,
    ARRTIME DATE,
    WHEELSOFF VARCHAR(100),
    WHEELSON VARCHAR(100),
    ORIGAIRPORTID NUMBER(38,0),
    DESTAIRPORTID NUMBER(38,0),
    AIRLINE_ID NUMBER(38,0),
    TAXIOUT VARCHAR(100),
    TAXIIN VARCHAR(100),
    CRSELAPSEDTIME DATE,
    ACTUALELAPSEDTIME DATE,
    DIVERTED BOOLEAN,
    DISTANCE VARCHAR(200),
    DISTANCEGROUP VARCHAR(200),
    NEXTDAYARR NUMBER(38,0),
    DEPDELAYGT15 NUMBER(38,0),
    PRIMARY KEY (FLIGHT_ID),
    FOREIGN KEY (ORIGAIRPORTID) REFERENCES RECRUITMENT_DB.CANDIDATE_00184.DIM_AIRPORT(AIRPORT_ID),
    FOREIGN KEY (DESTAIRPORTID) REFERENCES RECRUITMENT_DB.CANDIDATE_00184.DIM_AIRPORT(AIRPORT_ID),
    FOREIGN KEY (AIRLINE_ID) REFERENCES RECRUITMENT_DB.CANDIDATE_00184.DIM_AIRLINE(AIRLINE_ID)
);
```

## **3. View Creation**

```sql
CREATE OR REPLACE VIEW RECRUITMENT_DB.CANDIDATE_00184.VW_FLIGHTS(
    TRANSACTIONID,
    DISTANCEGROUP,
    DEPDELAYGT15,
    NEXTDAYARR,
    AIRLINENAME,
    ORIGAIRPORTNAME,
    DESTAIRPORTNAME
) AS
SELECT 
    f."TRANSACTIONID", 
    f."DISTANCEGROUP",
    f."DEPDELAYGT15",
    f."NEXTDAYARR",
    a."AIRLINENAME", 
    o."AIRPORTNAME" AS "ORIGAIRPORTNAME", 
    d."AIRPORTNAME" AS "DESTAIRPORTNAME"
FROM 
    "RECRUITMENT_DB"."CANDIDATE_00184"."FACT_FLIGHTS" f
JOIN 
    "RECRUITMENT_DB"."CANDIDATE_00184"."DIM_AIRLINE" a ON f."AIRLINE_ID" = a."AIRLINE_ID"
JOIN 
    "RECRUITMENT_DB"."CANDIDATE_00184"."DIM_AIRPORT" o ON f."ORIGAIRPORTID" = o."AIRPORT_ID"
JOIN 
    "RECRUITMENT_DB"."CANDIDATE_00184"."DIM_AIRPORT" d ON f."DESTAIRPORTID" = d."AIRPORT_ID";
```
## **4. Tables and Views created**

![alt text](https://github.com/veronica-sa-dataeng/flights-case-study/blob/dev/images/tables%2C%20views%20and%20file%20formats.png "Snowflake")

## **4.1 Data Loaded**

![alt text](https://github.com/veronica-sa-dataeng/flights-case-study/blob/dev/images/loadedtables.gif "Data Loaded")

## **5. Airflow dag**

![alt text](https://github.com/veronica-sa-dataeng/flights-case-study/blob/dev/images/airflow.gif "Airflow")



## **6. Apache Airflow Docker Compose**
<a href="https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml" target="_blank">Docker Compose for Airflow</a>
