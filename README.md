# Flights Case Study

In this project, I am working with a compressed flat file (flights.gz) containing data about domestic flights in the United States. The data is stored as a stage file in S3 and will serve as the foundation for the ETL process. I am using Airflow for workflow orchestration, Python and Snowflake for data storage and querying. The goal is to load the data into Snowflake, model it appropriately, and make it ready for business intelligence analysis with Tableau.

Flat File:![alt text](https://github.com/veronica-sa-dataeng/flights-case-study/blob/dev/images/flat%20file.png?raw=true "Flat File")

## Project Directory Structure
```
├── dags
│   └── data-eng-dag-etl
│       ├── constants.py
│       ├── dag.py
│       ├── Dockerfile
│       ├── main.py
│       ├── __pycache__
│       │   ├── dag.cpython-312.pyc
│       │   └── dag.cpython-37.pyc
│       ├── queries.py
│       └── utils.py
├── docker-compose.yaml
├── images
│   ├── airflow.gif 
│   ├── dataarchitecture.png 
│   ├── dataquality.png 
│   ├── flat file.png 
│   ├── loadedtables.gif 
│   ├── starschema.png 
│   └── tables, views and file formats.png
├── README.md
└── requirements.txt
```

## **1. Data Architecture**
### Data layers

- **Datasource Layer - S3**:  
  S3 stores the flat file, which is the foundation of the project.

- **Orchestration Layer - Airflow**:  
  Easy integration with Snowflake, automates ETL tasks, and manages workflow dependencies.

- **Storage Layer - Snowflake**:  
  Scalable, cloud-native data warehouse for storing and processing large datasets with high performance and flexibility.

- **Data Visualization Layer - Tableau**:  
  Example tool for visualizing data stored in Snowflake (not currently in use, but could be used for dashboards and reports).
    
![alt text](https://github.com/veronica-sa-dataeng/flights-case-study/blob/dev/images/dataarchitecture.png "Data Architecture")

### Database Design
The star schema was chosen for data modeling because it is simple and efficient for the data structure, which consists of two dimensions and one fact table. The star schema is easy to understand and works well with small datasets.
The fact table in the center, surrounded by the two dimension tables, simplifies data joining and querying. Its straightforward structure enhances query performance, especially for analyzing and aggregating data.
Overall, the star schema is a good fit because it is simple, easy to maintain, and efficient for data analysis.

![alt text](https://github.com/veronica-sa-dataeng/flights-case-study/blob/dev/images/starschema.png "Star Schema")

## **2. Data Dictionary**


### Data Dictionary for `DIM_AIRLINE` Table
####Table Overview
The `DIM_AIRLINE` table stores information about airlines, including their unique identifier, code, and name.

| Column Name    | Data Type        | Description                                                    | Constraints                            |
|----------------|------------------|----------------------------------------------------------------|----------------------------------------|
| `AIRLINE_ID`   | `NUMBER(38,0)`    | Unique identifier for each airline.                           | Primary Key, Auto Increment (Start 1, Increment 1), Not Null |
| `AIRLINECODE`  | `VARCHAR(100)`    | The code used to identify the airline (e.g., IATA code).       | Nullable                               |
| `AIRLINENAME`  | `VARCHAR(200)`    | The name of the airline.                                      | Nullable                               |

#### Notes
- The `AIRLINE_ID` column is auto-incremented, meaning it will automatically generate a unique value for each new record inserted into the table.
- The `AIRLINECODE` and `AIRLINENAME` columns are nullable, allowing for flexibility when adding airline records without full information.
- The table is intended to store detailed information for each airline in the recruitment database.

### Data Dictionary for `DIM_AIRPORT` Table

#### Table Overview
The `DIM_AIRPORT` table stores information about airports, including their unique identifier, code, name, city, and state.

| Column Name     | Data Type        | Description                                                    | Constraints                            |
|-----------------|------------------|----------------------------------------------------------------|----------------------------------------|
| `AIRPORT_ID`    | `NUMBER(38,0)`    | Unique identifier for each airport.                           | Primary Key, Auto Increment (Start 1, Increment 1), Not Null |
| `AIRPORTCODE`   | `VARCHAR(100)`    | The code used to identify the airport (e.g., ROA code).       | Nullable                               |
| `AIRPORTNAME`   | `VARCHAR(200)`    | The name of the airport.                                      | Nullable                               |
| `CITYNAME`      | `VARCHAR(200)`    | The name of the city where the airport is located.            | Nullable                               |
| `STATECODE`     | `VARCHAR(100)`    | The code of the state where the airport is located (e.g., VA state code). | Nullable                               |
| `STATENAME`     | `VARCHAR(200)`    | The name of the state where the airport is located.           | Nullable                               |

#### Notes
- The `AIRPORT_ID` column is auto-incremented, meaning it will automatically generate a unique value for each new record inserted into the table.
- The `AIRPORTCODE`, `AIRPORTNAME`, `CITYNAME`, `STATECODE`, and `STATENAME` columns are nullable, allowing for flexibility when adding airport records without full information.
- The table is intended to store detailed information for each airport in the recruitment database.

### Data Dictionary for `FACT_FLIGHTS` Table

#### Table Overview
The `FACT_FLIGHTS` table stores detailed flight data, including flight-specific information, delays, and other operational attributes, with references to associated airports and airlines.

| Column Name         | Data Type             | Description                                                       | Constraints                                    |
|---------------------|-----------------------|-------------------------------------------------------------------|------------------------------------------------|
| `FLIGHT_ID`         | `NUMBER(38,0)`         | Unique identifier for each flight record.                        | Primary Key, Auto Increment (Start 1, Increment 1), Not Null |
| `TRANSACTIONID`     | `VARCHAR(200)`         | The unique transaction identifier for the flight.                | Nullable                                       |
| `FLIGHTDATE`        | `DATE`                 | The date of the flight.                                          | Nullable                                       |
| `TAILNUM`           | `VARCHAR(100)`         | The tail number of the aircraft (unique identifier).             | Nullable                                       |
| `FLIGHTNUM`         | `VARCHAR(100)`         | The flight number.                                               | Nullable                                       |
| `CANCELLED`         | `BOOLEAN`              | Indicates whether the flight was cancelled.                      | Nullable                                       |
| `DEPDELAY`          | `VARCHAR(100)`         | The departure delay (in minutes).                                | Nullable                                       |
| `ARRDELAY`          | `VARCHAR(100)`         | The arrival delay (in minutes).                                  | Nullable                                       |
| `CRSDEPTIME`        | `DATE`                 | The scheduled departure time.                                    | Nullable                                       |
| `DEPTIME`           | `TIMESTAMP_NTZ(9)`     | The actual departure time.                                       | Nullable                                       |
| `CRSARRTIME`        | `DATE`                 | The scheduled arrival time.                                      | Nullable                                       |
| `ARRTIME`           | `DATE`                 | The actual arrival time.                                         | Nullable                                       |
| `WHEELSOFF`         | `VARCHAR(100)`         | The time when the aircraft’s wheels leave the ground.            | Nullable                                       |
| `WHEELSON`          | `VARCHAR(100)`         | The time when the aircraft’s wheels touch down.                  | Nullable                                       |
| `ORIGAIRPORTID`     | `NUMBER(38,0)`         | The unique ID of the origin airport (foreign key to `DIM_AIRPORT`). | Foreign Key referencing `DIM_AIRPORT(AIRPORT_ID)` |
| `DESTAIRPORTID`     | `NUMBER(38,0)`         | The unique ID of the destination airport (foreign key to `DIM_AIRPORT`). | Foreign Key referencing `DIM_AIRPORT(AIRPORT_ID)` |
| `AIRLINE_ID`        | `NUMBER(38,0)`         | The unique ID of the airline (foreign key to `DIM_AIRLINE`).     | Foreign Key referencing `DIM_AIRLINE(AIRLINE_ID)` |
| `TAXIOUT`           | `VARCHAR(100)`         | The time spent taxiing out from the airport before departure.     | Nullable                                       |
| `TAXIIN`            | `VARCHAR(100)`         | The time spent taxiing into the airport after arrival.           | Nullable                                       |
| `CRSELAPSEDTIME`    | `DATE`                 | The scheduled elapsed time for the flight.                       | Nullable                                       |
| `ACTUALELAPSEDTIME` | `DATE`                 | The actual elapsed time for the flight.                          | Nullable                                       |
| `DIVERTED`          | `BOOLEAN`              | Indicates whether the flight was diverted.                       | Nullable                                       |
| `DISTANCE`          | `VARCHAR(200)`         | The distance traveled during the flight (in miles).              | Nullable                                       |
| `DISTANCEGROUP`     | `VARCHAR(200)`         | A grouping for the distance (e.g., short, medium, long).         | Nullable                                       |
| `NEXTDAYARR`        | `NUMBER(38,0)`         | The number of flights arriving the next day.                     | Nullable                                       |
| `DEPDELAYGT15`      | `NUMBER(38,0)`         | The number of flights with departure delays greater than 15 minutes. | Nullable                                       |

#### Foreign Key Relationships
- `ORIGAIRPORTID` references `DIM_AIRPORT(AIRPORT_ID)`, linking to the origin airport.
- `DESTAIRPORTID` references `DIM_AIRPORT(AIRPORT_ID)`, linking to the destination airport.
- `AIRLINE_ID` references `DIM_AIRLINE(AIRLINE_ID)`, linking to the airline.

#### Notes
- The `FLIGHT_ID` column is auto-incremented, meaning it will automatically generate a unique value for each new flight record inserted into the table.
- The table is designed to store detailed information about individual flights, including their schedule, delays, and other relevant data.
- The foreign key relationships ensure data integrity by linking to the corresponding airports and airline tables.

## **3. Data Quality**
In the project, "count_before" and "count_after" are used to track the data quality at different stages of the data processing pipeline. 
These counts represent the number of records before and after any transformation or cleaning process.
The "count_before" reflects the total number of records before any changes are applied, while the "count_after" represents the number of records after transformations have been made. By comparing these two values, it is possible to identify if any data was lost, altered, or corrupted during the ETL (Extract, Transform, Load) process.
This approach ensures that the data quality is maintained throughout the pipeline and helps identify potential issues that might require further cleaning or validation.

![alt text](https://github.com/veronica-sa-dataeng/flights-case-study/blob/dev/images/dataquality.png "Data Quality")

## **4. File Format Creation**
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

## **5. Table Creation**

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

## **6. View Creation**

```sql
create or replace view RECRUITMENT_DB.CANDIDATE_00184.VW_FLIGHTS(
	TRANSACTIONID,
	DISTANCEGROUP,
	DEPDELAYGT15,
	NEXTDAYARR,
	AIRLINENAME,
	ORIGAIRPORTNAME,
	DESTAIRPORTNAME,
	RANKAIRLINE,
	RANKCANCELLEDPERAIRLINE,
	AVG_DEPDELAYPERAIRLINE,
	AVG_TAXIOUT_ORIG,
	AVG_TAXIIN_DEST,
	AVG_WHEEL_TURNAROUND_ORIG,
	AVG_WHEEL_TURNAROUND_DEST
) as
SELECT 
    f."TRANSACTIONID", 
    f."DISTANCEGROUP",
    f."DEPDELAYGT15",
    f."NEXTDAYARR",
    a."AIRLINENAME", 
    o."AIRPORTNAME" AS "ORIGAIRPORTNAME", 
    d."AIRPORTNAME" AS "DESTAIRPORTNAME",
    r."RANK" AS "RANKAIRLINE",
    c."CANCELLED_RANK" AS "RANKCANCELLEDPERAIRLINE",
    dep_delay."AVG_DEPDELAY" AS "AVG_DEPDELAYPERAIRLINE",
    taxi_orig."AVG_TAXIOUT" AS "AVG_TAXIOUT_ORIG",
    taxi_dest."AVG_TAXIIN" AS "AVG_TAXIIN_DEST",
    wheel_orig."AVG_WHEEL_TURNAROUND" AS "AVG_WHEEL_TURNAROUND_ORIG",
    wheel_dest."AVG_WHEEL_TURNAROUND" AS "AVG_WHEEL_TURNAROUND_DEST"
FROM 
    "RECRUITMENT_DB"."CANDIDATE_00184"."FACT_FLIGHTS" f
JOIN 
    "RECRUITMENT_DB"."CANDIDATE_00184"."DIM_AIRLINE" a ON f."AIRLINE_ID" = a."AIRLINE_ID"
JOIN 
    "RECRUITMENT_DB"."CANDIDATE_00184"."DIM_AIRPORT" o ON f."ORIGAIRPORTID" = o."AIRPORT_ID"
JOIN 
    "RECRUITMENT_DB"."CANDIDATE_00184"."DIM_AIRPORT" d ON f."DESTAIRPORTID" = d."AIRPORT_ID"
JOIN 
(
    SELECT 
        "AIRLINE_ID", 
        DENSE_RANK() OVER (ORDER BY COUNT(*) DESC) AS "RANK"
    FROM 
        "RECRUITMENT_DB"."CANDIDATE_00184"."FACT_FLIGHTS"
    GROUP BY 
        "AIRLINE_ID"
) r ON f."AIRLINE_ID" = r."AIRLINE_ID"
JOIN 
(
    SELECT 
        "AIRLINE_ID",
        DENSE_RANK() OVER (ORDER BY COUNT(*) DESC) AS "CANCELLED_RANK"
    FROM 
        "RECRUITMENT_DB"."CANDIDATE_00184"."FACT_FLIGHTS"
    WHERE 
        "CANCELLED" = TRUE
    GROUP BY 
        "AIRLINE_ID"
) c ON f."AIRLINE_ID" = c."AIRLINE_ID"
LEFT JOIN 
(
    SELECT 
        "AIRLINE_ID", 
        CAST(AVG(CAST("DEPDELAY" AS INTEGER)) AS INTEGER) AS "AVG_DEPDELAY"
    FROM 
        "RECRUITMENT_DB"."CANDIDATE_00184"."FACT_FLIGHTS"
    GROUP BY 
        "AIRLINE_ID"
    HAVING 
        AVG(CAST("DEPDELAY" AS INTEGER)) <= 15
) dep_delay ON f."AIRLINE_ID" = dep_delay."AIRLINE_ID"
LEFT JOIN 
(
    SELECT 
        "ORIGAIRPORTID",
        AVG(CAST("TAXIOUT" AS INTEGER)) AS "AVG_TAXIOUT"
    FROM 
        "RECRUITMENT_DB"."CANDIDATE_00184"."FACT_FLIGHTS"
    GROUP BY 
        "ORIGAIRPORTID"
) taxi_orig ON f."ORIGAIRPORTID" = taxi_orig."ORIGAIRPORTID"
LEFT JOIN 
(
    SELECT 
        "DESTAIRPORTID",
        AVG(CAST("TAXIIN" AS INTEGER)) AS "AVG_TAXIIN"
    FROM 
        "RECRUITMENT_DB"."CANDIDATE_00184"."FACT_FLIGHTS"
    GROUP BY 
        "DESTAIRPORTID"
) taxi_dest ON f."DESTAIRPORTID" = taxi_dest."DESTAIRPORTID"
LEFT JOIN 
(
    SELECT 
        "ORIGAIRPORTID",
        AVG(DATEDIFF('minute', "WHEELSOFF", "WHEELSON")) AS "AVG_WHEEL_TURNAROUND"
    FROM 
        "RECRUITMENT_DB"."CANDIDATE_00184"."FACT_FLIGHTS"
    GROUP BY 
        "ORIGAIRPORTID"
) wheel_orig ON f."ORIGAIRPORTID" = wheel_orig."ORIGAIRPORTID"
LEFT JOIN 
(
    SELECT 
        "DESTAIRPORTID",
        AVG(DATEDIFF('minute', "WHEELSOFF", "WHEELSON")) AS "AVG_WHEEL_TURNAROUND"
    FROM 
        "RECRUITMENT_DB"."CANDIDATE_00184"."FACT_FLIGHTS"
    GROUP BY 
        "DESTAIRPORTID"
) wheel_dest ON f."DESTAIRPORTID" = wheel_dest."DESTAIRPORTID";
```
## **7. Tables and Views created**

![alt text](https://github.com/veronica-sa-dataeng/flights-case-study/blob/dev/images/tables%2C%20views%20and%20file%20formats.png "Snowflake")

## **7.1 Data Loaded**

![alt text](https://github.com/veronica-sa-dataeng/flights-case-study/blob/dev/images/loadedtables.gif "Data Loaded")

## **8. Airflow dag**

![alt text](https://github.com/veronica-sa-dataeng/flights-case-study/blob/dev/images/airflow.gif "Airflow")

## **9. Apache Airflow Docker Compose**
<a href="https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml" target="_blank">Docker Compose for Airflow</a>
