infer_schema_column_list = """ 
SELECT *
    FROM TABLE(
    INFER_SCHEMA(
      LOCATION=>'@RECRUITMENT_DB.PUBLIC.S3_FOLDER/flights.gz'
      , FILE_FORMAT=>'infer_schema_format'
      )
    );"""
insert_csv_stage_table = """ 
        COPY INTO CANDIDATE_00184.FLIGHTS
        FROM @PUBLIC.S3_FOLDER/flights.gz
        FILE_FORMAT = csv_format;
        """
insert_dim_airline_table = """ 
    TRUNCATE TABLE RECRUITMENT_DB.CANDIDATE_00184.DIM_AIRLINE;
    INSERT INTO RECRUITMENT_DB.CANDIDATE_00184.DIM_AIRLINE 
    SELECT  
        ROW_NUMBER() OVER (ORDER BY "AIRLINECODE") AS "AIRLINE_ID",
        "AIRLINECODE", 
        "AIRLINENAME"      
    FROM 
        (SELECT DISTINCT 
            "AIRLINECODE", 
            SUBSTRING("AIRLINENAME", 1, POSITION(':' IN "AIRLINENAME") - 1) AS "AIRLINENAME"
        FROM RECRUITMENT_DB.CANDIDATE_00184.FLIGHTS
        ) AS DISTINCT_FLIGHTS;
"""
insert_dim_airport_table = """
    TRUNCATE TABLE RECRUITMENT_DB.CANDIDATE_00184.DIM_AIRPORT;
    INSERT INTO RECRUITMENT_DB.CANDIDATE_00184."DIM_AIRPORT"
    SELECT 
        ROW_NUMBER() OVER (ORDER BY "AIRPORTNAME") AS "AIRPORT_ID",
        "AIRPORTCODE",
        SUBSTRING("AIRPORTNAME", 1, LENGTH("AIRPORTNAME") - 2) AS "AIRPORTNAME",
        "CITYNAME",
        CASE 
            WHEN "STATECODE" IS NULL 
                THEN  RIGHT("AIRPORTNAME",2)
            ELSE 
                "STATECODE"
        END AS "STATECODE",
        CASE 
            WHEN "STATENAME" IS NULL AND RIGHT("AIRPORTNAME",2) = 'KS' THEN 'Kansas'
            WHEN "STATENAME" IS NULL AND RIGHT("AIRPORTNAME",2) = 'OK' THEN 'Oklahoma'
            ELSE "STATENAME"
        END AS "STATENAME"
    FROM (
        SELECT 
            SUBSTRING(ORIGAIRPORTNAME, 1, POSITION(':' IN ORIGAIRPORTNAME) - 1) AS "AIRPORTNAME",
            ORIGINAIRPORTCODE AS "AIRPORTCODE",
            ORIGINCITYNAME AS "CITYNAME",
            ORIGINSTATE AS "STATECODE",
            ORIGINSTATENAME AS "STATENAME"
        FROM RECRUITMENT_DB.CANDIDATE_00184.FLIGHTS
        UNION ALL
        SELECT  
            SUBSTRING(DESTAIRPORTNAME, 1, POSITION(':' IN DESTAIRPORTNAME) - 1) AS "AIRPORTNAME",
            DESTAIRPORTCODE AS "AIRPORTCODE",
            DESTCITYNAME AS "CITYNAME",
            DESTSTATE AS "STATECODE",
            DESTSTATENAME AS "STATENAME"
        FROM RECRUITMENT_DB.CANDIDATE_00184.FLIGHTS
        ) as TEMP 
        GROUP BY "AIRPORTNAME","AIRPORTCODE","CITYNAME","STATECODE","STATENAME"
    """
insert_dim_fact_table = """
    TRUNCATE TABLE RECRUITMENT_DB.CANDIDATE_00184.FACT_FLIGHTS;
    INSERT INTO "RECRUITMENT_DB"."CANDIDATE_00184"."FACT_FLIGHTS" 
    SELECT 
        ROW_NUMBER() OVER (ORDER BY "TRANSACTIONID") AS "FLIGHT_ID",
        flatfile."TRANSACTIONID",
        flatfile."FLIGHTDATE",
        flatfile."TAILNUM",
        flatfile."FLIGHTNUM",
        flatfile."CANCELLED",
        flatfile."DEPDELAY",
        flatfile."ARRDELAY",
        flatfile."CRSDEPTIME",
        flatfile."DEPTIME",
        flatfile."CRSARRTIME",
        flatfile."ARRTIME",
        flatfile."WHEELSOFF",
        flatfile."WHEELSON",
        orig."AIRPORT_ID" AS "ORIGAIRPORTID",
        dest."AIRPORT_ID" AS "DESTAIRPORTID",
        airline."AIRLINE_ID",
        flatfile."TAXIOUT",
        flatfile."TAXIIN",
        flatfile."CRSELAPSEDTIME",
        flatfile."ACTUALELAPSEDTIME",
        flatfile."DIVERTED",
        flatfile."DISTANCE",
        CASE
            WHEN CAST(SUBSTRING("DISTANCE", 1, LENGTH("DISTANCE") - 6) AS INTEGER) BETWEEN 0 AND 100 THEN '0-100 miles'
            WHEN CAST(SUBSTRING("DISTANCE", 1, LENGTH("DISTANCE") - 6) AS INTEGER) BETWEEN 101 AND 200 THEN '101-200 miles'
            WHEN CAST(SUBSTRING("DISTANCE", 1, LENGTH("DISTANCE") - 6) AS INTEGER) BETWEEN 201 AND 300 THEN '201-300 miles'
            WHEN CAST(SUBSTRING("DISTANCE", 1, LENGTH("DISTANCE") - 6) AS INTEGER) BETWEEN 301 AND 400 THEN '301-400 miles'
            WHEN CAST(SUBSTRING("DISTANCE", 1, LENGTH("DISTANCE") - 6) AS INTEGER) BETWEEN 401 AND 500 THEN '401-500 miles'
        ELSE '500+ miles'
        END as "DISTANCEGROUP",
        CASE
            WHEN DATE("ARRTIME") > DATE("DEPTIME") THEN 1
        ELSE 0 
        END AS "NEXTDAYARR",
        CASE
            WHEN CAST("DEPDELAY" AS INTEGER) > 15 THEN 1
        ELSE 0 
        END AS "DEPDELAYGT15"
    FROM RECRUITMENT_DB.CANDIDATE_00184.FLIGHTS flatfile
    INNER JOIN RECRUITMENT_DB.CANDIDATE_00184.DIM_AIRLINE airline 
    on flatfile.airlinecode = airline."AIRLINECODE"
    LEFT JOIN RECRUITMENT_DB.CANDIDATE_00184.DIM_AIRPORT orig 
    ON flatfile.ORIGINAIRPORTCODE = orig.airportcode
    LEFT JOIN RECRUITMENT_DB.CANDIDATE_00184.DIM_AIRPORT dest 
    ON flatfile.DESTAIRPORTCODE = dest.airportcode;
    DROP TABLE RECRUITMENT_DB.CANDIDATE_00184.FLIGHTS;
    """
dim_airline_count ="""SELECT COUNT(*) FROM RECRUITMENT_DB.CANDIDATE_00184.DIM_AIRLINE """
dim_airport_count="""SELECT COUNT(*) FROM RECRUITMENT_DB.CANDIDATE_00184.DIM_AIRPORT"""
fact_count = """SELECT COUNT(*) FROM RECRUITMENT_DB.CANDIDATE_00184.FACT_FLIGHTS""" 