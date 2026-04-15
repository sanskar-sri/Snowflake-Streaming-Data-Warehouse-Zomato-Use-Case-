---------------------------------------------------------------------------
-- ZOMATO-LIKE DATA WAREHOUSE ON SNOWFLAKE
-- End-to-end project:
--   * Oracle → Azure Blob → Snowflake RAW
--   * RAW (STRING/VARIANT) → STG (typed views, JSON flattening)
--   * INT (SCD1, SCD2, SCD3 dims + facts via JS procedures)
--   * MARTS (complex KPIs)
--   * Governance: Masking + Row-Level Security
--   * Streamlit dashboard on top (separate Python file)
---------------------------------------------------------------------------

---------------------------------------------------------------------------
-- 0. GLOBAL CONTEXT
---------------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE WAREHOUSE WH_ZOMATO
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND   = 60
  AUTO_RESUME    = TRUE
  INITIALLY_SUSPENDED = TRUE;

CREATE OR REPLACE DATABASE ZOMATO_DWH;

USE WAREHOUSE WH_ZOMATO;
USE DATABASE  ZOMATO_DWH;

CREATE OR REPLACE SCHEMA RAW;
CREATE OR REPLACE SCHEMA STG;
CREATE OR REPLACE SCHEMA INT;
CREATE OR REPLACE SCHEMA MARTS;
CREATE OR REPLACE SCHEMA UTIL;
CREATE OR REPLACE SCHEMA APP;

---------------------------------------------------------------------------
-- 1. INTEGRATIONS, STAGE, FILE FORMATS
---------------------------------------------------------------------------

USE ROLE ACCOUNTADMIN;
USE DATABASE ZOMATO_DWH;
USE SCHEMA RAW;

-- 1.1 Storage Integration to Azure Blob
CREATE OR REPLACE STORAGE INTEGRATION AZURE_BLOB_INT
  TYPE                     = EXTERNAL_STAGE
  STORAGE_PROVIDER         = AZURE
  ENABLED                  = TRUE
  STORAGE_ALLOWED_LOCATIONS = (
    'azure://snowflake2azuredemo1.blob.core.windows.net/zomato-landing/'
  )
  AZURE_TENANT_ID          = 'f031fa1a-7a7f-4523-97e4-e3053ecf3690';

  DESC INTEGRATION AZURE_BLOB_INT;

-- 1.2 Notification Integration to Azure Storage Queue (Snowpipe auto-ingest)
CREATE OR REPLACE NOTIFICATION INTEGRATION AZURE_QUEUE_INT
  TYPE                       = QUEUE
  ENABLED                    = TRUE
  NOTIFICATION_PROVIDER      = AZURE_STORAGE_QUEUE
  AZURE_STORAGE_QUEUE_PRIMARY_URI = 'https://snowflake2azuredemo1.queue.core.windows.net/zomatoproject'
  AZURE_TENANT_ID            = 'f031fa1a-7a7f-4523-97e4-e3053ecf3690';

  DESC INTEGRATION AZURE_QUEUE_INT;

-- 1.3 External Stage pointing to landing container
CREATE OR REPLACE STAGE RAW.ZOMATO_EXT_STAGE
  URL                = 'azure://snowflake2azuredemo1.blob.core.windows.net/zomato-landing/'
  STORAGE_INTEGRATION = AZURE_BLOB_INT;

-- 1.4 File formats
CREATE OR REPLACE FILE FORMAT RAW.FF_CSV_STD
  TYPE                      = CSV
  SKIP_HEADER               = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  NULL_IF                   = ('', 'NULL', 'null');

CREATE OR REPLACE FILE FORMAT RAW.FF_JSON_STD
  TYPE = JSON;

---------------------------------------------------------------------------
-- 2. RAW LAYER (landing tables, STRING / VARIANT)
---------------------------------------------------------------------------

USE ROLE ACCOUNTADMIN;
USE DATABASE ZOMATO_DWH;
USE SCHEMA RAW;

-- 2.1 CUSTOMER_RAW
CREATE OR REPLACE TABLE CUSTOMER_RAW (
  CUSTOMER_ID        STRING,
  CUSTOMER_NAME      STRING,
  EMAIL              STRING,
  PRIMARY_PHONE      STRING,
  REGISTERED_AT      STRING,
  CITY               STRING,
  AREA               STRING,
  LAT                STRING,
  LNG                STRING,
  SEGMENT            STRING,
  IS_PRIME_MEMBER    STRING,
  STATUS             STRING
);

-- 2.2 RESTAURANT_RAW
CREATE OR REPLACE TABLE RESTAURANT_RAW (
  RESTAURANT_ID      STRING,
  RESTAURANT_NAME    STRING,
  CUISINE_PRIMARY    STRING,
  CUISINE_SECONDARY  STRING,
  CITY               STRING,
  AREA               STRING,
  LAT                STRING,
  LNG                STRING,
  AVG_RATING         STRING,
  COMMISSION_RATE    STRING,
  IS_ACTIVE          STRING,
  ONBOARDED_AT       STRING
);

-- 2.3 DELIVERY_AGENT_RAW
CREATE OR REPLACE TABLE DELIVERY_AGENT_RAW (
  AGENT_ID           STRING,
  AGENT_NAME         STRING,
  PHONE              STRING,
  HIRE_DATE          STRING,
  CITY               STRING,
  VEHICLE_TYPE       STRING,
  STATUS             STRING
);

-- 2.4 MENU_ITEM_RAW
CREATE OR REPLACE TABLE MENU_ITEM_RAW (
  MENU_ITEM_ID       STRING,
  RESTAURANT_ID      STRING,
  ITEM_NAME          STRING,
  CATEGORY           STRING,
  PRICE              STRING,
  IS_VEG             STRING,
  IS_ACTIVE          STRING
);

-- 2.5 PROMOTION_RAW
CREATE OR REPLACE TABLE PROMOTION_RAW (
  PROMO_ID           STRING,
  PROMO_CODE         STRING,
  DISCOUNT_PERCENT   STRING,
  START_DATE         STRING,
  END_DATE           STRING,
  MAX_DISCOUNT_AMT   STRING,
  TARGET_SEGMENT     STRING,
  TARGET_CITY        STRING
);

-- 2.6 ORDER_HEADER_RAW
CREATE OR REPLACE TABLE ORDER_HEADER_RAW (
  ORDER_ID             STRING,
  CUSTOMER_ID          STRING,
  RESTAURANT_ID        STRING,
  ORDER_CREATED_AT     STRING,
  ORDER_STATUS         STRING,
  PAYMENT_METHOD       STRING,
  PROMO_ID             STRING,
  ORDER_SUBTOTAL       STRING,
  ORDER_DISCOUNT       STRING,
  DELIVERY_FEE         STRING,
  TOTAL_AMOUNT         STRING,
  EXPECTED_DELIVERY_AT STRING,
  ACTUAL_DELIVERY_AT   STRING,
  CANCELLATION_REASON  STRING
);

-- 2.7 ORDER_ITEM_RAW
CREATE OR REPLACE TABLE ORDER_ITEM_RAW (
  ORDER_ID           STRING,
  ORDER_ITEM_ID      STRING,
  MENU_ITEM_ID       STRING,
  QTY                STRING,
  ITEM_PRICE         STRING,
  ITEM_DISCOUNT      STRING,
  TOTAL_ITEM_AMOUNT  STRING
);

-- 2.8 DELIVERY_TRIP_RAW
CREATE OR REPLACE TABLE DELIVERY_TRIP_RAW (
  TRIP_ID            STRING,
  ORDER_ID           STRING,
  AGENT_ID           STRING,
  PICKUP_TIME        STRING,
  DROP_TIME          STRING,
  DISTANCE_KM        STRING,
  ESTIMATED_TIME_MIN STRING,
  ACTUAL_TIME_MIN    STRING,
  SLA_BREACH_FLAG    STRING
);

-- 2.9 PAYMENT_RAW
CREATE OR REPLACE TABLE PAYMENT_RAW (
  PAYMENT_ID         STRING,
  ORDER_ID           STRING,
  PAYMENT_STATUS     STRING,
  PAYMENT_AT         STRING,
  PAYMENT_AMOUNT     STRING
);

-- 2.10 CUSTOMER_FEEDBACK_RAW
CREATE OR REPLACE TABLE CUSTOMER_FEEDBACK_RAW (
  FEEDBACK_ID        STRING,
  ORDER_ID           STRING,
  CUSTOMER_ID        STRING,
  RATING             STRING,
  COMMENT            STRING,
  SENTIMENT_SCORE    STRING,
  CREATED_AT         STRING
);

-- 2.11 CUSTOMER_EVENTS_RAW (JSON lines)
CREATE OR REPLACE TABLE CUSTOMER_EVENTS_RAW (
  EVENT_RAW VARIANT
);

---------------------------------------------------------------------------
-- 3. SNOWPIPE FOR ALL RAW TABLES (AUTO_INGEST)
---------------------------------------------------------------------------

USE ROLE ACCOUNTADMIN;
USE DATABASE ZOMATO_DWH;
USE SCHEMA RAW;

-- CUSTOMER
CREATE OR REPLACE PIPE CUSTOMER_PIPE
  AUTO_INGEST = TRUE
  INTEGRATION = AZURE_QUEUE_INT
AS
COPY INTO RAW.CUSTOMER_RAW
FROM @RAW.ZOMATO_EXT_STAGE/customer/
FILE_FORMAT = (FORMAT_NAME = RAW.FF_CSV_STD);

--SELECT SYSTEM$PIPE_STATUS('CUSTOMER_PIPE');
--SELECT COUNT(*) FROM RAW.CUSTOMER_RAW;
---ALTER PIPE CUSTOMER_PIPE REFRESH;
--SELECT $1 FROM  @RAW.ZOMATO_EXT_STAGE/customer/

-- RESTAURANT
--SELECT COUNT(*) FROM RAW.RESTAURANT_RAW;
CREATE OR REPLACE PIPE RESTAURANT_PIPE
  AUTO_INGEST = TRUE
  INTEGRATION = AZURE_QUEUE_INT
AS
COPY INTO RAW.RESTAURANT_RAW
FROM @RAW.ZOMATO_EXT_STAGE/restaurant/
FILE_FORMAT = (FORMAT_NAME = RAW.FF_CSV_STD);

--SELECT COUNT(*) FROM RAW.RESTAURANT_RAW;
--ALTER PIPE RESTAURANT_PIPE REFRESH;


-- DELIVERY_AGENT
CREATE OR REPLACE PIPE DELIVERY_AGENT_PIPE
  AUTO_INGEST = TRUE
  INTEGRATION = AZURE_QUEUE_INT
AS
COPY INTO RAW.DELIVERY_AGENT_RAW
FROM @RAW.ZOMATO_EXT_STAGE/delivery_agent/
FILE_FORMAT = (FORMAT_NAME = RAW.FF_CSV_STD);
SELECT COUNT(*) FROM RAW.DELIVERY_AGENT_RAW;

-- MENU_ITEM
CREATE OR REPLACE PIPE MENU_ITEM_PIPE
  AUTO_INGEST = TRUE
  INTEGRATION = AZURE_QUEUE_INT
AS
COPY INTO RAW.MENU_ITEM_RAW
FROM @RAW.ZOMATO_EXT_STAGE/menu_item/
FILE_FORMAT = (FORMAT_NAME = RAW.FF_CSV_STD);

-- PROMOTION
CREATE OR REPLACE PIPE PROMOTION_PIPE
  AUTO_INGEST = TRUE
  INTEGRATION = AZURE_QUEUE_INT
AS
COPY INTO RAW.PROMOTION_RAW
FROM @RAW.ZOMATO_EXT_STAGE/promotion/
FILE_FORMAT = (FORMAT_NAME = RAW.FF_CSV_STD);

-- ORDER_HEADER
CREATE OR REPLACE PIPE ORDER_HEADER_PIPE
  AUTO_INGEST = TRUE
  INTEGRATION = AZURE_QUEUE_INT
AS
COPY INTO RAW.ORDER_HEADER_RAW
FROM @RAW.ZOMATO_EXT_STAGE/order_header/
FILE_FORMAT = (FORMAT_NAME = RAW.FF_CSV_STD);

-- ORDER_ITEM
CREATE OR REPLACE PIPE ORDER_ITEM_PIPE
  AUTO_INGEST = TRUE
  INTEGRATION = AZURE_QUEUE_INT
AS
COPY INTO RAW.ORDER_ITEM_RAW
FROM @RAW.ZOMATO_EXT_STAGE/order_item/
FILE_FORMAT = (FORMAT_NAME = RAW.FF_CSV_STD);

-- DELIVERY_TRIP
CREATE OR REPLACE PIPE DELIVERY_TRIP_PIPE
  AUTO_INGEST = TRUE
  INTEGRATION = AZURE_QUEUE_INT
AS
COPY INTO RAW.DELIVERY_TRIP_RAW
FROM @RAW.ZOMATO_EXT_STAGE/delivery_trip
FILE_FORMAT = (FORMAT_NAME = RAW.FF_CSV_STD);

-- PAYMENT
CREATE OR REPLACE PIPE PAYMENT_PIPE
  AUTO_INGEST = TRUE
  INTEGRATION = AZURE_QUEUE_INT
AS
COPY INTO RAW.PAYMENT_RAW
FROM @RAW.ZOMATO_EXT_STAGE/payment/
FILE_FORMAT = (FORMAT_NAME = RAW.FF_CSV_STD);

-- CUSTOMER_FEEDBACK
CREATE OR REPLACE PIPE CUSTOMER_FEEDBACK_PIPE
  AUTO_INGEST = TRUE
  INTEGRATION = AZURE_QUEUE_INT
AS
COPY INTO RAW.CUSTOMER_FEEDBACK_RAW
FROM @RAW.ZOMATO_EXT_STAGE/customer_feedback/
FILE_FORMAT = (FORMAT_NAME = RAW.FF_CSV_STD);

-- CUSTOMER_EVENTS (JSON)
CREATE OR REPLACE PIPE CUSTOMER_EVENTS_PIPE
  AUTO_INGEST = TRUE
  INTEGRATION = AZURE_QUEUE_INT
AS
COPY INTO RAW.CUSTOMER_EVENTS_RAW
FROM @RAW.ZOMATO_EXT_STAGE/customer_events/
FILE_FORMAT = (FORMAT_NAME = RAW.FF_JSON_STD);
SELECT COUNT(*) FROM RAW.CUSTOMER_EVENTS_RAW;


---------------------------------------------------------------------------
-- 4. STAGING (STG) LAYER - VIEWS FOR TYPING + CLEANSING + JSON FLATTENING
---------------------------------------------------------------------------

USE ROLE ACCOUNTADMIN;
USE DATABASE ZOMATO_DWH;
USE SCHEMA STG;

-- 4.1 CUSTOMER STG VIEW
CREATE OR REPLACE VIEW V_CUSTOMER_STG AS
SELECT
  TRY_TO_NUMBER(CUSTOMER_ID)                                  AS CUSTOMER_ID,
  TRIM(CUSTOMER_NAME)                                         AS CUSTOMER_NAME,
  LOWER(TRIM(EMAIL))                                          AS EMAIL,
  REGEXP_REPLACE(PRIMARY_PHONE, '[^0-9]', '')                 AS PRIMARY_PHONE,
  TRY_TO_TIMESTAMP_NTZ(REGISTERED_AT)                         AS REGISTERED_AT,
  NULLIF(TRIM(CITY), '')                                      AS CITY,
  NULLIF(TRIM(AREA), '')                                      AS AREA,
  TRY_TO_DOUBLE(LAT)                                          AS LAT,
  TRY_TO_DOUBLE(LNG)                                          AS LNG,
  COALESCE(NULLIF(TRIM(SEGMENT),''), 'New')                   AS SEGMENT,
  COALESCE(
    TRY_TO_BOOLEAN(
      CASE UPPER(TRIM(IS_PRIME_MEMBER))
        WHEN 'Y' THEN 'TRUE'
        WHEN '1' THEN 'TRUE'
        WHEN 'TRUE' THEN 'TRUE'
        ELSE 'FALSE'
      END
    ), FALSE
  )                                                           AS IS_PRIME_MEMBER,
  COALESCE(NULLIF(TRIM(STATUS),''), 'ACTIVE')                 AS STATUS
FROM RAW.CUSTOMER_RAW;

-- 4.2 RESTAURANT STG VIEW
CREATE OR REPLACE VIEW V_RESTAURANT_STG AS
SELECT
  TRY_TO_NUMBER(RESTAURANT_ID)                                AS RESTAURANT_ID,
  TRIM(RESTAURANT_NAME)                                       AS RESTAURANT_NAME,
  TRIM(CUISINE_PRIMARY)                                       AS CUISINE_PRIMARY,
  TRIM(CUISINE_SECONDARY)                                     AS CUISINE_SECONDARY,
  NULLIF(TRIM(CITY),'')                                       AS CITY,
  NULLIF(TRIM(AREA),'')                                       AS AREA,
  TRY_TO_DOUBLE(LAT)                                          AS LAT,
  TRY_TO_DOUBLE(LNG)                                          AS LNG,
  COALESCE(TRY_TO_DOUBLE(AVG_RATING), 0.0)                    AS AVG_RATING,
  COALESCE(TRY_TO_DOUBLE(COMMISSION_RATE), 0.20)              AS COMMISSION_RATE,
  COALESCE(
    TRY_TO_BOOLEAN(
      CASE UPPER(TRIM(IS_ACTIVE))
        WHEN 'Y' THEN 'TRUE'
        WHEN '1' THEN 'TRUE'
        WHEN 'TRUE' THEN 'TRUE'
        ELSE 'FALSE'
      END
    ), FALSE
  )                                                           AS IS_ACTIVE,
  TRY_TO_TIMESTAMP_NTZ(ONBOARDED_AT)                          AS ONBOARDED_AT
FROM RAW.RESTAURANT_RAW;

-- 4.3 DELIVERY_AGENT STG VIEW
CREATE OR REPLACE VIEW V_DELIVERY_AGENT_STG AS
SELECT
  TRY_TO_NUMBER(AGENT_ID)                                     AS AGENT_ID,
  TRIM(AGENT_NAME)                                            AS AGENT_NAME,
  REGEXP_REPLACE(PHONE, '[^0-9]', '')                         AS PHONE,
  TRY_TO_TIMESTAMP_NTZ(HIRE_DATE)                             AS HIRE_DATE,
  NULLIF(TRIM(CITY),'')                                       AS CITY,
  TRIM(VEHICLE_TYPE)                                          AS VEHICLE_TYPE,
  COALESCE(NULLIF(TRIM(STATUS),''),'ACTIVE')                  AS STATUS
FROM RAW.DELIVERY_AGENT_RAW;

-- 4.4 MENU_ITEM STG VIEW
CREATE OR REPLACE VIEW V_MENU_ITEM_STG AS
SELECT
  TRY_TO_NUMBER(MENU_ITEM_ID)                                 AS MENU_ITEM_ID,
  TRY_TO_NUMBER(RESTAURANT_ID)                                AS RESTAURANT_ID,
  TRIM(ITEM_NAME)                                             AS ITEM_NAME,
  TRIM(CATEGORY)                                              AS CATEGORY,
  COALESCE(TRY_TO_NUMBER(PRICE), 0)                           AS PRICE,
  COALESCE(
    TRY_TO_BOOLEAN(
      CASE UPPER(TRIM(IS_VEG))
        WHEN 'Y' THEN 'TRUE'
        WHEN '1' THEN 'TRUE'
        WHEN 'TRUE' THEN 'TRUE'
        ELSE 'FALSE'
      END
    ), FALSE
  )                                                           AS IS_VEG,
  COALESCE(
    TRY_TO_BOOLEAN(
      CASE UPPER(TRIM(IS_ACTIVE))
        WHEN 'Y' THEN 'TRUE'
        WHEN '1' THEN 'TRUE'
        WHEN 'TRUE' THEN 'TRUE'
        ELSE 'FALSE'
      END
    ), TRUE
  )                                                           AS IS_ACTIVE
FROM RAW.MENU_ITEM_RAW;

-- 4.5 PROMOTION STG VIEW
CREATE OR REPLACE VIEW V_PROMOTION_STG AS
SELECT
  TRY_TO_NUMBER(PROMO_ID)                                     AS PROMO_ID,
  TRIM(PROMO_CODE)                                            AS PROMO_CODE,
  COALESCE(TRY_TO_NUMBER(DISCOUNT_PERCENT),0)                 AS DISCOUNT_PERCENT,
  TRY_TO_TIMESTAMP_NTZ(START_DATE)                            AS START_DATE,
  TRY_TO_TIMESTAMP_NTZ(END_DATE)                              AS END_DATE,
  COALESCE(TRY_TO_NUMBER(MAX_DISCOUNT_AMT),0)                 AS MAX_DISCOUNT_AMT,
  TRIM(TARGET_SEGMENT)                                        AS TARGET_SEGMENT,
  TRIM(TARGET_CITY)                                           AS TARGET_CITY
FROM RAW.PROMOTION_RAW;

-- 4.6 ORDER_HEADER STG VIEW
CREATE OR REPLACE VIEW V_ORDER_HEADER_STG AS
SELECT
  TRY_TO_NUMBER(ORDER_ID)                                     AS ORDER_ID,
  TRY_TO_NUMBER(CUSTOMER_ID)                                  AS CUSTOMER_ID,
  TRY_TO_NUMBER(RESTAURANT_ID)                                AS RESTAURANT_ID,
  TRY_TO_TIMESTAMP_NTZ(ORDER_CREATED_AT)                      AS ORDER_CREATED_AT,
  UPPER(TRIM(ORDER_STATUS))                                   AS ORDER_STATUS,
  UPPER(TRIM(PAYMENT_METHOD))                                 AS PAYMENT_METHOD,
  TRY_TO_NUMBER(PROMO_ID)                                     AS PROMO_ID,
  COALESCE(TRY_TO_NUMBER(ORDER_SUBTOTAL), 0)                  AS ORDER_SUBTOTAL,
  COALESCE(TRY_TO_NUMBER(ORDER_DISCOUNT), 0)                  AS ORDER_DISCOUNT,
  COALESCE(TRY_TO_NUMBER(DELIVERY_FEE), 0)                    AS DELIVERY_FEE,
  COALESCE(TRY_TO_NUMBER(TOTAL_AMOUNT), 0)                    AS TOTAL_AMOUNT,
  TRY_TO_TIMESTAMP_NTZ(EXPECTED_DELIVERY_AT)                  AS EXPECTED_DELIVERY_AT,
  TRY_TO_TIMESTAMP_NTZ(ACTUAL_DELIVERY_AT)                    AS ACTUAL_DELIVERY_AT,
  NULLIF(TRIM(CANCELLATION_REASON), '')                       AS CANCELLATION_REASON
FROM RAW.ORDER_HEADER_RAW;

-- 4.7 ORDER_ITEM STG VIEW
CREATE OR REPLACE VIEW V_ORDER_ITEM_STG AS
SELECT
  TRY_TO_NUMBER(ORDER_ID)                                     AS ORDER_ID,
  TRY_TO_NUMBER(ORDER_ITEM_ID)                                AS ORDER_ITEM_ID,
  TRY_TO_NUMBER(MENU_ITEM_ID)                                 AS MENU_ITEM_ID,
  COALESCE(TRY_TO_NUMBER(QTY), 0)                             AS QTY,
  COALESCE(TRY_TO_NUMBER(ITEM_PRICE), 0)                      AS ITEM_PRICE,
  COALESCE(TRY_TO_NUMBER(ITEM_DISCOUNT), 0)                   AS ITEM_DISCOUNT,
  COALESCE(TRY_TO_NUMBER(TOTAL_ITEM_AMOUNT), 0)               AS TOTAL_ITEM_AMOUNT
FROM RAW.ORDER_ITEM_RAW;

-- 4.8 DELIVERY_TRIP STG VIEW
CREATE OR REPLACE VIEW V_DELIVERY_TRIP_STG AS
SELECT
  TRY_TO_NUMBER(TRIP_ID)                                      AS TRIP_ID,
  TRY_TO_NUMBER(ORDER_ID)                                     AS ORDER_ID,
  TRY_TO_NUMBER(AGENT_ID)                                     AS AGENT_ID,
  TRY_TO_TIMESTAMP_NTZ(PICKUP_TIME)                           AS PICKUP_TIME,
  TRY_TO_TIMESTAMP_NTZ(DROP_TIME)                             AS DROP_TIME,
  COALESCE(TRY_TO_DOUBLE(DISTANCE_KM), 0.0)                   AS DISTANCE_KM,
  COALESCE(TRY_TO_NUMBER(ESTIMATED_TIME_MIN), 0)              AS ESTIMATED_TIME_MIN,
  COALESCE(TRY_TO_NUMBER(ACTUAL_TIME_MIN), 0)                 AS ACTUAL_TIME_MIN,
  COALESCE(TRY_TO_NUMBER(SLA_BREACH_FLAG), 0)                 AS SLA_BREACH_FLAG
FROM RAW.DELIVERY_TRIP_RAW;

-- 4.9 PAYMENT STG VIEW
CREATE OR REPLACE VIEW V_PAYMENT_STG AS
SELECT
  TRY_TO_NUMBER(PAYMENT_ID)                                  AS PAYMENT_ID,
  TRY_TO_NUMBER(ORDER_ID)                                    AS ORDER_ID,
  UPPER(TRIM(PAYMENT_STATUS))                                AS PAYMENT_STATUS,
  TRY_TO_TIMESTAMP_NTZ(PAYMENT_AT)                           AS PAYMENT_AT,
  COALESCE(TRY_TO_NUMBER(PAYMENT_AMOUNT), 0)                 AS PAYMENT_AMOUNT
FROM RAW.PAYMENT_RAW;

-- 4.10 CUSTOMER_FEEDBACK STG VIEW
CREATE OR REPLACE VIEW V_CUSTOMER_FEEDBACK_STG AS
SELECT
  TRY_TO_NUMBER(FEEDBACK_ID)                                 AS FEEDBACK_ID,
  TRY_TO_NUMBER(ORDER_ID)                                    AS ORDER_ID,
  TRY_TO_NUMBER(CUSTOMER_ID)                                 AS CUSTOMER_ID,
  COALESCE(TRY_TO_NUMBER(RATING),0)                          AS RATING,
  COMMENT                                                    AS COMMENT,
  TRY_TO_DOUBLE(SENTIMENT_SCORE)                             AS SENTIMENT_SCORE,
  TRY_TO_TIMESTAMP_NTZ(CREATED_AT)                           AS CREATED_AT
FROM RAW.CUSTOMER_FEEDBACK_RAW;

-- 4.11 CUSTOMER_EVENTS STG VIEW (JSON → columns)
CREATE OR REPLACE VIEW V_CUSTOMER_EVENTS_STG AS
SELECT
  EVENT_RAW:"event_id"::NUMBER                               AS EVENT_ID,
  EVENT_RAW:"customer_id"::NUMBER                            AS CUSTOMER_ID,
  EVENT_RAW:"event_type"::STRING                             AS EVENT_TYPE,
  TRY_TO_TIMESTAMP_NTZ(EVENT_RAW:"event_ts"::STRING)         AS EVENT_TS,
  EVENT_RAW:"metadata":"search_query"::STRING                AS SEARCH_QUERY,
  EVENT_RAW:"metadata":"device_os"::STRING                   AS DEVICE_OS,
  EVENT_RAW:"metadata":"app_version"::STRING                 AS APP_VERSION
FROM RAW.CUSTOMER_EVENTS_RAW;

---------------------------------------------------------------------------
-- 5. INT LAYER - TABLE DDL (NO CTAS)
---------------------------------------------------------------------------

USE ROLE ACCOUNTADMIN;
USE DATABASE ZOMATO_DWH;
USE SCHEMA INT;

-- 5.1 DIM_DATE (structure only; data via procedure)
CREATE OR REPLACE TABLE DIM_DATE (
  DATE_KEY           NUMBER,
  FULL_DATE          DATE,
  YEAR               NUMBER(4,0),
  MONTH              NUMBER(2,0),
  MONTH_SHORT        STRING,
  DAY_OF_MONTH       NUMBER(2,0),
  DAY_OF_WEEK        NUMBER(2,0),
  DAY_OF_WEEK_SHORT  STRING,
  WEEK_OF_YEAR       NUMBER(2,0)
);

-- 5.2 DIM_CUSTOMER (SCD2)
CREATE OR REPLACE TABLE DIM_CUSTOMER (
  CUSTOMER_SK        NUMBER AUTOINCREMENT,
  CUSTOMER_ID        NUMBER,
  CUSTOMER_NAME      STRING,
  EMAIL              STRING,
  PRIMARY_PHONE      STRING,
  CITY               STRING,
  AREA               STRING,
  SEGMENT            STRING,
  IS_PRIME_MEMBER    BOOLEAN,
  STATUS             STRING,
  EFFECTIVE_FROM     TIMESTAMP_NTZ,
  EFFECTIVE_TO       TIMESTAMP_NTZ,
  IS_CURRENT         BOOLEAN,
  RECORD_SOURCE      STRING
);

-- 5.3 DIM_RESTAURANT (SCD2)
CREATE OR REPLACE TABLE DIM_RESTAURANT (
  RESTAURANT_SK      NUMBER AUTOINCREMENT,
  RESTAURANT_ID      NUMBER,
  RESTAURANT_NAME    STRING,
  CUISINE_PRIMARY    STRING,
  CUISINE_SECONDARY  STRING,
  CITY               STRING,
  AREA               STRING,
  AVG_RATING         FLOAT,
  COMMISSION_RATE    FLOAT,
  IS_ACTIVE          BOOLEAN,
  ONBOARDED_AT       TIMESTAMP_NTZ,
  EFFECTIVE_FROM     TIMESTAMP_NTZ,
  EFFECTIVE_TO       TIMESTAMP_NTZ,
  IS_CURRENT         BOOLEAN,
  RECORD_SOURCE      STRING
);

-- 5.4 DIM_RESTAURANT_SCD3
CREATE OR REPLACE TABLE DIM_RESTAURANT_SCD3 (
  RESTAURANT_ID        NUMBER,
  RESTAURANT_NAME      STRING,
  CURRENT_CUISINE      STRING,
  PREVIOUS_CUISINE     STRING,
  LAST_CHANGE_DATE     TIMESTAMP_NTZ,
  CITY                 STRING,
  AREA                 STRING
);

-- 5.5 DIM_DELIVERY_AGENT (SCD1)
CREATE OR REPLACE TABLE DIM_DELIVERY_AGENT (
  AGENT_ID           NUMBER,
  AGENT_NAME         STRING,
  PHONE              STRING,
  HIRE_DATE          TIMESTAMP_NTZ,
  CITY               STRING,
  VEHICLE_TYPE       STRING,
  STATUS             STRING
);

-- 5.6 DIM_PROMOTION (SCD1)
CREATE OR REPLACE TABLE DIM_PROMOTION (
  PROMO_ID           NUMBER,
  PROMO_CODE         STRING,
  DISCOUNT_PERCENT   NUMBER(5,2),
  START_DATE         TIMESTAMP_NTZ,
  END_DATE           TIMESTAMP_NTZ,
  MAX_DISCOUNT_AMT   NUMBER(10,2),
  TARGET_SEGMENT     STRING,
  TARGET_CITY        STRING
);

-- 5.7 FCT_ORDER_ENHANCED
CREATE OR REPLACE TABLE FCT_ORDER_ENHANCED (
  ORDER_ID             NUMBER,
  CUSTOMER_ID          NUMBER,
  RESTAURANT_ID        NUMBER,
  ORDER_DATE_KEY       NUMBER,
  ORDER_STATUS         STRING,
  PAYMENT_METHOD       STRING,
  PROMO_ID             NUMBER,
  ORDER_SUBTOTAL       NUMBER(12,2),
  ORDER_DISCOUNT       NUMBER(12,2),
  DELIVERY_FEE         NUMBER(10,2),
  TOTAL_AMOUNT         NUMBER(12,2),
  NET_FOOD_AMOUNT      NUMBER(12,2),
  DISCOUNT_PCT         FLOAT,
  SLA_BREACHED         NUMBER,
  PLATFORM_COMMISSION  NUMBER(12,2),
  RESTAURANT_PAYOUT    NUMBER(12,2),
  END_TO_END_MIN       NUMBER,
  IS_DELIVERED         NUMBER,
  IS_CANCELLED         NUMBER
);

-- 5.8 FCT_DELIVERY
CREATE OR REPLACE TABLE FCT_DELIVERY (
  TRIP_ID            NUMBER,
  ORDER_ID           NUMBER,
  AGENT_ID           NUMBER,
  PICKUP_TIME        TIMESTAMP_NTZ,
  DROP_TIME          TIMESTAMP_NTZ,
  DISTANCE_KM        NUMBER(10,2),
  ESTIMATED_TIME_MIN NUMBER,
  ACTUAL_TIME_MIN    NUMBER,
  SLA_BREACH_FLAG    NUMBER
);

-- 5.9 FCT_FEEDBACK
CREATE OR REPLACE TABLE FCT_FEEDBACK (
  FEEDBACK_ID        NUMBER,
  ORDER_ID           NUMBER,
  CUSTOMER_ID        NUMBER,
  RATING             NUMBER,
  COMMENT            STRING,
  SENTIMENT_SCORE    FLOAT,
  CREATED_AT         TIMESTAMP_NTZ
);

-- 5.10 FCT_CUSTOMER_EVENT
CREATE OR REPLACE TABLE FCT_CUSTOMER_EVENT (
  EVENT_ID        NUMBER,
  CUSTOMER_ID     NUMBER,
  EVENT_TYPE      STRING,
  EVENT_TS        TIMESTAMP_NTZ,
  SEARCH_QUERY    STRING,
  DEVICE_OS       STRING,
  APP_VERSION     STRING
);

---------------------------------------------------------------------------
-- 6. UTIL SCHEMA OBJECTS: RLS, MASKING, FUNCTIONS, AUDIT TABLE
---------------------------------------------------------------------------

USE ROLE ACCOUNTADMIN;
USE DATABASE ZOMATO_DWH;
USE SCHEMA UTIL;

-- 6.1 Row-level mapping table
CREATE OR REPLACE TABLE RLS_USER_ACCESS (
  USER_NAME    STRING,
  ACCESS_TYPE  STRING,      -- 'CITY' or 'RESTAURANT'
  CITY         STRING,
  RESTAURANT_ID NUMBER
);

-- 6.2 Masking policies for PII
CREATE OR REPLACE MASKING POLICY MP_EMAIL_MASK
AS (VAL STRING) RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('ZOMATO_PII_ADMIN_ROLE','ACCOUNTADMIN') THEN VAL
    ELSE REGEXP_REPLACE(VAL, '(^.).*(@.*$)', '\\1*****\\2')
  END;

CREATE OR REPLACE MASKING POLICY MP_PHONE_MASK
AS (VAL STRING) RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('ZOMATO_PII_ADMIN_ROLE','ACCOUNTADMIN') THEN VAL
    ELSE CONCAT('XXXXXX', RIGHT(VAL, 4))
  END;

-- 6.3 SLA bucket helper function
CREATE OR REPLACE FUNCTION FN_DELIVERY_SLA_BUCKET(
  ESTIMATED_TIME_MIN NUMBER,
  ACTUAL_TIME_MIN    NUMBER
)
RETURNS STRING
LANGUAGE SQL
AS
$$
CASE
  WHEN ACTUAL_TIME_MIN IS NULL THEN 'UNKNOWN'
  WHEN ACTUAL_TIME_MIN <= ESTIMATED_TIME_MIN THEN 'ON_TIME'
  WHEN ACTUAL_TIME_MIN <= ESTIMATED_TIME_MIN + 10 THEN 'SLIGHT_DELAY'
  ELSE 'HEAVY_DELAY'
END
$$;

-- 6.4 Simple audit log for procedures
CREATE OR REPLACE TABLE PROC_AUDIT_LOG (
  PROC_NAME   STRING,
  RUN_TS      TIMESTAMP_NTZ,
  STATUS      STRING,
  MESSAGE     STRING
);

---------------------------------------------------------------------------
-- 7. JS PROCEDURES
---------------------------------------------------------------------------

USE ROLE ACCOUNTADMIN;
USE DATABASE ZOMATO_DWH;
USE SCHEMA UTIL;

CREATE OR REPLACE PROCEDURE SP_LOAD_DIM_DATE()
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
try {
  // Start explicit transaction
  snowflake.execute({ sqlText: 'BEGIN' });

  // 1) Truncate target dimension
  snowflake.execute({
    sqlText: `TRUNCATE TABLE ZOMATO_DWH.INT.DIM_DATE`
  });

  // 2) Insert date rows (2019-01-01 to 2028-12-31)
  var insertSql = `
    INSERT INTO ZOMATO_DWH.INT.DIM_DATE (
      DATE_KEY,
      FULL_DATE,
      YEAR,
      MONTH,
      MONTH_SHORT,
      DAY_OF_MONTH,
      DAY_OF_WEEK,
      DAY_OF_WEEK_SHORT,
      WEEK_OF_YEAR
    )
    WITH date_range AS (
      SELECT
        DATEADD('day', SEQ4(), '2019-01-01'::DATE) AS d
      FROM TABLE(GENERATOR(ROWCOUNT => 365 * 10))   -- ~10 years
    )
    SELECT
      TO_NUMBER(TO_CHAR(d, 'YYYYMMDD'))  AS DATE_KEY,
      d                                  AS FULL_DATE,
      YEAR(d)                            AS YEAR,
      MONTH(d)                           AS MONTH,
      TO_CHAR(d, 'MON')                  AS MONTH_SHORT,
      DAY(d)                             AS DAY_OF_MONTH,
      DAYOFWEEK(d)                       AS DAY_OF_WEEK,
      TO_CHAR(d, 'DY')                   AS DAY_OF_WEEK_SHORT,
      WEEKOFYEAR(d)                      AS WEEK_OF_YEAR
    FROM date_range
    WHERE d <= '2028-12-31'::DATE
  `;

  snowflake.execute({ sqlText: insertSql });

  // 3) Commit
  snowflake.execute({ sqlText: 'COMMIT' });

  return 'SP_LOAD_DIM_DATE completed successfully';
} catch (err) {
  // Rollback on error
  try {
    snowflake.execute({ sqlText: 'ROLLBACK' });
  } catch (e2) {
    // ignore rollback failure
  }
  return 'ERROR in SP_LOAD_DIM_DATE: ' + err;
}
$$;
CALL UTIL.SP_LOAD_DIM_DATE();
SELECT COUNT(*) FROM INT.DIM_DATE

-- 7.2 DIM_CUSTOMER (SCD2)
CREATE OR REPLACE PROCEDURE SP_LOAD_DIM_CUSTOMER_SCD2()
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
  try {
    snowflake.execute({sqlText: 'BEGIN'});

    var sql_merge = `
      MERGE INTO INT.DIM_CUSTOMER d
      USING STG.V_CUSTOMER_STG s
        ON d.CUSTOMER_ID = s.CUSTOMER_ID
       AND d.IS_CURRENT = TRUE
       AND (
         NVL(d.CUSTOMER_NAME,'')      <> NVL(s.CUSTOMER_NAME,'') OR
         NVL(d.EMAIL,'')              <> NVL(s.EMAIL,'') OR
         NVL(d.PRIMARY_PHONE,'')      <> NVL(s.PRIMARY_PHONE,'') OR
         NVL(d.CITY,'')               <> NVL(s.CITY,'') OR
         NVL(d.AREA,'')               <> NVL(s.AREA,'') OR
         NVL(d.SEGMENT,'')            <> NVL(s.SEGMENT,'') OR
         NVL(d.IS_PRIME_MEMBER,FALSE) <> NVL(s.IS_PRIME_MEMBER,FALSE) OR
         NVL(d.STATUS,'')             <> NVL(s.STATUS,'')
       )
      WHEN MATCHED THEN UPDATE SET
        EFFECTIVE_TO = CURRENT_TIMESTAMP(),
        IS_CURRENT   = FALSE
    `;
    snowflake.execute({sqlText: sql_merge});

    var sql_insert = `
      INSERT INTO INT.DIM_CUSTOMER (
        CUSTOMER_ID, CUSTOMER_NAME, EMAIL, PRIMARY_PHONE,
        CITY, AREA, SEGMENT, IS_PRIME_MEMBER, STATUS,
        EFFECTIVE_FROM, EFFECTIVE_TO, IS_CURRENT, RECORD_SOURCE
      )
      SELECT
        s.CUSTOMER_ID,
        s.CUSTOMER_NAME,
        s.EMAIL,
        s.PRIMARY_PHONE,
        s.CITY,
        s.AREA,
        s.SEGMENT,
        s.IS_PRIME_MEMBER,
        s.STATUS,
        CURRENT_TIMESTAMP(),
        TO_TIMESTAMP_NTZ('9999-12-31'),
        TRUE,
        'STG.V_CUSTOMER_STG'
      FROM STG.V_CUSTOMER_STG s
      LEFT JOIN INT.DIM_CUSTOMER d
        ON d.CUSTOMER_ID = s.CUSTOMER_ID
       AND d.IS_CURRENT = TRUE
      WHERE d.CUSTOMER_ID IS NULL
    `;
    snowflake.execute({sqlText: sql_insert});

    snowflake.execute({sqlText: `
      INSERT INTO UTIL.PROC_AUDIT_LOG(PROC_NAME,RUN_TS,STATUS,MESSAGE)
      VALUES ('SP_LOAD_DIM_CUSTOMER_SCD2', CURRENT_TIMESTAMP(), 'SUCCESS', 'DIM_CUSTOMER SCD2 loaded')
    `});

    snowflake.execute({sqlText: 'COMMIT'});
    return 'DIM_CUSTOMER SCD2 loaded successfully';
  } catch (err) {
    try { snowflake.execute({sqlText: 'ROLLBACK'}); } catch (e2) {}
    snowflake.execute({
      sqlText: `
        INSERT INTO UTIL.PROC_AUDIT_LOG(PROC_NAME,RUN_TS,STATUS,MESSAGE)
        VALUES ('SP_LOAD_DIM_CUSTOMER_SCD2', CURRENT_TIMESTAMP(), 'ERROR', :msg)
      `,
      binds: { msg: err.toString() }
    });
    return 'ERROR in SP_LOAD_DIM_CUSTOMER_SCD2: ' + err;
  }
$$;

CALL UTIL.SP_LOAD_DIM_CUSTOMER_SCD2();

-- 7.3 DIM_RESTAURANT (SCD2)
CREATE OR REPLACE PROCEDURE SP_LOAD_DIM_RESTAURANT_SCD2()
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
  try {
    snowflake.execute({sqlText: 'BEGIN'});

    var sql_merge = `
      MERGE INTO INT.DIM_RESTAURANT d
      USING STG.V_RESTAURANT_STG s
        ON d.RESTAURANT_ID = s.RESTAURANT_ID
       AND d.IS_CURRENT = TRUE
       AND (
         NVL(d.RESTAURANT_NAME,'')    <> NVL(s.RESTAURANT_NAME,'') OR
         NVL(d.CUISINE_PRIMARY,'')    <> NVL(s.CUISINE_PRIMARY,'') OR
         NVL(d.CUISINE_SECONDARY,'')  <> NVL(s.CUISINE_SECONDARY,'') OR
         NVL(d.CITY,'')               <> NVL(s.CITY,'') OR
         NVL(d.AREA,'')               <> NVL(s.AREA,'') OR
         NVL(d.AVG_RATING,0)          <> NVL(s.AVG_RATING,0) OR
         NVL(d.COMMISSION_RATE,0)     <> NVL(s.COMMISSION_RATE,0) OR
         NVL(d.IS_ACTIVE,FALSE)       <> NVL(s.IS_ACTIVE,FALSE)
       )
      WHEN MATCHED THEN UPDATE SET
        EFFECTIVE_TO = CURRENT_TIMESTAMP(),
        IS_CURRENT   = FALSE
    `;
    snowflake.execute({sqlText: sql_merge});

    var sql_insert = `
      INSERT INTO INT.DIM_RESTAURANT (
        RESTAURANT_ID, RESTAURANT_NAME,
        CUISINE_PRIMARY, CUISINE_SECONDARY,
        CITY, AREA,
        AVG_RATING, COMMISSION_RATE, IS_ACTIVE, ONBOARDED_AT,
        EFFECTIVE_FROM, EFFECTIVE_TO, IS_CURRENT, RECORD_SOURCE
      )
      SELECT
        s.RESTAURANT_ID,
        s.RESTAURANT_NAME,
        s.CUISINE_PRIMARY,
        s.CUISINE_SECONDARY,
        s.CITY,
        s.AREA,
        s.AVG_RATING,
        s.COMMISSION_RATE,
        s.IS_ACTIVE,
        s.ONBOARDED_AT,
        CURRENT_TIMESTAMP(),
        TO_TIMESTAMP_NTZ('9999-12-31'),
        TRUE,
        'STG.V_RESTAURANT_STG'
      FROM STG.V_RESTAURANT_STG s
      LEFT JOIN INT.DIM_RESTAURANT d
        ON d.RESTAURANT_ID = s.RESTAURANT_ID
       AND d.IS_CURRENT = TRUE
      WHERE d.RESTAURANT_ID IS NULL
    `;
    snowflake.execute({sqlText: sql_insert});

    snowflake.execute({sqlText: `
      INSERT INTO UTIL.PROC_AUDIT_LOG(PROC_NAME,RUN_TS,STATUS,MESSAGE)
      VALUES ('SP_LOAD_DIM_RESTAURANT_SCD2', CURRENT_TIMESTAMP(), 'SUCCESS', 'DIM_RESTAURANT SCD2 loaded')
    `});

    snowflake.execute({sqlText: 'COMMIT'});
    return 'DIM_RESTAURANT SCD2 loaded successfully';
  } catch (err) {
    try { snowflake.execute({sqlText: 'ROLLBACK'}); } catch (e2) {}
    snowflake.execute({
      sqlText: `
        INSERT INTO UTIL.PROC_AUDIT_LOG(PROC_NAME,RUN_TS,STATUS,MESSAGE)
        VALUES ('SP_LOAD_DIM_RESTAURANT_SCD2', CURRENT_TIMESTAMP(), 'ERROR', :msg)
      `,
      binds: { msg: err.toString() }
    });
    return 'ERROR in SP_LOAD_DIM_RESTAURANT_SCD2: ' + err;
  }
$$;

CALL UTIL.SP_LOAD_DIM_RESTAURANT_SCD2();
SELECT COUNT(*) FROM INT.DIM_RESTAURANT;

-- 7.4 DIM_RESTAURANT_SCD3
CREATE OR REPLACE PROCEDURE SP_LOAD_DIM_RESTAURANT_SCD3()
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
  try {
    snowflake.execute({sqlText: 'BEGIN'});

    var sql = `
      MERGE INTO INT.DIM_RESTAURANT_SCD3 d
      USING STG.V_RESTAURANT_STG s
        ON d.RESTAURANT_ID = s.RESTAURANT_ID
      WHEN MATCHED AND NVL(d.CURRENT_CUISINE,'') <> NVL(s.CUISINE_PRIMARY,'') THEN
        UPDATE SET
          PREVIOUS_CUISINE = d.CURRENT_CUISINE,
          CURRENT_CUISINE  = s.CUISINE_PRIMARY,
          LAST_CHANGE_DATE = CURRENT_TIMESTAMP(),
          CITY             = s.CITY,
          AREA             = s.AREA
      WHEN NOT MATCHED THEN
        INSERT (RESTAURANT_ID, RESTAURANT_NAME, CURRENT_CUISINE, PREVIOUS_CUISINE, LAST_CHANGE_DATE, CITY, AREA)
        VALUES (
          s.RESTAURANT_ID,
          s.RESTAURANT_NAME,
          s.CUISINE_PRIMARY,
          NULL,
          CURRENT_TIMESTAMP(),
          s.CITY,
          s.AREA
        )
    `;
    snowflake.execute({sqlText: sql});

    snowflake.execute({sqlText: `
      INSERT INTO UTIL.PROC_AUDIT_LOG(PROC_NAME,RUN_TS,STATUS,MESSAGE)
      VALUES ('SP_LOAD_DIM_RESTAURANT_SCD3', CURRENT_TIMESTAMP(), 'SUCCESS', 'DIM_RESTAURANT_SCD3 loaded')
    `});

    snowflake.execute({sqlText: 'COMMIT'});
    return 'DIM_RESTAURANT_SCD3 loaded successfully';
  } catch (err) {
    try { snowflake.execute({sqlText: 'ROLLBACK'}); } catch (e2) {}
    snowflake.execute({
      sqlText: `
        INSERT INTO UTIL.PROC_AUDIT_LOG(PROC_NAME,RUN_TS,STATUS,MESSAGE)
        VALUES ('SP_LOAD_DIM_RESTAURANT_SCD3', CURRENT_TIMESTAMP(), 'ERROR', :msg)
      `,
      binds: { msg: err.toString() }
    });
    return 'ERROR in SP_LOAD_DIM_RESTAURANT_SCD3: ' + err;
  }
$$;
CALL UTIL.SP_LOAD_DIM_RESTAURANT_SCD3();


-- 7.5 DIM_DELIVERY_AGENT (SCD1 via MERGE)
CREATE OR REPLACE PROCEDURE SP_LOAD_DIM_DELIVERY_AGENT()
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
  try {
    snowflake.execute({sqlText: 'BEGIN'});

    var sql = `
      MERGE INTO INT.DIM_DELIVERY_AGENT d
      USING STG.V_DELIVERY_AGENT_STG s
        ON d.AGENT_ID = s.AGENT_ID
      WHEN MATCHED THEN UPDATE SET
        AGENT_NAME   = s.AGENT_NAME,
        PHONE        = s.PHONE,
        HIRE_DATE    = s.HIRE_DATE,
        CITY         = s.CITY,
        VEHICLE_TYPE = s.VEHICLE_TYPE,
        STATUS       = s.STATUS
      WHEN NOT MATCHED THEN
        INSERT (AGENT_ID, AGENT_NAME, PHONE, HIRE_DATE, CITY, VEHICLE_TYPE, STATUS)
        VALUES (s.AGENT_ID, s.AGENT_NAME, s.PHONE, s.HIRE_DATE, s.CITY, s.VEHICLE_TYPE, s.STATUS)
    `;
    snowflake.execute({sqlText: sql});

    snowflake.execute({sqlText: `
      INSERT INTO UTIL.PROC_AUDIT_LOG(PROC_NAME,RUN_TS,STATUS,MESSAGE)
      VALUES ('SP_LOAD_DIM_DELIVERY_AGENT', CURRENT_TIMESTAMP(), 'SUCCESS', 'DIM_DELIVERY_AGENT loaded')
    `});

    snowflake.execute({sqlText: 'COMMIT'});
    return 'DIM_DELIVERY_AGENT (SCD1) loaded successfully';
  } catch (err) {
    try { snowflake.execute({sqlText: 'ROLLBACK'}); } catch (e2) {}
    snowflake.execute({
      sqlText: `
        INSERT INTO UTIL.PROC_AUDIT_LOG(PROC_NAME,RUN_TS,STATUS,MESSAGE)
        VALUES ('SP_LOAD_DIM_DELIVERY_AGENT', CURRENT_TIMESTAMP(), 'ERROR', :msg)
      `,
      binds: { msg: err.toString() }
    });
    return 'ERROR in SP_LOAD_DIM_DELIVERY_AGENT: ' + err;
  }
$$;
CALL UTIL.SP_LOAD_DIM_DELIVERY_AGENT();

-- 7.6 DIM_PROMOTION (SCD1 via MERGE)
CREATE OR REPLACE PROCEDURE SP_LOAD_DIM_PROMOTION()
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
  try {
    snowflake.execute({sqlText: 'BEGIN'});

    var sql = `
      MERGE INTO INT.DIM_PROMOTION d
      USING STG.V_PROMOTION_STG s
        ON d.PROMO_ID = s.PROMO_ID
      WHEN MATCHED THEN UPDATE SET
        PROMO_CODE       = s.PROMO_CODE,
        DISCOUNT_PERCENT = s.DISCOUNT_PERCENT,
        START_DATE       = s.START_DATE,
        END_DATE         = s.END_DATE,
        MAX_DISCOUNT_AMT = s.MAX_DISCOUNT_AMT,
        TARGET_SEGMENT   = s.TARGET_SEGMENT,
        TARGET_CITY      = s.TARGET_CITY
      WHEN NOT MATCHED THEN
        INSERT (PROMO_ID, PROMO_CODE, DISCOUNT_PERCENT, START_DATE, END_DATE,
                MAX_DISCOUNT_AMT, TARGET_SEGMENT, TARGET_CITY)
        VALUES (s.PROMO_ID, s.PROMO_CODE, s.DISCOUNT_PERCENT, s.START_DATE, s.END_DATE,
                s.MAX_DISCOUNT_AMT, s.TARGET_SEGMENT, s.TARGET_CITY)
    `;
    snowflake.execute({sqlText: sql});

    snowflake.execute({sqlText: `
      INSERT INTO UTIL.PROC_AUDIT_LOG(PROC_NAME,RUN_TS,STATUS,MESSAGE)
      VALUES ('SP_LOAD_DIM_PROMOTION', CURRENT_TIMESTAMP(), 'SUCCESS', 'DIM_PROMOTION loaded')
    `});

    snowflake.execute({sqlText: 'COMMIT'});
    return 'DIM_PROMOTION (SCD1) loaded successfully';
  } catch (err) {
    try { snowflake.execute({sqlText: 'ROLLBACK'}); } catch (e2) {}
    snowflake.execute({
      sqlText: `
        INSERT INTO UTIL.PROC_AUDIT_LOG(PROC_NAME,RUN_TS,STATUS,MESSAGE)
        VALUES ('SP_LOAD_DIM_PROMOTION', CURRENT_TIMESTAMP(), 'ERROR', :msg)
      `,
      binds: { msg: err.toString() }
    });
    return 'ERROR in SP_LOAD_DIM_PROMOTION: ' + err;
  }
$$;
CALL UTIL.SP_LOAD_DIM_PROMOTION();

-- 7.7 FCT_ORDER_ENHANCED
CREATE OR REPLACE PROCEDURE SP_LOAD_FCT_ORDER_ENHANCED()
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
  try {
    snowflake.execute({sqlText: 'BEGIN'});

    snowflake.execute({sqlText: 'TRUNCATE TABLE INT.FCT_ORDER_ENHANCED'});

    var sql_insert = `
      INSERT INTO INT.FCT_ORDER_ENHANCED (
        ORDER_ID, CUSTOMER_ID, RESTAURANT_ID, ORDER_DATE_KEY,
        ORDER_STATUS, PAYMENT_METHOD, PROMO_ID,
        ORDER_SUBTOTAL, ORDER_DISCOUNT, DELIVERY_FEE, TOTAL_AMOUNT,
        NET_FOOD_AMOUNT, DISCOUNT_PCT, SLA_BREACHED,
        PLATFORM_COMMISSION, RESTAURANT_PAYOUT,
        END_TO_END_MIN, IS_DELIVERED, IS_CANCELLED
      )
      SELECT
        oh.ORDER_ID,
        oh.CUSTOMER_ID,
        oh.RESTAURANT_ID,
        dd.DATE_KEY AS ORDER_DATE_KEY,
        oh.ORDER_STATUS,
        oh.PAYMENT_METHOD,
        oh.PROMO_ID,
        oh.ORDER_SUBTOTAL,
        oh.ORDER_DISCOUNT,
        oh.DELIVERY_FEE,
        oh.TOTAL_AMOUNT,
        (oh.ORDER_SUBTOTAL - oh.ORDER_DISCOUNT)                          AS NET_FOOD_AMOUNT,
        CASE WHEN oh.ORDER_SUBTOTAL > 0 THEN oh.ORDER_DISCOUNT/oh.ORDER_SUBTOTAL ELSE 0 END AS DISCOUNT_PCT,
        CASE WHEN dt.SLA_BREACH_FLAG = 1 THEN 1 ELSE 0 END              AS SLA_BREACHED,
        (oh.TOTAL_AMOUNT * r.COMMISSION_RATE)                            AS PLATFORM_COMMISSION,
        (oh.TOTAL_AMOUNT - (oh.TOTAL_AMOUNT * r.COMMISSION_RATE))       AS RESTAURANT_PAYOUT,
        DATEDIFF('minute', oh.ORDER_CREATED_AT, oh.ACTUAL_DELIVERY_AT)  AS END_TO_END_MIN,
        CASE WHEN oh.ORDER_STATUS = 'DELIVERED' THEN 1 ELSE 0 END       AS IS_DELIVERED,
        CASE WHEN oh.ORDER_STATUS = 'CANCELLED' THEN 1 ELSE 0 END       AS IS_CANCELLED
      FROM STG.V_ORDER_HEADER_STG oh
      LEFT JOIN INT.DIM_DATE dd
        ON dd.FULL_DATE = CAST(oh.ORDER_CREATED_AT AS DATE)
      LEFT JOIN STG.V_DELIVERY_TRIP_STG dt
        ON oh.ORDER_ID = dt.ORDER_ID
      LEFT JOIN STG.V_RESTAURANT_STG r
        ON oh.RESTAURANT_ID = r.RESTAURANT_ID
    `;
    snowflake.execute({sqlText: sql_insert});

    snowflake.execute({sqlText: `
      INSERT INTO UTIL.PROC_AUDIT_LOG(PROC_NAME,RUN_TS,STATUS,MESSAGE)
      VALUES ('SP_LOAD_FCT_ORDER_ENHANCED', CURRENT_TIMESTAMP(), 'SUCCESS', 'FCT_ORDER_ENHANCED loaded')
    `});

    snowflake.execute({sqlText: 'COMMIT'});
    return 'FCT_ORDER_ENHANCED loaded successfully';
  } catch (err) {
    try { snowflake.execute({sqlText: 'ROLLBACK'}); } catch (e2) {}
    snowflake.execute({
      sqlText: `
        INSERT INTO UTIL.PROC_AUDIT_LOG(PROC_NAME,RUN_TS,STATUS,MESSAGE)
        VALUES ('SP_LOAD_FCT_ORDER_ENHANCED', CURRENT_TIMESTAMP(), 'ERROR', :msg)
      `,
      binds: { msg: err.toString() }
    });
    return 'ERROR in SP_LOAD_FCT_ORDER_ENHANCED: ' + err;
  }
$$;
CALL UTIL.SP_LOAD_FCT_ORDER_ENHANCED();

-- 7.8 FCT_DELIVERY
CREATE OR REPLACE PROCEDURE SP_LOAD_FCT_DELIVERY()
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
  try {
    snowflake.execute({sqlText: 'BEGIN'});

    snowflake.execute({sqlText: 'TRUNCATE TABLE INT.FCT_DELIVERY'});

    var sql = `
      INSERT INTO INT.FCT_DELIVERY
      SELECT
        TRIP_ID,
        ORDER_ID,
        AGENT_ID,
        PICKUP_TIME,
        DROP_TIME,
        DISTANCE_KM,
        ESTIMATED_TIME_MIN,
        ACTUAL_TIME_MIN,
        SLA_BREACH_FLAG
      FROM STG.V_DELIVERY_TRIP_STG
    `;
    snowflake.execute({sqlText: sql});

    snowflake.execute({sqlText: `
      INSERT INTO UTIL.PROC_AUDIT_LOG(PROC_NAME,RUN_TS,STATUS,MESSAGE)
      VALUES ('SP_LOAD_FCT_DELIVERY', CURRENT_TIMESTAMP(), 'SUCCESS', 'FCT_DELIVERY loaded')
    `});

    snowflake.execute({sqlText: 'COMMIT'});
    return 'FCT_DELIVERY loaded successfully';
  } catch (err) {
    try { snowflake.execute({sqlText: 'ROLLBACK'}); } catch (e2) {}
    snowflake.execute({
      sqlText: `
        INSERT INTO UTIL.PROC_AUDIT_LOG(PROC_NAME,RUN_TS,STATUS,MESSAGE)
        VALUES ('SP_LOAD_FCT_DELIVERY', CURRENT_TIMESTAMP(), 'ERROR', :msg)
      `,
      binds: { msg: err.toString() }
    });
    return 'ERROR in SP_LOAD_FCT_DELIVERY: ' + err;
  }
$$;
CALL UTIL.SP_LOAD_FCT_DELIVERY();

-- 7.9 FCT_FEEDBACK
CREATE OR REPLACE PROCEDURE SP_LOAD_FCT_FEEDBACK()
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
  try {
    snowflake.execute({sqlText: 'BEGIN'});

    snowflake.execute({sqlText: 'TRUNCATE TABLE INT.FCT_FEEDBACK'});

    var sql = `
      INSERT INTO INT.FCT_FEEDBACK
      SELECT
        FEEDBACK_ID,
        ORDER_ID,
        CUSTOMER_ID,
        RATING,
        COMMENT,
        SENTIMENT_SCORE,
        CREATED_AT
      FROM STG.V_CUSTOMER_FEEDBACK_STG
    `;
    snowflake.execute({sqlText: sql});

    snowflake.execute({sqlText: `
      INSERT INTO UTIL.PROC_AUDIT_LOG(PROC_NAME,RUN_TS,STATUS,MESSAGE)
      VALUES ('SP_LOAD_FCT_FEEDBACK', CURRENT_TIMESTAMP(), 'SUCCESS', 'FCT_FEEDBACK loaded')
    `});

    snowflake.execute({sqlText: 'COMMIT'});
    return 'FCT_FEEDBACK loaded successfully';
  } catch (err) {
    try { snowflake.execute({sqlText: 'ROLLBACK'}); } catch (e2) {}
    snowflake.execute({
      sqlText: `
        INSERT INTO UTIL.PROC_AUDIT_LOG(PROC_NAME,RUN_TS,STATUS,MESSAGE)
        VALUES ('SP_LOAD_FCT_FEEDBACK', CURRENT_TIMESTAMP(), 'ERROR', :msg)
      `,
      binds: { msg: err.toString() }
    });
    return 'ERROR in SP_LOAD_FCT_FEEDBACK: ' + err;
  }
$$;
CALL UTIL.SP_LOAD_FCT_FEEDBACK();

-- 7.10 FCT_CUSTOMER_EVENT
CREATE OR REPLACE PROCEDURE SP_LOAD_FCT_CUSTOMER_EVENT()
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
  try {
    snowflake.execute({sqlText: 'BEGIN'});

    snowflake.execute({sqlText: 'TRUNCATE TABLE INT.FCT_CUSTOMER_EVENT'});

    var sql = `
      INSERT INTO INT.FCT_CUSTOMER_EVENT
      SELECT
        EVENT_ID,
        CUSTOMER_ID,
        EVENT_TYPE,
        EVENT_TS,
        SEARCH_QUERY,
        DEVICE_OS,
        APP_VERSION
      FROM STG.V_CUSTOMER_EVENTS_STG
    `;
    snowflake.execute({sqlText: sql});

    snowflake.execute({sqlText: `
      INSERT INTO UTIL.PROC_AUDIT_LOG(PROC_NAME,RUN_TS,STATUS,MESSAGE)
      VALUES ('SP_LOAD_FCT_CUSTOMER_EVENT', CURRENT_TIMESTAMP(), 'SUCCESS', 'FCT_CUSTOMER_EVENT loaded')
    `});

    snowflake.execute({sqlText: 'COMMIT'});
    return 'FCT_CUSTOMER_EVENT loaded successfully';
  } catch (err) {
    try { snowflake.execute({sqlText: 'ROLLBACK'}); } catch (e2) {}
    snowflake.execute({
      sqlText: `
        INSERT INTO UTIL.PROC_AUDIT_LOG(PROC_NAME,RUN_TS,STATUS,MESSAGE)
        VALUES ('SP_LOAD_FCT_CUSTOMER_EVENT', CURRENT_TIMESTAMP(), 'ERROR', :msg)
      `,
      binds: { msg: err.toString() }
    });
    return 'ERROR in SP_LOAD_FCT_CUSTOMER_EVENT: ' + err;
  }
$$;
CALL UTIL.SP_LOAD_FCT_CUSTOMER_EVENT();
SELECT COUNT(*) FROM INT.FCT_CUSTOMER_EVENT;

---------------------------------------------------------------------------
-- 8. INITIAL DATA LOAD CALLS (CAN BE COMMENTED AFTER FIRST RUN)
---------------------------------------------------------------------------

USE ROLE ACCOUNTADMIN;

CALL UTIL.SP_LOAD_DIM_DATE();
CALL UTIL.SP_LOAD_DIM_CUSTOMER_SCD2();
CALL UTIL.SP_LOAD_DIM_RESTAURANT_SCD2();
CALL UTIL.SP_LOAD_DIM_RESTAURANT_SCD3();
CALL UTIL.SP_LOAD_DIM_DELIVERY_AGENT();
CALL UTIL.SP_LOAD_DIM_PROMOTION();
CALL UTIL.SP_LOAD_FCT_ORDER_ENHANCED();
CALL UTIL.SP_LOAD_FCT_DELIVERY();
CALL UTIL.SP_LOAD_FCT_FEEDBACK();
CALL UTIL.SP_LOAD_FCT_CUSTOMER_EVENT();

---------------------------------------------------------------------------
-- 9. TASKS FOR AUTOMATED REFRESH
---------------------------------------------------------------------------

USE ROLE ACCOUNTADMIN;
USE DATABASE ZOMATO_DWH;
USE SCHEMA INT;

CREATE OR REPLACE TASK TASK_LOAD_DIMS
  WAREHOUSE = WH_ZOMATO
  SCHEDULE  = 'USING CRON 0 * * * * Asia/Kolkata'
AS
BEGIN
  CALL UTIL.SP_LOAD_DIM_CUSTOMER_SCD2();
  CALL UTIL.SP_LOAD_DIM_RESTAURANT_SCD2();
  CALL UTIL.SP_LOAD_DIM_RESTAURANT_SCD3();
  CALL UTIL.SP_LOAD_DIM_DELIVERY_AGENT();
  CALL UTIL.SP_LOAD_DIM_PROMOTION();
END;

CREATE OR REPLACE TASK TASK_LOAD_FACTS
  WAREHOUSE = WH_ZOMATO
  SCHEDULE  = 'USING CRON 15 * * * * Asia/Kolkata'
AS
BEGIN
  CALL UTIL.SP_LOAD_FCT_ORDER_ENHANCED();
  CALL UTIL.SP_LOAD_FCT_DELIVERY();
  CALL UTIL.SP_LOAD_FCT_FEEDBACK();
  CALL UTIL.SP_LOAD_FCT_CUSTOMER_EVENT();
END;

ALTER TASK TASK_LOAD_DIMS  RESUME;
ALTER TASK TASK_LOAD_FACTS RESUME;

---------------------------------------------------------------------------
-- 10. APPLY MASKING POLICIES ON DIM_CUSTOMER
---------------------------------------------------------------------------

USE ROLE ACCOUNTADMIN;
USE DATABASE ZOMATO_DWH;
USE SCHEMA INT;

ALTER TABLE DIM_CUSTOMER
  MODIFY COLUMN EMAIL SET MASKING POLICY UTIL.MP_EMAIL_MASK;

ALTER TABLE DIM_CUSTOMER
  MODIFY COLUMN PRIMARY_PHONE SET MASKING POLICY UTIL.MP_PHONE_MASK;

---------------------------------------------------------------------------
-- 11. MARTS LAYER: KPI VIEWS
---------------------------------------------------------------------------

USE ROLE ACCOUNTADMIN;
USE DATABASE ZOMATO_DWH;
USE SCHEMA MARTS;

-- 11.1 V_ORDER_ENRICHED
SELECT * FROM INT.DIM_DATE LIMIT 10;
SELECT * FROM V_ORDER_ENRICHED LIMIT 10;
SELECT * FROM INT.FCT_ORDER_ENHANCED LIMIT 10;
CREATE OR REPLACE VIEW V_ORDER_ENRICHED AS
SELECT
  fo.ORDER_ID,
  fo.ORDER_DATE_KEY,
  dd.FULL_DATE,
  fo.TOTAL_AMOUNT,
  fo.ORDER_SUBTOTAL,
  fo.ORDER_DISCOUNT,
  fo.DELIVERY_FEE,
  fo.NET_FOOD_AMOUNT,
  fo.DISCOUNT_PCT,
  fo.SLA_BREACHED,
  fo.PLATFORM_COMMISSION,
  fo.RESTAURANT_PAYOUT,
  fo.END_TO_END_MIN,
  fo.ORDER_STATUS,
  fo.PAYMENT_METHOD,
  fo.PROMO_ID,
  fo.IS_DELIVERED,
  fo.IS_CANCELLED,
  c.CUSTOMER_ID,
  c.CUSTOMER_NAME,
  c.CITY         AS CUSTOMER_CITY,
  c.AREA         AS CUSTOMER_AREA,
  c.SEGMENT      AS CUSTOMER_SEGMENT,
  r.RESTAURANT_ID,
  r.RESTAURANT_NAME,
  r.CITY         AS RESTAURANT_CITY,
  r.AREA         AS RESTAURANT_AREA,
  r.CUISINE_PRIMARY,
  r.COMMISSION_RATE,
  dtrip.DISTANCE_KM,
  dtrip.SLA_BREACH_FLAG,
  fb.RATING,
  fb.SENTIMENT_SCORE
FROM INT.FCT_ORDER_ENHANCED fo
LEFT JOIN INT.DIM_DATE dd
  ON fo.ORDER_DATE_KEY = dd.DATE_KEY
LEFT JOIN INT.DIM_CUSTOMER c
  ON fo.CUSTOMER_ID = c.CUSTOMER_ID
 AND c.IS_CURRENT = TRUE
LEFT JOIN INT.DIM_RESTAURANT r
  ON fo.RESTAURANT_ID = r.RESTAURANT_ID
 AND r.IS_CURRENT = TRUE
LEFT JOIN INT.FCT_DELIVERY dtrip
  ON fo.ORDER_ID = dtrip.ORDER_ID
LEFT JOIN INT.FCT_FEEDBACK fb
  ON fo.ORDER_ID = fb.ORDER_ID;

-- 11.2 V_RESTAURANT_ADVANCED_KPI
CREATE OR REPLACE VIEW V_RESTAURANT_ADVANCED_KPI AS
WITH DAILY AS (
  SELECT
    RESTAURANT_ID,
    RESTAURANT_NAME,
    RESTAURANT_CITY,
    CUISINE_PRIMARY,                     -- 🔹 add cuisine as a dimension
    FULL_DATE,
    SUM(TOTAL_AMOUNT)           AS GMV,
    SUM(IS_DELIVERED)           AS DELIVERED_ORDERS,
    SUM(IS_CANCELLED)           AS CANCELLED_ORDERS,
    SUM(RESTAURANT_PAYOUT)      AS RESTAURANT_PAYOUT,
    SUM(PLATFORM_COMMISSION)    AS PLATFORM_COMMISSION,
    AVG(SLA_BREACHED)           AS SLA_BREACH_RATE,
    AVG(RATING)                 AS AVG_RATING,
    AVG(END_TO_END_MIN)         AS AVG_DELIVERY_TIME_MIN
  FROM MARTS.V_ORDER_ENRICHED
  GROUP BY
    RESTAURANT_ID,
    RESTAURANT_NAME,
    RESTAURANT_CITY,
    CUISINE_PRIMARY,
    FULL_DATE
),
WINDOWED AS (
  SELECT
    RESTAURANT_ID,
    RESTAURANT_NAME,
    RESTAURANT_CITY,
    CUISINE_PRIMARY,
    FULL_DATE,
    GMV,
    DELIVERED_ORDERS,
    CANCELLED_ORDERS,
    RESTAURANT_PAYOUT,
    PLATFORM_COMMISSION,
    SLA_BREACH_RATE,
    AVG_RATING,
    AVG_DELIVERY_TIME_MIN,
    -- rolling windows
    SUM(GMV)  OVER (
      PARTITION BY RESTAURANT_ID
      ORDER BY FULL_DATE
      ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS GMV_7D,
    SUM(GMV)  OVER (
      PARTITION BY RESTAURANT_ID
      ORDER BY FULL_DATE
      ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS GMV_30D
  FROM DAILY
)
SELECT
  *,
  LAG(GMV_7D, 7) OVER (
    PARTITION BY RESTAURANT_ID
    ORDER BY FULL_DATE
  ) AS GMV_7D_PREV,
  CASE
    WHEN LAG(GMV_7D, 7) OVER (PARTITION BY RESTAURANT_ID ORDER BY FULL_DATE) IS NULL
      OR LAG(GMV_7D, 7) OVER (PARTITION BY RESTAURANT_ID ORDER BY FULL_DATE) = 0
    THEN NULL
    ELSE
      (GMV_7D - LAG(GMV_7D, 7) OVER (PARTITION BY RESTAURANT_ID ORDER BY FULL_DATE))
      / LAG(GMV_7D, 7) OVER (PARTITION BY RESTAURANT_ID ORDER BY FULL_DATE)
  END AS GMV_7D_WOW_GROWTH
FROM WINDOWED;


-- 11.3 V_AGENT_EFFICIENCY_KPI
CREATE OR REPLACE VIEW V_AGENT_EFFICIENCY_KPI AS
SELECT
  da.AGENT_ID,
  da.AGENT_NAME,
  da.CITY AS AGENT_CITY,
  COUNT(DISTINCT d.ORDER_ID)                        AS ORDERS_DELIVERED,
  AVG(d.DISTANCE_KM)                                AS AVG_DISTANCE_PER_ORDER,
  AVG(d.ACTUAL_TIME_MIN)                            AS AVG_DELIVERY_TIME_MIN,
  AVG(d.SLA_BREACH_FLAG)                            AS SLA_BREACH_RATE,
  COUNT(DISTINCT DATE_TRUNC('day', d.PICKUP_TIME))  AS DAYS_WORKED,
  CASE WHEN COUNT(DISTINCT DATE_TRUNC('day', d.PICKUP_TIME)) > 0
       THEN COUNT(DISTINCT d.ORDER_ID) / COUNT(DISTINCT DATE_TRUNC('day', d.PICKUP_TIME))
       ELSE NULL
  END                                               AS AVG_ORDERS_PER_DAY
FROM INT.FCT_DELIVERY d
JOIN INT.DIM_DELIVERY_AGENT da
  ON d.AGENT_ID = da.AGENT_ID
GROUP BY da.AGENT_ID, da.AGENT_NAME, da.CITY;

-- 11.4 V_CITY_SEARCH_TO_ORDER
CREATE OR REPLACE VIEW V_CITY_SEARCH_TO_ORDER AS
WITH SEARCHES AS (
  SELECT
    CUSTOMER_ID,
    DATE_TRUNC('day', EVENT_TS) AS DT,
    COUNT(*) AS SEARCH_COUNT
  FROM INT.FCT_CUSTOMER_EVENT
  WHERE EVENT_TYPE = 'SEARCH'
  GROUP BY CUSTOMER_ID, DATE_TRUNC('day', EVENT_TS)
),
ORDERS AS (
  SELECT
    CUSTOMER_ID,
    DATE_TRUNC('day', FULL_DATE) AS DT,
    COUNT(*) AS ORDER_COUNT
  FROM MARTS.V_ORDER_ENRICHED
  WHERE IS_DELIVERED = 1
  GROUP BY CUSTOMER_ID, DATE_TRUNC('day', FULL_DATE)
)
SELECT
  c.CITY AS CUSTOMER_CITY,          -- 🔹 FIXED: use c.CITY, not c.CUSTOMER_CITY
  s.DT,
  SUM(s.SEARCH_COUNT) AS TOTAL_SEARCHES,
  SUM(o.ORDER_COUNT)  AS TOTAL_ORDERS,
  CASE WHEN SUM(s.SEARCH_COUNT) > 0
       THEN SUM(o.ORDER_COUNT) / SUM(s.SEARCH_COUNT)
       ELSE NULL
  END AS SEARCH_TO_ORDER_RATIO
FROM SEARCHES s
JOIN ORDERS o
  ON s.CUSTOMER_ID = o.CUSTOMER_ID
 AND s.DT = o.DT
JOIN INT.DIM_CUSTOMER c
  ON s.CUSTOMER_ID = c.CUSTOMER_ID
 AND c.IS_CURRENT = TRUE
GROUP BY c.CITY, s.DT;

-- 11.5 V_RESTAURANT_FEEDBACK_KPI
CREATE OR REPLACE VIEW V_RESTAURANT_FEEDBACK_KPI AS
SELECT
  RESTAURANT_ID,
  RESTAURANT_NAME,
  RESTAURANT_CITY,
  CUISINE_PRIMARY,
  
  COUNT(*)                              AS TOTAL_ORDERS,
  SUM(IS_DELIVERED)                     AS DELIVERED_ORDERS,
  SUM(IS_CANCELLED)                     AS CANCELLED_ORDERS,
  CASE 
    WHEN COUNT(*) > 0 
      THEN SUM(IS_CANCELLED) / COUNT(*)::FLOAT 
    ELSE 0 
  END                                   AS CANCEL_RATE,

  AVG(RATING)                           AS AVG_CUSTOMER_RATING,
  AVG(SENTIMENT_SCORE)                  AS AVG_SENTIMENT_SCORE,
  SUM(
    CASE 
      WHEN RATING IS NOT NULL AND RATING <= 3 THEN 1 
      ELSE 0 
    END
  )                                     AS LOW_RATING_ORDERS,
  CASE 
    WHEN SUM(IS_DELIVERED) > 0 THEN 
      SUM(
        CASE 
          WHEN RATING IS NOT NULL 
           AND RATING <= 3 
           AND IS_DELIVERED = 1 
          THEN 1 ELSE 0 
        END
      ) 
      / SUM(IS_DELIVERED)::FLOAT
    ELSE NULL 
  END                                   AS LOW_RATING_DELIVERED_PCT,

  AVG(END_TO_END_MIN)                   AS AVG_END_TO_END_MIN,
  AVG(DISTANCE_KM)                      AS AVG_DISTANCE_KM,
  AVG(SLA_BREACH_FLAG)                  AS SLA_BREACH_RATE
FROM MARTS.V_ORDER_ENRICHED
GROUP BY
  RESTAURANT_ID,
  RESTAURANT_NAME,
  RESTAURANT_CITY,
  CUISINE_PRIMARY;

-- 11.6 V_CUSTOMER_EXPERIENCE_KPI
CREATE OR REPLACE VIEW V_CUSTOMER_EXPERIENCE_KPI AS
SELECT
  CUSTOMER_ID,
  CUSTOMER_NAME,
  CUSTOMER_CITY,
  CUSTOMER_AREA,
  CUSTOMER_SEGMENT,

  COUNT(*)                             AS TOTAL_ORDERS,
  SUM(IS_DELIVERED)                    AS DELIVERED_ORDERS,
  SUM(IS_CANCELLED)                    AS CANCELLED_ORDERS,
  AVG(TOTAL_AMOUNT)                    AS AVG_ORDER_VALUE,

  AVG(RATING)                          AS AVG_RATING,
  AVG(SENTIMENT_SCORE)                 AS AVG_SENTIMENT_SCORE,
  SUM(
    CASE 
      WHEN RATING IS NOT NULL AND RATING <= 3 THEN 1 
      ELSE 0 
    END
  )                                    AS LOW_RATING_ORDERS,

  MIN(FULL_DATE)                       AS FIRST_ORDER_DATE,
  MAX(FULL_DATE)                       AS LAST_ORDER_DATE,
  DATEDIFF('day', MAX(FULL_DATE), CURRENT_DATE()) 
                                       AS DAYS_SINCE_LAST_ORDER
FROM MARTS.V_ORDER_ENRICHED
GROUP BY
  CUSTOMER_ID,
  CUSTOMER_NAME,
  CUSTOMER_CITY,
  CUSTOMER_AREA,
  CUSTOMER_SEGMENT;

-- 11.7 V_AGENT_FEEDBACK_KPI
CREATE OR REPLACE VIEW V_AGENT_FEEDBACK_KPI AS
SELECT
  da.AGENT_ID,
  da.AGENT_NAME,
  da.CITY          AS AGENT_CITY,
  da.VEHICLE_TYPE,
  da.STATUS        AS AGENT_STATUS,

  COUNT(DISTINCT d.ORDER_ID)           AS ORDERS_DELIVERED,
  AVG(d.DISTANCE_KM)                   AS AVG_DISTANCE_KM,
  AVG(d.ACTUAL_TIME_MIN)               AS AVG_DELIVERY_TIME_MIN,
  AVG(d.SLA_BREACH_FLAG)               AS SLA_BREACH_RATE,

  AVG(o.RATING)                        AS AVG_CUSTOMER_RATING,
  AVG(o.SENTIMENT_SCORE)               AS AVG_SENTIMENT_SCORE,
  SUM(
    CASE 
      WHEN o.RATING IS NOT NULL AND o.RATING <= 3 THEN 1 
      ELSE 0 
    END
  )                                    AS LOW_RATING_ORDERS,

  COUNT(DISTINCT DATE_TRUNC('day', d.PICKUP_TIME)) 
                                       AS DAYS_WORKED,
  CASE 
    WHEN COUNT(DISTINCT DATE_TRUNC('day', d.PICKUP_TIME)) > 0 THEN
      COUNT(DISTINCT d.ORDER_ID) 
      / COUNT(DISTINCT DATE_TRUNC('day', d.PICKUP_TIME))::FLOAT
    ELSE NULL 
  END                                   AS AVG_ORDERS_PER_DAY
FROM INT.FCT_DELIVERY d
JOIN INT.DIM_DELIVERY_AGENT da
  ON d.AGENT_ID = da.AGENT_ID
LEFT JOIN MARTS.V_ORDER_ENRICHED o
  ON d.ORDER_ID = o.ORDER_ID
GROUP BY
  da.AGENT_ID,
  da.AGENT_NAME,
  da.CITY,
  da.VEHICLE_TYPE,
  da.STATUS;



---------------------------------------------------------------------------
-- 12. SECURE FACT + RLS POLICY
---------------------------------------------------------------------------

USE ROLE ACCOUNTADMIN;
USE DATABASE ZOMATO_DWH;
USE SCHEMA MARTS;

-- 12.1 Secure fact table definition
CREATE OR REPLACE TABLE FCT_ORDER_SECURE (
  ORDER_ID             NUMBER,
  ORDER_DATE_KEY       NUMBER,
  FULL_DATE            DATE,
  TOTAL_AMOUNT         NUMBER(12,2),
  ORDER_SUBTOTAL       NUMBER(12,2),
  ORDER_DISCOUNT       NUMBER(12,2),
  DELIVERY_FEE         NUMBER(10,2),
  NET_FOOD_AMOUNT      NUMBER(12,2),
  DISCOUNT_PCT         FLOAT,
  SLA_BREACHED         NUMBER,
  PLATFORM_COMMISSION  NUMBER(12,2),
  RESTAURANT_PAYOUT    NUMBER(12,2),
  END_TO_END_MIN       NUMBER,
  ORDER_STATUS         STRING,
  PAYMENT_METHOD       STRING,
  PROMO_ID             NUMBER,
  IS_DELIVERED         NUMBER,
  IS_CANCELLED         NUMBER,
  CUSTOMER_ID          NUMBER,
  CUSTOMER_NAME        STRING,
  CUSTOMER_CITY        STRING,
  CUSTOMER_AREA        STRING,
  CUSTOMER_SEGMENT     STRING,
  RESTAURANT_ID        NUMBER,
  RESTAURANT_NAME      STRING,
  RESTAURANT_CITY      STRING,
  RESTAURANT_AREA      STRING,
  CUISINE_PRIMARY      STRING,
  COMMISSION_RATE      FLOAT,
  DISTANCE_KM          NUMBER(10,2),
  SLA_BREACH_FLAG      NUMBER,
  RATING               NUMBER,
  SENTIMENT_SCORE      FLOAT
);

---------------------------------------------------------------------------
-- 12.2 Procedure to refresh secure fact from V_ORDER_ENRICHED
---------------------------------------------------------------------------

USE ROLE ACCOUNTADMIN;
USE DATABASE ZOMATO_DWH;
USE SCHEMA UTIL;

CREATE OR REPLACE PROCEDURE SP_REFRESH_FCT_ORDER_SECURE()
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
  try {
    snowflake.execute({sqlText: 'BEGIN'});

    snowflake.execute({sqlText: 'TRUNCATE TABLE MARTS.FCT_ORDER_SECURE'});

    var sql = `
      INSERT INTO MARTS.FCT_ORDER_SECURE (
        ORDER_ID,
        ORDER_DATE_KEY,
        FULL_DATE,
        TOTAL_AMOUNT,
        ORDER_SUBTOTAL,
        ORDER_DISCOUNT,
        DELIVERY_FEE,
        NET_FOOD_AMOUNT,
        DISCOUNT_PCT,
        SLA_BREACHED,
        PLATFORM_COMMISSION,
        RESTAURANT_PAYOUT,
        END_TO_END_MIN,
        ORDER_STATUS,
        PAYMENT_METHOD,
        PROMO_ID,
        IS_DELIVERED,
        IS_CANCELLED,
        CUSTOMER_ID,
        CUSTOMER_NAME,
        CUSTOMER_CITY,
        CUSTOMER_AREA,
        CUSTOMER_SEGMENT,
        RESTAURANT_ID,
        RESTAURANT_NAME,
        RESTAURANT_CITY,
        RESTAURANT_AREA,
        CUISINE_PRIMARY,
        COMMISSION_RATE,
        DISTANCE_KM,
        SLA_BREACH_FLAG,
        RATING,
        SENTIMENT_SCORE
      )
      SELECT
        ORDER_ID,
        ORDER_DATE_KEY,
        FULL_DATE,
        TOTAL_AMOUNT,
        ORDER_SUBTOTAL,
        ORDER_DISCOUNT,
        DELIVERY_FEE,
        NET_FOOD_AMOUNT,
        DISCOUNT_PCT,
        SLA_BREACHED,
        PLATFORM_COMMISSION,
        RESTAURANT_PAYOUT,
        END_TO_END_MIN,
        ORDER_STATUS,
        PAYMENT_METHOD,
        PROMO_ID,
        IS_DELIVERED,
        IS_CANCELLED,
        CUSTOMER_ID,
        CUSTOMER_NAME,
        CUSTOMER_CITY,
        CUSTOMER_AREA,
        CUSTOMER_SEGMENT,
        RESTAURANT_ID,
        RESTAURANT_NAME,
        RESTAURANT_CITY,
        RESTAURANT_AREA,
        CUISINE_PRIMARY,
        COMMISSION_RATE,
        DISTANCE_KM,
        SLA_BREACH_FLAG,
        RATING,
        SENTIMENT_SCORE
      FROM MARTS.V_ORDER_ENRICHED
    `;
    snowflake.execute({sqlText: sql});

    snowflake.execute({sqlText: `
      INSERT INTO UTIL.PROC_AUDIT_LOG(PROC_NAME,RUN_TS,STATUS,MESSAGE)
      VALUES ('SP_REFRESH_FCT_ORDER_SECURE', CURRENT_TIMESTAMP(), 'SUCCESS', 'FCT_ORDER_SECURE refreshed')
    `});

    snowflake.execute({sqlText: 'COMMIT'});
    return 'FCT_ORDER_SECURE refreshed successfully';
  } catch (err) {
    try { snowflake.execute({sqlText: 'ROLLBACK'}); } catch (e2) {}
    snowflake.execute({
      sqlText: `
        INSERT INTO UTIL.PROC_AUDIT_LOG(PROC_NAME,RUN_TS,STATUS,MESSAGE)
        VALUES ('SP_REFRESH_FCT_ORDER_SECURE', CURRENT_TIMESTAMP(), 'ERROR', :msg)
      `,
      binds: { msg: err.toString() }
    });
    return 'ERROR in SP_REFRESH_FCT_ORDER_SECURE: ' + err;
  }
$$;

-- Initial load (you can re-run anytime)
CALL UTIL.SP_REFRESH_FCT_ORDER_SECURE();

---------------------------------------------------------------------------
-- 12.3 Row Access Policy implementation
---------------------------------------------------------------------------
--Prerequisites:
USE ROLE ACCOUNTADMIN;
USE DATABASE ZOMATO_DWH;

-- Make sure secure fact is loaded
CALL UTIL.SP_REFRESH_FCT_ORDER_SECURE();

-- Quick sanity check 
SELECT RESTAURANT_CITY, RESTAURANT_ID, COUNT(*) AS ORDER_CNT
FROM MARTS.FCT_ORDER_SECURE
GROUP BY 1,2
ORDER BY 1,2;
---------------------------------------------------------------
-- A) Clean up any previous Row Access Policy on FCT_ORDER_SECURE
---------------------------------------------------------------
USE ROLE ACCOUNTADMIN;
USE DATABASE ZOMATO_DWH;
USE SCHEMA MARTS;

-- If old policy already attached, drop it first (ignore error if not present)
BEGIN
  ALTER TABLE FCT_ORDER_SECURE
    DROP ROW ACCESS POLICY UTIL.RLP_MARTS_CITY_RESTAURANT;
EXCEPTION
  WHEN OTHER THEN
    -- do nothing if policy not attached
    NULL;
END;

-- (Re-)create mapping table in UTIL schema
USE SCHEMA UTIL;

CREATE OR REPLACE TABLE RLS_ROLE_ACCESS (
  ROLE_NAME     STRING,
  ACCESS_TYPE   STRING,      -- 'CITY' or 'RESTAURANT'
  CITY          STRING,
  RESTAURANT_ID NUMBER
);
---------------------------------------------------------------
-- B) Insert sample role → access mappings
--    Each row defines what a role is allowed to see
---------------------------------------------------------------

INSERT INTO RLS_ROLE_ACCESS (ROLE_NAME, ACCESS_TYPE, CITY, RESTAURANT_ID) VALUES
  -- State managers (city-based access)
  ('ZOMATO_STATE_MANAGER_BLR',   'CITY',       'Bengaluru', NULL),
  ('ZOMATO_STATE_MANAGER_MUMBAI','CITY',       'Mumbai',    NULL),

  -- Restaurant-specific roles (single restaurant)
  ('ZOMATO_RESTAURANT_R100',     'RESTAURANT', NULL,        100),
  ('ZOMATO_RESTAURANT_R200',     'RESTAURANT', NULL,        200);
---------------------------------------------------------------
-- C) Create demo roles + hook them to the mapping
---------------------------------------------------------------
USE ROLE ACCOUNTADMIN;
USE DATABASE ZOMATO_DWH;

-- Parent "functional" roles if not already created
CREATE ROLE IF NOT EXISTS ZOMATO_SYSADMIN;
CREATE ROLE IF NOT EXISTS ZOMATO_ANALYST;

-- Demo RLS roles
CREATE ROLE IF NOT EXISTS ZOMATO_STATE_MANAGER_BLR;
CREATE ROLE IF NOT EXISTS ZOMATO_STATE_MANAGER_MUMBAI;
CREATE ROLE IF NOT EXISTS ZOMATO_RESTAURANT_R100;
CREATE ROLE IF NOT EXISTS ZOMATO_RESTAURANT_R200;

-- Give these roles access to warehouse + MARTS schema
GRANT USAGE ON WAREHOUSE WH_ZOMATO TO ROLE ZOMATO_STATE_MANAGER_BLR;
GRANT USAGE ON WAREHOUSE WH_ZOMATO TO ROLE ZOMATO_STATE_MANAGER_MUMBAI;
GRANT USAGE ON WAREHOUSE WH_ZOMATO TO ROLE ZOMATO_RESTAURANT_R100;
GRANT USAGE ON WAREHOUSE WH_ZOMATO TO ROLE ZOMATO_RESTAURANT_R200;

GRANT USAGE ON DATABASE ZOMATO_DWH TO ROLE ZOMATO_STATE_MANAGER_BLR;
GRANT USAGE ON DATABASE ZOMATO_DWH TO ROLE ZOMATO_STATE_MANAGER_MUMBAI;
GRANT USAGE ON DATABASE ZOMATO_DWH TO ROLE ZOMATO_RESTAURANT_R100;
GRANT USAGE ON DATABASE ZOMATO_DWH TO ROLE ZOMATO_RESTAURANT_R200;

GRANT USAGE ON SCHEMA ZOMATO_DWH.MARTS TO ROLE ZOMATO_STATE_MANAGER_BLR;
GRANT USAGE ON SCHEMA ZOMATO_DWH.MARTS TO ROLE ZOMATO_STATE_MANAGER_MUMBAI;
GRANT USAGE ON SCHEMA ZOMATO_DWH.MARTS TO ROLE ZOMATO_RESTAURANT_R100;
GRANT USAGE ON SCHEMA ZOMATO_DWH.MARTS TO ROLE ZOMATO_RESTAURANT_R200;

GRANT SELECT ON TABLE ZOMATO_DWH.MARTS.FCT_ORDER_SECURE
       TO ROLE ZOMATO_STATE_MANAGER_BLR;
GRANT SELECT ON TABLE ZOMATO_DWH.MARTS.FCT_ORDER_SECURE
       TO ROLE ZOMATO_STATE_MANAGER_MUMBAI;
GRANT SELECT ON TABLE ZOMATO_DWH.MARTS.FCT_ORDER_SECURE
       TO ROLE ZOMATO_RESTAURANT_R100;
GRANT SELECT ON TABLE ZOMATO_DWH.MARTS.FCT_ORDER_SECURE
       TO ROLE ZOMATO_RESTAURANT_R200;
---------------------------------------------------------------
-- D) Create demo users and assign roles
--    (For training, each trainee can log in with one user)
---------------------------------------------------------------

CREATE OR REPLACE USER U_STATE_BLR
  PASSWORD = 'Pass@123'
  DEFAULT_ROLE = ZOMATO_STATE_MANAGER_BLR
  MUST_CHANGE_PASSWORD = FALSE;

CREATE OR REPLACE USER U_STATE_MUMBAI
  PASSWORD = 'Pass@123'
  DEFAULT_ROLE = ZOMATO_STATE_MANAGER_MUMBAI
  MUST_CHANGE_PASSWORD = FALSE;

CREATE OR REPLACE USER U_REST_R100
  PASSWORD = 'Pass@123'
  DEFAULT_ROLE = ZOMATO_RESTAURANT_R100
  MUST_CHANGE_PASSWORD = FALSE;

CREATE OR REPLACE USER U_REST_R200
  PASSWORD = 'Pass@123'
  DEFAULT_ROLE = ZOMATO_RESTAURANT_R200
  MUST_CHANGE_PASSWORD = FALSE;

GRANT ROLE ZOMATO_STATE_MANAGER_BLR    TO USER U_STATE_BLR;
GRANT ROLE ZOMATO_STATE_MANAGER_MUMBAI TO USER U_STATE_MUMBAI;
GRANT ROLE ZOMATO_RESTAURANT_R100      TO USER U_REST_R100;
GRANT ROLE ZOMATO_RESTAURANT_R200      TO USER U_REST_R200;

GRANT ROLE ZOMATO_STATE_MANAGER_BLR    TO ROLE ACCOUNTADMIN;
GRANT ROLE ZOMATO_STATE_MANAGER_MUMBAI TO ROLE ACCOUNTADMIN;
GRANT ROLE ZOMATO_RESTAURANT_R100      TO ROLE ACCOUNTADMIN;
GRANT ROLE ZOMATO_RESTAURANT_R200      TO ROLE ACCOUNTADMIN;
---------------------------------------------------------------
-- E) Define / re-define the Row Access Policy itself
--    Uses CURRENT_ROLE() to check UTIL.RLS_ROLE_ACCESS
---------------------------------------------------------------
USE ROLE ACCOUNTADMIN;
USE DATABASE ZOMATO_DWH;
USE SCHEMA UTIL;

CREATE OR REPLACE ROW ACCESS POLICY RLP_MARTS_CITY_RESTAURANT
AS (RESTAURANT_CITY STRING, RESTAURANT_ID NUMBER) RETURNS BOOLEAN ->
  CASE
    -- Admin/system roles see everything
    WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'ZOMATO_SYSADMIN') THEN TRUE

    -- For other roles, check mapping table
    ELSE EXISTS (
      SELECT 1
      FROM UTIL.RLS_ROLE_ACCESS r
      WHERE r.ROLE_NAME = CURRENT_ROLE()
        AND (
             (r.ACCESS_TYPE = 'CITY'
              AND r.CITY = RESTAURANT_CITY)
          OR (r.ACCESS_TYPE = 'RESTAURANT'
              AND r.RESTAURANT_ID = RESTAURANT_ID)
        )
    )
  END;

---------------------------------------------------------------
-- F) Attach policy to MARTS.FCT_ORDER_SECURE
---------------------------------------------------------------
USE ROLE ACCOUNTADMIN;
USE DATABASE ZOMATO_DWH;
USE SCHEMA MARTS;

ALTER TABLE FCT_ORDER_SECURE
  ADD ROW ACCESS POLICY UTIL.RLP_MARTS_CITY_RESTAURANT
  ON (RESTAURANT_CITY, RESTAURANT_ID);

USE SECONDARY ROLES NONE;
USE ROLE ZOMATO_STATE_MANAGER_BLR;
SELECT * FROM MARTS.FCT_ORDER_SECURE;
  
USE SECONDARY ROLES NONE;
USE ROLE ZOMATO_STATE_MANAGER_MUMBAI;
SELECT * FROM MARTS.FCT_ORDER_SECURE;

USE SECONDARY ROLES NONE;
USE ROLE ZOMATO_RESTAURANT_R100;
SELECT * FROM MARTS.FCT_ORDER_SECURE;

USE SECONDARY ROLES NONE;
USE ROLE ZOMATO_RESTAURANT_R200;
SELECT * FROM MARTS.FCT_ORDER_SECURE;

---------------------------------------------------------------------------
-- 13. ROLES & GRANTS (ACCOUNTADMIN ONLY) 
---------------------------------------------------------------------------

USE ROLE ACCOUNTADMIN;
USE DATABASE ZOMATO_DWH;

-- Create roles
CREATE ROLE IF NOT EXISTS ZOMATO_SYSADMIN;
CREATE ROLE IF NOT EXISTS ZOMATO_ANALYST;
CREATE ROLE IF NOT EXISTS ZOMATO_STATE_MANAGER;
CREATE ROLE IF NOT EXISTS ZOMATO_RESTAURANT_USER;
CREATE ROLE IF NOT EXISTS ZOMATO_PII_ADMIN_ROLE;

-- Warehouse usage
GRANT USAGE ON WAREHOUSE WH_ZOMATO TO ROLE ZOMATO_SYSADMIN;
GRANT USAGE ON WAREHOUSE WH_ZOMATO TO ROLE ZOMATO_ANALYST;
GRANT USAGE ON WAREHOUSE WH_ZOMATO TO ROLE ZOMATO_STATE_MANAGER;
GRANT USAGE ON WAREHOUSE WH_ZOMATO TO ROLE ZOMATO_RESTAURANT_USER;

-- Database usage
GRANT USAGE ON DATABASE ZOMATO_DWH TO ROLE ZOMATO_SYSADMIN;
GRANT USAGE ON DATABASE ZOMATO_DWH TO ROLE ZOMATO_ANALYST;
GRANT USAGE ON DATABASE ZOMATO_DWH TO ROLE ZOMATO_STATE_MANAGER;
GRANT USAGE ON DATABASE ZOMATO_DWH TO ROLE ZOMATO_RESTAURANT_USER;

-- Schema usage on existing schemas
GRANT USAGE ON ALL SCHEMAS IN DATABASE ZOMATO_DWH TO ROLE ZOMATO_SYSADMIN;
GRANT USAGE ON ALL SCHEMAS IN DATABASE ZOMATO_DWH TO ROLE ZOMATO_ANALYST;
GRANT USAGE ON ALL SCHEMAS IN DATABASE ZOMATO_DWH TO ROLE ZOMATO_STATE_MANAGER;
GRANT USAGE ON ALL SCHEMAS IN DATABASE ZOMATO_DWH TO ROLE ZOMATO_RESTAURANT_USER;

-- Schema usage on future schemas
GRANT USAGE ON FUTURE SCHEMAS IN DATABASE ZOMATO_DWH TO ROLE ZOMATO_SYSADMIN;
GRANT USAGE ON FUTURE SCHEMAS IN DATABASE ZOMATO_DWH TO ROLE ZOMATO_ANALYST;
GRANT USAGE ON FUTURE SCHEMAS IN DATABASE ZOMATO_DWH TO ROLE ZOMATO_STATE_MANAGER;
GRANT USAGE ON FUTURE SCHEMAS IN DATABASE ZOMATO_DWH TO ROLE ZOMATO_RESTAURANT_USER;

-- SELECT on MARTS
GRANT SELECT ON ALL TABLES IN SCHEMA ZOMATO_DWH.MARTS TO ROLE ZOMATO_ANALYST;
GRANT SELECT ON ALL TABLES IN SCHEMA ZOMATO_DWH.MARTS TO ROLE ZOMATO_STATE_MANAGER;
GRANT SELECT ON ALL TABLES IN SCHEMA ZOMATO_DWH.MARTS TO ROLE ZOMATO_RESTAURANT_USER;

-- SELECT on MARTS
GRANT SELECT ON FUTURE TABLES IN SCHEMA ZOMATO_DWH.MARTS TO ROLE ZOMATO_ANALYST;
GRANT SELECT ON FUTURE TABLES IN SCHEMA ZOMATO_DWH.MARTS TO ROLE ZOMATO_STATE_MANAGER;
GRANT SELECT ON FUTURE TABLES IN SCHEMA ZOMATO_DWH.MARTS TO ROLE ZOMATO_RESTAURANT_USER;

-- SELECT on INT
GRANT SELECT ON ALL TABLES IN SCHEMA ZOMATO_DWH.INT TO ROLE ZOMATO_ANALYST;
GRANT SELECT ON ALL TABLES IN SCHEMA ZOMATO_DWH.INT TO ROLE ZOMATO_STATE_MANAGER;
GRANT SELECT ON ALL TABLES IN SCHEMA ZOMATO_DWH.INT TO ROLE ZOMATO_RESTAURANT_USER;

-- Security-related grants (account-level)
GRANT APPLY MASKING POLICY ON ACCOUNT TO ROLE ZOMATO_PII_ADMIN_ROLE;
GRANT APPLY ROW ACCESS POLICY ON ACCOUNT TO ROLE ZOMATO_SYSADMIN;

---------------------------------------------------------------------------
-- 14. TIME TRAVEL & CLONE 

---------------------------------------------------------------------------

USE ROLE ACCOUNTADMIN;
USE DATABASE ZOMATO_DWH;
USE SCHEMA INT;

-- Show current row
SELECT * FROM DIM_CUSTOMER WHERE CUSTOMER_ID = 100 AND IS_CURRENT = TRUE;

-- Simulate mistake
UPDATE DIM_CUSTOMER
  SET SEGMENT = 'BROKEN_SEGMENT'
WHERE CUSTOMER_ID = 100
  AND IS_CURRENT = TRUE;

-- Check wrong data
SELECT * FROM DIM_CUSTOMER WHERE CUSTOMER_ID = 100 AND IS_CURRENT = TRUE;

-- Look back 5 minutes
SELECT * FROM DIM_CUSTOMER AT (OFFSET => -300)
WHERE CUSTOMER_ID = 100 AND IS_CURRENT = TRUE;

-- Restore table from time travel if desired
CREATE OR REPLACE TABLE DIM_CUSTOMER AS
SELECT * FROM DIM_CUSTOMER AT (OFFSET => -300);

-- Zero-copy clone
CREATE OR REPLACE DATABASE ZOMATO_DWH_DEV CLONE ZOMATO_DWH;

--15. Analysis of data loaded

USE ROLE ACCOUNTADMIN;
USE DATABASE ZOMATO_DWH;
USE SCHEMA MARTS;

USE ROLE ACCOUNTADMIN;
USE DATABASE ZOMATO_DWH;
USE SCHEMA MARTS;
-----------------------------------------------
-- Q1. City-level orders, GMV, ratings, SLA
-----------------------------------------------
SELECT
  RESTAURANT_CITY                         AS CITY,
  COUNT(*)                                AS TOTAL_ORDERS,
  SUM(TOTAL_AMOUNT)                       AS GMV,
  SUM(IS_DELIVERED)                       AS DELIVERED_ORDERS,
  SUM(IS_CANCELLED)                       AS CANCELLED_ORDERS,
  ROUND(AVG(RATING), 2)                   AS AVG_RATING,
  ROUND(AVG(SENTIMENT_SCORE), 2)          AS AVG_SENTIMENT_SCORE,
  ROUND(AVG(SLA_BREACH_FLAG), 4)          AS AVG_SLA_BREACH_RATE
FROM V_ORDER_ENRICHED
GROUP BY RESTAURANT_CITY
ORDER BY GMV DESC;

-----------------------------------------------
-- Q2. Top 15 restaurants by GMV
--     with delivery time and rating
-----------------------------------------------
SELECT
  RESTAURANT_ID,
  RESTAURANT_NAME,
  RESTAURANT_CITY,
  CUISINE_PRIMARY,
  COUNT(*)                          AS TOTAL_ORDERS,
  SUM(TOTAL_AMOUNT)                 AS GMV,
  ROUND(AVG(END_TO_END_MIN), 2)     AS AVG_END_TO_END_MIN,
  ROUND(AVG(SLA_BREACH_FLAG), 4)    AS SLA_BREACH_RATE,
  ROUND(AVG(RATING), 2)             AS AVG_RATING
FROM V_ORDER_ENRICHED
GROUP BY
  RESTAURANT_ID,
  RESTAURANT_NAME,
  RESTAURANT_CITY,
  CUISINE_PRIMARY
ORDER BY GMV DESC
LIMIT 15;

-----------------------------------------------
-- Q3. Restaurant “problem index”:
--     low rating + high cancel + high SLA breach
-----------------------------------------------
SELECT
  RESTAURANT_ID,
  RESTAURANT_NAME,
  RESTAURANT_CITY,
  CUISINE_PRIMARY,
  COUNT(*)                                AS TOTAL_ORDERS,
  ROUND(AVG(RATING), 2)                   AS AVG_RATING,
  ROUND(AVG(SENTIMENT_SCORE), 2)          AS AVG_SENTIMENT_SCORE,
  ROUND(SUM(IS_CANCELLED) / NULLIF(COUNT(*),0), 4) AS CANCEL_RATE,
  ROUND(AVG(SLA_BREACH_FLAG), 4)          AS SLA_BREACH_RATE
FROM V_ORDER_ENRICHED
GROUP BY
  RESTAURANT_ID,
  RESTAURANT_NAME,
  RESTAURANT_CITY,
  CUISINE_PRIMARY
HAVING COUNT(*) >= 5      -- need some data for stability
ORDER BY
  AVG_RATING ASC,
  CANCEL_RATE DESC,
  SLA_BREACH_RATE DESC
LIMIT 20;

-----------------------------------------------
-- Q4. Cuisine performance by city
-----------------------------------------------
SELECT
  RESTAURANT_CITY,
  CUISINE_PRIMARY,
  COUNT(*)                          AS TOTAL_ORDERS,
  SUM(TOTAL_AMOUNT)                 AS GMV,
  ROUND(AVG(RATING), 2)             AS AVG_RATING,
  ROUND(AVG(SENTIMENT_SCORE), 2)    AS AVG_SENTIMENT_SCORE
FROM V_ORDER_ENRICHED
GROUP BY RESTAURANT_CITY, CUISINE_PRIMARY
ORDER BY RESTAURANT_CITY, GMV DESC;

-----------------------------------------------
-- Q5. Customer segment KPIs
--     (value, satisfaction, cancellations)
-----------------------------------------------
SELECT
  CUSTOMER_SEGMENT,
  COUNT(*)                                AS TOTAL_ORDERS,
  COUNT(DISTINCT CUSTOMER_ID)             AS UNIQUE_CUSTOMERS,
  SUM(TOTAL_AMOUNT)                       AS GMV,
  ROUND(AVG(TOTAL_AMOUNT), 2)             AS AVG_ORDER_VALUE,
  ROUND(AVG(RATING), 2)                   AS AVG_RATING,
  ROUND(AVG(SENTIMENT_SCORE), 2)          AS AVG_SENTIMENT_SCORE,
  ROUND(SUM(IS_CANCELLED) / NULLIF(COUNT(*),0), 4) AS CANCEL_RATE
FROM V_ORDER_ENRICHED
GROUP BY CUSTOMER_SEGMENT
ORDER BY GMV DESC;

-----------------------------------------------
-- Q6. Prime vs Non-Prime customers
--     using current DIM_CUSTOMER (SCD2)
-----------------------------------------------
WITH current_customers AS (
  SELECT
    CUSTOMER_ID,
    IS_PRIME_MEMBER
  FROM ZOMATO_DWH.INT.DIM_CUSTOMER
  WHERE IS_CURRENT = TRUE
),
orders_with_prime AS (
  SELECT
    o.*,
    c.IS_PRIME_MEMBER
  FROM V_ORDER_ENRICHED o
  LEFT JOIN current_customers c
    ON o.CUSTOMER_ID = c.CUSTOMER_ID
)
SELECT
  COALESCE(IS_PRIME_MEMBER, FALSE) AS IS_PRIME_MEMBER,
  COUNT(*)                         AS TOTAL_ORDERS,
  COUNT(DISTINCT CUSTOMER_ID)      AS UNIQUE_CUSTOMERS,
  SUM(TOTAL_AMOUNT)                AS GMV,
  ROUND(AVG(TOTAL_AMOUNT), 2)      AS AVG_ORDER_VALUE,
  ROUND(AVG(RATING), 2)            AS AVG_RATING,
  ROUND(AVG(SENTIMENT_SCORE), 2)   AS AVG_SENTIMENT_SCORE
FROM orders_with_prime
GROUP BY COALESCE(IS_PRIME_MEMBER, FALSE)
ORDER BY GMV DESC;

-----------------------------------------------
-- Q7. Delivery agent performance + ratings
--     (joins FCT_DELIVERY + DIM_DELIVERY_AGENT + orders)
-----------------------------------------------
USE SCHEMA ZOMATO_DWH.INT;

WITH delivery_base AS (
  SELECT
    d.TRIP_ID,
    d.ORDER_ID,
    d.AGENT_ID,
    d.DISTANCE_KM,
    d.ESTIMATED_TIME_MIN,
    d.ACTUAL_TIME_MIN,
    d.SLA_BREACH_FLAG
  FROM FCT_DELIVERY d
),
agent_perf AS (
  SELECT
    da.AGENT_ID,
    da.AGENT_NAME,
    da.CITY              AS AGENT_CITY,
    da.VEHICLE_TYPE,
    da.STATUS            AS AGENT_STATUS,
    db.ORDER_ID,
    db.DISTANCE_KM,
    db.ACTUAL_TIME_MIN,
    db.SLA_BREACH_FLAG
  FROM delivery_base db
  JOIN DIM_DELIVERY_AGENT da
    ON db.AGENT_ID = da.AGENT_ID
)
SELECT
  ap.AGENT_ID,
  ap.AGENT_NAME,
  ap.AGENT_CITY,
  ap.VEHICLE_TYPE,
  ap.AGENT_STATUS,
  COUNT(DISTINCT ap.ORDER_ID)              AS ORDERS_DELIVERED,
  ROUND(AVG(ap.DISTANCE_KM), 2)            AS AVG_DISTANCE_KM,
  ROUND(AVG(ap.ACTUAL_TIME_MIN), 2)        AS AVG_DELIVERY_TIME_MIN,
  ROUND(AVG(ap.SLA_BREACH_FLAG), 4)        AS SLA_BREACH_RATE
FROM agent_perf ap
GROUP BY
  ap.AGENT_ID,
  ap.AGENT_NAME,
  ap.AGENT_CITY,
  ap.VEHICLE_TYPE,
  ap.AGENT_STATUS
ORDER BY ORDERS_DELIVERED DESC;

-----------------------------------------------
-- Q8. Feedback distribution by rating and city
--     using FCT_FEEDBACK + DIM_CUSTOMER + DIM_RESTAURANT
-----------------------------------------------
USE SCHEMA ZOMATO_DWH.INT;

WITH fb AS (
  SELECT
    f.FEEDBACK_ID,
    f.ORDER_ID,
    f.CUSTOMER_ID,
    f.RATING,
    f.SENTIMENT_SCORE
  FROM FCT_FEEDBACK f
),
ord AS (
  SELECT
    o.ORDER_ID,
    o.RESTAURANT_ID,
    o.RESTAURANT_CITY
  FROM ZOMATO_DWH.MARTS.V_ORDER_ENRICHED o
)
SELECT
  ord.RESTAURANT_CITY,
  fb.RATING,
  COUNT(*)                        AS FEEDBACK_COUNT,
  ROUND(AVG(fb.SENTIMENT_SCORE), 2) AS AVG_SENTIMENT_SCORE
FROM fb
LEFT JOIN ord
  ON fb.ORDER_ID = ord.ORDER_ID
GROUP BY ord.RESTAURANT_CITY, fb.RATING
ORDER BY ord.RESTAURANT_CITY, fb.RATING;

-----------------------------------------------
-- Q9. Commission and payout by city
--     (how much platform earns vs restaurant)
-----------------------------------------------
USE SCHEMA ZOMATO_DWH.MARTS;

SELECT
  RESTAURANT_CITY                         AS CITY,
  SUM(TOTAL_AMOUNT)                       AS GMV,
  SUM(PLATFORM_COMMISSION)                AS TOTAL_PLATFORM_COMMISSION,
  SUM(RESTAURANT_PAYOUT)                  AS TOTAL_RESTAURANT_PAYOUT,
  ROUND(
    SUM(PLATFORM_COMMISSION) / NULLIF(SUM(TOTAL_AMOUNT),0),
    4
  )                                       AS EFFECTIVE_COMMISSION_PCT
FROM V_ORDER_ENRICHED
GROUP BY RESTAURANT_CITY
ORDER BY GMV DESC;

-----------------------------------------------
-- Q10. RLS demo-friendly query on FCT_ORDER_SECURE
--      (shows aggregated view; different per role)
-----------------------------------------------
USE SCHEMA ZOMATO_DWH.MARTS;

SELECT
  RESTAURANT_CITY,
  RESTAURANT_ID,
  RESTAURANT_NAME,
  COUNT(*)                    AS TOTAL_ORDERS,
  SUM(TOTAL_AMOUNT)           AS GMV,
  ROUND(AVG(RATING), 2)       AS AVG_RATING,
  ROUND(AVG(SENTIMENT_SCORE), 2) AS AVG_SENTIMENT_SCORE
FROM FCT_ORDER_SECURE
GROUP BY
  RESTAURANT_CITY,
  RESTAURANT_ID,
  RESTAURANT_NAME
ORDER BY RESTAURANT_CITY, GMV DESC;

------------SCD Hands on

-- SCD1 (Overwrite) with DIM_DELIVERY_AGENT
USE ROLE ACCOUNTADMIN;
USE DATABASE ZOMATO_DWH;

SELECT * FROM STG.V_DELIVERY_AGENT_STG LIMIT 5;

SELECT * FROM INT.DIM_DELIVERY_AGENT WHERE AGENT_ID = 1;

UPDATE RAW.DELIVERY_AGENT_RAW
SET CITY   = 'Kolkata',
    STATUS = 'INACTIVE'
WHERE TRY_TO_NUMBER(AGENT_ID) = 1;

SELECT * FROM STG.V_DELIVERY_AGENT_STG WHERE AGENT_ID = 1;

CALL UTIL.SP_LOAD_DIM_DELIVERY_AGENT();

SELECT * FROM INT.DIM_DELIVERY_AGENT WHERE AGENT_ID = 101;

--SCD2 (History with versions) on DIM_CUSTOMER

USE ROLE ACCOUNTADMIN;
USE DATABASE ZOMATO_DWH;

SELECT *
FROM STG.V_CUSTOMER_STG
LIMIT 5;

SELECT *
FROM INT.DIM_CUSTOMER
WHERE CUSTOMER_ID = 5
ORDER BY EFFECTIVE_FROM;

UPDATE RAW.CUSTOMER_RAW
SET CITY    = 'Noida',
    SEGMENT = 'High Value'
WHERE TRY_TO_NUMBER(CUSTOMER_ID) = 5;

SELECT *
FROM STG.V_CUSTOMER_STG
WHERE CUSTOMER_ID = 5;

CALL UTIL.SP_LOAD_DIM_CUSTOMER_SCD2();

SELECT
  CUSTOMER_SK,
  CUSTOMER_ID,
  CUSTOMER_NAME,
  CITY,
  SEGMENT,
  EFFECTIVE_FROM,
  EFFECTIVE_TO,
  IS_CURRENT
FROM INT.DIM_CUSTOMER
WHERE CUSTOMER_ID = 5
ORDER BY EFFECTIVE_FROM;

--SCD2 + SCD3 on Restaurant (DIM_RESTAURANT & DIM_RESTAURANT_SCD3)

--DIM_RESTAURANT = SCD2 (full history like customers)

--DIM_RESTAURANT_SCD3 = SCD3 holding only current and previous cuisine in the same row.

USE ROLE ACCOUNTADMIN;
USE DATABASE ZOMATO_DWH;

SELECT *
FROM STG.V_RESTAURANT_STG
LIMIT 5;

SELECT
  RESTAURANT_SK,
  RESTAURANT_ID,
  RESTAURANT_NAME,
  CUISINE_PRIMARY,
  CITY,
  EFFECTIVE_FROM,
  EFFECTIVE_TO,
  IS_CURRENT
FROM INT.DIM_RESTAURANT
WHERE RESTAURANT_ID = 3
ORDER BY EFFECTIVE_FROM;

SELECT
  RESTAURANT_ID,
  RESTAURANT_NAME,
  CURRENT_CUISINE,
  PREVIOUS_CUISINE,
  LAST_CHANGE_DATE,
  CITY,
  AREA
FROM INT.DIM_RESTAURANT_SCD3
WHERE RESTAURANT_ID = 3;

UPDATE RAW.RESTAURANT_RAW
SET CUISINE_PRIMARY = 'Continental'
WHERE TRY_TO_NUMBER(RESTAURANT_ID) = 3;

SELECT RESTAURANT_ID, RESTAURANT_NAME, CUISINE_PRIMARY, CITY
FROM STG.V_RESTAURANT_STG
WHERE RESTAURANT_ID = 3;

CALL UTIL.SP_LOAD_DIM_RESTAURANT_SCD2();
CALL UTIL.SP_LOAD_DIM_RESTAURANT_SCD3();

SELECT
  RESTAURANT_SK,
  RESTAURANT_ID,
  RESTAURANT_NAME,
  CUISINE_PRIMARY,
  CITY,
  EFFECTIVE_FROM,
  EFFECTIVE_TO,
  IS_CURRENT
FROM INT.DIM_RESTAURANT
WHERE RESTAURANT_ID = 3
ORDER BY EFFECTIVE_FROM;


SELECT
  RESTAURANT_ID,
  RESTAURANT_NAME,
  CURRENT_CUISINE,
  PREVIOUS_CUISINE,
  LAST_CHANGE_DATE,
  CITY,
  AREA
FROM INT.DIM_RESTAURANT_SCD3
WHERE RESTAURANT_ID = 3;

--------------------Dynamic Table for Daily City KPI----------

USE ROLE ACCOUNTADMIN;
USE DATABASE ZOMATO_DWH;
USE SCHEMA MARTS;

-- Base table
SELECT COUNT(*) FROM V_ORDER_ENRICHED;

-- DYNAMIC TABLE: Daily KPIs by customer city + restaurant city
CREATE OR REPLACE DYNAMIC TABLE DT_DAILY_CITY_KPI
  TARGET_LAG = '1 minutes'
  WAREHOUSE  = WH_ZOMATO
AS
SELECT
  FULL_DATE,
  CUSTOMER_CITY,
  RESTAURANT_CITY,
  COUNT(*)                          AS TOTAL_ORDERS,
  SUM(TOTAL_AMOUNT)                 AS GMV,
  SUM(IS_DELIVERED)                 AS DELIVERED_ORDERS,
  SUM(IS_CANCELLED)                 AS CANCELLED_ORDERS,
  ROUND(AVG(RATING), 2)             AS AVG_RATING,
  ROUND(AVG(SENTIMENT_SCORE), 2)    AS AVG_SENTIMENT_SCORE,
  ROUND(AVG(SLA_BREACH_FLAG), 4)    AS AVG_SLA_BREACH_RATE
FROM V_ORDER_ENRICHED
GROUP BY
  FULL_DATE,
  CUSTOMER_CITY,
  RESTAURANT_CITY;


CREATE OR REPLACE VIEW V_DAILY_CITY_KPI_DT AS
SELECT *
FROM DT_DAILY_CITY_KPI;

---new simulation DATA

USE SCHEMA RAW;

INSERT INTO ORDER_HEADER_RAW (
  ORDER_ID, CUSTOMER_ID, RESTAURANT_ID,
  ORDER_CREATED_AT, ORDER_STATUS, PAYMENT_METHOD,
  PROMO_ID, ORDER_SUBTOTAL, ORDER_DISCOUNT,
  DELIVERY_FEE, TOTAL_AMOUNT,
  EXPECTED_DELIVERY_AT, ACTUAL_DELIVERY_AT,
  CANCELLATION_REASON
)
SELECT
  '900000' || SEQ4()::STRING           AS ORDER_ID,
  '1'                                  AS CUSTOMER_ID,
  '1'                                  AS RESTAURANT_ID,
  TO_CHAR(CURRENT_TIMESTAMP(), 'YYYY-MM-DD HH24:MI:SS') AS ORDER_CREATED_AT,
  'DELIVERED'                          AS ORDER_STATUS,
  'UPI'                                AS PAYMENT_METHOD,
  ''                                   AS PROMO_ID,
  '500.00'                             AS ORDER_SUBTOTAL,
  '50.00'                              AS ORDER_DISCOUNT,
  '30.00'                              AS DELIVERY_FEE,
  '480.00'                             AS TOTAL_AMOUNT,
  TO_CHAR(CURRENT_TIMESTAMP() + INTERVAL '40 MINUTE', 'YYYY-MM-DD HH24:MI:SS'),
  TO_CHAR(CURRENT_TIMESTAMP() + INTERVAL '45 MINUTE', 'YYYY-MM-DD HH24:MI:SS'),
  ''
FROM TABLE(GENERATOR(ROWCOUNT => 10));

USE SCHEMA UTIL;

CALL SP_LOAD_FCT_ORDER_ENHANCED();
CALL SP_LOAD_FCT_DELIVERY();         
CALL SP_REFRESH_FCT_ORDER_SECURE();

USE SCHEMA MARTS;

SELECT FULL_DATE, CUSTOMER_CITY, RESTAURANT_CITY,
       TOTAL_ORDERS, GMV, AVG_RATING
FROM V_DAILY_CITY_KPI_DT
WHERE FULL_DATE = CURRENT_DATE()
ORDER BY CUSTOMER_CITY, RESTAURANT_CITY;




