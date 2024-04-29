CREATE OR REPLACE STREAM ALBUM_TABLE_CHANGES ON TABLE ALBUM;

CREATE OR REPLACE STORAGE INTEGRATION ALBUM_S3_INIT
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = S3
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN='arn:aws:iam::798585594565:role/spotify-snowflake-s3-connection'
    STORAGE_ALLOWED_LOCATIONS = ('s3://spotify-etl-project-devi/transformed_data/album_data/')
    COMMENT  =  'CREATE CONNECTION TO S3';

DESC INTEGRATION ALBUM_S3_INIT;

CREATE OR REPLACE STAGE SPOTIFY_ETL.SPOTIFY_SCHEMA.ALBUM_EXT_STAGE
URL = 's3://spotify-etl-project-devi/transformed_data/album_data/'
STORAGE_INTEGRATION=ALBUM_S3_INIT;

CREATE OR REPLACE FILE FORMAT SPOTIFY_ETL.SPOTIFY_SCHEMA.CSV
TYPE = CSV
FIELD_DELIMITER = ','
RECORD_DELIMITER = '\n'
SKIP_HEADER = 1;


SHOW STAGES;

LIST @ALBUM_EXT_STAGE;

CREATE  OR  REPLACE PIPE ALBUM_S3_PIPE
auto_ingest=True
AS
COPY INTO ALBUM_RAW
FROM @ALBUM_EXT_STAGE
FILE_FORMAT=CSV,
ON_ERROR='CONTINUE';

SHOW PIPES

SELECT * FROM ALBUM_RAW;


SELECT SYSTEM$PIPE_STATUS('ALBUM_S3_PIPE')

SELECT * FROM ALBUM



//STORED PROCEDURE

CREATE OR REPLACE PROCEDURE pdr_album()
returns string not null
language javascript
as
    $$
      var cmd = `
                 MERGE INTO ALBUM A
                    USING ALBUM_RAW AR
                        ON A.ALBUM_ID = AR.ALBUM_ID
                    WHEN  MATCHED AND A.ALBUM_ID<>AR.ALBUM_ID OR
                                      A.ALBUM_NAME <> AR.NAME OR
                                      A.RELEASE_DATE <> AR.RELEASE_DATE OR
                                      A.TOTAL_TRACKS <> AR.TOTAL_TRACKS OR
                                      A.URL <> AR.URL  THEN UPDATE
                            SET A.ALBUM_ID = AR.ALBUM_ID,
                            A.ALBUM_NAME = AR.NAME,
                            A.RELEASE_DATE = AR.RELEASE_DATE,
                            A.TOTAL_TRACKS = AR.TOTAL_TRACKS,
                            A.URL = AR.URL,
                            A.UPDATE_TIMESTAMP = CURRENT_TIMESTAMP()
                    WHEN NOT MATCHED THEN INSERT
                        (
                            A.ALBUM_ID,
                            A.ALBUM_NAME,
                            A.RELEASE_DATE,
                            A.TOTAL_TRACKS,
                            A.URL
                        )
                    VALUES(
                        AR.ALBUM_ID,
                        AR.NAME,
                        AR.RELEASE_DATE,
                        AR.TOTAL_TRACKS,
                        AR.URL
                    );
      `
      
      var cmd1 = "truncate table SPOTIFY_ETL.SPOTIFY_SCHEMA.ALBUM_RAW;"
      var sql = snowflake.createStatement({sqlText: cmd});
      var sql1 = snowflake.createStatement({sqlText: cmd1});
      var result = sql.execute();
      var result1 = sql1.execute();
    return cmd+'\n'+cmd1;
    $$;

call pdr_album()

-- automate the task we need to create task 
--Set up TASKADMIN role
use role securityadmin;
create or replace role taskadmin;
-- Set the active role to ACCOUNTADMIN before granting the EXECUTE TASK privilege to TASKADMIN
use role accountadmin;
grant execute task on account to role taskadmin;

-- Set the active role to SECURITYADMIN to show that this role can grant a role to another role 
use role securityadmin;
grant role taskadmin to role sysadmin;

USE DATABASE SPOTIFY_ETL

-- Set the active database to SPOTIFY_ETL
USE DATABASE SPOTIFY_ETL;

-- Create or replace the task named album_scd_raw in the SPOTIFY_ETL database
CREATE OR REPLACE TASK album_scd_raw 
WAREHOUSE = COMPUTE_WH 
SCHEDULE = '1 minute' 
ERROR_ON_NONDETERMINISTIC_MERGE = FALSE
AS
CALL pdr_album();

show tasks;

SELECT * FROM SPOTIFY_ETL.SPOTIFY_SCHEMA.ALBUM LIMIT 10

-- SCD 2

INSERT INTO SPOTIFY_ETL.SPOTIFY_SCHEMA.ALBUM VALUES('2z7fcGI8oW7BXab2U9ikK2','CIGRATEES AFTER SEX','2023-12-5',8,'https://open.spotify.com/cigrattes-after-sex',current_timestamp())

UPDATE ALBUM SET ALBUM_NAME='DEVI',UPDATE_TIMESTAMP=CURRENT_TIMESTAMP() WHERE ALBUM_ID='2z7fcGI8oW7BXab2U9ikK1'

delete from ALBUM where ALBUM_ID ='4mgbVqnpa3jydOETasg5vN' ;

--View Creation--
CREATE OR REPLACE VIEW v_album_change_data AS
SELECT 
    ALBUM_ID, 
    ALBUM_NAME, 
    RELEASE_DATE, 
    TOTAL_TRACKS, 
    URL,
    start_time, 
    end_time, 
    is_current, 
    'I' AS dml_type
FROM (
    SELECT 
        ALBUM_ID, 
        ALBUM_NAME, 
        RELEASE_DATE, 
        TOTAL_TRACKS, 
        URL,
        update_timestamp AS start_time,
        LAG(update_timestamp) OVER (PARTITION BY ALBUM_ID ORDER BY update_timestamp DESC) AS end_time_raw,
        CASE WHEN end_time_raw IS NULL THEN '9999-12-31'::TIMESTAMP_NTZ ELSE end_time_raw END AS end_time,
        CASE WHEN end_time_raw IS NULL THEN TRUE ELSE FALSE END AS is_current
    FROM (
        SELECT 
            ALBUM_ID, 
            ALBUM_NAME, 
            RELEASE_DATE, 
            TOTAL_TRACKS, 
            URL,
            UPDATE_TIMESTAMP
        FROM SPOTIFY_ETL.SPOTIFY_SCHEMA.album_table_changes
        WHERE metadata$action = 'INSERT'
        AND metadata$isupdate = 'FALSE'
    )
)
UNION
SELECT 
    ALBUM_ID, 
    ALBUM_NAME, 
    RELEASE_DATE, 
    TOTAL_TRACKS, 
    URL,  
    start_time, 
    end_time, 
    is_current
FROM (
    SELECT 
        ALBUM_ID, 
        ALBUM_NAME, 
        RELEASE_DATE, 
        TOTAL_TRACKS, 
        URL, 
        update_timestamp AS start_time,
        LAG(update_timestamp) OVER (PARTITION BY ALBUM_ID ORDER BY update_timestamp DESC) AS end_time_raw,
        CASE WHEN end_time_raw IS NULL THEN '9999-12-31'::TIMESTAMP_NTZ ELSE end_time_raw END AS end_time,
        CASE WHEN end_time_raw IS NULL THEN TRUE ELSE FALSE END AS is_current
    FROM (
        -- Identify data to insert into album_history table
        SELECT 
            ALBUM_ID, 
            ALBUM_NAME, 
            RELEASE_DATE, 
            TOTAL_TRACKS, 
            UPDATE_TIMESTAMP
        FROM album_table_changes
        WHERE metadata$action = 'INSERT'
        AND metadata$isupdate = 'TRUE'
        UNION
        -- Identify data in album_history table that needs to be updated
        SELECT 
            AH.ALBUM_ID, 
            NULL, 
            NULL, 
            NULL, 
            NULL, 
            AH.START_TIME
        FROM album_history AH
        INNER JOIN album_table_changes ATC ON AH.ALBUM_ID = ATC.ALBUM_ID
        WHERE ATC.metadata$action = 'DELETE'
        AND ATC.metadata$isupdate = 'TRUE'
        AND AH.is_current = TRUE
    )
)
UNION
SELECT 
    ATC.ALBUM_ID, 
    NULL, 
    NULL, 
    NULL, 
    NULL, 
    CH.START_TIME, 
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
FROM album_history AH
INNER JOIN album_table_changes ATC ON AH.ALBUM_ID = ATC.ALBUM_ID
WHERE ATC.metadata$action = 'DELETE'
AND ATC.metadata$isupdate = 'FALSE'
AND AH.is_current = TRUE;

select * from v_album_change_data

create or replace task tsk_scd_hist warehouse= COMPUTE_WH schedule='1 minute'
ERROR_ON_NONDETERMINISTIC_MERGE=FALSE
as
merge into customer_history ch -- Target table to merge changes from NATION into
using v_customer_change_data ccd -- v_customer_change_data is a view that holds the logic that determines what to insert/update into the customer_history table.
   on ch.CUSTOMER_ID = ccd.CUSTOMER_ID -- CUSTOMER_ID and start_time determine whether there is a unique record in the customer_history table
   and ch.start_time = ccd.start_time
when matched and ccd.dml_type = 'U' then update -- Indicates the record has been updated and is no longer current and the end_time needs to be stamped
    set ch.end_time = ccd.end_time,
        ch.is_current = FALSE
when matched and ccd.dml_type = 'D' then update -- Deletes are essentially logical deletes. The record is stamped and no newer version is inserted
   set ch.end_time = ccd.end_time,
       ch.is_current = FALSE
when not matched and ccd.dml_type = 'I' then insert -- Inserting a new CUSTOMER_ID and updating an existing one both result in an insert
          (CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, STREET, CITY,STATE,COUNTRY, start_time, end_time, is_current)
    values (ccd.CUSTOMER_ID, ccd.FIRST_NAME, ccd.LAST_NAME, ccd.EMAIL, ccd.STREET, ccd.CITY,ccd.STATE,ccd.COUNTRY, ccd.start_time, ccd.end_time, ccd.is_current);



