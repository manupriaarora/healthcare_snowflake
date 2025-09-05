import snowflake.connector
import csv
from io import StringIO

from cryptography.hazmat.primitives import serialization

# Load your private key
with open("/Users/manupriyaarora/rsa_private_key.pem", "rb") as key_file:
    p_key = serialization.load_pem_private_key(
        key_file.read(),
        password=None
    )

conn = snowflake.connector.connect(
    user='MANUSNOWFLAKE',
    account='MWOPCMB-JC54670',
    private_key=p_key,
    warehouse='COMPUTE_WH',
    database='HEALTHCARE',
    schema='RAW'
)
cursor = conn.cursor()

file_path = '/Users/manupriyaarora/Documents/Personal/Data_Engineering_academy/EndToEndProjects/Project2_HealthCareMetrics/PBJ_Daily_Nurse_Staffing_Q2_2024.csv'
# Sample header (replace with actual header from CSV)
with open(file_path, 'r') as f:
    reader = csv.reader(f)
    headers = next(reader)  # Get first row as header

columns = ', '.join([f'"{h}" STRING' for h in headers])  # Quote column names
print(f"List all columns: {columns}")
cursor.execute(f"""
    CREATE OR REPLACE TABLE daily_nurse_staffing (
        {columns}
    )
""")
cursor.execute("""
CREATE OR REPLACE PIPE daily_nurse_staffing_raw_pipe
AUTO_INGEST = TRUE
AS
COPY INTO daily_nurse_staffing
FROM @S3_stage/PBJ_Daily_Nurse_Staffing_Q2_2024.csv
FILE_FORMAT = csv_no_header;
""")
# -- Stream on daily_nurse_staffing raw table
cursor.execute("""
CREATE OR REPLACE STREAM HEALTHCARE.RAW.daily_nurse_staffing_raw_stream
ON TABLE HEALTHCARE.RAW.DAILY_NURSE_STAFFING
APPEND_ONLY = TRUE;
""")
# Creating staging table
cursor.execute("""
CREATE OR REPLACE TABLE staging.daily_nurse_staffing_staging (
    PROVNUM VARCHAR,
	PROVNAME VARCHAR,
	CITY VARCHAR,
	STATE VARCHAR,
	COUNTY_NAME VARCHAR,
	COUNTY_FIPS INT,
	"CY_Qtr" VARCHAR,
	"WorkDate" date,
	"MDScensus" int,
	"Hrs_RNDON" float,
	"Hrs_RNDON_emp" float,
	"Hrs_RNDON_ctr" float,
	"Hrs_RNadmin" float,
	"Hrs_RNadmin_emp" float,
	"Hrs_RNadmin_ctr" float,
	"Hrs_RN" float,
	"Hrs_RN_emp" float,
	"Hrs_RN_ctr" float,
	"Hrs_LPNadmin" float,
	"Hrs_LPNadmin_emp" float,
	"Hrs_LPNadmin_ctr" float,
	"Hrs_LPN" float,
	"Hrs_LPN_emp" float,
	"Hrs_LPN_ctr" float,
	"Hrs_CNA" float,
	"Hrs_CNA_emp" float,
	"Hrs_CNA_ctr" float,
	"Hrs_NAtrn" float,
	"Hrs_NAtrn_emp" float,
	"Hrs_NAtrn_ctr" float,
	"Hrs_MedAide" float,
	"Hrs_MedAide_emp" float,
	"Hrs_MedAide_ctr" float
);  
""")
# -- create task on raw stream
cursor.execute("""
CREATE OR REPLACE TASK healthcare.staging.load_nursing_staging_task
WAREHOUSE = 'compute_wh'
SCHEDULE = '5 MINUTE'
WHEN SYSTEM$STREAM_HAS_DATA('HEALTHCARE.RAW.daily_nurse_staffing_raw_stream') AS
INSERT INTO healthcare.staging.daily_nurse_staffing_staging (
    PROVNUM,
    PROVNAME,
    CITY,
    STATE,
    "CY_Qtr",
    "WorkDate",
    "MDScensus",
    "Hrs_RNDON",
    "Hrs_RNDON_emp",
    "Hrs_RNDON_ctr",
    "Hrs_RNadmin",
    "Hrs_RNadmin_emp",
    "Hrs_RNadmin_ctr",
    "Hrs_RN",
    "Hrs_RN_emp",
    "Hrs_RN_ctr",
    "Hrs_LPNadmin",
    "Hrs_LPNadmin_emp",
    "Hrs_LPNadmin_ctr",
    "Hrs_LPN",
    "Hrs_LPN_emp",
    "Hrs_LPN_ctr",
    "Hrs_CNA",
    "Hrs_CNA_emp",
    "Hrs_CNA_ctr",
    "Hrs_NAtrn",
    "Hrs_NAtrn_emp",
    "Hrs_NAtrn_ctr",
    "Hrs_MedAide",
    "Hrs_MedAide_emp",
    "Hrs_MedAide_ctr"
)
SELECT
    PROVNUM,
    PROVNAME,
    CITY,
    STATE,
    "CY_Qtr",
    TO_DATE("WorkDate", 'YYYYMMDD'),           
    TRY_CAST("MDScensus" AS INT),
    TRY_CAST("Hrs_RNDON" AS FLOAT),
    TRY_CAST("Hrs_RNDON_emp" AS FLOAT),
    TRY_CAST("Hrs_RNDON_ctr" AS FLOAT),
    TRY_CAST("Hrs_RNadmin" AS FLOAT),
    TRY_CAST("Hrs_RNadmin_emp" AS FLOAT),
    TRY_CAST("Hrs_RNadmin_ctr" AS FLOAT),
    TRY_CAST("Hrs_RN" AS FLOAT),
    TRY_CAST("Hrs_RN_emp" AS FLOAT),
    TRY_CAST("Hrs_RN_ctr" AS FLOAT),
    TRY_CAST("Hrs_LPNadmin" AS FLOAT),
    TRY_CAST("Hrs_LPNadmin_emp" AS FLOAT),
    TRY_CAST("Hrs_LPNadmin_ctr" AS FLOAT),
    TRY_CAST("Hrs_LPN" AS FLOAT),
    TRY_CAST("Hrs_LPN_emp" AS FLOAT),
    TRY_CAST("Hrs_LPN_ctr" AS FLOAT),
    TRY_CAST("Hrs_CNA" AS FLOAT),
    TRY_CAST("Hrs_CNA_emp" AS FLOAT),
    TRY_CAST("Hrs_CNA_ctr" AS FLOAT),
    TRY_CAST("Hrs_NAtrn" AS FLOAT),
    TRY_CAST("Hrs_NAtrn_emp" AS FLOAT),
    TRY_CAST("Hrs_NAtrn_ctr" AS FLOAT),
    TRY_CAST("Hrs_MedAide" AS FLOAT),
    TRY_CAST("Hrs_MedAide_emp" AS FLOAT),
    TRY_CAST("Hrs_MedAide_ctr" AS FLOAT)
FROM HEALTHCARE.RAW.daily_nurse_staffing_raw_stream;
""")
# Creating target table
cursor.execute("""
CREATE OR REPLACE TABLE public.daily_nurse_staffing_target (
    PROVNUM VARCHAR,
	PROVNAME VARCHAR,
	CITY VARCHAR,
	STATE VARCHAR,
	COUNTY_NAME VARCHAR,
	COUNTY_FIPS INT,
	"CY_Qtr" VARCHAR,
	"WorkDate" date,
	"MDScensus" int,
	"Hrs_RNDON" float,
	"Hrs_RNDON_emp" float,
	"Hrs_RNDON_ctr" float,
	"Hrs_RNadmin" float,
	"Hrs_RNadmin_emp" float,
	"Hrs_RNadmin_ctr" float,
	"Hrs_RN" float,
	"Hrs_RN_emp" float,
	"Hrs_RN_ctr" float,
	"Hrs_LPNadmin" float,
	"Hrs_LPNadmin_emp" float,
	"Hrs_LPNadmin_ctr" float,
	"Hrs_LPN" float,
	"Hrs_LPN_emp" float,
	"Hrs_LPN_ctr" float,
	"Hrs_CNA" float,
	"Hrs_CNA_emp" float,
	"Hrs_CNA_ctr" float,
	"Hrs_NAtrn" float,
	"Hrs_NAtrn_emp" float,
	"Hrs_NAtrn_ctr" float,
	"Hrs_MedAide" float,
	"Hrs_MedAide_emp" float,
	"Hrs_MedAide_ctr" float
);  
""")
# -- create stream on staging table
cursor.execute("""
CREATE OR REPLACE STREAM HEALTHCARE.STAGING.daily_nurse_staffing_staging_stream
ON TABLE healthcare.staging.daily_nurse_staffing_staging
APPEND_ONLY = TRUE;
""")
# incremental load from the staging table to the final target table
# child task that runs only AFTER the staging load task completes successfully.
cursor.execute("""
CREATE OR REPLACE TASK healthcare.staging.load_nursing_target_task
WAREHOUSE = 'compute_wh'
AFTER healthcare.staging.load_nursing_staging_task
WHEN SYSTEM$STREAM_HAS_DATA('healthcare.staging.daily_nurse_staffing_staging_stream') AS
MERGE INTO public.daily_nurse_staffing_target AS target
USING healthcare.staging.daily_nurse_staffing_staging_stream AS staging
ON target.PROVNUM = staging.PROVNUM
WHEN MATCHED THEN
    UPDATE SET
        target.PROVNAME = staging.PROVNAME,
        target.CITY = staging.CITY,
        target.STATE = staging.STATE,
        target.COUNTY_NAME = staging.COUNTY_NAME,
        target.COUNTY_FIPS = staging.COUNTY_FIPS,
        target."CY_Qtr" = staging."CY_Qtr",
        target."WorkDate" = staging."WorkDate",
        target."MDScensus" = staging."MDScensus",
        target."Hrs_RNDON" = staging."Hrs_RNDON",
        target."Hrs_RNDON_emp" = staging."Hrs_RNDON_emp",
        target."Hrs_RNDON_ctr" = staging."Hrs_RNDON_ctr",
        target."Hrs_RNadmin" = staging."Hrs_RNadmin",
        target."Hrs_RNadmin_emp" = staging."Hrs_RNadmin_emp",
        target."Hrs_RNadmin_ctr" = staging."Hrs_RNadmin_ctr",
        target."Hrs_RN" = staging."Hrs_RN",
        target."Hrs_RN_emp" = staging."Hrs_RN_emp",
        target."Hrs_RN_ctr" = staging."Hrs_RN_ctr",
        target."Hrs_LPNadmin" = staging."Hrs_LPNadmin",
        target."Hrs_LPNadmin_emp" = staging."Hrs_LPNadmin_emp",
        target."Hrs_LPNadmin_ctr" = staging."Hrs_LPNadmin_ctr",
        target."Hrs_LPN" = staging."Hrs_LPN",
        target."Hrs_LPN_emp" = staging."Hrs_LPN_emp",
        target."Hrs_LPN_ctr" = staging."Hrs_LPN_ctr",
        target."Hrs_CNA" = staging."Hrs_CNA",
        target."Hrs_CNA_emp" = staging."Hrs_CNA_emp",
        target."Hrs_CNA_ctr" = staging."Hrs_CNA_ctr",
        target."Hrs_NAtrn" = staging."Hrs_NAtrn",
        target."Hrs_NAtrn_emp" = staging."Hrs_NAtrn_emp",
        target."Hrs_NAtrn_ctr" = staging."Hrs_NAtrn_ctr",
        target."Hrs_MedAide" = staging."Hrs_MedAide",
        target."Hrs_MedAide_emp" = staging."Hrs_MedAide_emp",
        target."Hrs_MedAide_ctr" = staging."Hrs_MedAide_ctr"
WHEN NOT MATCHED THEN
    INSERT (
        PROVNUM,
        PROVNAME,
        CITY,
        STATE,
        COUNTY_NAME,
        COUNTY_FIPS,
        "CY_Qtr",
        "WorkDate",
        "MDScensus",
        "Hrs_RNDON",
        "Hrs_RNDON_emp",
        "Hrs_RNDON_ctr",
        "Hrs_RNadmin",
        "Hrs_RNadmin_emp",
        "Hrs_RNadmin_ctr",
        "Hrs_RN",
        "Hrs_RN_emp",
        "Hrs_RN_ctr",
        "Hrs_LPNadmin",
        "Hrs_LPNadmin_emp",
        "Hrs_LPNadmin_ctr",
        "Hrs_LPN",
        "Hrs_LPN_emp",
        "Hrs_LPN_ctr",
        "Hrs_CNA",
        "Hrs_CNA_emp",
        "Hrs_CNA_ctr",
        "Hrs_NAtrn",
        "Hrs_NAtrn_emp",
        "Hrs_NAtrn_ctr",
        "Hrs_MedAide",
        "Hrs_MedAide_emp",
        "Hrs_MedAide_ctr"
    )
    VALUES (
        staging.PROVNUM,
        staging.PROVNAME,
        staging.CITY,
        staging.STATE,
        staging.COUNTY_NAME,
        staging.COUNTY_FIPS,
        staging."CY_Qtr",
        staging."WorkDate",
        staging."MDScensus",
        staging."Hrs_RNDON",
        staging."Hrs_RNDON_emp",
        staging."Hrs_RNDON_ctr",
        staging."Hrs_RNadmin",
        staging."Hrs_RNadmin_emp",
        staging."Hrs_RNadmin_ctr",
        staging."Hrs_RN",
        staging."Hrs_RN_emp",
        staging."Hrs_RN_ctr",
        staging."Hrs_LPNadmin",
        staging."Hrs_LPNadmin_emp",
        staging."Hrs_LPNadmin_ctr",
        staging."Hrs_LPN",
        staging."Hrs_LPN_emp",
        staging."Hrs_LPN_ctr",
        staging."Hrs_CNA",
        staging."Hrs_CNA_emp",
        staging."Hrs_CNA_ctr",
        staging."Hrs_NAtrn",
        staging."Hrs_NAtrn_emp",
        staging."Hrs_NAtrn_ctr",
        staging."Hrs_MedAide",
        staging."Hrs_MedAide_emp",
        staging."Hrs_MedAide_ctr"
    );

""")
cursor.close()
conn.close()