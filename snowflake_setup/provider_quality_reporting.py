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

file_path = '/Users/manupriyaarora/Documents/Personal/Data_Engineering_academy/EndToEndProjects/Project2_HealthCareMetrics/Skilled_Nursing_Facility_Quality_Reporting_Program_Provider_Data_Oct2024.csv'
# Sample header (replace with actual header from CSV)
with open(file_path, 'r') as f:
    reader = csv.reader(f)
    headers = next(reader)  # Get first row as header

columns = ', '.join([f'"{h}" STRING' for h in headers])  # Quote column names
print(f"List all columns: {columns}")
cursor.execute(f"""
    CREATE OR REPLACE TABLE quality_reporting_provider (
        {columns}
    )
""")
cursor.execute("""
CREATE OR REPLACE PIPE quality_reporting_provider_raw_pipe
AUTO_INGEST = TRUE
AS
COPY INTO quality_reporting_provider
FROM @S3_stage/Skilled_Nursing_Facility_Quality_Reporting_Program_Provider_Data_Oct2024.csv
FILE_FORMAT = csv_no_header;
""")
cursor.execute("""
CREATE OR REPLACE TABLE staging.provider_quality_reporting_staging (
    "CMS Certification Number (CCN)" VARCHAR,
	"Provider Name" VARCHAR,
	"Address Line 1" VARCHAR,
	"City/Town" VARCHAR,
	"State" VARCHAR,
	"ZIP Code" VARCHAR,
	"County/Parish" VARCHAR,
	"Telephone Number" VARCHAR,
	"CMS Region" int,
	"Measure Code" VARCHAR,
	"Score" float,
	"Footnote" VARCHAR,
	"Start Date" date,
	"End Date" date,
	"Measure Date Range" VARCHAR,
	LOCATION1 VARCHAR
);  
""")
cursor.execute("""
CREATE OR REPLACE TABLE public.provider_quality_reporting_target (
    "CMS Certification Number (CCN)" VARCHAR,
	"Provider Name" VARCHAR,
	"Address Line 1" VARCHAR,
	"City/Town" VARCHAR,
	"State" VARCHAR,
	"ZIP Code" VARCHAR,
	"County/Parish" VARCHAR,
	"Telephone Number" VARCHAR,
	"CMS Region" int,
	"Measure Code" VARCHAR,
	"Score" float,
	"Footnote" VARCHAR,
	"Start Date" date,
	"End Date" date,
	"Measure Date Range" VARCHAR,
	LOCATION1 VARCHAR
); 
""")
cursor.execute("""
CREATE OR REPLACE STREAM HEALTHCARE.RAW.quality_reporting_provider_stream
ON TABLE HEALTHCARE.RAW.QUALITY_REPORTING_PROVIDER
APPEND_ONLY = TRUE;
""")
cursor.execute("""
CREATE OR REPLACE TASK healthcare.staging.load_quality_reporting_staging_task
WAREHOUSE = 'compute_wh'
SCHEDULE = '5 MINUTE'
WHEN SYSTEM$STREAM_HAS_DATA('HEALTHCARE.RAW.quality_reporting_provider_stream') AS
INSERT INTO healthcare.staging.provider_quality_reporting_staging (
    "CMS Certification Number (CCN)",
	"Provider Name",
	"Address Line 1",
	"City/Town",
	"State",
	"ZIP Code",
	"County/Parish",
	"Telephone Number",
	"CMS Region",
	"Measure Code",
	"Score",
	"Footnote",
	"Start Date",
	"End Date",
	"Measure Date Range",
	"LOCATION1"
)
SELECT
    "CMS Certification Number (CCN)",
    "Provider Name",
    "Address Line 1",
    "City/Town",
    "State",
    "ZIP Code",
    "County/Parish",
    "Telephone Number",
    TRY_CAST("CMS Region" AS INT),
    "Measure Code",
    TRY_CAST("Score" AS FLOAT),
    "Footnote",
    TRY_CAST("Start Date" AS DATE),
    TRY_CAST("End Date" AS DATE),
    "Measure Date Range",
    "LOCATION1"
FROM HEALTHCARE.RAW.quality_reporting_provider_stream;
""")
cursor.execute("""
CREATE OR REPLACE STREAM HEALTHCARE.STAGING.quality_reporting_provider_staging_stream
ON TABLE HEALTHCARE.staging.provider_quality_reporting_staging
APPEND_ONLY = TRUE;
""")
cursor.execute("""
CREATE OR REPLACE TASK healthcare.staging.load_quality_reporting_target_task
WAREHOUSE = 'compute_wh'
AFTER healthcare.staging.load_quality_reporting_staging_task
WHEN SYSTEM$STREAM_HAS_DATA('HEALTHCARE.STAGING.quality_reporting_provider_staging_stream')
AS
MERGE INTO public.provider_quality_reporting_target AS target
USING HEALTHCARE.STAGING.quality_reporting_provider_staging_stream AS staging
ON target."CMS Certification Number (CCN)" = staging."CMS Certification Number (CCN)"
WHEN MATCHED THEN
    UPDATE SET
        target."Provider Name" = staging."Provider Name",
        target."Address Line 1" = staging."Address Line 1",
        target."City/Town" = staging."City/Town",
        target."State" = staging."State",
        target."ZIP Code" = staging."ZIP Code",
        target."County/Parish" = staging."County/Parish",
        target."Telephone Number" = staging."Telephone Number",
        target."CMS Region" = staging."CMS Region",
        target."Measure Code" = staging."Measure Code",
        target."Score" = staging."Score",
        target."Footnote" = staging."Footnote",
        target."Start Date" = staging."Start Date",
        target."End Date" = staging."End Date",
        target."Measure Date Range" = staging."Measure Date Range",
        target."LOCATION1" = staging."LOCATION1"
WHEN NOT MATCHED THEN
    INSERT (
        "CMS Certification Number (CCN)",
        "Provider Name",
        "Address Line 1",
        "City/Town",
        "State",
        "ZIP Code",
        "County/Parish",
        "Telephone Number",
        "CMS Region",
        "Measure Code",
        "Score",
        "Footnote",
        "Start Date",
        "End Date",
        "Measure Date Range",
        "LOCATION1"
    )
    VALUES (
        staging."CMS Certification Number (CCN)",
        staging."Provider Name",
        staging."Address Line 1",
        staging."City/Town",
        staging."State",
        staging."ZIP Code",
        staging."County/Parish",
        staging."Telephone Number",
        staging."CMS Region",
        staging."Measure Code",
        staging."Score",
        staging."Footnote",
        staging."Start Date",
        staging."End Date",
        staging."Measure Date Range",
        staging."LOCATION1"
    );
""")
cursor.close()
conn.close()