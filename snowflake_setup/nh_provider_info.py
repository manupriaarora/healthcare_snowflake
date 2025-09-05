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

file_path = '/Users/manupriyaarora/Documents/Personal/Data_Engineering_academy/EndToEndProjects/Project2_HealthCareMetrics/NH_ProviderInfo_Oct2024.csv'
# Sample header (replace with actual header from CSV)
with open(file_path, 'r') as f:
    reader = csv.reader(f)
    headers = next(reader)  # Get first row as header

columns = ', '.join([f'"{h}" STRING' for h in headers])  # Quote column names
print(f"List all columns: {columns}")
cursor.execute(f"""
    CREATE OR REPLACE TABLE nh_provider_info (
        {columns}
    )
""")
cursor.execute("""
CREATE OR REPLACE PIPE nh_provider_info_raw_pipe
  AUTO_INGEST = TRUE
AS
COPY INTO nh_provider_info
FROM @S3_stage/NH_ProviderInfo_Oct2024.csv
FILE_FORMAT = csv_no_header;
""")
cursor.execute("""
CREATE OR REPLACE TABLE staging.nh_provider_info_staging (
  "CMS Certification Number (CCN)" VARCHAR, 
  "Provider Name" VARCHAR,
  "Provider Address" VARCHAR,
  "City/Town" VARCHAR,
  "State" VARCHAR,
  "Average Number of Residents per Day" FLOAT, 
  "Number of Certified Beds" INT, 
  "Reported Total Nurse Staffing Hours per Resident per Day" FLOAT,
  "Reported RN Staffing Hours per Resident per Day" FLOAT,
  "Reported LPN Staffing Hours per Resident per Day" FLOAT,
  "Reported Nurse Aide Staffing Hours per Resident per Day" FLOAT,
  "Number of Facility Reported Incidents" INT,
  "Total nursing staff turnover" FLOAT, 
  "Registered Nurse turnover" FLOAT,
  "Processing Date" DATE
);  
""")
cursor.execute("""
CREATE OR REPLACE TABLE public.nh_provider_info_target (
  "CMS Certification Number (CCN)" VARCHAR, 
  "Provider Name" VARCHAR,
  "Provider Address" VARCHAR,
  "City/Town" VARCHAR,
  "State" VARCHAR,
  "Average Number of Residents per Day" FLOAT, 
  "Number of Certified Beds" INT, 
  "Reported Total Nurse Staffing Hours per Resident per Day" FLOAT,
  "Reported RN Staffing Hours per Resident per Day" FLOAT,
  "Reported LPN Staffing Hours per Resident per Day" FLOAT,
  "Reported Nurse Aide Staffing Hours per Resident per Day" FLOAT,
  "Number of Facility Reported Incidents" INT,
  "Total nursing staff turnover" FLOAT, 
  "Registered Nurse turnover" FLOAT,
  "Processing Date" DATE
); 
""")
cursor.execute("""
CREATE OR REPLACE STREAM HEALTHCARE.RAW.provider_info_raw_stream
ON TABLE HEALTHCARE.RAW.NH_PROVIDER_INFO
APPEND_ONLY = TRUE; 
""")
cursor.execute("""
CREATE OR REPLACE TASK healthcare.staging.load_provider_info_staging_task 
WAREHOUSE = 'compute_wh' 
SCHEDULE = '5 MINUTE' 
WHEN SYSTEM$STREAM_HAS_DATA('healthcare.raw.provider_info_raw_stream') AS 
INSERT INTO healthcare.staging.nh_provider_info_staging ("CMS Certification Number (CCN)", "Provider Name", "Provider Address", "City/Town", "State", "Average Number of Residents per Day", "Number of Certified Beds", "Reported Total Nurse Staffing Hours per Resident per Day", "Reported RN Staffing Hours per Resident per Day", "Reported LPN Staffing Hours per Resident per Day", "Reported Nurse Aide Staffing Hours per Resident per Day", "Number of Facility Reported Incidents", "Total nursing staff turnover", "Registered Nurse turnover", "Processing Date") SELECT "CMS Certification Number (CCN)", "Provider Name", "Provider Address", "City/Town", "State", TRY_CAST("Average Number of Residents per Day" AS FLOAT), TRY_CAST("Number of Certified Beds" AS INTEGER), TRY_CAST("Reported Total Nurse Staffing Hours per Resident per Day" AS FLOAT), TRY_CAST("Reported RN Staffing Hours per Resident per Day" AS FLOAT), TRY_CAST("Reported LPN Staffing Hours per Resident per Day" AS FLOAT), TRY_CAST("Reported Nurse Aide Staffing Hours per Resident per Day" AS FLOAT), TRY_CAST("Number of Facility Reported Incidents" AS INTEGER), TRY_CAST("Total nursing staff turnover" AS FLOAT), TRY_CAST("Registered Nurse turnover" AS FLOAT), TRY_CAST("Processing Date" AS DATE) FROM healthcare.raw.provider_info_raw_stream;
""")
cursor.execute("""
CREATE OR REPLACE STREAM HEALTHCARE.STAGING.provider_info_staging_stream
ON TABLE HEALTHCARE.staging.nh_provider_info_staging
APPEND_ONLY = TRUE;
""")
cursor.execute("""
CREATE OR REPLACE TASK healthcare.staging.provider_info_target_task
WAREHOUSE = 'compute_wh'
AFTER healthcare.staging.load_provider_info_staging_task
WHEN SYSTEM$STREAM_HAS_DATA('healthcare.staging.provider_info_staging_stream')
AS
MERGE INTO healthcare.public.nh_provider_info_target AS target
USING healthcare.staging.nh_provider_info_staging AS source
ON target."CMS Certification Number (CCN)" = source."CMS Certification Number (CCN)"
WHEN MATCHED THEN UPDATE SET
    "Provider Name" = source."Provider Name",
    "Provider Address" = source."Provider Address",
    "City/Town" = source."City/Town",
    "State" = source."State",
    "Average Number of Residents per Day" = source."Average Number of Residents per Day",
    "Number of Certified Beds" = source."Number of Certified Beds",
    "Reported Total Nurse Staffing Hours per Resident per Day" = source."Reported Total Nurse Staffing Hours per Resident per Day",
    "Reported RN Staffing Hours per Resident per Day" = source."Reported RN Staffing Hours per Resident per Day",
    "Reported LPN Staffing Hours per Resident per Day" = source."Reported LPN Staffing Hours per Resident per Day",
    "Reported Nurse Aide Staffing Hours per Resident per Day" = source."Reported Nurse Aide Staffing Hours per Resident per Day",
    "Number of Facility Reported Incidents" = source."Number of Facility Reported Incidents",
    "Total nursing staff turnover" = source."Total nursing staff turnover",
    "Registered Nurse turnover" = source."Registered Nurse turnover",
    "Processing Date" = source."Processing Date"
WHEN NOT MATCHED THEN INSERT (
    "CMS Certification Number (CCN)",
    "Provider Name",
    "Provider Address",
    "City/Town",
    "State",
    "Average Number of Residents per Day",
    "Number of Certified Beds",
    "Reported Total Nurse Staffing Hours per Resident per Day",
    "Reported RN Staffing Hours per Resident per Day",
    "Reported LPN Staffing Hours per Resident per Day",
    "Reported Nurse Aide Staffing Hours per Resident per Day",
    "Number of Facility Reported Incidents",
    "Total nursing staff turnover",
    "Registered Nurse turnover",
    "Processing Date"
)
VALUES (
    source."CMS Certification Number (CCN)",
    source."Provider Name",
    source."Provider Address",
    source."City/Town",
    source."State",
    source."Average Number of Residents per Day",
    source."Number of Certified Beds",
    source."Reported Total Nurse Staffing Hours per Resident per Day",
    source."Reported RN Staffing Hours per Resident per Day",
    source."Reported LPN Staffing Hours per Resident per Day",
    source."Reported Nurse Aide Staffing Hours per Resident per Day",
    source."Number of Facility Reported Incidents",
    source."Total nursing staff turnover",
    source."Registered Nurse turnover",
    source."Processing Date"
);
""")
cursor.close()
conn.close()