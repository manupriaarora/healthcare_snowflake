import streamlit as st
import pandas as pd
import snowflake.connector
import toml
import os
from cryptography.hazmat.primitives import serialization
from staffing_metrics import staffing_metrics
from facility_metrics import facility_metrics

# Title for the Streamlit app
st.set_page_config(layout="wide")
st.title("Nursing Home Staffing Dashboard")
st.write("This dashboard visualizes key staffing and resident data from the Snowflake tables.")

# --- Snowflake Connection and Data Loading ---
try:
    # Get the absolute path to the secrets.toml file
    current_dir = os.path.dirname(os.path.abspath(__file__))
    secrets_path = os.path.join(current_dir, '.streamlit', 'secrets.toml')
    
    # Load credentials from secrets.toml
    secrets = toml.load(secrets_path)
    snowflake_secrets = secrets.get('snowflake')

    # Get the private key path and passphrase from the secrets
    private_key_path = snowflake_secrets.get('private_key_path')
    private_key_passphrase = snowflake_secrets.get('private_key_passphrase')

    # Load the private key
    with open(private_key_path, "rb") as key_file:
        p_key = serialization.load_pem_private_key(
            key_file.read(),
            password=private_key_passphrase.encode() if private_key_passphrase else None
        )

    # Establish the connection directly using snowflake.connector
    conn = snowflake.connector.connect(
        user=snowflake_secrets.get('user'),
        account=snowflake_secrets.get('account'),
        private_key=p_key,
        warehouse=snowflake_secrets.get('warehouse'),
        database=snowflake_secrets.get('database'),
        schema=snowflake_secrets.get('schema')
    )

except Exception as e:
    st.error("Failed to connect to Snowflake.")
    st.markdown(
        """
        <div style="padding: 1rem; border: 1px solid #ff4b4b; border-radius: 0.5rem; background-color: #ffcccc;">
        <p style="font-weight: bold; color: #ff4b4b;">Connection Error:</p>
        <p style="font-size: 0.9rem;">
        The application was unable to connect to Snowflake. Please ensure your credentials in
        <code>.streamlit/secrets.toml</code> are correct and that the private key path is valid.
        </p>
        <p style="font-size: 0.8rem; font-style: italic;">
        For a more detailed error message, please check the terminal output where you ran `streamlit run`.
        </p>
        </div>
        """,
        unsafe_allow_html=True
    )
    st.stop()

# Function to run the State-level aggregation query and cache the results.
@st.cache_data(ttl=600)  # Cache for 10 minutes
def load_state_data():
    """Loads and aggregates the staffing data by state."""
    query = """
    SELECT
        "State",
        AVG("Average Number of Residents per Day") AS "Average Residents Per Day",
        AVG("Reported Total Nurse Staffing Hours per Resident per Day") AS "Average Total Nurse Staffing Hours",
        AVG("Reported RN Staffing Hours per Resident per Day") AS "Average RN Staffing Hours",
        AVG("Reported LPN Staffing Hours per Resident per Day") AS "Average LPN Staffing Hours",
        AVG("Reported Nurse Aide Staffing Hours per Resident per Day") AS "Average Nurse Aide Staffing Hours",
        DIV0(AVG("Average Number of Residents per Day"), AVG("Reported Total Nurse Staffing Hours per Resident per Day")) AS "Residents to Total Nurse Ratio",
        DIV0(AVG("Average Number of Residents per Day"), AVG("Reported RN Staffing Hours per Resident per Day")) AS "Residents to RN Ratio"
    FROM
        healthcare.staging.nh_provider_info_staging
    GROUP BY
        "State"
    ORDER BY
        "Residents to Total Nurse Ratio" DESC;
    """
    with conn.cursor() as cursor:
        cursor.execute(query)
        df = cursor.fetch_pandas_all()
    return df

# Function to run the Provider-level aggregation query and cache the results.
@st.cache_data(ttl=600)  # Cache for 10 minutes
def load_provider_data():
    """Loads and aggregates the staffing data by Provider"""
    query = """
    SELECT
        "Provider Name",
        AVG("Average Number of Residents per Day") AS "Average Residents Per Day",
        AVG("Reported Total Nurse Staffing Hours per Resident per Day") AS "Average Total Nurse Staffing Hours",
        AVG("Reported RN Staffing Hours per Resident per Day") AS "Average RN Staffing Hours",
        AVG("Reported LPN Staffing Hours per Resident per Day") AS "Average LPN Staffing Hours",
        AVG("Reported Nurse Aide Staffing Hours per Resident per Day") AS "Average Nurse Aide Staffing Hours",
        DIV0(AVG("Average Number of Residents per Day"), AVG("Reported Total Nurse Staffing Hours per Resident per Day")) AS "Residents to Total Nurse Ratio",
        DIV0(AVG("Average Number of Residents per Day"), AVG("Reported RN Staffing Hours per Resident per Day")) AS "Residents to RN Ratio"
    FROM
        healthcare.staging.nh_provider_info_staging
    GROUP BY
        "Provider Name"
    ORDER BY
        "Residents to Total Nurse Ratio" DESC ;
    """
    with conn.cursor() as cursor:
        cursor.execute(query)
        df = cursor.fetch_pandas_all()
    return df


@st.cache_data(ttl=600)  # Cache for 10 minutes
def load_nurse_hours_data():
    """The SQL query to get the aggregated nurse hour data."""
    query = """
    SELECT
        PROVNAME,
        STATE,
        DATE_TRUNC('month', "WorkDate") AS WorkMonth,
        SUM("Hrs_RNDON" + "Hrs_RNadmin" + "Hrs_LPNadmin" + "Hrs_LPN" + "Hrs_CNA" + "Hrs_NAtrn" + "Hrs_MedAide") AS TotalNurseHours
    FROM
        HEALTHCARE.PUBLIC.DAILY_NURSE_STAFFING_TARGET
    GROUP BY
        PROVNAME,
        STATE,
        WorkMonth
    ORDER BY
        TotalNurseHours DESC;
    """
    with conn.cursor() as cursor:
        cursor.execute(query)
        df = cursor.fetch_pandas_all()
    return df


@st.cache_data(ttl=600)  # Cache for 10 minutes
def load_contract_hours_data():
    # This query calculates the total contracted hours for each hospital,
    # using it as a proxy for overtime or high-demand staffing.
    # The results are ordered to show the hospitals with the highest hours at the top.
    query = """
    SELECT
        PROVNAME, -- Hospital name
        STATE,    -- Hospital's state
        SUM("Hrs_RNDON_ctr" + "Hrs_RNadmin_ctr" + "Hrs_LPNadmin_ctr" + "Hrs_LPN_ctr" + "Hrs_CNA_ctr" + "Hrs_NAtrn_ctr" + "Hrs_MedAide_ctr") AS "TotalContractedHours"
    FROM
        HEALTHCARE.PUBLIC.DAILY_NURSE_STAFFING_TARGET
    GROUP BY
        PROVNAME,
        STATE
    ORDER BY
        "TotalContractedHours" DESC
    LIMIT 10; 

    """
    with conn.cursor() as cursor:
        cursor.execute(query)
        df = cursor.fetch_pandas_all()
    return df


@st.cache_data(ttl=600)  # Cache for 10 minutes
def load_health_occupancy_rate_data():
# This query calculates the average monthly hospital occupancy rate for the month of
#  October 1st, 2024 as this is the only date present in the source table. 
# Occupancy Rate is calculated as (Total Residents / Total Certified Beds).
    query = """
    SELECT
        DATE_TRUNC('month', "Processing Date") AS "ReportingMonth",
        SUM("Average Number of Residents per Day") AS "TotalResidents",
        SUM("Number of Certified Beds") AS "TotalCertifiedBeds",
        ROUND(
            SUM("Average Number of Residents per Day") * 100.0 / NULLIF(SUM("Number of Certified Beds"), 0),
            2
        ) AS "AverageOccupancyRate"
    FROM
        HEALTHCARE.PUBLIC.NH_PROVIDER_INFO_TARGET
    WHERE
        "Processing Date" >= '2024-10-01' -- Filters data from October 1st, 2024, onwards
    GROUP BY
        "ReportingMonth"
    ORDER BY
        "ReportingMonth" ASC; -- Orders the results chronologically
    """
    with conn.cursor() as cursor:
        cursor.execute(query)
        df = cursor.fetch_pandas_all()
    return df

@st.cache_data(ttl=600)  # Cache for 10 minutes
def load_bed_utilization_rate_data():
    query = """
        SELECT
            "Provider Name",
            SUM("Average Number of Residents per Day") AS "TotalResidents",
            SUM("Number of Certified Beds") AS "TotalCertifiedBeds",
            ROUND(
                SUM("Average Number of Residents per Day") * 100.0 / NULLIF(SUM("Number of Certified Beds"), 0),
                2
            ) AS "BedUtilizationRate"
        FROM
            HEALTHCARE.PUBLIC.NH_PROVIDER_INFO_TARGET
        WHERE
            "Processing Date" >= '2024-10-01'
        GROUP BY
            "Provider Name"
        HAVING 
            SUM("Average Number of Residents per Day") is not null
        ORDER BY
            "BedUtilizationRate" DESC;
    """
    with conn.cursor() as cursor:
        cursor.execute(query)
        df = cursor.fetch_pandas_all()
    return df

@st.cache_data(ttl=600)  # Cache for 10 minutes
def load_staffing_occupancy_comp_data():
    query = """
    SELECT
        "Provider Name",
        COALESCE(SUM("Average Number of Residents per Day"), 0) AS "TotalResidents",
        COALESCE(SUM("Reported Total Nurse Staffing Hours per Resident per Day" * "Average Number of Residents per Day"), 0) AS "TotalResidentStaffingHours",
        ROUND(
            SUM("Average Number of Residents per Day") * 100.0 / NULLIF(SUM("Number of Certified Beds"), 0),
            2
        ) AS "BedUtilizationRate"
    FROM
        HEALTHCARE.PUBLIC.NH_PROVIDER_INFO_TARGET
    GROUP BY
        "Provider Name"
    HAVING 
        SUM("Average Number of Residents per Day") is not null
    ORDER BY
        "BedUtilizationRate" DESC;
        """
    with conn.cursor() as cursor:
        cursor.execute(query)
        df = cursor.fetch_pandas_all()
    return df

@st.cache_data(ttl=600)  # Cache for 10 minutes
def load_hospital_througput_data():
    query = """
    SELECT 
        "Provider Name",
        "Score" as PatientThroughputScore
    FROM
        HEALTHCARE.PUBLIC.PROVIDER_QUALITY_REPORTING_TARGET
    WHERE
        "Measure Code" = 'S_005_02_DTC_OBS_RATE'
    And "Score" is not null
    ORDER BY
        "Score" DESC
    LIMIT 10;
        """
    with conn.cursor() as cursor:
        cursor.execute(query)
        df = cursor.fetch_pandas_all()
    return df

@st.cache_data(ttl=600)  # Cache for 10 minutes
def load_provider_staffing_data():
    query = """
    SELECT
        "Provider Name",
        "Reported Total Nurse Staffing Hours per Resident per Day" AS "StaffingHoursPerResident"
    FROM
        HEALTHCARE.PUBLIC.NH_PROVIDER_INFO_TARGET
    ORDER BY
        "StaffingHoursPerResident" ASC
    LIMIT 10;
        """
    with conn.cursor() as cursor:
        cursor.execute(query)
        df = cursor.fetch_pandas_all()
    return df

# Load both dataframes
state_df = load_state_data()
provider_df = load_provider_data()
nurse_hours_df = load_nurse_hours_data()
contracting_hours_df = load_contract_hours_data()
occupancy_rate_df = load_health_occupancy_rate_data()
bed_utilization_df = load_bed_utilization_rate_data()
staffing_occupancy_df = load_staffing_occupancy_comp_data()
hospital_throughput_df = load_hospital_througput_data()
provider_staffing_data = load_provider_staffing_data()

# --- Dashboard Layout with a Sidebar for Navigation ---
st.sidebar.header("Dashboard Navigation")
dashboard_group = st.sidebar.radio(
    "Select a Dashboard Group:",
    ["Staffing Metrics", "Facility Metrics"]
)

if dashboard_group == "Staffing Metrics":
    staffing_metrics(state_df, provider_df, nurse_hours_df, contracting_hours_df)
if dashboard_group == "Facility Metrics":
    facility_metrics(occupancy_rate_df, bed_utilization_df, staffing_occupancy_df, hospital_throughput_df, provider_staffing_data)
elif dashboard_group == "Coming Soon!":
    st.markdown("<h3 style='text-align: center;'>More dashboards are on the way!</h3>", unsafe_allow_html=True)
    st.image("https://placehold.co/800x400/D3D3D3/000000?text=Placeholder+for+Future+Dashboard")
