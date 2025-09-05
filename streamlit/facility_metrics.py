import streamlit as st
import pandas as pd
import plotly.express as px

def facility_metrics(occupancy_rate_df, bed_utilization_df, staffing_occupancy_df, hospital_throughput_df, provider_staffing_df):
    st.header("Facility Metrics")
    hospital_occupancy_tab, bed_utilization_rate_tab, staffing_occupancy_tab, hospital_throughput_tab, provider_staffing_tab = st.tabs(["Hospital Occupancy Rate Trend - By Month", "Hospital Occupancy Rate Trend - By Provider", "Staffing and Hospital Occupancy Comparison", "Hospital Throughput", " Staffing & Patient Load Comparison"])

    with hospital_occupancy_tab:
        occupancy_rate_df['ReportingMonth'] = pd.to_datetime(occupancy_rate_df['ReportingMonth'])

        # --- Main Dashboard ---
        st.title("Hospital Occupancy Rate Trends")
        st.markdown("This dashboard visualizes the monthly occupancy rate trends over time.")

        st.markdown("---")

        # Display Key Metrics
        col1, col2 = st.columns(2)
        with col1:
            latest_occupancy = occupancy_rate_df['AverageOccupancyRate'].iloc[-1] if not occupancy_rate_df.empty else 0
            st.metric("Latest Occupancy Rate", f"{latest_occupancy}%")
        with col2:
            latest_month = occupancy_rate_df['ReportingMonth'].iloc[-1].strftime('%B %Y') if not occupancy_rate_df.empty else "N/A"
            st.metric("Latest Data Month", latest_month)
        st.markdown("---")
        st.header("Raw Data")
        st.dataframe(occupancy_rate_df)

        st.markdown("""_**Conclusion:**_ This table provides the average monthly hospital occupancy rate for the month of
                    October 1st, 2024 as this is the only date present in the source table. 
                    Occupancy Rate is calculated as (Total Residents / Total Certified Beds).
                    The source data did not have enough information to calculate more information than this.""")


    with bed_utilization_rate_tab:
        st.title("Hospital Bed Utilization Rates")
        st.markdown("This dashboard visualizes the bed utilization rate for each hospital.")

        st.markdown("---")

        # Display Key Metrics
        col1, col2 = st.columns(2)
        with col1:
            top_hospital = bed_utilization_df.iloc[0]['Provider Name']
            top_rate = bed_utilization_df.iloc[0]['BedUtilizationRate']
            st.metric("Highest Bed Utilization", f"{top_hospital}: {top_rate}%")
        with col2:
            avg_utilization = bed_utilization_df['BedUtilizationRate'].mean()
            st.metric("Average Bed Utilization", f"{avg_utilization:.2f}%")
        
        st.markdown("---")

        # --- Visualizations ---
        st.header("Bed Utilization Rates by Hospital")
        
        # Use a bar chart to visualize the rates
        # Drop rows with NaN values to prevent plotting errors
        bed_utilization_df = bed_utilization_df.dropna()
        fig_scatter = px.scatter(
            bed_utilization_df,
            x="TotalCertifiedBeds",
            y="BedUtilizationRate",
            size="TotalResidents",
            color="BedUtilizationRate",
            hover_name="Provider Name",
            title="Total Certified Beds vs. Bed Utilization Rate",
            labels={
                "TotalCertifiedBeds": "Total Certified Beds",
                "BedUtilizationRate": "Bed Utilization Rate (%)",
                "TotalResidents": "Total Residents"
            },
            color_continuous_scale=px.colors.sequential.Viridis,
        )
        fig_scatter.update_layout(font=dict(family="Inter", size=14))
        st.plotly_chart(fig_scatter, use_container_width=True)

        st.markdown("---")
        st.header("Raw Data")
        st.dataframe(bed_utilization_df)
            
        st.markdown("""_**Conclusion:**_ This dashboard highlights the average bed utilization rate across all providers and identifies the provider with the highest utilization. 
                    The data table offers detailed occupancy information for each provider. The scatter plot shows that providers with more certified beds but fewer residents tend to have lower utilization rates, while those with fewer beds and higher resident counts demonstrate higher utilization.""")

    with staffing_occupancy_tab:
        st.title("Staffing vs. Occupancy Analysis")
        st.markdown("This dashboard compares staffing levels with bed occupancy rates for various healthcare providers.")

        st.markdown("---")

        # Display Key Metrics
        col1, col2 = st.columns(2)
        with col1:
            top_hospital = staffing_occupancy_df.iloc[0]['Provider Name']
            top_rate = staffing_occupancy_df.iloc[0]['BedUtilizationRate']
            st.metric("Highest Bed Utilization", f"{top_hospital}: {top_rate}%")
        with col2:
            avg_utilization = staffing_occupancy_df['BedUtilizationRate'].mean()
            st.metric("Average Bed Utilization", f"{avg_utilization:.2f}%")
        
        st.markdown("---")

        # --- Visualization ---
        st.header("Staffing vs. Bed Utilization Rate")
        st.markdown("This scatter plot shows the relationship between total resident staffing hours and the bed utilization rate.")
        
        fig = px.scatter(
            staffing_occupancy_df,
            x="TotalResidentStaffingHours",
            y="BedUtilizationRate",
            size="TotalResidents",
            color="BedUtilizationRate",
            hover_name="Provider Name",
            title="Total Staffing Hours vs. Bed Utilization Rate",
            labels={
                "TotalResidentStaffingHours": "Total Resident Staffing Hours",
                "BedUtilizationRate": "Bed Utilization Rate (%)",
                "TotalResidents": "Total Residents"
            },
            color_continuous_scale=px.colors.sequential.Viridis
        )
        fig.update_layout(font=dict(family="Inter", size=14))
        st.plotly_chart(fig, use_container_width=True)

        st.header("Raw Data")
        st.dataframe(staffing_occupancy_df)

        st.markdown("""_**Conclusion:**_ The scatter plot indicates that hospitals with a larger number of residents tend to have higher bed utilization rates, and these hospitals also report more staffing hours. 
                    A few outliers show utilization rates above 100%, which may reflect emergency situations. 
                    Conversely, some hospitals display low bed utilization despite high staffing hours, likely due to unusually high resident counts.""")

    with hospital_throughput_tab:
        hospital_throughput_df = hospital_throughput_df.sort_values(by="PATIENTTHROUGHPUTSCORE", ascending=True)

        # --- Main Dashboard ---
        st.title("Top 10 Hospitals by Patient Throughput")
        st.markdown("This dashboard ranks the top 10 hospitals based on their patient throughput, measured by the rate of successful return to home or community.")

        st.markdown("---")

        # Display Key Metrics
        col1, col2 = st.columns(2)
        with col1:
            top_hospital = hospital_throughput_df.iloc[-1]['Provider Name']
            top_rate = hospital_throughput_df.iloc[-1]['PATIENTTHROUGHPUTSCORE']
            st.metric("Top Performer", f"{top_hospital}")
        with col2:
            st.metric("Top Score", f"{top_rate}%")
        
        st.markdown("---")

        # --- Visualization ---
        st.header("Patient Throughput Scores")
        st.markdown("This bar chart visualizes the successful patient return rate for the top 10 hospitals.")
        
        fig = px.bar(
            hospital_throughput_df,
            x="PATIENTTHROUGHPUTSCORE",
            y="Provider Name",
            orientation='h',
            title="Patient Throughput Rate (Rate of Successful Return)",
            labels={
                "PATIENTTHROUGHPUTSCORE": "Patient Throughput Score (%)",
                "Provider Name": "Hospital Provider"
            },
            color_discrete_sequence=px.colors.sequential.Viridis_r,
        )
        fig.update_layout(font=dict(family="Inter", size=14))
        st.plotly_chart(fig, use_container_width=True)

        st.header("Raw Data")
        st.dataframe(hospital_throughput_df)
    
    with provider_staffing_tab:
        # Sort for consistent visualization
        provider_staffing_df = provider_staffing_df.sort_values(by="StaffingHoursPerResident", ascending=True)

        # --- Main Dashboard ---
        st.title("Hospitals with Lowest Staffing per Patient")
        st.markdown("This dashboard ranks the top 10 hospitals with the lowest reported total nurse staffing hours per resident per day.")

        st.markdown("---")

        # Display Key Metrics
        col1, col2 = st.columns(2)
        with col1:
            lowest_staffing_provider = provider_staffing_df.iloc[0]['Provider Name']
            lowest_staffing_rate = provider_staffing_df.iloc[0]['StaffingHoursPerResident']
            st.metric("Lowest Staffing", f"{lowest_staffing_provider}")
        with col2:
            st.metric("Staffing Rate", f"{lowest_staffing_rate} hours/day")
        
        st.markdown("---")

        # --- Visualization ---
        st.header("Staffing Levels per Resident")
        st.markdown("This bar chart visualizes the total staffing hours per resident for the 10 facilities with the lowest levels.")
        
        fig = px.bar(
            provider_staffing_df,
            x="StaffingHoursPerResident",
            y="Provider Name",
            orientation='h',
            title="Staffing Hours per Resident per Day",
            labels={
                "StaffingHoursPerResident": "Staffing Hours per Resident (hours/day)",
                "Provider Name": "Hospital Provider"
            },
            color_discrete_sequence=px.colors.sequential.Inferno_r,
        )
        fig.update_layout(font=dict(family="Inter", size=14))
        st.plotly_chart(fig, use_container_width=True)

        st.header("Raw Data")
        st.dataframe(provider_staffing_df)

        st.markdown("""_**Conclusion:**_ The dashboard highlights the 10 hospitals with the lowest staffing per patient, indicating potential areas where additional staff may be needed to improve care quality.""")
