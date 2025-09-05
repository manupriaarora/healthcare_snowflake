import streamlit as st
import pandas as pd
import plotly.express as px

def staffing_metrics(state_df, provider_df, nurse_hours_df, contracting_hours_df):
    """
    Displays the Staffing Metrics Dashboard with State-level and Provider-level data.
    """
    st.header("Staffing Metrics")
    state_tab, provider_tab, nurse_hours_tab, contracting_hours_tab = st.tabs(["State - Resident Nurse Ratio", "Provider - Resident Nurse Ratio", "Nurse Hours", "Contract Hours"])

    with state_tab:
        st.header("State-level Aggregation")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Chart 1: Average Residents per Day by State
            st.subheader("Average Residents per Day by State")
            fig1 = px.bar(
                state_df,
                x="State",
                y="Average Residents Per Day",
                title="Average Residents Per Day by State",
                color="State"
            )
            st.plotly_chart(fig1, use_container_width=True)
        
        with col2:
            # Chart 2: Residents to Total Nurse Ratio by State
            st.subheader("Residents to Total Nurse Ratio by State")
            fig2 = px.bar(
                state_df,
                x="State",
                y="Residents to Total Nurse Ratio",
                title="Average Residents to Total Nurse Ratio",
                color="State"
            )
            st.plotly_chart(fig2, use_container_width=True)

        # Chart 3: Nurse Staffing Hours Distribution
        st.subheader("Distribution of Nurse Staffing Hours")
        staffing_cols = [
            "Average RN Staffing Hours",
            "Average LPN Staffing Hours",
            "Average Nurse Aide Staffing Hours"
        ]
        staffing_df_sum = state_df[staffing_cols].sum().reset_index()
        staffing_df_sum.columns = ['Type', 'Hours']
        fig3 = px.pie(
            staffing_df_sum,
            values='Hours',
            names='Type',
            title='Total Nurse Staffing Hours by Type Across All States'
        )
        st.plotly_chart(fig3, use_container_width=True)

        # Display the data table for state-level data
        st.markdown("---")
        st.subheader("State-level Data Table")
        st.dataframe(state_df, use_container_width=True)

        st.markdown("""_**Conclusion:**_ The first two charts show that New York has the highest average residents per day and the highest resident-to-nurse staffing hours ratio, indicating that nurses in New York care for more residents than in other states. The distribution chart highlights that nurse aide staffing hours exceed those of both RNs and LPNs. The data table provides detailed information on average staffing hours and the resident-to-staffing-hour ratios.""")

    with provider_tab:
        st.header("Provider-level Aggregation")

        # Add a filter for providers
        provider_names = sorted(provider_df['Provider Name'].unique())
        selected_providers = st.multiselect(
            "Select Provider(s) to view:",
            options=provider_names,
            default=provider_names[:10]  # Show first 10 providers by default
        )
        
        if not selected_providers:
            st.warning("Please select at least one provider.")
        else:
            filtered_provider_df = provider_df[provider_df['Provider Name'].isin(selected_providers)]
            
            col1, col2 = st.columns(2)
            
            with col1:
                # Chart 1: Average Residents per Day by Provider
                st.subheader("Average Residents per Day by Provider")
                fig4 = px.bar(
                    filtered_provider_df,
                    x="Provider Name",
                    y="Average Residents Per Day",
                    title="Average Residents Per Day",
                    color="Provider Name"
                )
                st.plotly_chart(fig4, use_container_width=True)

            with col2:
                # Chart 2: Residents to Total Nurse Ratio by Provider
                st.subheader("Residents to Total Nurse Ratio by Provider")
                fig5 = px.bar(
                    filtered_provider_df,
                    x="Provider Name",
                    y="Residents to Total Nurse Ratio",
                    title="Residents to Total Nurse Ratio",
                    color="Provider Name"
                )
                st.plotly_chart(fig5, use_container_width=True)

            # Chart 3: Scatter Plot - Residents vs. Total Nurse Staffing
            st.subheader("Residents vs. Total Nurse Staffing Hours")
            fig6 = px.scatter(
                filtered_provider_df,
                x="Average Residents Per Day",
                y="Average Total Nurse Staffing Hours",
                hover_name="Provider Name",
                title="Residents vs. Total Nurse Staffing Hours"
            )
            st.plotly_chart(fig6, use_container_width=True)

            # Display the data table for provider-level data
            st.markdown("---")
            st.subheader("Provider-level Data Table")
            st.dataframe(filtered_provider_df, use_container_width=True)

            st.markdown("""_**Conclusion:**_ The first two bar charts show that A Holly Patterson Extended Care Facility has the highest average residents per day 
                        and the highest resident-to-nurse staffing hours ratio, suggesting potential nurse 
                        overwork and a possible need for additional staff. However, this could also point to data quality issues. 
                        The Residents vs. Total Nurse Staffing Hours scatter plot does not indicate a clear correlation, as the data points are widely dispersed. The accompanying data table provides detailed insights into daily resident counts and staffing hours.
                        """)

    
    with nurse_hours_tab:
        st.title("Daily Nurse Staffing Analysis")
        st.markdown("Use the filters below to analyze total nurse hours by provider, state, and month.")

        # --- Filters on the Main Page ---
        st.header("Filter Data")
        df = nurse_hours_df
        # Get unique values for filters
        all_states = sorted(df['STATE'].unique())
        selected_states = st.multiselect("Select State(s)", all_states, default=all_states)

        # Filter providers based on selected states
        filtered_providers = df[df['STATE'].isin(selected_states)]['PROVNAME'].unique()
        all_providers = sorted(df['PROVNAME'].unique())
        selected_providers = st.multiselect("Select Provider(s)", all_providers, default=all_providers)

        # --- Apply Filters ---
        filtered_df = df[
            df['STATE'].isin(selected_states) &
            df['PROVNAME'].isin(selected_providers)
        ].copy()

        # Display Key Metrics
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Nurse Hours", f"{filtered_df['TOTALNURSEHOURS'].sum():,.0f} hrs")
        with col2:
            st.metric("Number of Providers", f"{filtered_df['PROVNAME'].nunique():,}")
        with col3:
            st.metric("Number of States", f"{filtered_df['STATE'].nunique():,}")

        st.markdown("---")

        # --- Visualizations ---

        st.header("Total Nurse Hours by Month")
        if not filtered_df.empty:
            monthly_data = filtered_df.groupby('WORKMONTH')['TOTALNURSEHOURS'].sum().reset_index()
            fig_monthly = px.bar(
                monthly_data,
                x='WORKMONTH',
                y='TOTALNURSEHOURS',
                title='Total Nurse Hours Over Time',
                labels={'WORKMONTH': 'Month', 'TOTALNURSEHOURS': 'Total Nurse Hours'},
                color_discrete_sequence=px.colors.qualitative.Plotly
            )
            fig_monthly.update_layout(xaxis_title="Month", yaxis_title="Total Nurse Hours", showlegend=False)
            st.plotly_chart(fig_monthly, use_container_width=True)
        else:
            st.warning("No data to display. Please adjust your filters.")

        st.header("Nurse Hours by Provider and State")
        if not filtered_df.empty:
            provider_state_data = filtered_df.groupby(['PROVNAME', 'STATE'])['TOTALNURSEHOURS'].sum().reset_index()
            fig_provider = px.bar(
                provider_state_data,
                x='TOTALNURSEHOURS',
                y='PROVNAME',
                color='STATE',
                title='Total Nurse Hours by Provider and State',
                labels={'TOTALNURSEHOURS': 'Total Nurse Hours', 'PROVNAME': 'Provider Name'},
                orientation='h'
            )
            st.plotly_chart(fig_provider, use_container_width=True)
        else:
            st.warning("No data to display. Please adjust your filters.")

        st.header("Raw Data")
        st.dataframe(filtered_df)
        st.markdown("""_**Conclusion:**_ The vertical bar graph represents the total nurse hours by month. 
                    The horizontal bar graph presents the total nurse hours by provider and state. 
                    Miller's Merry Manor has the highest nurse hours. The data table gives the detailed 
                    information on total nurse hours for each provider and state.""")

    with contracting_hours_tab:
        # --- Main Dashboard ---
        st.title("Hospitals with Highest Contracted Hours")
        st.markdown("This dashboard identifies hospitals with the highest total contracted nursing hours, which can serve as a proxy for overtime or high-demand staffing.")

        # --- Filters ---
        st.header("Filter Data")

        df = contracting_hours_df

        all_states = sorted(df['STATE'].unique())
        selected_states = st.multiselect("Select State(s)", all_states, default=all_states)

        # Filter providers based on selected states
        filtered_providers = df[df['STATE'].isin(selected_states)]['PROVNAME'].unique()
        all_providers = sorted(df['PROVNAME'].unique())
        selected_providers = st.multiselect("Select Provider(s)", all_providers, default=all_providers)

        # --- Apply Filters ---
        filtered_df = df[
            df['STATE'].isin(selected_states) &
            df['PROVNAME'].isin(selected_providers)
        ].copy()

        # Display Key Metrics
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Total Contracted Hours", f"{filtered_df['TotalContractedHours'].sum():,.0f} hrs")
        with col2:
            st.metric("Number of Providers", f"{filtered_df['PROVNAME'].nunique():,}")

        st.markdown("---")

        # --- Visualizations ---

        st.header("Top 10 Hospitals by Contracted Hours")
        if not filtered_df.empty:
            top_hospitals = filtered_df.nlargest(20, 'TotalContractedHours')
            fig_top = px.bar(
                top_hospitals,
                x="TotalContractedHours",
                y="PROVNAME",
                orientation='h',
                color="STATE",
                title='Total Contracted Hours by Hospital',
                labels={'TotalContractedHours': 'Total Contracted Hours', 'PROVNAME': 'Provider Name'}
            )
            fig_top.update_layout(yaxis={'categoryorder':'total ascending'})
            st.plotly_chart(fig_top, use_container_width=True)
        else:
            st.warning("No data to display. Please adjust your filters.")

        st.header("Raw Data")
        st.dataframe(filtered_df)

        st.markdown("""_**Conclusion:**_ Used total contracted hours for each hospital as a 
                    proxy for overtime or high-demand staffing. The results are ordered to 
                    show the hospitals with the highest hours at the top.""")


