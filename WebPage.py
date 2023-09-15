import folium
from Code.acs_pums import determine_eligibility
from Code.vizualizations import load_map
import pandas as pd
import streamlit as st
from streamlit_folium import folium_static as fs, st_folium as stf

# Set page name
st.set_page_config(page_title='ACP Eligibility', page_icon=':bar_chart:', layout='wide')

# Set page title
st.header('ACP Eligibility Criteria')

# List of geographies
geographies = ["Public-use microdata area (PUMA)", "118th Congress (2023-2024)", "State", "County", "ZIP/ZCTA",
               "Metropolitan division"]

# Collect the geography
geography = st.selectbox('Select Geography', geographies)

# Criteria
st.subheader('Eligibility Criteria')

# Slider for POVPIP
povpip = st.slider('POVPIP', min_value=0, max_value=200)

# Checkboxes for other criteria, turned into integers
has_pap = int(st.checkbox('Has PAP'))
has_ssip = int(st.checkbox('Has SSIP'))
has_hins4 = int(st.checkbox('Has HINS4'))
has_snap = int(st.checkbox('Has SNAP'))

# Demographic Criteria
st.subheader('Demographic Criteria')

# Checkboxes for demographic criteria, turned into integers
aian = int(st.checkbox('American Indian or Alaska Native'))
asian = int(st.checkbox('Asian'))
black = int(st.checkbox('Black or African American'))
nhpi = int(st.checkbox('Native Hawaiian or Other Pacific Islander'))
white = int(st.checkbox('White'))
hispanic = int(st.checkbox('Hispanic or Latino'))
veteran = int(st.checkbox('Veteran'))
elderly = int(st.checkbox('Elderly'))
disability = int(st.checkbox('Disability'))
not_eng_very_well = int(st.checkbox('Does Not Speak English Very Well'))

# Submit button
st.subheader('Download Data')
if st.button('Submit'):

    df, file_name = determine_eligibility("Data/", povpip, has_pap, has_ssip, has_hins4, has_snap, geography, aian,
                                          asian, black, nhpi, white, hispanic, veteran, elderly, disability,
                                          not_eng_very_well)
    st.download_button(label='Download Data', data=df.to_csv(index=False), file_name=file_name, mime='text/csv')


    if geography != "ZIP/ZCTA":

        st.subheader('Map')

        if st.button('Load Map'):

            m = load_map("Data/", geography, df)

            # Display the map
            fs(m, width=100, height=500)
