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
    st.write("Wait for file to download...")


    @st.cache
    def load_temp_df(temp_povpip, temp_has_pap, temp_has_ssip, temp_has_hins4, temp_has_snap, temp_geography, temp_aian,
                     temp_asian, temp_black, temp_nhpi, temp_white, temp_hispanic, temp_veteran, temp_elderly,
                     temp_disability, temp_not_eng_very_well):
        temp_df, temp_file_name = determine_eligibility("Data/", temp_povpip, temp_has_pap, temp_has_ssip,
                                                        temp_has_hins4, temp_has_snap, temp_geography, temp_aian,
                                                        temp_asian, temp_black, temp_nhpi, temp_white, temp_hispanic,
                                                        temp_veteran, temp_elderly, temp_disability,
                                                        temp_not_eng_very_well)
        return temp_df, temp_file_name


    df, file_name = load_temp_df(povpip, has_pap, has_ssip, has_hins4, has_snap, geography, aian, asian, black, nhpi,
                                 white, hispanic, veteran, elderly, disability, not_eng_very_well)
    st.download_button(label='Download Data', data=df.to_csv(index=False), file_name=file_name, mime='text/csv')

    if geography != "ZIP/ZCTA":
        st.subheader('Map')


        @st.cache
        def load_temp_map(geo, data_frame):
            m = load_map("Data/", geo, data_frame)
            return m


        # Display the map
        fs(load_temp_map(geography, df), width=1800, height=600)
