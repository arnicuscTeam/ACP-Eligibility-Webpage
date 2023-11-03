import folium
from Code.acs_pums import determine_eligibility
import pandas as pd
import streamlit as st

# Set page name
st.set_page_config(page_title='ACP Eligibility', page_icon=':bar_chart:', layout='wide')

st.cache_data.clear()

# Set page title
st.header('Estimating Eligibility for the Affordable Connectivity Program (ACP) Under Different Criteria')

st.text("")

st.write("This tool uses microdata from the American Community Survey (ACS) 1-year 2021 [estimates]"
         "(https://www.census.gov/programs-surveys/acs/technical-documentation/table-and-geography-changes/2021/1"
         "-year.html) and the geographic correspondence [files](https://mcdc.missouri.edu/applications/geocorr.html) "
         "developed by the Missouri Census Data Center to estimate the number of ACP eligible households using "
         "different eligibility criteria for different geographic units. To begin, select the desired geographic level"
         " of analysis in the dropdown menu at the top (state, county, PUMA, zipcode, metro area or Congressional "
         "District). Next, use the slider to select an income eligibility threshold between 200% (the current income "
         "threshold) and 120% of the Federal Poverty Line. There are four ACS variables that capture information about "
         "additional ACP qualification criteria for households (see table below for description of these ACS "
         "variables). Please check or uncheck the boxes to obtain estimates that include/exclude these programs. "
         "Finally, select the demographic characteristics to obtain estimates for specific sub-populations (this is "
         "optional, leave blank for entire population). Then click “Submit” and download the dataset corresponding to "
         "these eligibility criteria. The numbers in the output csv files represent number of households. If "
         "demographic characteristics are selected, the numbers represent the number of households in which at least "
         "one member is part of the selected sub population.")

st.text("")

table_data = [
    ["Medicaid, Medical Assistance, or any kind of government-assistance plan for those with low incomes or a "
     "disability", "HINS4"],
    ["Yearly food stamp/Supplemental Nutrition Assistance Program (SNAP)", "FS"],
    ["Public assistance income over the past 12 months (any amount)", "PAP"],
    ["Supplemental Security Income over the past 12 months (any amount)", "SSIP"]
]

# Create an HTML table
table_html = ("<table style='border-collapse: collapse; width: 100%; border: 1px solid #ccc; background-color: "
              "transparent;'>")
table_html += "<tr style='background-color: transparent;'>"
table_html += ("<th style='border: 1px solid #ccc; padding: 8px; text-align: left; background-color: transparent;'>ACS "
               "Variable Description</th>")
table_html += ("<th style='border: 1px solid #ccc; padding: 8px; text-align: left; background-color: "
               "transparent;'>Variable name</th>")
table_html += "</tr>"

for row in table_data:
    table_html += "<tr>"
    table_html += f"<td style='border: 1px solid #ccc; padding: 8px;'>{row[0]}</td>"
    table_html += f"<td style='border: 1px solid #ccc; padding: 8px;'>{row[1]}</td>"
    table_html += "</tr>"

table_html += "</table>"

# Display the HTML table
st.markdown(table_html, unsafe_allow_html=True)

st.text("")
st.text("")

# List of geographies
geographies = ["Public-use microdata area (PUMA)", "118th Congress (2023-2024)", "State", "County", "ZIP/ZCTA",
               "Metropolitan division"]

# Collect the geography
geography = st.selectbox('Select Geography', geographies)

st.text("")

# Criteria
st.subheader('Eligibility Criteria')

st.text("")

# Slider for POVPIP
povpip = st.slider('Income Eligibility Threshold (% of FPL)', min_value=0, max_value=200, value=200, step=1)

st.write("Check/uncheck boxes for participation in qualifying programs:")
st.text("")

# Checkboxes for other criteria, turned into integers
has_pap = int(st.checkbox('Receives Public Assistance Income (variable: PAP)', value=True))
has_ssip = int(st.checkbox('Receives Supplemental Security Income (variable: SSIP)', value=True))
has_hins4 = int(st.checkbox('Receives Medicaid, Medical Assistance, or any kind of government-assistance plan ('
                            'variable: HINS4)', value=True))
has_snap = int(st.checkbox('Receives Supplemental Nutrition Assistance Program (variable: SNAP)', value=True))

# Demographic Criteria
st.subheader('Sub-Populations')

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

bottom_text = True

df = pd.DataFrame()

# Submit button
st.subheader('Download Data')
if st.button('Submit'):

    df, file_name = determine_eligibility("Data/", povpip, has_pap, has_ssip, has_hins4, has_snap, geography, aian,
                                          asian, black, nhpi, white, hispanic, veteran, elderly, disability,
                                          not_eng_very_well)
    st.download_button(label='Download Data', data=df.to_csv(index=False), file_name=file_name, mime='text/csv')



if bottom_text:

    for i in range(10):
        st.text("")

    st.markdown("<span style='font-size: 14px;'>This tool was developed by a team at the University of Southern "
                "California led by [Prof. Hernan Galperin](https://annenberg.usc.edu/faculty/hernan-galperin) and ["
                "Prof. François Bar](https://annenberg.usc.edu/faculty/fran%C3%A7ois-bar), with research assistance "
                "from Angel Chavez-Penate. The Pew Research Trust has generously provided funding for this "
                "project.</span>", unsafe_allow_html=True)
