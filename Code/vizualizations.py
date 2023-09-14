import os
import urllib.request
import zipfile
import matplotlib.pyplot as plt
import seaborn as sns

import geopandas as gpd
import pandas as pd
import folium
from branca.colormap import LinearColormap
from streamlit_folium import folium_static as fs, st_folium as stf


def load_state_map(data_dir: str, eligibility_df: pd.DataFrame) -> folium.Map:
    eligibility_df["Total Change Percentage Eligible"] = (eligibility_df["Current Percentage Eligible"] -
                                                          eligibility_df["New Percentage Eligible"])


    columns = eligibility_df.columns.tolist()

    columns.remove("state")
    columns.insert(0, "NAME")

    eligibility_df["state"] = eligibility_df["state"].astype(str).str.zfill(2)

    state_shapefile = data_dir + 'ShapeFiles/STATES/tl_2022_us_state.shp'

    # Read the Shapefile
    gdf = gpd.read_file(state_shapefile)

    merged_data = gdf.merge(eligibility_df, left_on='GEOID', right_on='state', how='right')

    colormap = LinearColormap(
        colors=['green', 'yellow', 'red'],
        vmin=merged_data['Total Change Percentage Eligible'].min(),
        vmax=merged_data['Total Change Percentage Eligible'].max(),
    )

    def color_function(feature):
        value = feature['properties']['Total Change Percentage Eligible']
        return colormap(value)


    # Create a map without specifying a center or zoom level
    m = folium.Map([37.090240, -95.712891], zoom_start=4)

    folium.GeoJson(
        merged_data,
        style_function=lambda feature: {
            'fillColor': color_function(feature),
            'color': 'black',
            'weight': 1,
            'fillOpacity': 0.6
        },
        tooltip=folium.GeoJsonTooltip(fields=['NAME'], aliases=['State'], sticky=True),
        popup=folium.GeoJsonPopup(fields=columns, localize=True),
    ).add_to(m)

    colormap.add_to(m)

    return m


def load_map(data_dir: str, geography: str, eligibility_df: pd.DataFrame) -> folium.Map:
    """
    Loads a map based on the geography selected and the eligibility dataframe
    :param data_dir:
    :param geography: The geography selected
    :param eligibility_df: The eligibility dataframe
    :return: The folium map
    """

    eligibility_df["Total Change Percentage Eligible"] = (eligibility_df["Current Percentage Eligible"] -
                                                          eligibility_df["New Percentage Eligible"])

    geo_dict = {"Public-use microdata area (PUMA)": "puma22",
                "118th Congress (2023-2024)": "cd118",
                "State": "state",
                "County": "county",
                "Metropolitan division": "metdiv20"}

    columns = eligibility_df.columns.tolist()

    columns.remove(geo_dict[geography])
    columns.insert(0, "NAME")

    aliases = columns.copy()
    aliases[0] = geo_dict[geography]


    if geography == "Public-use microdata area (PUMA)":
        eligibility_df[geo_dict[geography]] = eligibility_df[geo_dict[geography]].astype(str).str.zfill(7)

        shapefiles = data_dir + 'ShapeFiles/PUMA/'
        gdf = gpd.GeoDataFrame()

        for folder in os.listdir(shapefiles):

            for file in os.listdir(shapefiles + folder):

                if file.endswith(".shp"):

                    if gdf.empty:
                        gdf = gpd.read_file(shapefiles + folder + "/" + file)
                    else:
                        temp_gdf = gpd.read_file(shapefiles + folder + "/" + file)
                        gdf = gpd.GeoDataFrame(pd.concat([gdf, temp_gdf], ignore_index=True))

        mergeon = "GEOID20"

    elif geography == "118th Congress (2023-2024)":
        eligibility_df[geo_dict[geography]] = eligibility_df[geo_dict[geography]].astype(str).str.zfill(4)

        shapefiles = data_dir + 'ShapeFiles/CD/'

        gdf = gpd.GeoDataFrame()


        for folder in os.listdir(shapefiles):

            for file in os.listdir(shapefiles + folder):

                if file.endswith(".shp"):

                    if gdf.empty:
                        gdf = gpd.read_file(shapefiles + folder + "/" + file)
                    else:
                        temp_gdf = gpd.read_file(shapefiles + folder + "/" + file)
                        gdf = gpd.GeoDataFrame(pd.concat([gdf, temp_gdf], ignore_index=True))

        mergeon = "GEOID20"

    elif geography == "State":
        eligibility_df[geo_dict[geography]] = eligibility_df[geo_dict[geography]].astype(str).str.zfill(2)

        shapefile = data_dir + 'ShapeFiles/STATES/tl_2022_us_state.shp'
        gdf = gpd.read_file(shapefile)

        mergeon = "GEOID"

    elif geography == "County":
        eligibility_df[geo_dict[geography]] = eligibility_df[geo_dict[geography]].astype(str).str.zfill(5)

        zip_folder = data_dir + 'ShapeFiles/COUNTY/tl_2022_us_county.zip'

        gdf = gpd.read_file(f"zip://{zip_folder}!tl_2022_us_county.shp")

        mergeon = "GEOID"

    elif geography == "Metropolitan division":
        eligibility_df[geo_dict[geography]] = eligibility_df[geo_dict[geography]].astype(str).str.zfill(5)

        shapefile = data_dir + 'ShapeFiles/Metropolitan_Division/tl_2021_us_metdiv.shp'
        gdf = gpd.read_file(shapefile)

        mergeon = "METDIVFP"

    else:
        raise ValueError("Invalid geography selected")

    # Merge the dataframes
    merged_data = gdf.merge(eligibility_df, left_on=mergeon, right_on=geo_dict[geography], how='left')

    merged_data = merged_data.dropna(subset=["Total Change Percentage Eligible"])

    colormap = LinearColormap(
        colors=['green', 'yellow', 'red'],
        vmin=merged_data['Total Change Percentage Eligible'].min(),
        vmax=merged_data['Total Change Percentage Eligible'].max(),
    )

    def color_function(feature):
        value = feature['properties']['Total Change Percentage Eligible']
        return colormap(value)

    # Create a map without specifying a center or zoom level
    m = folium.Map([37.090240, -95.712891], zoom_start=4)

    folium.GeoJson(
        merged_data,
        style_function=lambda feature: {
            'fillColor': color_function(feature),
            'color': 'black',
            'weight': 1,
            'fillOpacity': 0.6
        },
        tooltip=folium.GeoJsonTooltip(fields=['NAME'], aliases=[geo_dict[geography]], sticky=True),
        popup=folium.GeoJsonPopup(fields=columns, localize=True),
    ).add_to(m)

    return m


def download_shape_files(data_dir: str):
    shape_folder = data_dir + "ShapeFiles/"
    cd_folder = shape_folder + "CD/"
    puma_folder = shape_folder + "PUMA/"

    # Create a dictionary to store the state abbreviations
    state_dict = {"01": "AL", "02": "AK", "04": "AZ", "05": "AR", "06": "CA", "08": "CO", "09": "CT", "10": "DE",
                  "11": "DC", "12": "FL", "13": "GA", "15": "HI", "16": "ID", "17": "IL", "18": "IN", "19": "IA",
                  "20": "KS", "21": "KY", "22": "LA", "23": "ME", "24": "MD", "25": "MA", "26": "MI", "27": "MN",
                  "28": "MS", "29": "MO", "30": "MT", "31": "NE", "32": "NV", "33": "NH", "34": "NJ", "35": "NM",
                  "36": "NY", "37": "NC", "38": "ND", "39": "OH", "40": "OK", "41": "OR", "42": "PA", "44": "RI",
                  "45": "SC", "46": "SD", "47": "TN", "48": "TX", "49": "UT", "50": "VT", "51": "VA", "53": "WA",
                  "54": "WV", "55": "WI", "56": "WY", "72": "PR"}

    # CD

    for key, value in state_dict.items():
        # Link for all 2022 block group shape files
        link = f"https://www2.census.gov/geo/tiger/TIGER2022/CD/tl_2022_{key}_cd118.zip"

        # Create the folder name where all the contents from the zip will be stored
        folder_name = cd_folder + value + "_Shape_Folder/"

        if not os.path.exists(folder_name):
            os.makedirs(folder_name)

        # Download the zip file
        urllib.request.urlretrieve(link, folder_name + f"tl_2022_{key}_cd118.zip")

        # Extract the zip file
        with zipfile.ZipFile(folder_name + f"tl_2022_{key}_cd118.zip", "r") as zip_ref:
            zip_ref.extractall(folder_name)

        # Delete the zip file
        os.remove(folder_name + f"tl_2022_{key}_cd118.zip")

    # PUMA

    for key, value in state_dict.items():

        # Link for all 2022 block group shape files
        link = f"https://www2.census.gov/geo/tiger/TIGER2022/PUMA/tl_2022_{key}_puma20.zip"

        # Create the folder name where all the contents from the zip will be stored
        folder_name = puma_folder + value + "_Shape_Folder/"

        if not os.path.exists(folder_name):
            os.makedirs(folder_name)

        # Download the zip file
        urllib.request.urlretrieve(link, folder_name + f"tl_2022_{key}_puma20.zip")

        # Extract the zip file
        with zipfile.ZipFile(folder_name + f"tl_2022_{key}_puma20.zip", "r") as zip_ref:
            zip_ref.extractall(folder_name)

        # Delete the zip file
        os.remove(folder_name + f"tl_2022_{key}_puma20.zip")


# if __name__ == '__main__':
#     download_shape_files("../Data/")
