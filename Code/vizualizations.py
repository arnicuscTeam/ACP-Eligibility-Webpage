import geopandas as gpd
import pandas as pd
import folium
from streamlit_folium import folium_static as fs, st_folium as stf



def load_state_map(eligibility_df: pd.DataFrame) -> folium.Map:

    columns = eligibility_df.columns.tolist()

    columns.remove("state")
    columns.insert(0, "NAME")

    eligibility_df["state"] = eligibility_df["state"].astype(str).str.zfill(2)

    state_shapefile = 'Data/ShapeFiles/States/tl_2022_us_state.shp'

    # Read the Shapefile
    gdf = gpd.read_file(state_shapefile)

    merged_data = gdf.merge(eligibility_df, left_on='GEOID', right_on='state', how='left')

    # Create a map without specifying a center or zoom level
    m = folium.Map([37.090240, -95.712891], zoom_start=4)

    folium.GeoJson(
        merged_data,
        style_function=lambda feature: {
            'fillColor': 'green',
            'color': 'black',
            'weight': 1,
            'fillOpacity': 0.6
        },
        tooltip=folium.GeoJsonTooltip(fields=['NAME'], aliases=['State'], sticky=True),
        popup=folium.GeoJsonPopup(fields=columns, localize=True),
    ).add_to(m)

    return m
