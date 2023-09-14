import matplotlib.pyplot as plt
import geopandas as gpd
from geopy.geocoders import Nominatim
import folium

# # 1534 E. 22nd St, Los Angeles, CA
# address = input("Enter an address: ")
#
# geolocator = Nominatim(user_agent="acp-eligibility")
#
# location = geolocator.geocode(address)
#
# if location:
#
#     m = folium.Map(location=[location.latitude, location.longitude], zoom_start=15)
#
#     folium.Marker([location.latitude, location.longitude], popup=address).add_to(m)
#
#     m.save("map.html")


shape_file = '../Data/ShapeFiles/States/tl_2022_us_state.shp'
gdf = gpd.read_file(shape_file)

gdf.to_file('output.geojson', driver='GeoJSON')

m = folium.Map(location=[37.090240,-95.712891], zoom_start=4)

# Add the GeoJSON data to the map
geojson_layer = folium.GeoJson('output.geojson').add_to(m)

# for feature in geojson_layer.data['features']:
#     popup_text = feature['properties']['your_property_name']
#     folium.Popup(popup_text).add_to(feature['properties']['popup'])

m.save('maptwo.html')
