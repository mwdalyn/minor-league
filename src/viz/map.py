"""
Docstring for viz.map

Status: Unused/Unfinished
- Placeholder for previous visualization code
- To be integrated back with processed data outputs 
- Previously was "all-in-one" long block code series in a single file, trying to modularize
"""

import folium 
import os

def map_folium(df):
	# Make a map in Folium
	## https://python-visualization.github.io/folium/latest/getting_started.html
	# Set up map
	m = folium.Map(location=(38.7946, -106.5348)) # Center of US

	# Pick Leagues to include
	leagues = df["League"].unique()[:7].tolist()
    # TODO: Add filter here for 999, -999 LLs

	# Attempt 1. Start with groups
	color_codes = ['lightblue', 'gray', 'blue', 'darkred', 'lightgreen', 'purple', 'red', 'green', 'lightred', 'white', 'darkblue', 'darkpurple', 'cadetblue', 'orange', 'pink', 'lightgray', 'darkgreen','black', 'beige']
	i = 0
	for l in leagues:
		i += 1
		group = folium.FeatureGroup(l).add_to(m) # NOTE: Probably need to establish groups before placing markers, so add Groups-> map and marks->Group
		color = color_codes[i] # Set color at league level
		for index, row in df.loc[df["League"]==l].iterrows(): # TODO: Vectorize this! Don't be lazy/get better.
			popup = str(row["City"]) + ", " + str(row["State"])
			tooltip = str(row["Team"])
			# tooltip_text = f"""
			# 	City: {city}<br>
			# 	State: {state}<br>
			# 	Population: {population}
			# 	"""
			lat, lon = row["Lat"], row["Lon"]
			folium.Marker(
					location=[lat, lon]
					,tooltip=tooltip # TODO: Make the map after the database is built and add details
					,popup=popup
					,icon=folium.Icon(color)).add_to(group)
	# Add LayerControl
	folium.LayerControl().add_to(m)
	# Return
	return m

if __name__ == '__main__':
    # Get team_df 
	team_df = '' # TODO: Import
    # Create map
	team_map = map_folium(team_df)
	# Save map
	team_map.save(os.path.join("output","map","team_map.html"))
