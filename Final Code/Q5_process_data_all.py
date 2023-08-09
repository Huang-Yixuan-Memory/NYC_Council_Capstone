import pandas as pd
import requests

# Setting up API key
api_key = "AIzaSyB2mZ4PARpnRj7tucYcuGTCSNbgo9XEFYk"


# Define functions to query geographic information
def query_geolocation(address):
    url = f"https://maps.googleapis.com/maps/api/geocode/json?address={address}&key={api_key}"

    # Send HTTP request
    try:
        response = requests.get(url, timeout=5)
    except Exception:
        return None, None, None, None

    data = response.json()

    latitude = None
    longitude = None
    borough = None
    zip_code = None

    # Parsing response data
    if data["status"] == "OK":
        # Extract latitude and longitude
        location = data["results"][0]["geometry"]["location"]
        latitude = location["lat"]
        longitude = location["lng"]

        # Withdrawal area and postal code
        components = data["results"][0]["address_components"]

        for component in components:
            if "administrative_area_level_2" in component["types"]:
                borough = component["long_name"]
            if "postal_code" in component["types"]:
                zip_code = component["long_name"]

        # output result
        # print(f"Latitude: {latitude}")
        # print(f"Longitude: {longitude}")
        # print(f"Borough: {borough}")
        # print(f"Zip Code: {zip_code}")
    else:
        print("Geocoding failed.")
    return 0.0 if latitude is None else latitude, 0.0 if longitude is None else longitude, "" if borough is None else borough.upper(), zip_code


# Constructing the URL of the request
# address = "LORIMER STREET, New York City"  # Street address to be queried

# Read collision details CSV file
data = pd.read_csv('collision_w_holidays_n_weather.csv')

# Delete the rows that satisfy conditions 1 and 2, and keep the valid latitude and longitude.
data = data[((data['LATITUDE'].notnull()) & (data['LATITUDE'] != 0)) |
            ((data['ON STREET NAME'].notnull() & data['CROSS STREET NAME'].notnull()) |
             (data['ON STREET NAME'].notnull() & data['OFF STREET NAME'].notnull()) |
             (data['CROSS STREET NAME'].notnull() & data['OFF STREET NAME'].notnull()))]
print(data['LATITUDE'].head(50))
# print(data['ON STREET NAME'].head(20).isnull())
# print(data['CROSS STREET NAME'].head(20).isnull())
# print(data['OFF STREET NAME'].head(20).isnull())

print(data.shape[0])
# Processing of data that satisfy condition 1 but not condition 2
for index, row in data.iterrows():
    print("current index:", index)
    if pd.isnull(row['LATITUDE']) or row['LATITUDE'] == 0:
        intersection = []
        if pd.notnull(row['ON STREET NAME']):
            intersection.append(row['ON STREET NAME'])
        if pd.notnull(row['CROSS STREET NAME']):
            intersection.append(row['CROSS STREET NAME'])
        if pd.notnull(row['OFF STREET NAME']):
            intersection.append(row['OFF STREET NAME'])
        print("intersection:", intersection)
        address = 'New York City, ' + 'and'.join(intersection) + " Intersection"
        latitude, longitude, borough, zip_code = query_geolocation(address)
        data.at[index, 'LATITUDE'] = latitude
        data.at[index, 'LONGITUDE'] = longitude
        data.at[index, 'BOROUGH'] = borough
        data.at[index, 'ZIP CODE'] = zip_code
        data.at[index, 'LOCATION'] = '(' + str(latitude) + ', ' + str(longitude) + ')'

    # Update a new CSV file every 100,000 pieces of data to prevent google map API breaks
    if index % 100000 == 0:
        # Store to CSV file
        data.to_csv('processed_data.csv', index=False)

# Store to CSV file
data.to_csv('processed_data.csv', index=False)

################################################################################################
# Statistics on the number of collisions at improved intersections before and after improvements
################################################################################################

import pandas as pd
from geopy.distance import geodesic

threshold = 150

bound = 0.002
# CONTRIBUTING FACTOR VEHICLE 1 = Turning Improperly
# Read original and new data files
df_old = pd.read_csv('processed_data.csv')
crash_detail = pd.read_csv('Motor_Vehicle_Collisions_-_Vehicles.csv')
crash_detail = crash_detail[['COLLISION_ID', 'PRE_CRASH', 'CRASH_DATE']]
df_old = pd.merge(df_old, crash_detail, left_on='COLLISION_ID', right_on='COLLISION_ID', how='left')

df_old = df_old.drop_duplicates(subset=['COLLISION_ID'], keep='first')
# Save as 'processed_data_with_PRE_CRASH'
df_old.to_csv("processed_data_with_PRE_CRASH.csv", index=False)

left_turn_data_new = pd.read_csv('dot_VZV_Turn_Traffic_Calming_20230512.csv')
left_turn_data_new['IMPROVE_DATE'] = pd.to_datetime(left_turn_data_new['completion'].str[:10])


# Calculating distances based on latitude and longitude
def calculate_distance(lat1, lon1, lat2, lon2):
    coord1 = (lat1, lon1)
    coord2 = (lat2, lon2)
    return geodesic(coord1, coord2).feet


# convert to datetime
df_old['CRASH DATE'] = pd.to_datetime(df_old['CRASH DATE'])
# Find the latest CRASH_DATE in collision_data
latest_crash_date = max(df_old['CRASH DATE'])
print("latest_crash_date:", latest_crash_date)

# Setting Filter Criteria
condition1 = df_old['CRASH DATE'] < '2021-12-09'
condition2 = df_old['CRASH DATE'] >= '2021-12-09'

# Filtering data according to conditions
filtered_df = df_old[condition1 & (df_old['PRE_CRASH'] == 'Making Left Turn') |
                 condition2 & (df_old['CONTRIBUTING FACTOR VEHICLE 1'] == 'Turning Improperly')]

collision_data = filtered_df[['LATITUDE', 'LONGITUDE', 'CRASH DATE']]

# Initialize result data
result_data = []
for index, turn_point in left_turn_data_new.iterrows():
    print("index:", index)
    lat1, lon1 = turn_point['LAT'], turn_point['LONG']
    collisions_before = []
    collisions_after = []
    for _, collision in collision_data.iterrows():
        lat2, lon2 = collision['LATITUDE'], collision['LONGITUDE']

        if lat2 < lat1 - bound or lat2 > lat1 + bound or lon2 < lon1 - bound or lon2 > lon1 + bound:
            continue

        distance = calculate_distance(lat1, lon1, lat2, lon2)
        if distance <= 150:
            if collision['CRASH DATE'] <= turn_point['IMPROVE_DATE']:
                collisions_before.append(collision)
            else:
                collisions_after.append(collision)

    # Contains data
    if len(collisions_before) > 0 or len(collisions_after) > 0:
        improvement_date = turn_point['IMPROVE_DATE']
        total_collisions_before = len(collisions_before)
        total_collisions_after = len(collisions_after)

        # Earliest time of collision before improvement
        min_collision_time = min(collisions_before, key=lambda x: x['CRASH DATE'])[
            'CRASH DATE'] if collisions_before else None

        # Latest time of collision after improvement
        max_collision_time = latest_crash_date

        avg_collisions_before = total_collisions_before / (
                    improvement_date - min_collision_time).days * 365 if total_collisions_before != 0 else 0.0
        avg_collisions_after = total_collisions_after / (
                    max_collision_time - improvement_date).days * 365 if total_collisions_after != 0 else 0.0
        row_result = {
            'LAT': lat1,
            'LONG': lon1,
            'Improvement Date': improvement_date,
            'Total Collisions Before': total_collisions_before,
            'Earliest Collision Date': min_collision_time,
            'Average Collisions Before': avg_collisions_before,
            'Total Collisions After': total_collisions_after,
            'Latest Collision Date': max_collision_time,
            'Average Collisions After': avg_collisions_after
        }
        # print(row_result)
        result_data.append(row_result)

# Write result data to CSV file
result_df = pd.DataFrame(result_data)
# Calculating Average crash count per year difference
result_df['Average crash difference'] = result_df['Average Collisions After'] - result_df['Average Collisions Before']

overall_mean_before = result_df['Average Collisions Before'].mean()
overall_mean_after = result_df['Average Collisions After'].mean()
print("overall_mean_before:", overall_mean_before)
print("overall_mean_after:", overall_mean_after)

output_filename = 'collision_analysis.csv'
result_df.to_csv(output_filename, index=False)


#########################################################################################################
# Set the intersection id, whether it is an improvement measure intersection, and the time of improvement
# for the CSV file based on latitude and longitude clustering.
#########################################################################################################
def calculate_distance(lat1, lon1, lat2, lon2):
    coord1 = (lat1, lon1)
    coord2 = (lat2, lon2)
    return geodesic(coord1, coord2).feet

# read processed_data_with_PRE_CRASH
df_A = pd.read_csv('processed_data_with_PRE_CRASH.csv')
# convert to datetime
df_A['CRASH DATE'] = pd.to_datetime(df_A['CRASH DATE'])

# read 'dot_VZV_Turn_Traffic_Calming_20230512.csv'
df_B = pd.read_csv('dot_VZV_Turn_Traffic_Calming_20230512.csv')
df_B['IMPROVE_DATE'] = pd.to_datetime(df_B['completion'].str[:10])

# Setting up the id of the intersection where the improvement measures be implemented
df_B['intersection_id'] = df_B.index

threshold = 150
bound = 0.002

condition1 = df_A['CRASH DATE'] < '2021-12-09'
condition2 = df_A['CRASH DATE'] >= '2021-12-09'
is_possible_treatement = condition1 & (df_A['PRE_CRASH'] == 'Making Left Turn') | condition2 & (df_A['CONTRIBUTING FACTOR VEHICLE 1'] == 'Turning Improperly')
df_A['is_possible_treatement'] = is_possible_treatement

# default left_turn_treatement_date
default_left_turn_treatement_date = '2021-12-09'
df_A['left_turn_treatement_date'] = default_left_turn_treatement_date
df_A['is_treatement'] = 0
df_A['intersection_id'] = -1

# Initialize an empty dictionary to store statistics for each intersection
intersections = {}
start_intersection_id = 1000

# Iterate over df_A
for index, row in df_A.iterrows():
    print("index:", index)

    lat1, lon1 = row['LATITUDE'], row['LONGITUDE']

    if row['is_possible_treatement']:
        # Find rows with distance <150 from LONG and LAT in df_B
        for index, turn_point in df_B.iterrows():
            lat2, lon2 = turn_point['LAT'], turn_point['LONG']

            if lat2 < lat1 - bound or lat2 > lat1 + bound or lon2 < lon1 - bound or lon2 > lon1 + bound:
                continue

            distance = calculate_distance(lat1, lon1, lat2, lon2)
            if distance <= 150:
                # Update left_turn_treatement_date
                df_A.at[index, 'left_turn_treatement_date'] = turn_point['IMPROVE_DATE']
                # Update if left_turn_treatement
                df_A.at[index, 'is_treatement'] = 1
                # Update intersection_id
                df_A.at[index, 'intersection_id'] = turn_point['intersection_id']
                break

    else:
        curr_intersection = None
        # Find matching intersections with distances less than 150feet at existing intersections
        for intersection in intersections:
            lat2, lon2 = intersection

            if lat2 < lat1 - bound or lat2 > lat1 + bound or lon2 < lon1 - bound or lon2 > lon1 + bound:
                continue

            distance = calculate_distance(lat1, lon1, lat2, lon2)

            if distance < 150:
                curr_intersection = intersection
                break

        # If no matching intersection is found, add new intersection statistics
        if curr_intersection is None:
            # If no matching intersection is found, add new intersection statistics
            intersections[(lat1, lon1)] = start_intersection_id
            # Update intersection_id
            df_A.at[index, 'intersection_id'] = start_intersection_id
            start_intersection_id += 1
        else:  # If the intersection is found, get the id of the intersection.
            intersection_id = intersections[curr_intersection]
            # Update intersection_id
            df_A.at[index, 'intersection_id'] = intersection_id

res = df_A['left_turn_treatement_date'] < df_A['CRASH DATE']
df_A['treated'] = res
# Use the replace() method to replace True with 1 and False with 0
df_A['treated'] = df_A['treated'].replace({True: 1, False: 0})

df_A.to_csv('processed_data_with_treated.csv', index=False)

###########################################################################################################
# Supplement processed_data_with_treated.csv with clustering of intersections with an intersection id of -1
# to generate a new intersection id.
###########################################################################################################

# read processed_data_with_treated
df_A = pd.read_csv('processed_data_with_treated.csv')


threshold = 150
bound = 0.002

# Initialize an empty dictionary to store statistics for each intersection
intersections = {}
start_intersection_id = 60000

# Iterate over df_A
for index, row in df_A.iterrows():
    print("index:", index)

    lat1, lon1 = row['LATITUDE'], row['LONGITUDE']

    if row['intersection_id'] == -1:
        curr_intersection = None
        # Find matching intersections with distances less than 150feet at existing intersections
        for intersection in intersections:
            lat2, lon2 = intersection

            if lat2 < lat1 - bound or lat2 > lat1 + bound or lon2 < lon1 - bound or lon2 > lon1 + bound:
                continue

            distance = calculate_distance(lat1, lon1, lat2, lon2)

            if distance < threshold:
                curr_intersection = intersection
                break

        # If no matching intersection is found, add new intersection statistics
        if curr_intersection is None:
            intersections[(lat1, lon1)] = start_intersection_id
            # Update intersection_id
            df_A.at[index, 'intersection_id'] = start_intersection_id
            start_intersection_id += 1
            print("A new intersection")
        else:  # If the intersection is found, get the id of the intersection.
            intersection_id = intersections[curr_intersection]
            # Update intersection_id
            df_A.at[index, 'intersection_id'] = intersection_id
            print("Updated to clustered intersection ids")

df_A.to_csv('processed_data_with_treated2.csv', index=False)
