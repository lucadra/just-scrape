import subprocess
import requests
import json
import csv
import re
import os

zip_code = input("Inserisci il tuo CAP: ")

url = f"https://www.justeat.it/area/{zip_code}"
headers = {'User-Agent': 'Mozilla/5.0'}

response = requests.get(url, headers=headers)
response.raise_for_status()

scripts = re.findall(rb'<script>(.*?)</script>', response.content, re.DOTALL)
for script in scripts:
    if script.startswith(b'window["__INITIAL_STATE__"]'):
        data = script.decode('utf-8')
        break

data = data[(data.index('(') + 1):(data.index(')'))]

data = data.replace('false', 'False')
data = data.replace('true', 'True')
data = data.replace('null', 'None')
data = data.replace('undefined', 'None')
data = data.replace('NaN', 'None')
data = data.replace('Infinity', 'None')
data = data.replace('-Infinity', 'None')

data = json.loads(data)
data = eval(data)

ids = list(data['restaurants'].keys())

restaurant_data = []

for idx in ids:
    rest = data['restaurants'].get(idx, {})
    times = data['restaurantTimes'].get(idx, {})
    ratings = data['ratings'].get(idx, {})
    cuisines = data['restaurantCuisines'].get(idx, [])
    analytics = data['additionalAnalytics']['restaurantAnalytics'].get(idx, {}) 
    r = {
        'id': idx,
        'name': rest.get('name', ''),
        'uniqueName': rest.get('uniqueName', ''),
        'isTemporaryBoost': rest.get('isTemporaryBoost', ''),
        'isTemporarilyOffline': rest.get('isTemporarilyOffline', ''),
        'isPremier': rest.get('isPremier', ''),
        'defaultPromoted': data['promotedPlacement']['defaultPromotedRestaurants'].get(idx, {}).get('defaultPromoted', ''),
        'isNew': rest.get('isNew', ''),
        'position': rest.get('position', ''),
        'deliveryCost': analytics.get('deliveryCost', ''),
        'minimumDeliveryValue': analytics.get('minimumDeliveryValue', ''),
        'address': rest.get('address', ''),
        'starRating': ratings.get('starRating', ''),
        'ratingCount': ratings.get('ratingCount', ''),
        'isOpenNowForCollection': times.get('isOpenNowForCollection', ''),
        'isOpenNowForDelivery': times.get('isOpenNowForDelivery', ''),
        'isOpenNowForPreOrder': times.get('isOpenNowForPreOrder', ''),
        'nextOpeningTime': times.get('nextOpeningTime', ''),
        'nextDeliveryTime': times.get('nextDeliveryTime', ''),
        'cuisineTypes_1': cuisines[0] if cuisines else '',
        'cuisineTypes_2': cuisines[1] if len(cuisines) >= 2 else ''
    }
    restaurant_data.append(r)

keys = restaurant_data[0].keys()

with open('restaurant_data.csv', 'w', newline='') as output_file:
    dict_writer = csv.DictWriter(output_file, keys)
    dict_writer.writeheader()
    dict_writer.writerows(restaurant_data)

if os.name == 'nt':
    os.startfile('restaurant_data.csv')
else:
    subprocess.call(['xdg-open', 'restaurant_data.csv'])