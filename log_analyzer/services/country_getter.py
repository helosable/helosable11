import json
from urllib.request import urlopen


def country_name_from_ip(ip, api_token):
    location = json.load(urlopen(f'http://api.ipstack.com/{ip}?access_key={api_token}'))
    location_list = [location['country_name'], location['city']]
    return location_list
