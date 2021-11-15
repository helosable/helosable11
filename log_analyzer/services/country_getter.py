import json
import ijson
from urllib.request import urlopen


def json_read(config):
    with open(config, 'r') as config:
        return next(ijson.items(config, '', multiple_values=True))

def country_name_from_ip(ip):
    settings = json_read('config.json')
    api_token = settings['api_key']
    data = urlopen(f'http://api.ipstack.com/{ip}?access_key={api_token}')
    raw_data = data.read()
    encoding = data.info().get_content_charset('utf8')
    location = json.loads(raw_data.decode(encoding))
    location_list = [location['country_name'], location['city']]
    return location_list
