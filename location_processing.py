from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut


# Used to map the twitter user location description to a standard format
def lookup_location(location):
    geo_locator = Nominatim(user_agent="tweets_location")
    try:
        data = geo_locator.geocode(location, language='en')
        if data:
            latitude = data.raw.get("lat")
            longitude = data.raw.get("lon")
            location = geo_locator.reverse(latitude + "," + longitude, language='en')
            address = location.raw['address']
        else:
            address = None
    except GeocoderTimedOut:
        return None
    return address

if __name__ == '__main__':
    locations = ['India', 'Houston, Texas', 'Sichuan, China', 'the united states', 'hogwarts']
    hashmap = {}
    for lo in locations:
        address = lookup_location(lo)
        if address:
            state = address.get('state', None)
            country_code = address.get('country_code', None)
            hashmap[lo] = (state, country_code)
        else:
            hashmap[lo] = None
    print(hashmap)
