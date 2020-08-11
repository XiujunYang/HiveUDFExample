import os,sys
# for udf running, cause it's os.getcwd() is root folder of project
module_path_from_app=os.path.join(os.getcwd() , 'py_module', 'geopy_1_23_0')
# for python command runing
module_path_from_python=os.path.join(os.getcwd() , 'geopy_1_23_0')
if module_path_from_app not in sys.path:
    sys.path.append(module_path_from_app)
if module_path_from_python not in sys.path:
    sys.path.append(module_path_from_python)

from geopy.geocoders import Nominatim

# https://geopy.readthedocs.io/en/stable/
geolocator = Nominatim(user_agent="my_app")

def convert_to_address(lat, lon, language):
    # loc: string "lat,lon"
    location = geolocator.reverse(query=str(lat)+','+str(lon), language=language, exactly_one=True)
    if location is None:
        return ""
    else:
        #print(location.raw)
        return location.address

def convert_to_location(addr, language):
    location = geolocator.geocode(query=addr, language=language, exactly_one=True)
    if location is None:
        return []
    else:
        #print(location.raw)
        return [location.latitude, location.longitude]

if __name__ == "__main__" :
    print(convert_to_location("dubai mall", language='en'))
