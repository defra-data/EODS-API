# Main set up

# account details:
USERNAME = 'firstname.secondname'           # ADD USERNAME
ACCESS_TOKEN = 'the_token'                  # ADD PERSONAL TOKEN
URL = 'https://earthobs.defra.gov.uk'       # CHANGE TO REQUIRED SERVER

# Used in training notebook
WPS_SUF = '/geoserver/wps'
OWS_SUF = '/geoserver/ows'


"""
****NOTES****

USERNAME = The username supplied to you by the system administrators
ACCESS_TOKEN = The access token supplied via your EODS account

URL = Base URL to EODS server

WPS_SUF = WPS suffix
OWS_SUF = OWS suffix

TESTDATA1 = 'geonode:S2A_20190724_lat51lon222_T30UWB_ORB037_utm30n_osgb_vmsk_sharp_rad_srefdem_stdsref' >> Full S2 image covering Hampshire
TESTDATA2 = 'ZonalStatsData' >> Clipped area in Hampshire for Zonal Stats tests
TESTGRANULE = 'T30UWB' >> Suggested granule to use as test data (Dorset/Hampshire)
"""


