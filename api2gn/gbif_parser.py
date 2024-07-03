from pygbif import occurrences , registry , species
from geojson import Feature
from shapely import wkt
from geoalchemy2.shape import from_shape
from api2gn.parsers import JSONParser

class GBIFParser(JSONParser):
    srid = 4326
    progress_bar = False  # Assuming no progress bar is needed for a single request

    def __init__(self, occurrence_id):
        # Initialize the parent class
        super().__init__()
        self.occurrence_id = occurrence_id
        self.data = self.fetch_data()
        self.validate_mapping()

    def fetch_data(self):
        # Using pygbif to fetch occurrence data
        response = occurrences.get(self.occurrence_id)
        if response.get('status') == 'success':
            return response['data']
        else:
            raise ValueError(f"Failed to fetch data for occurrence ID {self.occurrence_id}")

    @property
    def items(self):
        return [self.data]  # Return data as a list with a single item

    @property
    def total(self):
        return 1  # Only one item in this case

    def get_geom(self, row):
        if 'decimalLatitude' in row and 'decimalLongitude' in row:
            point = f"POINT({row['decimalLongitude']} {row['decimalLatitude']})"
            geom = wkt.loads(point)
            return from_shape(geom, srid=4326)
        return None

    mapping = {
        # "unique_id_sinp": 
        "eventDate": "date_min",  
        "scientificName": "cd_nom",
        "decimalLatitude": "latitude",
        "decimalLongitude": "longitude",
        "species": "species",
        "recordedBy": "observers",
        "identifiedBy": "determiner",
        "locality": "place_name",
        "habitat": "cd_hab",
        "occurrenceRemarks": "comment_context",
        # Add more fields as necessary
    }
