from pygbif import occurrences, registry, species
from geojson import Feature
from shapely import wkt
from sqlalchemy.sql import func
from geoalchemy2.shape import from_shape
from api2gn.parsers import JSONParser
import requests

class GBIFParser(JSONParser):
    srid = 4326
    progress_bar = False  # Assuming no progress bar is needed for a single request
    occurrence_ids = [4407389321]  # Occurence par défaut, Vous pouvez remplacer par une liste dynamique d'IDs

    def __init__(self):
        self.api_filters = {**GBIFParser.api_filters, **self.api_filters}
        self.mapping = {**GBIFParser.mapping, **self.mapping}
        self.constant_fields = {
            **GBIFParser.constant_fields,
            **self.constant_fields,
        }
        # Initialize the parent class
        super().__init__()

        self.occurrence_id = None  # Initialisation
        self.data = None
        self.organization_data = None
        self.dataset_data = None
        self.species_data = None
        self.subdivisions_data = None

        self.validate_maping()

    def fetch_occurrence_data(self, occurrence_id):
        response = occurrences.get(occurrence_id)
        print(response)  # Imprime la réponse complète pour inspecter la structure
        if 'results' in response and len(response['results']) > 0:
            return response['results'][0]
        elif 'data' in response:
            return response['data']
        elif len(response) > 0:
            return response
        else:
            raise ValueError(f"Failed to fetch data for occurrence ID {occurrence_id}")

    def fetch_organization_data(self):
        organization_key = self.data.get('publishingOrgKey')
        print(organization_key)
        if organization_key:
            return registry.organizations(uuid=organization_key)
        return {}

    def fetch_dataset_data(self):
        dataset_key = self.data.get('datasetKey')
        if dataset_key:
            return registry.datasets(uuid=dataset_key)
        return {}

    def fetch_species_data(self):
        taxon_key = self.data.get('taxonKey')
        if taxon_key:
            return species.name_usage(key=taxon_key)
        return {}

    def fetch_subdivisions_data(self):
        url = "https://api.gbif.org/v1/geocode/gadm/FRA.3_1/subdivisions"
        response = requests.get(url)
        response.raise_for_status()
        return response.json()

    @property
    def items(self):
        return [self.data]  # Return data as a list with a single item

    @property
    def total(self):
        return len(self.occurrence_ids)  # Nombre total d'occurrences

    def get_geom(self, row):
        if 'decimalLatitude' in row and 'decimalLongitude' in row:
            point = f"POINT({row['decimalLongitude']} {row['decimalLatitude']})"
            geom = wkt.loads(point)
            return from_shape(geom, srid=4326)
        return None

    def integrate_data(self):
        integrated_data = self.data.copy()
        integrated_data['organization'] = self.organization_data
        integrated_data['dataset'] = self.dataset_data
        integrated_data['species'] = self.species_data
        # integrated_data['subdivisions'] = self.subdivisions_data
        return integrated_data

    def next_row(self):
        for occurrence_id in self.occurrence_ids:
            self.occurrence_id = occurrence_id
            self.data = self.fetch_occurrence_data(occurrence_id)
            self.organization_data = self.fetch_organization_data()
            self.dataset_data = self.fetch_dataset_data()
            self.species_data = self.fetch_species_data()
            self.subdivisions_data = self.fetch_subdivisions_data()
            yield self.data

    # Surcouchage pour test
    # def run(self, dry_run):
    #     if dry_run:
    #         print("Running in dry run mode")
    #     else:
    #         print("Running in normal mode")
    #     integrated_data = self.integrate_data()
    #     print(integrated_data)
    #     print(self.total)
    #     for item in self.items:
    #         print(self.get_geom(item))

    mapping = {
        "date_min": "eventDate",
        "date_max": "eventDate",
        "cd_nom": "speciesKey", # ou taxonID, a vérifier
        "nom_cite": "scientificName",
        "count_min": 1,
        "count_max": 1,
        "observers": "recordedBy",
        "determiner": "recordedBy",
        "meta_create_date": "eventDate",
        "meta_update_date": "eventDate",
        "place_name": "verbatimLocality",
    }
# Mapping a améliorer