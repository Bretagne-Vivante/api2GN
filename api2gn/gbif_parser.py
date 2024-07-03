from pygbif import occurrences, registry, species
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
        self.data = self.fetch_occurrence_data()
        self.organization_data = self.fetch_organization_data()
        self.dataset_data = self.fetch_dataset_data()
        self.species_data = self.fetch_species_data()
        self.subdivisions_data = self.fetch_subdivisions_data()
        self.validate_mapping()

    def fetch_occurrence_data(self):
        response = occurrences.get(self.occurrence_id)
        if response.get('status') == 'success':
            return response['data']
        else:
            raise ValueError(f"Failed to fetch data for occurrence ID {self.occurrence_id}")

    def fetch_organization_data(self):
        organization_key = self.data.get('publishingOrgKey')
        if organization_key:
            return registry.organizations(organization_key)
        return {}

    def fetch_dataset_data(self):
        dataset_key = self.data.get('datasetKey')
        if dataset_key:
            return registry.datasets(dataset_key)
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
        return 1  # Only one item in this case

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
        integrated_data['subdivisions'] = self.subdivisions_data
        return integrated_data


    mapping = {
        "unique_id_sinp": "id_perm_sinp",
        "unique_id_sinp_grp": "id_perm_grp_sinp",
        "date_min": "eventDate",
        "date_max": "eventDate",
        "cd_nom": "cd_nom",
        "nom_cite": "species",
        "count_min": "nombre_min",
        "count_max": "nombre_max",
        # "altitude_min": "altitude_min",
        # "altitude_max": "altitude_max",
        # "depth_max": "profondeur_min",
        "observers": "recordedBy",
        "determiner": "recordedBy",
        # "sample_number_proof": "numero_preuve",
        # "digital_proof": "preuve_numerique",
        # "non_digital_proof": "preuve_non_numerique",
        # "comment_context": "comment_releve",
        # "comment_description": "comment_occurrence",
        "meta_create_date": "eventDate",
        "meta_update_date": "eventDate",
        "cd_hab": "code_habitat",
        "place_name": "nom_lieu",
        # "precision": "precision",
        # "grp_method": "methode_regroupement",
        # "id_nomenclature_info_geo_type": "type_info_geo",
        # "id_nomenclature_grp_typ": "type_regroupement",
        # "id_nomenclature_behaviour": "comportement",
        # "id_nomenclature_obs_technique": "technique_obs",
        # "id_nomenclature_bio_status": "statut_biologique",
        # "id_nomenclature_bio_condition": "etat_biologique",
        # "id_nomenclature_naturalness": "naturalite",
        # "id_nomenclature_exist_proof": "preuve_existante",
        # "id_nomenclature_obj_count": "objet_denombrement",
        # "id_nomenclature_sensitivity": "niveau_sensibilite",
        # "id_nomenclature_observation_status": "statut_observation",
        # "id_nomenclature_blurring": "floutage_dee",
        # "id_nomenclature_source_status": "statut_source",
        # "id_nomenclature_determination_method": "methode_determination",
    }

if __name__ == "__main__":
    occurrence_id = 4407389321  # Example occurrence ID
    parser = GBIFParser(occurrence_id)
    integrated_data = parser.integrate_data()
    print(integrated_data)
    print(parser.total)
    for item in parser.items:
        print(parser.get_geom(item))
