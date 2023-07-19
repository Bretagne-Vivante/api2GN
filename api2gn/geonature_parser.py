import click
from geojson import Feature
from sqlalchemy.sql import func
from shapely import wkt
from geoalchemy2.shape import from_shape


from api2gn.parsers import JSONParser
from api2gn.utils import from_cd_nomenclature


def test(row):
    return row * 2


class GeoNatureParser(JSONParser):
    srid = 4326

    def __init__(self):
        super().__init__()
        self.api_filters = {**GeoNatureParser.api_filters, **self.api_filters}
        last_import = self.parser_obj.last_import
        if last_import:
            self.api_filters["filter_d_up_date_modification"] = last_import
        self.mapping = {**GeoNatureParser.mapping, **self.mapping}
        self.constant_fields = {
            **GeoNatureParser.constant_fields,
            **self.constant_fields,
        }

    @property
    def items(self):
        return self.root["items"]

    def get_geom(self, row):
        geom = wkt.loads(row["wkt_4326"])
        return from_shape(geom, srid=4326)

    mapping = {
        "unique_id_sinp": "id_perm_sinp",
        "unique_id_sinp_grp": "id_perm_grp_sinp",
        "date_min": "date_debut",
        "date_max": "date_fin",
        "cd_nom": "cd_nom",
        "nom_cite": "nom_cite",
        "count_min": "nombre_min",
        "count_max": "nombre_max",
        "altitude_min": "altitude_min",
        "altitude_max": "altitude_max",
        "depth_max": "profondeur_min",
        "observers": "observateurs",
        "determiner": "determinateur",
        "sample_number_proof": "numero_preuve",
        "digital_proof": "preuve_numerique",
        "non_digital_proof": "preuve_non_numerique",
        "comment_context": "comment_releve",
        "comment_description": "comment_occurrence",
        "meta_create_date": "date_creation",
        "meta_update_date": "date_modification",
        "cd_hab": "code_habitat",
        "place_name": "nom_lieu",
        "precision": "precision",
        "grp_method": "methode_regroupement",
        "id_nomenclature_info_geo_type": "type_info_geo",
        "id_nomenclature_grp_typ": "type_regroupement",
        "id_nomenclature_behaviour": "comportement",
        "id_nomenclature_obs_technique": "technique_obs",
        "id_nomenclature_bio_status": "statut_biologique",
        "id_nomenclature_bio_condition": "etat_biologique",
        "id_nomenclature_naturalness": "naturalite",
        "id_nomenclature_exist_proof": "preuve_existante",
        "id_nomenclature_obj_count": "objet_denombrement",
        "id_nomenclature_sensitivity": "niveau_sensibilite",
        "id_nomenclature_observation_status": "statut_observation",
        "id_nomenclature_blurring": "floutage_dee",
        "id_nomenclature_source_status": "statut_source",
        "id_nomenclature_determination_method": "methode_determination",
    }
    # last action ?
