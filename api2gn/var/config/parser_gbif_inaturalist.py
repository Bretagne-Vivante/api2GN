from api2gn.parsers import WFSParser
from api2gn.geonature_parser import GeoNatureParser
from api2gn.gbif_parser import GBIFParser


def my_custom_func(value):
    """
    Custom function to fill "observers" synthese with a depending api value
    """
    if value == "Org 1":
        return "Observateur inconnu"
    else:
        return "Observateur 1"


class GBIFParserInaturalist(GBIFParser):
    name = "GBIF INaturalist"
    url = "http://geonature.fr/truc" # pas nécessaire si usage de la lib
    # filter to have only one dataset
    api_filters = {"jdd_uuid": "4d331cae-65e4-4948-b0b2-a11bc5bb46c2"}
    # override existant GeoNatureParser mapping
    # the key is the name of the column is synthese
    # the value could be a str of the column in the API or a dict for a custom value
    dynamic_fields = {"altitude_min": my_custom_func}
    mapping = {
        "observers": {"key": "col_from_api", "func": my_custom_func},
        "altitude_min": "my_api_altitude_column",
    }
    # pass constant from missing value in my API
    constant_fields = {
        "id_source": 1, # a creer ou a récupérer depuis metadonnées
        "id_dataset": 2 # Creer JDD test, a termet récupérer les métadonnées et creer JDD en auto
    }


