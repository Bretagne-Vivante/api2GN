from api2gn.parsers import WFSParser
from api2gn.geonature_parser import GeoNatureParser
from api2gn.gbif_parser import GBIFParser

# Fichier a renommer en parsers.py pour fonctionner car parsers.py dans .gitignore
# Amélioration a faire, un fichier par parser

def my_custom_func(value):
    """
    Custom function to fill "observers" synthese with a depending api value
    """
    if value == "Org 1":
        return "Observateur inconnu"
    else:
        return "Observateur 1"


class GBIFParserInaturalist(GBIFParser):
    name = "GBIF_INaturalist"
    description = "test intégration GBIF_INaturalist" 
    url = "" # pas nécessaire car usage de la lib mais obligatoire pour le moment
    # filter to have only one dataset
    # api_filters = {"jdd_uuid": "4d331cae-65e4-4948-b0b2-a11bc5bb46c2"}
    # override existant GeoNatureParser mapping
    # the key is the name of the column is synthese
    # the value could be a str of the column in the API or a dict for a custom value
    occurrence_ids = [4407389321] ## Vous pouvez remplacer par une liste dynamique d'IDs
    dynamic_fields = {
        # "unique_dataset_id" : "69f26484-08b6-4ccf-aeeb-42124d124fa1", # JDD test Inaturalist
        # "id_dataset" : 705
    #    # "occurence_id" : "4407389321",
    #     "altitude_min": my_custom_func
    }
    
    mapping = {
        # "unique_id_sinp": "id_perm_sinp",
        # "unique_id_sinp_grp": "id_perm_grp_sinp",
        "date_min": "eventDate",
        "date_max": "eventDate",
        "nom_cite": "scientificName",

        "observers": "recordedBy",
        "determiner": "recordedBy",
        "meta_create_date": "eventDate",
        "meta_update_date": "eventDate",
        "place_name": "verbatimLocality",
    }

    # pass constant from missing value in my API
    constant_fields = {
        "id_source": 1, # a creer ou a récupérer depuis metadonnées
        "id_dataset": 705, # Creer JDD test, a terme récupérer les métadonnées et creer JDD en auto
        "count_min": 1, # Non disponible dans api
        "count_max": 1, # Non disponible dans api
        "cd_nom":  4001,

    }
