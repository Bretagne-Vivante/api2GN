if __name__ == "__main__":
    occurrence_id = 4407389321  # Example occurrence ID
    parser = GBIFParser(occurrence_id)
    integrated_data = parser.integrate_data()
    print(integrated_data)
    print(parser.total)
    for item in parser.items:
        print(parser.get_geom(item))
