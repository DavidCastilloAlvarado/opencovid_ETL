import googlemaps

GCP_API_KEY = "AIzaSyB26MXUI4g2DyxxZJgIL8pMNp0MDP9WszY"


class Get_direction(object):
    GCP_API_KEY = GCP_API_KEY

    def __init__(self, ):
        self.gmaps = googlemaps.Client(key=self.GCP_API_KEY)

    def get_geometry(self, address):
        """
        address: string con la dirección que se prentede extraer sus posiciones GPS
        """
        customer = {}
        geo_result = self.gmaps.find_place(
            input_type="textquery", input=address, fields=["geometry"])
        if geo_result["status"] == "OK":
            customer["latitude"] = geo_result["candidates"][0]["geometry"]["location"]["lat"]
            customer["longitude"] = geo_result["candidates"][0]["geometry"]["location"]["lng"]
        return customer
