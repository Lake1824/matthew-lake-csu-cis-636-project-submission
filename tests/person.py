from opensearchpy import Document, Integer, Text, Nested, InnerDoc
from opensearchpy.helpers.query import GeoShape


class HouseLocation(InnerDoc):
    location = GeoShape()
    city = Text()


class Person(Document):
    id = Integer()
    name = Text()
    age = Integer()
    height = Integer()
    house_location = Nested(HouseLocation)
