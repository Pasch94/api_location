import grpc
import json
import os

from datetime import datetime

from shapely.geometry import Point
from geoalchemy2.shape import from_shape
from app.udaconnect.proto.location_pb2 import LocationRequest
from app.udaconnect.proto.location_pb2_grpc import LocationServiceStub
from app.udaconnect.models import Location
from app.udaconnect.schemas import LocationSchema
from app.udaconnect.services import LocationService 
from flask import request, Response, g
from flask_accepts import accepts, responds
from flask_restx import Namespace, Resource
from typing import Optional, List
from kafka import KafkaProducer

DATE_FORMAT = "%Y-%m-%d"

api = Namespace("UdaConnect", description="Connections via geolocation.")  # noqa

TOPIC_NAME = os.getenv('KAFKA_LOCATION_TOPIC', 'location')
KAFKA_HOST = os.getenv('KAFKA_HOST', '192.168.65.4')
KAFKA_PORT = os.getenv('KAFKA_PORT', '9092')
KAFKA_SERVER = ':'.join([KAFKA_HOST, KAFKA_PORT])
kafka_producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        api_version=(2, 8, 0),
        client_id=__name__,
        value_serializer=lambda m: json.dumps(m).encode('utf-8'))

GRPC_PORT = os.getenv('GRPC_PORT', '5005')
GRPC_HOST = os.getenv('GRPC_HOST', 'localhost')

print(':'.join([GRPC_HOST, GRPC_PORT]))
GRPC_CHANNEL = grpc.insecure_channel(':'.join([GRPC_HOST, GRPC_PORT]), options=(('grpc.enable_http_proxy', 0),))
grpc_stub = LocationServiceStub(GRPC_CHANNEL)

@api.route("/locations")
@api.route("/locations/<location_id>")
@api.param("location_id", "Unique ID for a given Location", _in="query")
class LocationResource(Resource):
    @accepts(schema=LocationSchema)
    @responds(schema=LocationSchema)
    def post(self) -> Location:
        kafka_data = request.get_json().encode()
        kafka_producer = kafka_producer
        kafka_producer.send(TOPIC_NAME, location)
        return Response(status=202)

    @responds(schema=LocationSchema)
    def get(self, location_id) -> Location:
        location = grpc_stub.Get(LocationRequest(id=int(location_id)))
        print("Got grpc response: {}".format(location))
        ret_location = Location()
        ret_location.person_id = location.person_id
        ret_location.coordinate = from_shape(Point(location.longitude, location.latitude))
        ret_location.creation_time = datetime.fromtimestamp(location.creation_time)
        return ret_location



