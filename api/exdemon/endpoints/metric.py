
from flask import Flask, request, abort
from flask import abort
from flask.views import MethodView
import json

from exdemon.application import app
from exdemon.constants import *
from exdemon.database import Metric, db

class MetricEndpoint(MethodView):
    def get(self):
        app.logger.info('Error metric')
        return "get"
        abort(NOT_IMPLEMENTED)
    def post(self):
        return "post"
    def delete(self):
        abort(NOT_IMPLEMENTED)
    def put(self):
        abort(NOT_IMPLEMENTED)
