
from flask import Flask, request, abort
from flask.views import MethodView
import json
import sqlalchemy

NOT_IMPLEMENTED = 501

class TestApi(MethodView):
    def get(self):
        abort(NOT_IMPLEMENTED)
    def post(self):
        abort(NOT_IMPLEMENTED)
    def delete(self):
        abort(NOT_IMPLEMENTED)
    def put(self):
        abort(NOT_IMPLEMENTED)

class Application():

    def __init__(self):
        app = Flask(__name__)

        app.add_url_rule('/api/v1/metric', view_func=TestApi.as_view('metric'), methods=['GET','POST','PUT','DELETE'])
        app.add_url_rule('/api/v1/schema', view_func=TestApi.as_view('schema'), methods=['GET','POST','PUT','DELETE'])
        app.add_url_rule('/api/v1/monitor', view_func=TestApi.as_view('monitor'), methods=['GET','POST','PUT','DELETE'])

        app.run(debug=True)

