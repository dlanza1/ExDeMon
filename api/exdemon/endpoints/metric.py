
import json

from flask import Flask, request, abort
from flask import abort
from flask.views import MethodView
from flask import jsonify
from sqlalchemy.sql import text

from exdemon.application import app
from exdemon.constants import *
from exdemon.database import Metric, MetricSchema, db

class MetricEndpoint(MethodView):
    def get(self, id):
        if id:
            try:
                metric = Metric.query.filter(Metric.id == id).first()
                metric_schema = MetricSchema()
                output = metric_schema.dump(metric).data
                return jsonify({"metric": output})
            except:
                abort(INTERNAL_ERROR)
        else:
            try:
                metric = Metric.query.all()
                metric_schema = MetricSchema(many=True)
                output = metric_schema.dump(metric).data
                return jsonify({"metrics": output})
            except:
                abort(INTERNAL_ERROR)

    def post(self, id):
        if request.json:
            r = request.json
            db.engine.execute(text("INSERT INTO metric (name, project, environment, data, enabled) VALUES (:name, :project, :environment, :data, True);"), 
                                    {'name': r['name'], 'project': r['project'], 'environment': r['environment'], 'data': json.dumps(r['data'])})

            metric_schema = MetricSchema()
            metric = Metric.query.filter(Metric.name == r['name']).first()
            output = metric_schema.dump(metric).data
            return jsonify({"metric": output}), 201
        else:
            abort(BAD_REQUEST)

    def delete(self, id):
        abort(NOT_IMPLEMENTED)
    def put(self, id):
        abort(NOT_IMPLEMENTED)
