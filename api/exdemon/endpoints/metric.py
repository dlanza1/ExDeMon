
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
    def get(self, id, project, environment, name):
        if id:
            # If id is defined, return the metric with that id in database.
            try:
                metric = Metric.query.filter(Metric.id == id).first()
                if metric:
                    metric_schema = MetricSchema()
                    output = metric_schema.dump(metric).data
                    return jsonify({"metric": output}), OK
                else:
                    return jsonify({"error": "Cannot find metric with id " % id}), NOT_FOUND
            except:
                return jsonify({"error": "Cannot establish connection with database."}), INTERNAL_ERROR
        elif project and environment and name:
            # If project, environment and name are defined, search that metric in database.
            try:
                metric = ( Metric.query.filter(Metric.project == project)
                                       .filter(Metric.environment == environment)
                                       .filter(Metric.name == name).first() )
                if metric:
                    metric_schema = MetricSchema()
                    output = metric_schema.dump(metric).data
                    return jsonify({"metric": output}), OK
                else:
                    return jsonify({"error": "Cannot find metric with project %s, environment %s and name %s" % 
                                    (project, environment, name)}), NOT_FOUND
            except:
                return jsonify({"error": "Cannot establish connection with database."}), INTERNAL_ERROR
        else:
            # If no parameter is defined, return all.
            try:
                metric = Metric.query.all()
                metric_schema = MetricSchema(many=True)
                output = metric_schema.dump(metric).data
                return jsonify({"metrics": output})
            except:
                return jsonify({"error": "Cannot establish connection with database."}), INTERNAL_ERROR

    def post(self, id, project, environment, name):
        if request.json:
            # Create a new metric. Overwrite if there is a existing one.
            try:
                r = request.json
                db.engine.execute(text("""INSERT INTO metric (name, project, environment, data, enabled) 
                                        VALUES (:name, :project, :environment, :data, True)
                                            ON CONFLICT (name, project, environment) DO UPDATE 
                                            SET data = excluded.data, enabled = excluded.enabled;"""), 
                                    {'name': r['name'], 'project': r['project'], 'environment': r['environment'], 
                                    'data': json.dumps(r['data'])})

                metric_schema = MetricSchema()
                metric = Metric.query.filter(Metric.name == r['name']).first()
                output = metric_schema.dump(metric).data
                return jsonify({"metric": output}), 201
            except:
                return jsonify({"error": "Cannot establish connection with database."}), INTERNAL_ERROR
        else:
            abort(BAD_REQUEST)

    def delete(self, id, project, environment, name):
        abort(NOT_IMPLEMENTED)
    def put(self, id, project, environment, name):
        abort(NOT_IMPLEMENTED)
