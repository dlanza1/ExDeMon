
from flask import Flask, request, abort
from flask import abort
from flask.views import MethodView
from flask import jsonify

from exdemon.application import app
from exdemon.constants import *
from exdemon.database import Metric, MetricSchema, db

class MetricEndpoint(MethodView):
    def get(self):
        try:
            metric = Metric.query.all()
            metric_schema = MetricSchema(many=True)
            output = metric_schema.dump(metric).data
            return jsonify({"metrics": output})
        except:
            abort(INTERNAL_ERROR)
    def post(self):
        if request.json:
            metric = Metric(request.json['name'], request.json['project'], request.json['environment'], json.dumps(request.json['data']), True)
        else:
            abort(BAD_REQUEST)

        db.session.add(metric)
        db.session.commit()
        return 'ok', 201
    def delete(self):
        abort(NOT_IMPLEMENTED)
    def put(self):
        abort(NOT_IMPLEMENTED)
