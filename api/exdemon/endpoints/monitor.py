
import json

from flask import Flask, request, abort
from flask import abort
from flask.views import MethodView
from flask import jsonify
from sqlalchemy.sql import text

from exdemon.application import app
from exdemon.constants import *
from exdemon.database import Monitor, MonitorSchema, db

class MonitorEndpoint(MethodView):
    def get(self, id, project, environment, name):
        if id:
            # If id is defined, return the monitor with that id in database.
            try:
                monitor = Monitor.query.filter(Monitor.id == id).first()
                if monitor:
                    monitor_schema = MonitorSchema()
                    output = monitor_schema.dump(monitor).data
                    return jsonify({"monitor": output}), OK
                else:
                    return jsonify({"error": "Cannot find monitor with id " % id}), NOT_FOUND
            except:
                return jsonify({"error": "Cannot establish connection with database."}), INTERNAL_ERROR
        elif project and environment and name:
            # If project, environment and name are defined, search that monitor in database.
            try:
                monitor = ( Monitor.query.filter(Monitor.project == project)
                                       .filter(Monitor.environment == environment)
                                       .filter(Monitor.name == name).first() )
                if monitor:
                    monitor_schema = MonitorSchema()
                    output = monitor_schema.dump(monitor).data
                    return jsonify({"monitor": output}), OK
                else:
                    return jsonify({"error": "Cannot find monitor with project %s, environment %s and name %s" % 
                                    (project, environment, name)}), NOT_FOUND
            except:
                return jsonify({"error": "Cannot establish connection with database."}), INTERNAL_ERROR
        else:
            # If no parameter is defined, return all.
            try:
                monitor = Monitor.query.all()
                monitor_schema = MonitorSchema(many=True)
                output = monitor_schema.dump(monitor).data
                return jsonify({"monitors": output})
            except:
                return jsonify({"error": "Cannot establish connection with database."}), INTERNAL_ERROR

    def post(self, id, project, environment, name):
        if request.json:
            # Create a new monitor. Overwrite if there is a existing one.
            try:
                r = request.json
                db.engine.execute(text("""INSERT INTO monitor (name, project, environment, data, enabled) 
                                        VALUES (:name, :project, :environment, :data, True)
                                            ON CONFLICT (name, project, environment) DO UPDATE 
                                            SET data = excluded.data, enabled = excluded.enabled;"""), 
                                    {'name': r['name'], 'project': r['project'], 'environment': r['environment'], 
                                    'data': json.dumps(r['data'])})

                monitor_schema = MonitorSchema()
                monitor = Monitor.query.filter(Monitor.name == r['name']).first()
                output = monitor_schema.dump(monitor).data
                return jsonify({"monitor": output}), 201
            except:
                return jsonify({"error": "Cannot establish connection with database."}), INTERNAL_ERROR
        else:
            abort(BAD_REQUEST)

    def delete(self, id, project, environment, name):
        abort(NOT_IMPLEMENTED)
    def put(self, id, project, environment, name):
        abort(NOT_IMPLEMENTED)
