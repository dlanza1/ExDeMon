
import json

from flask import Flask, request, abort
from flask import abort
from flask.views import MethodView
from flask import jsonify
from sqlalchemy.sql import text

from exdemon.application import app
from exdemon.constants import *
from exdemon.database import Actuator, ActuatorSchema, db

class ActuatorEndpoint(MethodView):
    def get(self, id, project, environment, name):
        if id:
            # If id is defined, return the actuator with that id in database.
            try:
                actuator = Actuator.query.filter(Actuator.id == id).first()
                if actuator:
                    actuator_schema = ActuatorSchema()
                    output = actuator_schema.dump(actuator).data
                    return jsonify({"actuator": output}), OK
                else:
                    return jsonify({"error": "Cannot find actuator with id " % id}), NOT_FOUND
            except:
                return jsonify({"error": "Cannot establish connection with database."}), INTERNAL_ERROR
        elif project and environment and name:
            # If project, environment and name are defined, search that actuator in database.
            try:
                actuator = ( Actuator.query.filter(Actuator.project == project)
                                       .filter(Actuator.environment == environment)
                                       .filter(Actuator.name == name).first() )
                if actuator:
                    actuator_schema = ActuatorSchema()
                    output = actuator_schema.dump(actuator).data
                    return jsonify({"actuator": output}), OK
                else:
                    return jsonify({"error": "Cannot find actuator with project %s, environment %s and name %s" % 
                                    (project, environment, name)}), NOT_FOUND
            except:
                return jsonify({"error": "Cannot establish connection with database."}), INTERNAL_ERROR
        else:
            # If no parameter is defined, return all.
            try:
                actuator = Actuator.query.all()
                actuator_schema = ActuatorSchema(many=True)
                output = actuator_schema.dump(actuator).data
                return jsonify({"actuators": output})
            except:
                return jsonify({"error": "Cannot establish connection with database."}), INTERNAL_ERROR

    def post(self, id, project, environment, name):
        if request.json:
            # Create a new actuator. Overwrite if there is a existing one.
            try:
                r = request.json
                db.engine.execute(text("""INSERT INTO actuator (name, project, environment, data, enabled) 
                                        VALUES (:name, :project, :environment, :data, True)
                                            ON CONFLICT (name, project, environment) DO UPDATE 
                                            SET data = excluded.data, enabled = excluded.enabled;"""), 
                                    {'name': r['name'], 'project': r['project'], 'environment': r['environment'], 
                                    'data': json.dumps(r['data'])})

                actuator_schema = ActuatorSchema()
                actuator = Actuator.query.filter(Actuator.name == r['name']).first()
                output = actuator_schema.dump(actuator).data
                return jsonify({"actuator": output}), 201
            except:
                return jsonify({"error": "Cannot establish connection with database."}), INTERNAL_ERROR
        else:
            abort(BAD_REQUEST)

    def delete(self, id, project, environment, name):
        abort(NOT_IMPLEMENTED)
    def put(self, id, project, environment, name):
        abort(NOT_IMPLEMENTED)
