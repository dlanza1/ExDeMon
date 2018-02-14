
import json

from flask import Flask, request, abort
from flask import abort
from flask.views import MethodView
from flask import jsonify
from sqlalchemy.sql import text

from exdemon.application import app
from exdemon.constants import *
from exdemon.database import Schema, SchemaSchema, db

class SchemaEndpoint(MethodView):
    def get(self, id, project, environment, name):
        if id:
            # If id is defined, return the schema with that id in database.
            try:
                schema = Schema.query.filter(Schema.id == id).first()
                if schema:
                    schema_schema = SchemaSchema()
                    output = schema_schema.dump(schema).data
                    return jsonify({"schema": output}), OK
                else:
                    return jsonify({"error": "Cannot find schema with id " % id}), NOT_FOUND
            except:
                return jsonify({"error": "Cannot establish connection with database."}), INTERNAL_ERROR
        elif project and environment and name:
            # If project, environment and name are defined, search that schema in database.
            try:
                schema = ( Schema.query.filter(Schema.project == project)
                                       .filter(Schema.environment == environment)
                                       .filter(Schema.name == name).first() )
                if schema:
                    schema_schema = SchemaSchema()
                    output = schema_schema.dump(schema).data
                    return jsonify({"schema": output}), OK
                else:
                    return jsonify({"error": "Cannot find schema with project %s, environment %s and name %s" % 
                                    (project, environment, name)}), NOT_FOUND
            except:
                return jsonify({"error": "Cannot establish connection with database."}), INTERNAL_ERROR
        else:
            # If no parameter is defined, return all.
            try:
                schema = Schema.query.all()
                schema_schema = SchemaSchema(many=True)
                output = schema_schema.dump(schema).data
                return jsonify({"schemas": output})
            except:
                return jsonify({"error": "Cannot establish connection with database."}), INTERNAL_ERROR

    def post(self, id, project, environment, name):
        if request.json:
            # Create a new schema. Overwrite if there is a existing one.
            try:
                r = request.json
                db.engine.execute(text("""INSERT INTO schema (name, project, environment, data, enabled) 
                                        VALUES (:name, :project, :environment, :data, True)
                                            ON CONFLICT (name, project, environment) DO UPDATE 
                                            SET data = excluded.data, enabled = excluded.enabled;"""), 
                                    {'name': r['name'], 'project': r['project'], 'environment': r['environment'], 
                                    'data': json.dumps(r['data'])})

                schema_schema = SchemaSchema()
                schema = Schema.query.filter(Schema.name == r['name']).first()
                output = schema_schema.dump(schema).data
                return jsonify({"schema": output}), 201
            except:
                return jsonify({"error": "Cannot establish connection with database."}), INTERNAL_ERROR
        else:
            abort(BAD_REQUEST)

    def delete(self, id, project, environment, name):
        abort(NOT_IMPLEMENTED)
    def put(self, id, project, environment, name):
        abort(NOT_IMPLEMENTED)
