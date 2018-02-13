
from flask import Flask

from exdemon.config import config

app = Flask(__name__)
app.config['DEBUG'] = config.getboolean('api', 'debug')
app.config['SQLALCHEMY_ECHO'] = config.getboolean('api', 'debug')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SQLALCHEMY_DATABASE_URI'] = config.get('database', 'connection')

import exdemon.api

class Application():
    def __init__(self):
        # Add endpoints
        exdemon.api.Api(app)
        
        # Run application
        app.run(host = config.get('api', 'host'), port = config.getint('api', 'port'))

