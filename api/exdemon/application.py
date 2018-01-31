
from flask import Flask

import exdemon.api

app = Flask(__name__)

class Application():
    def __init__(self):
        # Add endpoints
        exdemon.api.Api(app)
        
        # Run application
        app.run(debug=True)

