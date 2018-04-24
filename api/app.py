#!/usr/bin/env python
# -*- coding: utf-8 -*-

from flask import Flask, request
import json  
import sqlalchemy  

app = Flask(__name__)

@app.route('/')
def index():
    connection_string = 'postgresql://postgres:1234@localhost:5432/exdemon'
    
    db = sqlalchemy.create_engine(connection_string)  
    engine = db.connect()  
    meta = sqlalchemy.MetaData(engine) 

    result = engine.execute("SELECT 1")
    return result

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        #do_the_login()
        return "Login"
    else:
        return "Show login form"

if __name__ == '__main__':
    app.run(debug=True)