#!/bin/sh

echo "Installing environment..."
virtualenv venv
source venv/bin/activate
pip install -r requirements.txt
echo "Installing Exdemon..."
python setup.py install
echo "Done"
echo "To execute the API just run the command 'exdemon-api'.