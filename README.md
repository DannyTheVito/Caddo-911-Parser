# Caddo 911 Parser
A script to parse [Caddo 911](http://ias.ecc.caddo911.com/All_ActiveEvents.aspx) and input the calls into a MySQL database.

## What does this do?
This script will parse the Caddo 911 site every 5 minutes and store the calls in a MySQL database. It will make a table for every agency it scrapes except for the Caddo Fire Departments which get put into a single CFD table. It records when it was input into the database and generates a hash to avoid duplications.

## Requirements
 - MySQL
 - [mysql-connector-python](https://pypi.python.org/pypi/mysql-connector-python/2.0.4)
 - [beautifulsoup4](https://pypi.org/project/beautifulsoup4/)
 - [requests](https://pypi.org/project/requests/)
 - Python 3

## To Use:
 - pip install -r requirements
 - Setup your MySQL database
 - Setup your database credentials in config.py
 - Run cops.py
