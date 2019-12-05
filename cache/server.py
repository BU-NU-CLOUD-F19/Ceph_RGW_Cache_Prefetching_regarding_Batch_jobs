#!/usr/bin/python
"""
Main module of the server file
"""

# 3rd party moudles
import connexion
import daemon
from flask import render_template


cache_port=3187 #FIXME: Later add it as a configuration variable
cache_host='0.0.0.0'

# Create the application instance
app = connexion.App(__name__, specification_dir="./")

# Read the swagger.yml file to configure the end points
app.add_api("cache.yml")

cache = daemon.start_cache();

# create a URL route in our application for "/"
@app.route("/")
def home():
	return render_template("home.html")

if __name__ == "__main__":
    app.run(debug=True, port=cache_port, host=cache_host)
