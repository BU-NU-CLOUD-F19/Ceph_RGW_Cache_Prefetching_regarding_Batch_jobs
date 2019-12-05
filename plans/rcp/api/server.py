#!/usr/bin/python
"""
Main module of the server file
"""

# 3rd party moudles
from flask import render_template
import connexion
import daemon



cache_port=3188 #FIXME: Later add it as a configuration variable
cache_host='0.0.0.0'


# Create the application instance
app = connexion.App(__name__, specification_dir="./")

# Read the swagger.yml file to configure the endpoints
app.add_api("rcp.yml")

objectstore = daemon.start_objectstore()
collector = daemon.start_estimator();
mirab = daemon.start_kariz();

# create a URL route in our application for "/"
@app.route("/")
def home():
	return render_template("home.html")


if __name__ == "__main__":
    app.run(debug=True, port=cache_port,host=cache_host)
