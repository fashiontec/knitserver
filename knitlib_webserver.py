# -*- coding: utf-8 -*-
# This file is part of Knitlib.
#
#    Knitlib is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    Knitlib is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with Knitlib.  If not, see <http://www.gnu.org/licenses/>.
#
#    Copyright 2015 Sebastian Oliva <http://github.com/fashiontec/knitlib>
__author__ = "tian"

import logging
from flask import Flask, jsonify, request
from flask.ext.socketio import SocketIO, emit
from flask_cors import cross_origin
from gevent import spawn

import knitlib
from knitlib.knitting_job import KnittingJob

app = Flask(__name__)
# app.config['SECRET_KEY'] = 'secret'
app.config.from_object('config_module.DevelopmentConfig')
socketio = SocketIO(app)
# A reference for creating new RESTful endpoints:
# http://blog.luisrei.com/articles/flaskrest.html


job_dict = {}


@app.route('/')
def hello_world():
    return 'Hello World!'


@app.route('/v1/get_machine_plugins')
@cross_origin()
def get_machine_plugins():
    return jsonify({"active_plugins": knitlib.machine_handler.get_active_machine_plugins_names()})


@app.route('/v1/get_ports')
@cross_origin()
def get_ports():
    port_dict = dict([(p[0], p[1]) for p in knitlib.machine_handler.get_available_ports()])
    return jsonify(port_dict)


@app.route('/v1/get_job_status/<job_id>')
@cross_origin()
def get_job_status(job_id):
    job_d = job_dict[job_id].get_job_public_dict()
    return jsonify(job_d)


@app.route('/v1/create_job/', methods=["POST"])
@cross_origin()
def create_knitting_job():
    """Creates a knitting job and inits the Machine plugin returning the job id."""
    plugin_id = request.form['plugin_id']
    port_str = request.form['port']
    plugin_class = knitlib.machine_handler.get_machine_plugin_by_id(plugin_id)
    job = KnittingJob(plugin_class, port_str)
    job_string_id = str(job.id)
    job_dict[job_string_id] = job
    return jsonify({"job_id": job_string_id})


@app.route('/v1/init_job/<job_id>', methods=["POST"])
@cross_origin()
def init_job(job_id):
    job = job_dict.get(job_id)
    job.init_job()
    return get_job_status(job_id)


@app.route('/v1/configure_job/<job_id>', methods=["POST"])
@cross_origin()
def configure_knitting_job(job_id):
    """Configures job based on Knitpat file."""
    knitpat_string = request.form['knitpat_dict']
    knitpat_dict = knitlib.knitpat.parse_ustring(knitpat_string)
    knitlib.knitpat.validate_dict(knitpat_dict)
    job = job_dict.get(job_id)
    job.configure_job(knitpat_dict)
    return get_job_status(job_id)


@app.route('/v1/knit_job/<job_id>', methods=["POST"])
@cross_origin()
def knit_job(job_id):
    """Starts the knitting process for Job ID."""
    job = job_dict.get(job_id)
    try:
        job = job_dict.get(job_id)
        spawn(job.knit_job)
    except Exception as e:
        logging.error(e)
        return jsonify({"error": "Error on init of knitjob."})
    return get_job_status(job_id)


@socketio.on('get_progress', namespace='/v1/knitting_socket')
def emit_progress(message):
    emit('progress', {'data': message['data']})


def emit_message_dict():
    pass


def emit_blocking_action_notification_dict():
    pass


def receive_blocking_action():
    pass


def emit_progress():
    pass

if __name__ == '__main__':
    # app.run(debug=True)
    app.debug = False
    socketio.run(app)