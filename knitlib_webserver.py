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

import time
from tempfile import NamedTemporaryFile
import shutil
from six.moves import cStringIO
import re
import uuid
import os
from werkzeug.utils import secure_filename
from geventwebsocket import websocket
import json
from flask import Flask, jsonify, request, render_template, abort
from flask_sockets import Sockets
from flask_cors import cross_origin
from gevent import spawn, sleep
from threading import Thread

import knitlib

from knitlib.knitting_job import KnittingJob

app = Flask(__name__)
# app.config['SECRET_KEY'] = 'secret'
UPLOAD_FOLDER = '/tmp/knitlib_web_uploads/'
app.config.from_object('config_module.DevelopmentConfig')
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER


sockets = Sockets(app)
# A reference for creating new RESTful endpoints:
# http://blog.luisrei.com/articles/flaskrest.html


job_dict = {}
msg_queue = []
progress = None
thread = None


# if not app.debug:
import logging
from logging.handlers import RotatingFileHandler
file_handler = RotatingFileHandler('tmp/knitlib-webserver.log', 'a', 1 * 1024 * 1024, 10)
file_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]'))
app.logger.setLevel(logging.INFO)
file_handler.setLevel(logging.INFO)
app.logger.addHandler(file_handler)
app.logger.info('knitlib-webserver')


@app.route('/')
def hello_world():
    return 'Hello World!'


@app.route("/test_operation")
def socket_test_page():
    #TODO: simple page to test REST and Sockets operation.
    return render_template("demo_operation_template.html")


@app.route('/v1/get_machine_plugins')
@cross_origin()
def get_machine_plugins():
    return jsonify({"active_plugins": knitlib.machine_handler.get_active_machine_plugins_names()})


@app.route("/v1/plugin/<machine_id>/supported_features")
@cross_origin()
def get_machine_plugin_supported_features(machine_id):
    machine = knitlib.machine_handler.get_machine_plugin_by_id(machine_id, None)
    if machine:
        features = machine.supported_config_features()
        return jsonify(features)
    else:
        abort(404)


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
    job = KnittingJob(plugin_class, port_str, callbacks_dict={
        "blocking_user_action": emit_blocking_message,
        "progress": emit_progress,
        "message": emit_nonblocking_message
    })
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
    """Configures job based on Knitpat file and embedded Image."""
    knitpat_string = request.form['knitpat_dict']
    knitpat_dict = knitlib.knitpat.parse_ustring(knitpat_string)
    knitlib.knitpat.validate_dict(knitpat_dict)

    if "embed" in knitpat_dict.get("file_url"):
        image_data_string = knitpat_dict.get("image_data")
        image_data_obj = cStringIO(re.sub('^data:image/.+;base64,', '', image_data_string).decode('base64'))
        temp_file_obj = NamedTemporaryFile(mode='w+b', suffix='.png', delete=False)
        shutil.copyfileobj(image_data_obj, temp_file_obj)
        temp_file_obj.flush()
        # os.fsync(temp_file_obj)
        # pil_img = Image.open(cStringIO(image_data), "r")
        knitpat_dict["old_file_url"] = knitpat_dict.get("file_url")
        knitpat_dict["file_url"] = temp_file_obj.name

    job = job_dict.get(job_id)
    job.configure_job(knitpat_dict)
    return get_job_status(job_id)


@app.route('/v1/knit_job/<job_id>', methods=["POST"])
@cross_origin()
def knit_job(job_id):
    """Starts the knitting process for Job ID."""
    job = job_dict.get(job_id)
    try:
        spawn(job.knit_job)
    except Exception as e:
        logging.error(e)
        return jsonify({"error": "Error on init of knitjob."})
    return get_job_status(job_id)


@sockets.route('/v1/knitting_socket')
def knitting_socket(ws):
    """Handler for Knitting Socket operation."""
    break_emission = False

    def handle_socket_reception(ws):
        """Greenlet loop for socket reception and processing."""
        while not break_emission:
            sleep(0.5)
            message = ws.receive()
            if message:
                logging.info("message received")
                logging.info(message)
                # TODO: process message for blocking messages.
                _process_input_ws_messages(message)

    def handle_socket_emission(ws):
        while not break_emission:
            sleep(0.5)
            while len(msg_queue) >= 1:
                ms = msg_queue.pop(0)
                ws.send(json.dumps(ms))
                logging.info("Emitted from queue: {}".format(ms))

    spawn(handle_socket_reception, ws)
    spawn(handle_socket_emission, ws)


@app.route('/v1/knitserver_info')
@cross_origin()
def knitserver_info():
    # TODO: add more knitserver info.
    return jsonify({"knitlib_version": knitlib.__version__})


@sockets.route('/echo')
def echo_socket(ws):
    while True:
        message = ws.receive()
        ws.send(message)


def _process_input_ws_messages(message):
    """Internal processor for messages from client via WS."""
    pass


def emit_nonblocking_message(msg, level):
    """Callback for emitting messages."""
    # emit('emit_message_dict', msg)
    msg_queue.append({"type": "message", "data": msg, "level": level})
    logging.log("Queued emit_message_dict: {}".format(msg))


def emit_progress(percent, done, total):
    """Callback for emitting progress."""
    # emit("progress", {"percent": percent, "done": done, "total": total}, namespace="/knit")
    msg_queue.append({"type": "progress", "data": {"percent": percent, "done": done, "total": total}})
    logging.info("log info {0}, {1}, {2}".format(percent, done, total))


blocking_queue = []

def emit_blocking_message(msg, level):
    """Callback for emitting blocking message progress."""
    msg_id = uuid.uuid4()
    logging.info("Blocking Action: {}".format(msg))
    msg_queue.append({"type": "message", "data": msg, "level": level, "msg_id": unicode(msg_id)})
    blocking_queue.append(unicode(msg_id))
    # TODO: block and locks.
    time.sleep(10)
    pass


if __name__ == "__main__":
    from gevent import pywsgi
    from geventwebsocket.handler import WebSocketHandler
    server = pywsgi.WSGIServer(('', 5000), app, handler_class=WebSocketHandler)
    server.serve_forever()
