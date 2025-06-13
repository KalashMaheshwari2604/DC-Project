import uuid
import datetime
import logging
import json
from flask import Flask, flash, request, redirect, url_for, render_template_string, send_file
from pathlib import Path
import os
import grpc
import dataverse_pb2 as service
import dataverse_pb2_grpc as rpc
from google.protobuf.json_format import MessageToJson

ALLOWED_EXTENSIONS = {'png', 'jpg', 'jpeg', 'mp3'}
CHUNK_SIZE = 1024 * 1024  # 1 MB
app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

stub = None
channel = None


def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


def connectTo(ip, port):
    global stub, channel
    if channel:
        channel.close()
    channel = grpc.insecure_channel(f"{ip}:{port}")
    stub = rpc.DataVerseStub(channel)


@app.after_request
def add_header(r):
    r.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    r.headers["Pragma"] = "no-cache"
    r.headers["Expires"] = "0"
    return r


@app.template_filter('json_loads')
def json_loads_filter(s):
    return json.loads(s) if s else None


@app.route('/', methods=['GET', 'POST'])
def upload_file():
    if request.method == 'POST':
        if 'selected_files' not in request.files:
            flash("No 'selected_files' part")
            return redirect(request.url)

        selected_files = request.files.getlist("selected_files")
        uname = request.form["username"]
        IP = request.form["IP"]
        PORT = request.form["PORT"]
        connectTo(IP, PORT)

        results = []
        for i, file in enumerate(selected_files, 1):
            if file.filename == '':
                flash('No selected file')
                return redirect(request.url)

            if file and allowed_file(file.filename):
                def upload_request_generator():
                    while True:
                        chunk = file.read(CHUNK_SIZE)
                        if chunk:
                            yield service.ImageUploadRequest(Content=chunk, Id=file.filename,
                                                             StatusCode=service.ImageUploadStatusCode.InProgress,
                                                             Username=uname)
                        else:
                            yield service.ImageUploadRequest(Id=file.filename,
                                                             StatusCode=service.ImageUploadStatusCode.Ok,
                                                             Username=uname)
                            file.seek(0)
                            break

                output_result = stub.Upload(upload_request_generator())
                results.append(MessageToJson(output_result))
                if output_result.nodeConnections:
                    arr = output_result.nodeConnections[0].split(":")
                    connectTo(arr[0], arr[1])
                    stub.Upload(upload_request_generator())

        return redirect(url_for('upload_file', json=json.dumps(results)))

    return render_template_string('''
    <!doctype html>
    <title>Upload new File</title>
    <h1>Upload new File</h1>
    <form method=post enctype=multipart/form-data>
        <h3>Server</h3>
            IPv4 Address <input type=text name=IP required><br><br>
            Port Number  <input type=text name=PORT required><br><br>
        <input type=file name=selected_files multiple required>
        <input type=text name=username required>
        <input type=submit value=Upload>
    </form>
    {% if json_response %}
    <h1>Last upload</h1>
    <ol>
    {% for item in (json_response|json_loads) %}
    <li>{{ (item|json_loads) }}</li>
    {% endfor %}
    </ol>
    {% endif %}
    ''', json_response=request.args.get('json'))


@app.route('/search', methods=['GET', 'POST'])
def search_file():
    if request.method == 'POST':
        IP = request.form["IP"]
        PORT = request.form["PORT"]
        uname = request.form["username"]
        fname = request.form["filename"]

        connectTo(IP, PORT)
        results = []
        visited = set()
        queue = [f"{IP}:{PORT}"]
        content = None

        while queue:
            node = queue.pop(0)
            if node in visited:
                continue
            visited.add(node)
            ip, port = node.split(":")
            connectTo(ip, port)

            result = stub.Search(service.SearchRequest(Filename=fname, Username=uname))
            results.append(MessageToJson(result))

            if result.found == "YES":
                content = result.Content
                break

            for neighbor in result.nodeConnections:
                if neighbor not in visited:
                    queue.append(neighbor)

        if content:
            path = "./downloadsTemp"
            Path(path).mkdir(parents=True, exist_ok=True)
            with open(f"{path}/{fname}", 'wb') as f:
                f.write(content)

        return redirect(url_for('search_file', json=json.dumps(results)))

    return render_template_string('''
    <!doctype html>
    <title>Search File</title>
    <h1>Search File</h1>
    <form method=post>
      <h3>Server </h3>
      IPv4 Address <input type=text name=IP required><br><br>
      Port Number <input type=text name=PORT required><br><br>
      Username <input type=text name=username required><br><br>
      Filename <input type=text name=filename required><br><br>
      <input type=submit value=download>
    </form>
    ''', json_response=request.args.get('json'))


@app.route('/config', methods=['GET', 'POST'])
def config():
    if request.method == 'POST':
        IP1 = request.form["IP1"]
        PORT1 = request.form["PORT1"]
        IP2 = request.form["IP2"]
        PORT2 = request.form["PORT2"]

        connectTo(IP1, PORT1)
        result1 = stub.Config(service.ConfigRequest(Server=f"{IP2}:{PORT2}"))

        connectTo(IP2, PORT2)
        result2 = stub.Config(service.ConfigRequest(Server=f"{IP1}:{PORT1}"))

        results = [MessageToJson(result1), MessageToJson(result2)]
        return redirect(url_for('config', json=json.dumps(results)))

    return render_template_string('''
    <!doctype html>
    <title>Connect to a Node</title>
    <h1>Connect to a Node</h1>
    <form method=post>
        <h3>Server 1 </h3>
        IPv4 Address: <input type=text name=IP1 required><br><br>
        Port Number: <input type=text name=PORT1 required><br><br>
        <h3>Server 2 </h3>
        IPv4 Address: <input type=text name=IP2 required><br><br>
        Port Number: <input type=text name=PORT2 required><br><br>
        <input type=submit value=Connect>
    </form>
    {% if json_response %}
    <h1>Configuration Results</h1>
    <ol>
    {% for item in (json_response|json_loads) %}
    <li>{{ (item|json_loads) }}</li>
    {% endfor %}
    </ol>
    {% endif %}
    ''', json_response=request.args.get('json'))


if __name__ == '__main__':
    app.run(debug=True, port=5000)
