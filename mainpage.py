from flask import Flask, jsonify, render_template, request
from process import *
import time, subprocess

app = Flask(__name__)
debug_mode = True
fetch_realtime = True

@app.route("/")
def page_home():
    return render_template('Home.html')

@app.route("/bat-status")
def page_bat_status():
    data = get_bat_status()
    return render_template('BAT Status.html', data = data)

@app.route("/docker-status")
def page_docker_status():
    docker_status = get_docker_status()
    return render_template('Docker Status.html', data = docker_status)

@app.route("/restartservices")
def restartservices():
    data = do_restart_services()
    return render_template("Restartresponse.html", data = data)

@app.route("/daily-report")
def daily_report():
    return render_template("Daily Report.html")

@app.route("/daily-report-BAT")
def daily_report_BAT():
    inputs = dict(request.args)
    data = get_daily_report_BAT(unitname = inputs['unitName'],date = inputs['dateInput'])
    return render_template("Daily Report Monitoring BAT UJTA.html", data=data)


if __name__ == "__main__":
    if debug_mode:
        app.run('0.0.0.0', port=5002, debug=debug_mode)