from flask import Flask, render_template, redirect
from flask import request, send_from_directory
from process import *
from dailyreporting import generate_report
from config import FOLDER_NAME
import time, os

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

@app.route("/daily-report-html")
def daily_report_html():
    inputs = dict(request.args)
    if inputs["unitName"] == 'Excelfile':
        filename = generate_report(datestart = inputs['datestart'], dateend = inputs['dateend'])
        return redirect(f"/download/{filename}")
    elif inputs["unitName"] == "HTMLfile":
        data = generate_report(datestart=inputs['datestart'], dateend=inputs['dateend'], kind="html")
        return render_template("Perhitungan Kertas Kerja BATTJA.htm", home = data['home'], s1 = data['s1'], s2 = data['s2'])
    else:
        return render_template("Daily Report Monitoring BAT UJTA.html", data={})

@app.route("/download/<filename>")
def download(filename):
    path = os.path.join(FOLDER_NAME, filename)
    if os.path.isfile(path):
        return send_from_directory(FOLDER_NAME, filename, as_attachment=True)
    else:
        listfiles = os.listdir(FOLDER_NAME)
        return f"""file {filename} not found. \nFile found: {listfiles}"""

if __name__ == "__main__":
    app.run('0.0.0.0', port=5002, debug=True)