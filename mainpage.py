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
    data = get_bat_status()
    return render_template('Main-page.html', data = data)

@app.route("/Main-page.html")
def mainpage():
    return redirect("/")

@app.route("/Reset-Page.html")
def page_bat_status():
    return render_template('Reset-Page.html')

@app.route("/Docker-status.html")
def page_docker_status():
    docker_status = get_docker_status()
    return render_template('Docker-status.html', data = docker_status)

@app.route("/restartservices")
def restartservices():
    data = do_restart_services()
    return render_template("Restartresponse.html", data = data)

@app.route("/Reporting.html")
def daily_report():
    return render_template("Reporting.html")

@app.route("/daily-report-html")
def daily_report_html():
    inputs = dict(request.args)
    print(inputs)
    if 'Excel' in inputs["document"]:
        filename = generate_report(datestart = inputs['datestart'], dateend = inputs['dateend'])
        return redirect(f"/download/{filename}")
    elif 'HTML' in inputs["document"]:
        data = generate_report(datestart=inputs['datestart'], dateend=inputs['dateend'], kind="html")
        return render_template("Sheet 0 Kertas Kerja BATTJA.html", home = data['home'], s1 = data['s1'], s2 = data['s2'])
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