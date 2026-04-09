import json
import os
import uuid
from pathlib import Path

from flask import Flask, Response, redirect, render_template, request, url_for
from werkzeug.utils import secure_filename

from app.services.analyzer import ANALYSIS_MODULES, run_deterministic_analysis, to_csv

BASE_DIR = Path(__file__).resolve().parent.parent
UPLOAD_DIR = BASE_DIR / "uploads"
UPLOAD_DIR.mkdir(exist_ok=True)

ALLOWED_EXTENSIONS = {"txt", "log", "csv", "html", "htm", "sql"}


def allowed_file(filename: str) -> bool:
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


def severity_badge(severity: str) -> str:
    sev = (severity or "YELLOW").upper()
    return {"RED": "🔴", "YELLOW": "🟡", "GREEN": "🟢"}.get(sev, "🟡")


app = Flask(__name__)
app.secret_key = "deterministic-awr-analyzer-secret"
REPORT_CACHE = {}


@app.after_request
def disable_cache(response):
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


@app.get("/")
def index():
    return render_template(
        "index.html",
        app_name="Oracle AWR Deterministic Miner",
        modules=ANALYSIS_MODULES,
    )


@app.post("/analyze")
def analyze():
    files = request.files.getlist("files")
    user_question = request.form.get("question", "")

    saved_files = []
    for file in files:
        if not file or not file.filename:
            continue
        if not allowed_file(file.filename):
            continue
        filename = secure_filename(file.filename)
        target = UPLOAD_DIR / f"{uuid.uuid4().hex}_{filename}"
        file.save(target)
        saved_files.append(target)

    if not saved_files:
        return redirect(url_for("index"))

    try:
        result = run_deterministic_analysis(saved_files, user_question)
    except Exception as exc:
        result = {
            "executive_summary": f"Analysis failed: {exc}",
            "overall_severity": "RED",
            "wait_events_table": [],
            "findings_table": [
                {
                    "finding": "Execution error",
                    "severity": "RED",
                    "evidence": str(exc),
                    "business_impact": "Analysis could not complete.",
                }
            ],
            "module_status_table": [],
            "recommendations_table": [
                {
                    "priority": "P1",
                    "area": "Runtime",
                    "recommendation": "Verify provider settings and install dependencies, then retry.",
                    "expected_outcome": "Successful report generation.",
                }
            ],
        }
    finally:
        for f in saved_files:
            try:
                os.remove(f)
            except OSError:
                pass

    report_id = uuid.uuid4().hex
    REPORT_CACHE[report_id] = result

    return redirect(url_for("result_page", report_id=report_id))


@app.get("/result/<report_id>")
def result_page(report_id: str):
    result = REPORT_CACHE.get(report_id)
    if not result:
        return redirect(url_for("index"))
    return render_template(
        "result.html",
        app_name="Oracle AWR Deterministic Miner",
        result=result,
        severity_badge=severity_badge,
        analysis_mode="deterministic",
        report_id=report_id,
        raw_json=json.dumps(result, indent=2),
    )


@app.get("/download/<report_id>/<report_type>")
def download_report(report_id: str, report_type: str):
    result = REPORT_CACHE.get(report_id)
    if not result:
        return Response("Report not found or expired", status=404)

    table_map = {
        "wait-events": "wait_events_table",
        "top-sql": "top_sql_table",
        "findings": "findings_table",
        "modules": "module_status_table",
        "recommendations": "recommendations_table",
    }
    if report_type not in table_map:
        return Response("Invalid report type", status=400)

    rows = result.get(table_map[report_type], [])
    csv_data = to_csv(rows)
    filename = f"awr_{report_type}_{report_id[:8]}.csv"
    return Response(
        csv_data,
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@app.get("/download/<report_id>/full-html")
def download_full_html(report_id: str):
    result = REPORT_CACHE.get(report_id)
    if not result:
        return Response("Report not found or expired", status=404)

    html = render_template(
        "report_export.html",
        app_name="Oracle AWR Deterministic Miner",
        result=result,
        severity_badge=severity_badge,
        analysis_mode="deterministic",
        report_id=report_id,
        raw_json=json.dumps(result, indent=2),
    )
    return Response(
        html,
        mimetype="text/html",
        headers={"Content-Disposition": f"attachment; filename=awr_report_{report_id[:8]}.html"},
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=False)
