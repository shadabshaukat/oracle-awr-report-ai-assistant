import json
import os
import uuid
from pathlib import Path

from dotenv import load_dotenv
from flask import Flask, redirect, render_template, request, url_for
from werkzeug.utils import secure_filename

from app.config import settings
from app.services.analyzer import ANALYSIS_MODULES, run_analysis, run_deterministic_analysis

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent
UPLOAD_DIR = BASE_DIR / "uploads"
UPLOAD_DIR.mkdir(exist_ok=True)

ALLOWED_EXTENSIONS = {"txt", "log", "csv", "html", "htm", "sql"}


def allowed_file(filename: str) -> bool:
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


def load_system_prompt() -> str:
    prompt_path = Path(__file__).resolve().parent / "prompts" / "system_prompt.txt"
    return prompt_path.read_text(encoding="utf-8")


def severity_badge(severity: str) -> str:
    sev = (severity or "YELLOW").upper()
    return {"RED": "🔴", "YELLOW": "🟡", "GREEN": "🟢"}.get(sev, "🟡")


app = Flask(__name__)
app.secret_key = settings.secret_key


@app.get("/")
def index():
    return render_template(
        "index.html",
        app_name=settings.app_name,
        modules=ANALYSIS_MODULES,
        provider=settings.llm_provider,
        model=settings.llm_model,
    )


@app.post("/analyze")
def analyze():
    files = request.files.getlist("files")
    user_question = request.form.get("question", "")
    analysis_mode = request.form.get("analysis_mode", "ai").lower()

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
        if analysis_mode == "non-ai":
            result = run_deterministic_analysis(saved_files, user_question)
        else:
            result = run_analysis(load_system_prompt(), saved_files, user_question)
    except Exception as exc:
        result = {
            "executive_summary": f"Analysis failed: {exc}",
            "overall_severity": "RED",
            "top_findings": [
                {
                    "title": "Execution error",
                    "severity": "RED",
                    "evidence": str(exc),
                    "recommendation": "Verify LLM provider settings in .env and retry.",
                    "priority": "P1",
                }
            ],
            "module_insights": [],
            "optimization_plan": [],
            "questions_for_user": [],
        }
    finally:
        for f in saved_files:
            try:
                os.remove(f)
            except OSError:
                pass

    return render_template(
        "result.html",
        app_name=settings.app_name,
        result=result,
        severity_badge=severity_badge,
        provider=settings.llm_provider,
        model=settings.llm_model,
        analysis_mode=analysis_mode,
        raw_json=json.dumps(result, indent=2),
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=False)
