import json
import re
from pathlib import Path
from typing import Dict, List

from app.services.llm import provider_factory


ANALYSIS_MODULES = [
    "Database Load Profile",
    "Top SQL by Elapsed Time",
    "Wait Events & Bottlenecks",
    "IO and Storage Health",
    "Memory (SGA/PGA) Efficiency",
    "Instance Efficiency Ratios",
    "RAC / Cluster Interconnect (if present)",
    "Alert Log Critical Events",
    "Security & Privilege Risk Review",
    "Action Plan & Quick Wins",
]


def _safe_text(path: Path) -> str:
    try:
        raw = path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        raw = ""
    # Keep payload manageable for LLM
    return raw[:140000]


def _build_user_prompt(files: List[Path], user_question: str) -> str:
    parts = [
        "Analyze these uploaded Oracle performance artifacts.",
        "Use these modules:",
        *[f"- {m}" for m in ANALYSIS_MODULES],
    ]
    if user_question.strip():
        parts.append(f"User focus: {user_question.strip()}")

    for file_path in files:
        content = _safe_text(file_path)
        parts.append(f"\n### FILE: {file_path.name}\n{content}")
    return "\n".join(parts)


def _fallback_output() -> Dict:
    return {
        "executive_summary": "File-based analysis completed with limited model output parsing. Review findings and refine query.",
        "overall_severity": "YELLOW",
        "top_findings": [
            {
                "title": "Model output parsing fallback",
                "severity": "YELLOW",
                "evidence": "The model response was not strict JSON.",
                "recommendation": "Retry with a more focused question or switch model/provider.",
                "priority": "P2",
            }
        ],
        "module_insights": [
            {"module": m, "status": "YELLOW", "insight": "Needs deeper validation.", "actions": ["Run targeted follow-up analysis."]}
            for m in ANALYSIS_MODULES
        ],
        "optimization_plan": [
            {"phase": "Immediate", "tasks": ["Validate highest-impact SQL and wait events"]},
            {"phase": "Short-term", "tasks": ["Tune memory/IO configuration based on AWR trends"]},
            {"phase": "Long-term", "tasks": ["Establish periodic AWR baseline and regression detection"]},
        ],
        "questions_for_user": ["Can you upload a larger AWR window (peak + baseline) for better comparison?"],
    }


def _parse_model_json(raw: str) -> Dict:
    raw = raw.strip()
    if not raw:
        return _fallback_output()

    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        pass

    match = re.search(r"\{[\s\S]*\}", raw)
    if match:
        try:
            return json.loads(match.group(0))
        except json.JSONDecodeError:
            return _fallback_output()
    return _fallback_output()


def run_analysis(system_prompt: str, files: List[Path], user_question: str) -> Dict:
    provider = provider_factory()
    user_prompt = _build_user_prompt(files, user_question)
    raw = provider.generate(system_prompt, user_prompt)
    return _parse_model_json(raw)


def run_deterministic_analysis(files: List[Path], user_question: str = "") -> Dict:
    combined = "\n".join(_safe_text(p) for p in files).lower()

    waits = [
        "db file sequential read",
        "db file scattered read",
        "log file sync",
        "log file parallel write",
        "latch: cache buffers chains",
        "enq: tx - row lock contention",
    ]
    wait_hits = [w for w in waits if w in combined]

    findings = []
    if "ora-" in combined:
        findings.append(
            {
                "title": "Oracle errors detected in uploaded artifacts",
                "severity": "RED",
                "evidence": "Detected ORA- patterns in report/log text.",
                "recommendation": "Review corresponding incident timestamps and trace files first.",
                "priority": "P1",
            }
        )
    if wait_hits:
        findings.append(
            {
                "title": "Top wait events identified",
                "severity": "YELLOW",
                "evidence": f"Detected waits: {', '.join(wait_hits[:5])}",
                "recommendation": "Correlate waits with top SQL and IO/memory sections in AWR.",
                "priority": "P1",
            }
        )
    if "cpu" in combined and "db time" in combined:
        findings.append(
            {
                "title": "CPU/DB Time pressure indicators found",
                "severity": "YELLOW",
                "evidence": "CPU and DB Time markers appear in report text.",
                "recommendation": "Check load profile trends and top SQL by elapsed/CPU time.",
                "priority": "P2",
            }
        )

    if not findings:
        findings.append(
            {
                "title": "No strong deterministic signals found",
                "severity": "GREEN",
                "evidence": "Known critical patterns were not detected.",
                "recommendation": "Use AI mode for deeper context-aware interpretation.",
                "priority": "P3",
            }
        )

    overall = "RED" if any(f["severity"] == "RED" for f in findings) else "YELLOW" if any(f["severity"] == "YELLOW" for f in findings) else "GREEN"

    module_insights = []
    for m in ANALYSIS_MODULES:
        status = "YELLOW" if wait_hits else "GREEN"
        if "Security" in m and ("grant dba" in combined or "sysdba" in combined):
            status = "RED"
        module_insights.append(
            {
                "module": m,
                "status": status,
                "insight": "Deterministic pattern scan completed.",
                "actions": ["Validate against exact AWR section metrics before change implementation."],
            }
        )

    return {
        "executive_summary": "Non-AI deterministic analysis completed using rule-based AWR/event pattern detection.",
        "overall_severity": overall,
        "top_findings": findings,
        "module_insights": module_insights,
        "optimization_plan": [
            {"phase": "Immediate", "tasks": ["Address RED findings and error signatures first"]},
            {"phase": "Short-term", "tasks": ["Tune top SQL and dominant waits from AWR sections"]},
            {"phase": "Long-term", "tasks": ["Create baseline and alert thresholds for recurring waits"]},
        ],
        "questions_for_user": [
            "Can you upload a baseline AWR from a healthy period for comparison?",
            f"Any specific focus area? {user_question}" if user_question else "Any specific business transaction impacted?",
        ],
    }
