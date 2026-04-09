import csv
import html
import io
import re
from collections import Counter
from pathlib import Path
from typing import Dict, List, Tuple


ANALYSIS_MODULES = [
    "Database Load Profile",
    "Top SQL by Elapsed/CPU Time",
    "Top Wait Events and Bottlenecks",
    "Redo / Commit Pressure",
    "IO and Storage Health",
    "Memory (SGA/PGA) Pressure",
    "Concurrency / Lock Contention",
    "Alert Log Error Signals",
    "Privilege / Security Risk Signals",
    "Action Plan and Quick Wins",
]


WAIT_EVENT_PATTERNS: List[Tuple[str, str, str]] = [
    ("db file sequential read", "IO", "Single-block read latency (index-driven random IO)"),
    ("db file scattered read", "IO", "Multi-block read pressure / full scan load"),
    ("log file sync", "COMMIT", "Commit latency and redo sync pressure"),
    ("log file parallel write", "REDO", "LGWR/device redo write latency"),
    ("enq: tx - row lock contention", "LOCK", "Transactional row lock waits"),
    ("latch: cache buffers chains", "CONCURRENCY", "Hot block contention"),
    ("gc buffer busy acquire", "RAC", "Global cache block contention"),
    ("gc cr request", "RAC", "RAC interconnect/global cache transfer load"),
    ("direct path read", "IO", "Direct path read load"),
    ("direct path write", "IO", "Direct path write load"),
]

SQL_RECOMMENDATION_MAP = {
    "high_elapsed": "Tune execution plan and reduce logical/physical IO for this SQL ID.",
    "high_cpu": "Review predicate selectivity and join method to lower CPU consumption.",
    "high_buffer_gets": "Optimize access path/indexing to reduce consistent gets and latch pressure.",
}


def _safe_text(path: Path) -> str:
    try:
        # Keep full report text so downstream parsing can reliably reach SQL sections
        # that commonly appear deep in large AWR HTML documents.
        return path.read_text(encoding="utf-8", errors="ignore")[:5000000]
    except Exception:
        return ""


def _all_text(files: List[Path]) -> str:
    return "\n".join(_safe_text(f) for f in files)


def _severity_from_impact(score: int) -> str:
    if score >= 8:
        return "RED"
    if score >= 4:
        return "YELLOW"
    return "GREEN"


def _to_float(value: str) -> float:
    if not value:
        return 0.0
    v = value.replace(",", "").strip()
    if v in {"", "-", "n/a"}:
        return 0.0
    if v.startswith("."):
        v = f"0{v}"
    try:
        return float(v)
    except ValueError:
        return 0.0


def _clean_html_cell(cell: str) -> str:
    no_tags = re.sub(r"<[^>]+>", " ", cell, flags=re.IGNORECASE)
    no_tags = html.unescape(no_tags)
    no_tags = no_tags.replace("\xa0", " ")
    return re.sub(r"\s+", " ", no_tags).strip()


def _extract_table_by_summary(text: str, summary_phrase: str) -> str:
    pattern = re.compile(
        rf"<table[^>]*summary=\"[^\"]*{re.escape(summary_phrase)}[^\"]*\"[^>]*>(.*?)</table>",
        re.IGNORECASE | re.DOTALL,
    )
    m = pattern.search(text)
    return m.group(1) if m else ""


def _parse_table_rows(table_html: str) -> List[List[str]]:
    if not table_html:
        return []
    out: List[List[str]] = []
    row_matches = re.findall(r"<tr[^>]*>(.*?)</tr>", table_html, flags=re.IGNORECASE | re.DOTALL)
    for row in row_matches:
        cells = re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", row, flags=re.IGNORECASE | re.DOTALL)
        cleaned = [_clean_html_cell(c) for c in cells]
        if cleaned:
            out.append(cleaned)
    return out


def _wait_impact_score(percent_db_time: float, total_wait_s: float, avg_wait_ms: float) -> int:
    # Deterministic score grounded on AWR metrics, not keyword frequency.
    score = min(10.0, (percent_db_time * 1.8) + min(3.0, total_wait_s / 20.0) + min(2.0, avg_wait_ms / 10.0))
    return max(1, int(round(score)))


def _detect_wait_events(text: str) -> List[Dict]:
    rows: List[Dict] = []

    fg_table = _extract_table_by_summary(text, "Foreground Wait Events")
    fg_rows = _parse_table_rows(fg_table)
    if fg_rows:
        matched = {}
        for cols in fg_rows:
            if not cols:
                continue
            event_name = cols[0].lower()
            for event, category, bottleneck in WAIT_EVENT_PATTERNS:
                if event in event_name:
                    total_wait_s = _to_float(cols[3] if len(cols) > 3 else "0")
                    avg_wait_ms = _to_float(cols[4] if len(cols) > 4 else "0")
                    pct_db_time = _to_float(cols[6] if len(cols) > 6 else "0")
                    waits = int(_to_float(cols[1] if len(cols) > 1 else "0"))
                    score = _wait_impact_score(pct_db_time, total_wait_s, avg_wait_ms)
                    existing = matched.get(event)
                    payload = {
                        "event": event,
                        "category": category,
                        "hits": 1,
                        "impact_score": score,
                        "severity": _severity_from_impact(score),
                        "bottleneck": bottleneck,
                        "recommendation": _event_recommendation(event),
                        "waits": waits,
                        "total_wait_s": total_wait_s,
                        "avg_wait_ms": avg_wait_ms,
                        "pct_db_time": pct_db_time,
                    }
                    if not existing or payload["impact_score"] > existing["impact_score"]:
                        matched[event] = payload
        rows = list(matched.values())

    # Fallback for non-standard text inputs where AWR table structure isn't available.
    if not rows:
        lower = text.lower()
        for event, category, bottleneck in WAIT_EVENT_PATTERNS:
            hits = len(re.findall(re.escape(event), lower))
            if hits > 0:
                score = min(6, 1 + hits)
                rows.append(
                    {
                        "event": event,
                        "category": category,
                        "hits": hits,
                        "impact_score": score,
                        "severity": _severity_from_impact(score),
                        "bottleneck": bottleneck,
                        "recommendation": _event_recommendation(event),
                        "waits": 0,
                        "total_wait_s": 0,
                        "avg_wait_ms": 0,
                        "pct_db_time": 0,
                    }
                )

    rows.sort(
        key=lambda x: (
            x.get("pct_db_time", 0),
            x.get("total_wait_s", 0),
            x["impact_score"],
        ),
        reverse=True,
    )
    return rows[:12]


def _event_recommendation(event: str) -> str:
    mapping = {
        "db file sequential read": "Tune top index access SQL, validate index clustering/selectivity, and reduce random IO.",
        "db file scattered read": "Review full scans, partition pruning, and optimizer stats; evaluate smart scan/storage strategy.",
        "log file sync": "Reduce commit frequency, optimize application commit design, and validate redo log/storage latency.",
        "log file parallel write": "Move redo logs to low-latency storage and validate LGWR write throughput.",
        "enq: tx - row lock contention": "Identify blocking sessions, shorten transactions, and improve DML access patterns.",
        "latch: cache buffers chains": "Find hot blocks/segments and distribute access via reverse key/hash/partitioning where appropriate.",
        "gc buffer busy acquire": "Investigate RAC block pinging; align service affinity and data access locality.",
        "gc cr request": "Review RAC interconnect health and cross-instance block shipping workload.",
        "direct path read": "Validate parallelism and temp usage; tune large scans and workarea sizing.",
        "direct path write": "Review temp spill causes and batch/write patterns.",
    }
    return mapping.get(event, "Investigate contributing SQL and correlate with AWR trend windows.")


def _detect_oracle_errors(text: str) -> List[str]:
    errors = re.findall(r"\bORA-\d{5}\b", text)
    top = [f"{code} ({cnt})" for code, cnt in Counter(errors).most_common(8)]
    return top


def _detect_sql_signals(text: str) -> List[Dict]:
    lower = text.lower()
    patterns = [
        ("full table scan pattern", r"table access full", "Consider indexing/partition pruning or SQL rewrite for high-volume scans."),
        ("nested loops pressure", r"nested loops", "Validate join cardinality/stats and index support on inner tables."),
        ("temp usage/sorts", r"direct path.*temp|temp space|sorts \(disk\)", "Tune workarea/PGA and reduce spill-heavy SQL operations."),
    ]
    out = []
    for name, pattern, rec in patterns:
        hits = len(re.findall(pattern, lower))
        if hits:
            out.append({"signal": name, "hits": hits, "recommendation": rec})
    return out


def _detect_top_sql(text: str) -> List[Dict]:
    sql_map: Dict[str, Dict] = {}

    def upsert(sql_id: str, **kwargs):
        row = sql_map.setdefault(
            sql_id,
            {
                "sql_id": sql_id,
                "elapsed": "n/a",
                "cpu": "n/a",
                "buffer_gets": "n/a",
                "sql_text": "SQL text not confidently parsed from uploaded AWR snippet.",
            },
        )
        row.update({k: v for k, v in kwargs.items() if v not in (None, "")})

    elapsed_tbl = _extract_table_by_summary(text, "top SQL by elapsed time")
    for cols in _parse_table_rows(elapsed_tbl):
        if len(cols) < 8:
            continue
        sql_id = cols[6].lower()
        if not re.fullmatch(r"[0-9a-z]{13}", sql_id):
            continue
        upsert(sql_id, elapsed=cols[0], sql_text=cols[9] if len(cols) > 9 else "")

    cpu_tbl = _extract_table_by_summary(text, "top SQL by CPU time")
    for cols in _parse_table_rows(cpu_tbl):
        if len(cols) < 9:
            continue
        sql_id = cols[7].lower()
        if not re.fullmatch(r"[0-9a-z]{13}", sql_id):
            continue
        upsert(sql_id, cpu=cols[0], elapsed=cols[4], sql_text=cols[10] if len(cols) > 10 else "")

    gets_tbl = _extract_table_by_summary(text, "top SQL by buffer gets")
    for cols in _parse_table_rows(gets_tbl):
        if len(cols) < 9:
            continue
        sql_id = cols[7].lower()
        if not re.fullmatch(r"[0-9a-z]{13}", sql_id):
            continue
        upsert(sql_id, buffer_gets=cols[0], elapsed=cols[4], sql_text=cols[10] if len(cols) > 10 else "")

    # Full SQL text block extraction (SQL ID -> SQL Text section near end of AWR report)
    for sql_id, sql_text_html in re.findall(
        r"<a\s+class=\"awr\"\s+name=\"([0-9a-z]{13})\"\s*>\s*</a>\s*\1\s*</td>\s*<td[^>]*>(.*?)</td>",
        text,
        flags=re.IGNORECASE | re.DOTALL,
    ):
        cleaned = _clean_html_cell(sql_text_html)
        if cleaned:
            upsert(sql_id.lower(), sql_text=cleaned)

    rows: List[Dict] = []
    for sql_id, r in sql_map.items():
        elapsed_f = _to_float(r.get("elapsed", "0"))
        cpu_f = _to_float(r.get("cpu", "0"))
        gets_f = _to_float(r.get("buffer_gets", "0"))
        dominant = "high_elapsed"
        if cpu_f > elapsed_f and cpu_f > gets_f:
            dominant = "high_cpu"
        elif gets_f > elapsed_f and gets_f > cpu_f:
            dominant = "high_buffer_gets"

        rows.append(
            {
                "sql_id": sql_id,
                "elapsed": r.get("elapsed", "n/a"),
                "cpu": r.get("cpu", "n/a"),
                "buffer_gets": r.get("buffer_gets", "n/a"),
                "dominant_issue": dominant.replace("_", " "),
                "recommendation": SQL_RECOMMENDATION_MAP[dominant],
                "sql_text": r.get("sql_text") or "SQL text not confidently parsed from uploaded AWR snippet.",
            }
        )

    rows.sort(key=lambda x: (_to_float(x["elapsed"]), _to_float(x["cpu"]), _to_float(x["buffer_gets"])), reverse=True)
    return rows[:12]


def _module_status(wait_events: List[Dict], errors: List[str], text: str) -> List[Dict]:
    lower = text.lower()
    has_commit = any(w["event"] in ["log file sync", "log file parallel write"] for w in wait_events)
    has_lock = any("lock" in w["event"] for w in wait_events)
    sec_risk = "grant dba" in lower or "sysdba" in lower
    io_pressure = any(w["category"] == "IO" for w in wait_events)
    rac_pressure = any(w["category"] == "RAC" for w in wait_events)

    def s(red: bool, yellow: bool = False) -> str:
        return "RED" if red else "YELLOW" if yellow else "GREEN"

    return [
        {"module": "Database Load Profile", "status": s(any(w["impact_score"] >= 8 for w in wait_events), bool(wait_events)), "insight": "Load inferred from frequency and impact of wait signatures."},
        {"module": "Top SQL by Elapsed/CPU Time", "status": s(False, "cpu" in lower or "elapsed" in lower), "insight": "SQL pressure signatures detected from plan/wait sections."},
        {"module": "Top Wait Events and Bottlenecks", "status": s(any(w["severity"] == "RED" for w in wait_events), bool(wait_events)), "insight": "Top waits mined from uploaded artifacts."},
        {"module": "Redo / Commit Pressure", "status": s(False, has_commit), "insight": "Commit/redo bottlenecks inferred from LGWR/log waits."},
        {"module": "IO and Storage Health", "status": s(False, io_pressure), "insight": "IO health inferred from read/write wait signatures."},
        {"module": "Memory (SGA/PGA) Pressure", "status": s(False, "pga" in lower or "sga" in lower or "free memory" in lower), "insight": "Memory pressure indicators checked from AWR text markers."},
        {"module": "Concurrency / Lock Contention", "status": s(False, has_lock), "insight": "Lock/concurrency pressure signals checked."},
        {"module": "Alert Log Error Signals", "status": s(bool(errors), False), "insight": "ORA errors mined from files."},
        {"module": "Privilege / Security Risk Signals", "status": s(sec_risk, False), "insight": "High-privilege grant markers validated from uploaded evidence."},
        {"module": "RAC / Interconnect Signals", "status": s(False, rac_pressure), "insight": "RAC global cache pressure markers analyzed."},
        {"module": "Action Plan and Quick Wins", "status": s(False, bool(wait_events or errors)), "insight": "Plan generated based on highest-impact bottlenecks."},
    ]


def run_deterministic_analysis(files: List[Path], user_question: str = "") -> Dict:
    text = _all_text(files)
    wait_events = _detect_wait_events(text)
    errors = _detect_oracle_errors(text)
    sql_signals = _detect_sql_signals(text)
    top_sql = _detect_top_sql(text)

    highlights = []
    ltext = text.lower()
    if "host cpu" in ltext or "cpu time" in ltext:
        highlights.append("CPU pressure markers found (Host CPU/CPU Time) — correlate with top SQL CPU consumers.")
    if "db time" in ltext:
        highlights.append("DB Time markers present — validate DB Time concentration by wait class and top SQL.")
    if "physical reads" in ltext or "physical read total bytes" in ltext:
        highlights.append("Physical IO intensity detected — review storage latency and full-scan SQL patterns.")
    if "pga" in ltext or "sga" in ltext:
        highlights.append("Memory advisories/markers present — review PGA/SGA sizing against workload profile.")
    if "library cache" in ltext or "parse" in ltext:
        highlights.append("Parse/library cache signals present — check hard-parse rate and cursor sharing strategy.")

    findings = []
    for e in wait_events[:6]:
        pct = e.get("pct_db_time", 0)
        waits = e.get("waits", 0)
        tws = e.get("total_wait_s", 0)
        findings.append(
            {
                "finding": f"Wait event pressure: {e['event']}",
                "severity": e["severity"],
                "evidence": f"%DB time={pct}, waits={waits}, total_wait_s={tws}",
                "business_impact": e["bottleneck"],
            }
        )

    if errors:
        findings.append(
            {
                "finding": "Alert/Error risk detected",
                "severity": "RED",
                "evidence": ", ".join(errors),
                "business_impact": "Potential service degradation or failed transactions.",
            }
        )

    for s in sql_signals:
        findings.append(
            {
                "finding": f"SQL signal: {s['signal']}",
                "severity": "YELLOW",
                "evidence": f"Pattern hits: {s['hits']}",
                "business_impact": "Higher DB time and throughput degradation risk.",
            }
        )

    for s in top_sql[:5]:
        findings.append(
            {
                "finding": f"Top SQL hotspot: {s['sql_id']}",
                "severity": "YELLOW",
                "evidence": f"Elapsed={s['elapsed']}, CPU={s['cpu']}, BufferGets={s['buffer_gets']}",
                "business_impact": "Potential high DB time contributor and response-time degradation.",
            }
        )

    if not findings:
        findings = [{
            "finding": "No critical bottleneck signature identified",
            "severity": "GREEN",
            "evidence": "No high-risk waits/errors from deterministic miner patterns.",
            "business_impact": "Low immediate risk based on uploaded samples.",
        }]

    recommendations = []
    for idx, e in enumerate(wait_events[:8], start=1):
        recommendations.append(
            {
                "priority": f"P{1 if e['severity']=='RED' else 2}",
                "area": e["category"],
                "recommendation": e["recommendation"],
                "expected_outcome": "Reduced DB time and lower wait contribution.",
            }
        )
    for s in sql_signals[:3]:
        recommendations.append(
            {
                "priority": "P2",
                "area": "SQL",
                "recommendation": s["recommendation"],
                "expected_outcome": "Improved SQL latency and reduced CPU/IO overhead.",
            }
        )
    for s in top_sql[:6]:
        recommendations.append(
            {
                "priority": "P2",
                "area": f"SQL ({s['sql_id']})",
                "recommendation": s["recommendation"],
                "expected_outcome": "Lower SQL elapsed time and reduced DB time concentration.",
            }
        )
    if errors:
        recommendations.insert(
            0,
            {
                "priority": "P1",
                "area": "Stability",
                "recommendation": "Triage top ORA errors by timestamp and affected modules before tuning changes.",
                "expected_outcome": "Faster incident containment and reduced repeat failures.",
            },
        )
    if any(w["event"] == "log file sync" for w in wait_events):
        recommendations.insert(
            0,
            {
                "priority": "P1",
                "area": "Commit Path",
                "recommendation": "Review commit frequency per transaction and verify redo log file size/group placement for LGWR efficiency.",
                "expected_outcome": "Lower commit latency and improved transactional throughput.",
            },
        )

    overall = "RED" if any(f["severity"] == "RED" for f in findings) else "YELLOW" if any(f["severity"] == "YELLOW" for f in findings) else "GREEN"
    module_table = _module_status(wait_events, errors, text)

    # Enterprise/audit-friendly confidence model (evidence-count based)
    evidence_points = len(wait_events) + len(top_sql) + len(errors) + len(sql_signals)
    base_conf = min(95, 55 + evidence_points * 3)
    conf_wait = min(98, 50 + len(wait_events) * 5)
    conf_sql = min(98, 45 + len(top_sql) * 6 + len(sql_signals) * 3)
    conf_stability = min(98, 50 + len(errors) * 8)

    recommendation_dashboard = [
        {
            "title": "Wait & Bottleneck Actions",
            "value": f"{len(wait_events)} events",
            "confidence": conf_wait,
            "status": "RED" if any(w["severity"] == "RED" for w in wait_events) else "YELLOW" if wait_events else "GREEN",
            "note": "Grounded on explicit wait-event signatures mined from uploaded AWR text.",
        },
        {
            "title": "SQL Optimization Actions",
            "value": f"{len(top_sql)} SQL IDs",
            "confidence": conf_sql,
            "status": "YELLOW" if top_sql else "GREEN",
            "note": "Based on SQL ID extraction, nearby metrics, and parsed SQL text blocks.",
        },
        {
            "title": "Stability & Error Actions",
            "value": f"{len(errors)} ORA signatures",
            "confidence": conf_stability,
            "status": "RED" if errors else "GREEN",
            "note": "Derived from ORA error signatures and deterministic remediation mapping.",
        },
        {
            "title": "Overall Recommendation Confidence",
            "value": f"{len(recommendations)} actions",
            "confidence": base_conf,
            "status": overall,
            "note": "Confidence reflects direct evidence density in the uploaded report artifacts.",
        },
    ]

    module_evidence = []
    for m in module_table:
        name = m["module"]
        evidence = []
        if "Load" in name and wait_events:
            evidence.append(f"Wait events mined: {len(wait_events)}")
        if "Top SQL" in name and top_sql:
            evidence.append(f"Top SQL mined: {len(top_sql)}")
        if "Wait Events" in name and wait_events:
            evidence.append(", ".join([w["event"] for w in wait_events[:3]]))
        if "Redo" in name and any(w["event"] in ["log file sync", "log file parallel write"] for w in wait_events):
            evidence.append("Detected log file sync/log file parallel write")
        if "IO" in name and any(w["category"] == "IO" for w in wait_events):
            evidence.append("Detected IO wait signatures")
        if "Memory" in name and ("pga" in ltext or "sga" in ltext):
            evidence.append("Detected PGA/SGA markers")
        if "Concurrency" in name and any("lock" in w["event"] for w in wait_events):
            evidence.append("Detected lock-related waits")
        if "Alert" in name and errors:
            evidence.append(", ".join(errors[:3]))
        if "Privilege" in name and ("grant dba" in ltext or "sysdba" in ltext):
            evidence.append("Detected high-privilege markers (GRANT DBA/SYSDBA)")
        if "Quick Wins" in name and recommendations:
            evidence.append(f"Generated actions: {len(recommendations)}")

        module_evidence.append(
            {
                "module": name,
                "status": m["status"],
                "evidence": "; ".join(evidence) if evidence else "No strong direct marker found in uploaded snippet.",
                "confidence": min(95, 50 + len(evidence) * 12),
                "grounding": "AWR text pattern match",
            }
        )

    return {
        "executive_summary": "Deterministic Oracle AWR miner completed. Top waits, error signatures, and SQL pressure indicators were correlated into a practical action plan.",
        "overall_severity": overall,
        "wait_events_table": wait_events,
        "top_sql_table": top_sql,
        "findings_table": findings,
        "module_status_table": module_table,
        "recommendations_table": recommendations,
        "recommendation_dashboard": recommendation_dashboard,
        "module_evidence_table": module_evidence,
        "awr_highlights": highlights,
        "focus": user_question or "General Oracle performance triage",
    }


def to_csv(rows: List[Dict]) -> str:
    if not rows:
        return ""
    out = io.StringIO()
    writer = csv.DictWriter(out, fieldnames=list(rows[0].keys()))
    writer.writeheader()
    writer.writerows(rows)
    return out.getvalue()
