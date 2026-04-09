import csv
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
        return path.read_text(encoding="utf-8", errors="ignore")[:200000]
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


def _detect_wait_events(text: str) -> List[Dict]:
    lower = text.lower()
    rows = []
    for event, category, bottleneck in WAIT_EVENT_PATTERNS:
        hits = len(re.findall(re.escape(event), lower))
        if hits > 0:
            score = min(10, hits * 2)
            rows.append(
                {
                    "event": event,
                    "category": category,
                    "hits": hits,
                    "impact_score": score,
                    "severity": _severity_from_impact(score),
                    "bottleneck": bottleneck,
                    "recommendation": _event_recommendation(event),
                }
            )
    rows.sort(key=lambda x: (x["impact_score"], x["hits"]), reverse=True)
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
    rows: List[Dict] = []
    lines = text.splitlines()

    sqlid_pattern = re.compile(r"\b([0-9a-z]{13})\b", re.IGNORECASE)
    metric_pattern = re.compile(r"(elapsed|cpu|buffer gets)\s*[:=]\s*([0-9][0-9,\.]*)", re.IGNORECASE)

    for i, line in enumerate(lines):
        l = line.lower()
        if "sql id" in l or "sqlid" in l:
            ids = sqlid_pattern.findall(line)
            if not ids and i + 1 < len(lines):
                ids = sqlid_pattern.findall(lines[i + 1])
            if not ids:
                continue

            sql_id = ids[0]
            window = " ".join(lines[i:i + 4])
            metrics = {k.lower(): v for k, v in metric_pattern.findall(window)}
            elapsed = metrics.get("elapsed", "n/a")
            cpu = metrics.get("cpu", "n/a")
            gets = metrics.get("buffer gets", "n/a")

            dominant = "high_elapsed"
            if cpu != "n/a" and elapsed == "n/a":
                dominant = "high_cpu"
            if gets != "n/a" and elapsed == "n/a" and cpu == "n/a":
                dominant = "high_buffer_gets"

            rows.append(
                {
                    "sql_id": sql_id,
                    "elapsed": elapsed,
                    "cpu": cpu,
                    "buffer_gets": gets,
                    "dominant_issue": dominant.replace("_", " "),
                    "recommendation": SQL_RECOMMENDATION_MAP[dominant],
                }
            )

    # unique by sql_id while preserving order
    seen = set()
    unique_rows = []
    for r in rows:
        if r["sql_id"] in seen:
            continue
        seen.add(r["sql_id"])
        unique_rows.append(r)
    return unique_rows[:12]


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

    findings = []
    for e in wait_events[:6]:
        findings.append(
            {
                "finding": f"Wait event pressure: {e['event']}",
                "severity": e["severity"],
                "evidence": f"Detected {e['hits']} occurrences in uploaded artifacts.",
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

    return {
        "executive_summary": "Deterministic Oracle AWR miner completed. Top waits, error signatures, and SQL pressure indicators were correlated into a practical action plan.",
        "overall_severity": overall,
        "wait_events_table": wait_events,
        "top_sql_table": top_sql,
        "findings_table": findings,
        "module_status_table": module_table,
        "recommendations_table": recommendations,
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
