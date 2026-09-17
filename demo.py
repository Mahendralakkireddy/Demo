"""
Cortex Analyst multi-model diagnostic
=====================================
Drop this into your app (or run the function from a Streamlit button) to find
out WHICH semantic model is causing the 500 / error_code 370001.

Why this is needed
------------------
In multi-model mode Cortex Analyst must LOAD ALL the semantic models listed in
`semantic_models` before it can route the question to one of them. If any single
file fails to load (invalid YAML, unresolvable base_table, bad verified query),
the whole request fails with an internal error — no matter which domain the
question actually belongs to.

That is exactly why "the individual semantic model works" and yet every
multi-model question fails.

This script tests each model on its own, THEN tests them in pairs, THEN all
three, so you can see precisely where it breaks.
"""

import requests
from typing import Dict, Any, List

# ---------------------------------------------------------------
# Reuse your existing config. These must match your main app.
# ---------------------------------------------------------------
INVENTORY_YAML_STAGE_PATH = (
    '@"INVENTORY_DW_DEMO"."INVENTORY_SCHEMA"."YAML"/INV_ANALYST_DEMO_90_VERIFIED_FIXED_1.yaml'
)
SALES_YAML_STAGE_PATH = (
    '@"CORTEX_DEMO"."CORTEX_SCHEMA"."YAML"/sales_intelligence_model_80_queries_fixed_FINAL.yaml'
)
SUPPLY_CHAIN_YAML_STAGE_PATH = (
    '@"SUPPLY_CHAIN_DW_DEMO"."GOLD"."YAML"/SUPPLY_CHAIN.yml'
)

MODELS = {
    "INVENTORY": INVENTORY_YAML_STAGE_PATH,
    "SALES": SALES_YAML_STAGE_PATH,
    "SUPPLY_CHAIN": SUPPLY_CHAIN_YAML_STAGE_PATH,
}


def _probe(conn, host: str, model_paths: List[str], question: str) -> Dict[str, Any]:
    """Send one Analyst request with a specific set of models."""
    endpoint = f"https://{host}/api/v2/cortex/analyst/message"
    headers = {
        "Authorization": f'Snowflake Token="{conn.rest.token}"',
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    body = {
        "messages": [{"role": "user", "content": [{"type": "text", "text": question}]}],
        "semantic_models": [{"semantic_model_file": p} for p in model_paths],
        "stream": False,
    }
    try:
        r = requests.post(endpoint, headers=headers, json=body, timeout=120)
    except Exception as exc:
        return {"ok": False, "status": None, "detail": f"transport error: {exc}"}

    if r.status_code >= 400:
        try:
            detail = r.json()
        except Exception:
            detail = r.text
        return {"ok": False, "status": r.status_code, "detail": detail}

    return {"ok": True, "status": r.status_code, "detail": r.json()}


def run_diagnostic(conn, host: str, question: str = "What is the total sales amount?"):
    """Print a report showing exactly which model / combination fails."""
    lines = []
    lines.append(f"Question used: {question}\n")

    # ---- 1. Each model alone -------------------------------------
    lines.append("STEP 1 — each semantic model on its own")
    lines.append("-" * 55)
    single_results = {}
    for name, path in MODELS.items():
        res = _probe(conn, host, [path], question)
        single_results[name] = res
        if res["ok"]:
            lines.append(f"  {name:<14} LOADS OK  (HTTP {res['status']})")
        else:
            lines.append(f"  {name:<14} FAILED    (HTTP {res['status']})")
            lines.append(f"      -> {res['detail']}")
    lines.append("")

    # ---- 2. Pairs -------------------------------------------------
    lines.append("STEP 2 — models in pairs")
    lines.append("-" * 55)
    names = list(MODELS.keys())
    for i in range(len(names)):
        for j in range(i + 1, len(names)):
            a, b = names[i], names[j]
            res = _probe(conn, host, [MODELS[a], MODELS[b]], question)
            label = f"{a} + {b}"
            if res["ok"]:
                lines.append(f"  {label:<32} OK")
            else:
                lines.append(f"  {label:<32} FAILED (HTTP {res['status']})")
                lines.append(f"      -> {res['detail']}")
    lines.append("")

    # ---- 3. All three --------------------------------------------
    lines.append("STEP 3 — all three together (what your app does today)")
    lines.append("-" * 55)
    res = _probe(conn, host, list(MODELS.values()), question)
    if res["ok"]:
        lines.append("  ALL THREE OK")
    else:
        lines.append(f"  ALL THREE FAILED (HTTP {res['status']})")
        lines.append(f"      -> {res['detail']}")
    lines.append("")

    # ---- Interpretation ------------------------------------------
    lines.append("INTERPRETATION")
    lines.append("-" * 55)
    broken = [n for n, r in single_results.items() if not r["ok"]]
    if broken:
        lines.append(f"  These model file(s) cannot be loaded at all: {', '.join(broken)}")
        lines.append("  Fix those YAML files first — they break every multi-model request,")
        lines.append("  including questions that belong to a different domain.")
    elif not res["ok"]:
        lines.append("  Every model loads individually, but the combination fails.")
        lines.append("  This points at the routing step rather than one bad file:")
        lines.append("    - duplicate logical table names across models")
        lines.append("    - duplicate semantic model 'name:' values across files")
        lines.append("    - combined input exceeding the Analyst context limit")
    else:
        lines.append("  No failure reproduced in this run.")

    return "\n".join(lines)


# ---------------------------------------------------------------
# Streamlit usage:
#
#   import streamlit as st
#   if st.button("Run Cortex model diagnostic"):
#       st.code(run_diagnostic(
#           st.session_state.snowflake_conn,
#           HOST,
#           "What is the total sales amount?",
#       ))
# ---------------------------------------------------------------
