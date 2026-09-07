import io
import os
import base64
import streamlit as st
import pandas as pd
from datetime import datetime
import snowflake.connector
from snowflake.snowpark import Session
import requests
from typing import Any, Dict, List, Optional
import re
import yaml

# ===================================================================
# Configuration
# ===================================================================
HOST = "WDSDGTL-XCC29288.snowflakecomputing.com"
ACCOUNT = "WDSDGTL-XCC29288"
DATABASE = "INVENTORY_DW_DEMO"
SCHEMA = "GOLD"
WAREHOUSE = "COMPUTE_WH"
ROLE = "ACCOUNTADMIN"

# FULL semantic-model YAML files on Snowflake stages.
INVENTORY_YAML_STAGE_PATH = (
    '@"INVENTORY_DW_DEMO"."INVENTORY_SCHEMA"."YAML"/INV_ANALYST_DEMO_90_VERIFIED_FIXED_1.yaml'
)
SALES_YAML_STAGE_PATH = (
    '@"CORTEX_DEMO"."CORTEX_SCHEMA"."YAML"/sales_intelligence_model_80_queries_fixed_FINAL.yaml'
)
SUPPLY_CHAIN_YAML_STAGE_PATH = (
    '@"SUPPLY_CHAIN_DW_DEMO"."GOLD"."YAML"/SUPPLY_CHAIN.yml'
)

ANALYST_ENDPOINT = f"https://{HOST}/api/v2/cortex/analyst/message"
ROBOT_IMAGE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dilytics_robot.png")

def _robot_data_uri():
    """Return the bundled Dilytics robot image as a data URI."""
    try:
        with open(ROBOT_IMAGE_PATH, "rb") as f:
            return "data:image/png;base64," + base64.b64encode(f.read()).decode("ascii")
    except Exception:
        return ""


st.set_page_config(
    page_title="Dilytics Enterprise AI",
    page_icon="📦",
    layout="wide",
)

st.markdown("""
<style>
/* ================================================================
   Dilytics Professional AI UI
   Visual-only layer: backend/chat/document logic is unchanged.
   ================================================================ */
:root {
    --dly-navy: #082d69;
    --dly-navy-2: #0b3f8f;
    --dly-blue: #1769d2;
    --dly-cyan: #25cfff;
    --dly-text: #173f6f;
    --dly-muted: #6481a2;
}

.stApp {
    background:
        radial-gradient(circle at 88% 4%, rgba(37,207,255,.18), transparent 24%),
        radial-gradient(circle at 10% 92%, rgba(23,105,210,.08), transparent 28%),
        linear-gradient(180deg, #f7fbff 0%, #edf7ff 55%, #ffffff 100%);
}

.main .block-container {
    max-width: 1180px;
    padding-top: 0.75rem;
    padding-bottom: 3rem;
}

/* Header / brand */
.dly-topbar {
    display:flex; align-items:center; justify-content:space-between;
    padding: 12px 18px;
    margin-bottom: 24px;
    border: 1px solid #dcecff;
    border-radius: 16px;
    background: rgba(255,255,255,.94);
    backdrop-filter: blur(14px);
    box-shadow: 0 10px 35px rgba(23,91,160,.08);
}
.dly-brand {
    display:flex; align-items:center; gap:10px;
    font-size: 1.05rem; font-weight:800; letter-spacing:.4px;
    color:#fff;
}
.dly-logo {
    display:inline-flex; align-items:center; justify-content:center;
    width:34px; height:28px; border-radius:7px;
    background:linear-gradient(135deg,#ff4b4b,#ff6262);
    color:white; font-size:.78rem; font-weight:900;
}
.dly-nav {
    display:flex; gap:22px; color:#6481a2; font-size:.78rem;
}
.dly-nav span:first-child { color:#1769d2; }
.dly-status {
    display:inline-flex; align-items:center; gap:6px;
    padding:6px 10px; border-radius:999px;
    background:rgba(34,197,94,.08);
    border:1px solid rgba(74,222,128,.28);
    color:#86efac; font-size:.72rem; font-weight:700;
}
.dly-hero {
    padding: 26px 8px 22px;
}
.dly-eyebrow {
    color:#1769d2; font-size:.78rem; font-weight:800;
    text-transform:uppercase; letter-spacing:1.5px;
}
.dly-hero h1 {
    margin: 7px 0 8px; color:#082d69;
    font-size: clamp(2rem, 4vw, 3.35rem);
    line-height:1.06; letter-spacing:-1.8px;
}
.dly-hero h1 span { color:#159fe8; }
.dly-hero p {
    max-width:650px; color:#587291; font-size:.92rem; line-height:1.65;
}

/* Status pill used in sidebar */
.status-pill {
    display:inline-flex; align-items:center; gap:6px;
    background:rgba(34,197,94,.08); color:#86efac;
    border:1px solid rgba(74,222,128,.28); border-radius:20px;
    padding:4px 10px; font-size:.72rem; font-weight:700;
}

/* Buttons */
div[data-testid="stButton"] > button {
    border-radius:10px; font-weight:600;
    min-height:42px;
    transition:all .18s ease-in-out;
}
.main div[data-testid="stButton"] > button {
    background:#ffffff;
    border:1px solid #cfe5fb;
    color:#164a82;
}
.main div[data-testid="stButton"] > button:hover {
    border-color:#1769d2;
    color:#1769d2; transform:translateY(-1px);
    box-shadow:0 7px 22px rgba(23,105,210,.12);
}

/* Intelligence tabs */
.stTabs [data-baseweb="tab-list"] {
    gap:8px; background:transparent;
    border-bottom:1px solid #dcecff;
}
.stTabs [data-baseweb="tab"] {
    color:#9eabc2; border-radius:10px 10px 0 0; padding:10px 18px;
}
.stTabs [aria-selected="true"] {
    color:#1769d2 !important;
    background:#eaf5ff;
}

/* Expander / cards */
[data-testid="stExpander"] {
    background:rgba(255,255,255,.96);
    border:1px solid #dcecff;
    border-radius:14px;
}

/* Chat bubbles */
[data-testid="stChatMessage"] {
    border:1px solid #dcecff;
    border-radius:15px;
    background:#ffffff;
    margin-bottom:10px;
}
[data-testid="stChatMessage"] p { line-height:1.6; }

/* Chat input */
[data-testid="stChatInput"] {
    background:rgba(255,255,255,.98);
}
[data-testid="stChatInput"] > div {
    border:1px solid #b9d9f5 !important;
    border-radius:15px !important;
    background:#ffffff !important;
}

/* Sidebar */
section[data-testid="stSidebar"] {
    background:linear-gradient(180deg,#f3f9ff 0%,#ffffff 100%);
    border-right:1px solid #dcecff;
}
section[data-testid="stSidebar"] .stMarkdown { color:#173f6f; }

/* File uploader */
[data-testid="stFileUploader"] {
    background:#ffffff;
    border:1px solid #dcecff;
    border-radius:12px;
}

/* Dataframes */
[data-testid="stDataFrame"] {
    border-radius:12px; overflow:hidden;
}

/* Remove excess Streamlit decoration */
#MainMenu { visibility:hidden; }
footer { visibility:hidden; }
header { background:transparent !important; }
</style>
""", unsafe_allow_html=True)



st.markdown("""
<style>
/* ================================================================
   Dilytics Landing Page
   Visual/navigation layer only. Backend functionality is unchanged.
   ================================================================ */
.dly-landing-wrap {
    min-height: 78vh;
    margin: -1.2rem -2rem 0;
    padding: 2rem 5vw 4rem;
    background:
        radial-gradient(circle at 78% 18%, rgba(29,190,255,.30), transparent 28%),
        radial-gradient(circle at 35% 100%, rgba(20,111,220,.24), transparent 35%),
        linear-gradient(135deg,#071a42 0%,#063b79 48%,#0b83bd 100%);
    border-radius: 0 0 28px 28px;
    position: relative;
    overflow: hidden;
}
.dly-landing-wrap:before,
.dly-landing-wrap:after {
    content:""; position:absolute; border:1px solid rgba(100,220,255,.18);
    border-radius:50%; pointer-events:none;
}
.dly-landing-wrap:before { width:760px; height:760px; right:-250px; top:-420px; }
.dly-landing-wrap:after { width:680px; height:680px; left:-360px; bottom:-470px; }
.dly-landing-nav {
    display:flex; align-items:center; justify-content:space-between;
    position:relative; z-index:2; margin-bottom:7vh;
}
.dly-landing-logo {
    display:inline-flex; align-items:center; justify-content:center;
    background:#ef222c; color:#fff; padding:9px 18px;
    font-size:1.25rem; font-weight:900; letter-spacing:.5px;
    border-radius:2px; box-shadow:0 8px 25px rgba(0,0,0,.18);
}
.dly-landing-powered {
    color:#bfeeff; font-size:.76rem; font-weight:700;
    letter-spacing:1px; text-transform:uppercase;
}
.dly-landing-grid {
    display:grid; grid-template-columns: 1.02fr .98fr; gap:4rem;
    align-items:center; position:relative; z-index:2;
}
.dly-landing-copy h1 {
    color:#fff; font-size:clamp(3rem,6vw,5.5rem); line-height:.98;
    letter-spacing:-3px; margin:0 0 22px; font-weight:850;
}
.dly-landing-copy h1 span { color:#20d7ff; }
.dly-landing-copy p {
    color:#d0e7ff; font-size:1.15rem; line-height:1.6;
    max-width:620px; margin-bottom:28px;
}
.dly-landing-actions {
    display:flex; gap:16px; align-items:center; flex-wrap:wrap;
}
.dly-landing-actions button {
    border:0; border-radius:30px; padding:13px 27px;
    font-size:1rem; font-weight:800; cursor:pointer;
}
.dly-explore {
    background:linear-gradient(90deg,#0da9ff,#2ed8c4); color:#fff;
    box-shadow:0 10px 30px rgba(0,183,255,.25);
}
.dly-ask {
    background:rgba(4,25,62,.35); color:#fff;
    border:1px solid rgba(140,225,255,.55) !important;
}
.dly-landing-search {
    margin-top:25px; max-width:680px; padding:10px;
    border-radius:17px; background:rgba(255,255,255,.10);
    border:1px solid rgba(255,255,255,.20); backdrop-filter:blur(12px);
}
.dly-landing-search input {
    color:#fff !important; background:rgba(2,18,48,.55) !important;
}
.dly-landing-search input::placeholder { color:#b8d4ee !important; }
.dly-landing-visual {
    min-height:470px; position:relative; display:flex;
    align-items:center; justify-content:center;
}
.dly-robot-orb {
    width:390px; height:390px; border-radius:50%;
    background:radial-gradient(circle at 35% 30%,#25cfff,#0878c8 58%,#073c80);
    box-shadow:0 0 90px rgba(30,208,255,.28);
    display:flex; align-items:center; justify-content:center;
    font-size:12rem; position:relative;
}
.dly-chat-bubble {
    position:absolute; right:0; top:15%; background:#eaf6ff;
    color:#073579; padding:18px 23px; border-radius:22px 22px 5px 22px;
    font-size:1.05rem; line-height:1.35; box-shadow:0 15px 35px rgba(0,0,0,.2);
}
.dly-chat-bubble strong { font-size:1.2rem; }
.dly-landing-stats {
    display:flex; gap:35px; margin-top:48px; color:#dff4ff;
}
.dly-stat { display:flex; gap:10px; align-items:center; }
.dly-stat-icon { font-size:1.4rem; }
.dly-stat-text { font-size:.9rem; line-height:1.2; }
.dly-stat-text b { display:block; color:#fff; font-size:1rem; }
@media (max-width: 900px) {
    .dly-landing-grid { grid-template-columns:1fr; gap:1rem; }
    .dly-landing-visual { min-height:300px; }
    .dly-robot-orb { width:280px; height:280px; }
    .dly-chat-bubble { right:3%; }
    .dly-landing-wrap { margin-left:-1rem; margin-right:-1rem; }
}
</style>
""", unsafe_allow_html=True)


# ===================================================================
# 1. LOGIN / SNOWFLAKE SESSION
# ===================================================================
if "authenticated" not in st.session_state:
    st.session_state.authenticated = False
    st.session_state.username = "PBCS"
    st.session_state.password = ""
    st.session_state.snowpark_session = None
    st.session_state.snowflake_conn = None
    st.session_state.app_page = "home"

if "captcha_a" not in st.session_state:
    import random
    st.session_state.captcha_a = random.randint(2, 9)
    st.session_state.captcha_b = random.randint(1, 9)

def _new_captcha():
    import random
    st.session_state.captcha_a = random.randint(2, 9)
    st.session_state.captcha_b = random.randint(1, 9)


def _login_page():
    st.markdown("""
    <style>
      .login-shell{max-width:1180px;margin:2.2rem auto 0;background:#fff;border:1px solid #d8ebff;border-radius:28px;overflow:hidden;box-shadow:0 24px 70px rgba(19,82,145,.12)}
      .login-grid{display:grid;grid-template-columns:1.02fr .98fr;min-height:650px}
      .login-copy{padding:58px 64px;background:linear-gradient(145deg,#ffffff 0%,#f2f9ff 100%);display:flex;flex-direction:column;justify-content:center}
      .login-visual{position:relative;display:flex;align-items:center;justify-content:center;background:radial-gradient(circle at 50% 45%,#e8f7ff 0,#d9efff 30%,#f7fbff 68%,#fff 100%);overflow:hidden}
      .login-logo{display:inline-flex;width:max-content;background:#e51f2b;color:#fff;font-weight:900;font-size:1.35rem;padding:9px 17px;border-radius:4px;letter-spacing:.6px;box-shadow:0 8px 20px rgba(229,31,43,.12)}
      .login-eyebrow{color:#1769d2;font-weight:800;letter-spacing:2px;text-transform:uppercase;font-size:.78rem;margin-top:38px}
      .login-title{font-size:3.15rem;line-height:1.04;font-weight:900;color:#082d69;margin:.5rem 0 1rem;letter-spacing:-1.4px}
      .login-title span{color:#1769d2}
      .login-sub{color:#55708f;font-size:1rem;line-height:1.7;max-width:510px;margin-bottom:26px}
      .login-feature-row{display:flex;gap:10px;flex-wrap:wrap;margin-top:8px}
      .login-feature{padding:9px 13px;background:#fff;border:1px solid #d7eaff;border-radius:999px;color:#245b91;font-size:.78rem;font-weight:700;box-shadow:0 7px 18px rgba(23,91,160,.05)}
      .login-orbit{width:450px;height:450px;border-radius:50%;border:1px solid #b8dcff;box-shadow:0 0 0 28px rgba(35,137,230,.05),0 0 0 58px rgba(35,137,230,.035);position:relative;animation:orbitPulse 4s ease-in-out infinite;display:flex;align-items:center;justify-content:center}
      .login-robot-img{width:410px;max-width:90%;height:auto;object-fit:contain;filter:drop-shadow(0 28px 40px rgba(20,94,170,.18));animation:robotFloat 3.4s ease-in-out infinite}
      .login-float{position:absolute;padding:10px 14px;background:#fff;border:1px solid #d7eaff;border-radius:14px;color:#15519b;font-weight:700;box-shadow:0 10px 25px rgba(22,91,164,.1);animation:floatCard 4s ease-in-out infinite;z-index:2}
      .login-float.one{top:16%;left:7%}.login-float.two{right:7%;top:23%;animation-delay:1s}.login-float.three{bottom:15%;left:11%;animation-delay:2s}
      .login-form{max-width:560px;margin:0 auto;padding:0 0 3rem}
      .login-form h3{color:#082d69;font-size:1.25rem;margin:0 0 12px}
      @keyframes robotFloat{50%{transform:translateY(-10px)}}
      @keyframes orbitPulse{50%{transform:scale(1.025)}}
      @keyframes floatCard{50%{transform:translateY(-10px)}}
      @media(max-width:900px){.login-shell{margin:1rem .5rem 0}.login-grid{grid-template-columns:1fr}.login-visual{min-height:460px;order:-1}.login-copy{padding:42px 28px}.login-title{font-size:2.35rem}.login-orbit{width:350px;height:350px}.login-robot-img{width:330px}}
    </style>
    <div class="login-shell"><div class="login-grid"><div class="login-copy">
      <div class="login-logo">DILYTICS</div>
      <div class="login-eyebrow">Enterprise AI Workspace</div>
      <div class="login-title">Turn your data into <span>answers.</span></div>
      <div class="login-sub">Sign in securely to explore Inventory, Sales, Supply Chain and Document AI with natural-language conversations powered by Snowflake.</div>
      <div class="login-feature-row"><span class="login-feature">📊 Live insights</span><span class="login-feature">🔐 Secure access</span><span class="login-feature">⚡ AI powered</span></div>
    </div><div class="login-visual">
      <div class="login-float one">📈 Smarter decisions</div><div class="login-float two">☁️ Cloud analytics</div><div class="login-float three">🤖 AI ready</div>
      <div class="login-orbit"><img class="login-robot-img" src="{robot_src}" alt="Dilytics AI assistant" /></div>
    </div></div></div>
    """.replace("{robot_src}", _robot_data_uri()), unsafe_allow_html=True)

    st.markdown('<div class="login-form">', unsafe_allow_html=True)
    st.markdown("### Sign in")
    c1, c2 = st.columns(2, gap="medium")
    with c1:
        st.session_state.username = st.text_input("Username", value=st.session_state.username, key="login_username")
    with c2:
        st.session_state.password = st.text_input("Password", type="password", key="login_password")

    captcha = f"{st.session_state.captcha_a} + {st.session_state.captcha_b} = ?"
    cc1, cc2 = st.columns([1, 1], gap="medium")
    with cc1:
        st.text_input("Security check", value=captcha, disabled=True, key="login_captcha_question")
    with cc2:
        captcha_answer = st.text_input("Enter answer", key="login_captcha_answer")

    b1, b2 = st.columns([4, 1], gap="small")
    with b1:
        login_clicked = st.button("Sign in to Dilytics", use_container_width=True, type="primary", key="login_submit")
    with b2:
        if st.button("↻", help="New CAPTCHA", use_container_width=True, key="login_refresh_captcha"):
            _new_captcha()
            st.rerun()

    st.markdown('</div>', unsafe_allow_html=True)

    if login_clicked:
        try:
            if int(captcha_answer.strip()) != st.session_state.captcha_a + st.session_state.captcha_b:
                st.error("Incorrect security check. Please try again.")
            else:
                with st.spinner("Connecting securely to Snowflake..."):
                    conn = snowflake.connector.connect(
                        user=st.session_state.username,
                        password=st.session_state.password,
                        account=ACCOUNT,
                        host=HOST,
                        port=443,
                        warehouse=WAREHOUSE,
                        role=ROLE,
                        database=DATABASE,
                        schema=SCHEMA,
                    )
                    st.session_state.snowflake_conn = conn
                    st.session_state.snowpark_session = Session.builder.configs({"connection": conn}).create()
                    st.session_state.authenticated = True
                    st.session_state.app_page = "home"
                    st.rerun()
        except Exception as e:
            st.error(f"Authentication failed: {e}")
    st.stop()

if not st.session_state.authenticated:
    _login_page()

session = st.session_state.snowpark_session
conn = st.session_state.snowflake_conn

# ===================================================================
# 2. CORTEX ANALYST
#
# No Python question -> SQL mapping.
# Cortex Analyst receives the complete YAML semantic model(s),
# understands the user's natural-language question, and generates SQL.
# ===================================================================
def get_analyst_headers() -> Dict[str, str]:
    token = conn.rest.token
    return {
        "Authorization": f'Snowflake Token="{token}"',
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def call_cortex_analyst(prompt: str) -> Dict[str, Any]:
    request_body = {
        "messages": [{
            "role": "user",
            "content": [{"type": "text", "text": prompt}],
        }],
        "semantic_models": [
            {"semantic_model_file": INVENTORY_YAML_STAGE_PATH},
            {"semantic_model_file": SALES_YAML_STAGE_PATH},
            {"semantic_model_file": SUPPLY_CHAIN_YAML_STAGE_PATH},
        ],
        "stream": False,
    }

    response = requests.post(
        ANALYST_ENDPOINT,
        headers=get_analyst_headers(),
        json=request_body,
        timeout=120,
    )

    if response.status_code >= 400:
        try:
            details = response.json()
        except Exception:
            details = response.text
        raise RuntimeError(
            f"Cortex Analyst API error ({response.status_code}): {details}"
        )

    return response.json()


def call_cortex_analyst_with_semantic_model(
    prompt: str,
    semantic_model_yaml: str,
) -> Dict[str, Any]:
    """Call Cortex Analyst with an inline, dynamically generated YAML model."""
    request_body = {
        "messages": [{
            "role": "user",
            "content": [{"type": "text", "text": prompt}],
        }],
        "semantic_model": semantic_model_yaml,
        "stream": False,
    }

    response = requests.post(
        ANALYST_ENDPOINT,
        headers=get_analyst_headers(),
        json=request_body,
        timeout=120,
    )

    if response.status_code >= 400:
        try:
            details = response.json()
        except Exception:
            details = response.text
        raise RuntimeError(
            f"Cortex Analyst API error ({response.status_code}): {details}"
        )

    data = response.json()
    if isinstance(data, dict) and data.get("error_code"):
        raise RuntimeError(
            f"Cortex Analyst returned error {data.get('error_code')}: "
            f"{data.get('message', data)}"
        )
    return data


def extract_analyst_response(data: Dict[str, Any]) -> Dict[str, Any]:
    result = {
        "text": "",
        "sql": None,
        "warnings": data.get("warnings", []) or [],
        "semantic_model_selection": data.get("semantic_model_selection"),
        "verified_query_used": None,
        "request_id": data.get("request_id"),
    }

    message = data.get("message", {})
    content = message.get("content", [])
    if isinstance(content, dict):
        content = [content]

    text_parts = []

    for block in content:
        block_type = block.get("type")

        if block_type == "text":
            if block.get("text"):
                text_parts.append(block["text"])

        elif block_type == "sql":
            result["sql"] = (
                block.get("statement")
                or block.get("sql")
                or block.get("query")
            )
            confidence = block.get("confidence", {})
            if isinstance(confidence, dict):
                result["verified_query_used"] = confidence.get(
                    "verified_query_used"
                )

        elif block_type == "suggestions":
            suggestions = block.get("suggestions", [])
            if isinstance(suggestions, list):
                text_parts.append(
                    "I could not generate SQL for this question. "
                    "Try one of these questions:\n\n"
                    + "\n".join(f"- {x}" for x in suggestions)
                )
            elif suggestions:
                text_parts.append(str(suggestions))

    result["text"] = "\n\n".join(text_parts).strip()

    if not result["sql"]:
        result["sql"] = message.get("statement")

    return result

# ===================================================================
# 2A. UPLOADED DOCUMENT ANALYSIS (ADDED - ORIGINAL CORTEX ANALYST
#     INVENTORY/SALES CODE IS PRESERVED)
# ===================================================================
# PDF/Word document Q&A uses the current AI_COMPLETE document capability.
# Excel/CSV continues to use the existing Cortex Analyst path unchanged.
DOCUMENT_AI_MODEL = "claude-sonnet-4-6"
DOCUMENT_STAGE_DB = "INVENTORY_DW_DEMO"
DOCUMENT_STAGE_SCHEMA = "GOLD"
DOCUMENT_STAGE_NAME = "DILYTICS_DOCUMENT_STAGE"

if "uploaded_document" not in st.session_state:
    st.session_state.uploaded_document = None
if "uploaded_document_name" not in st.session_state:
    st.session_state.uploaded_document_name = None
if "uploaded_document_df" not in st.session_state:
    st.session_state.uploaded_document_df = None
if "uploaded_document_text" not in st.session_state:
    st.session_state.uploaded_document_text = ""
if "uploaded_document_type" not in st.session_state:
    st.session_state.uploaded_document_type = None
if "uploaded_document_table" not in st.session_state:
    st.session_state.uploaded_document_table = None
if "uploaded_document_semantic_model" not in st.session_state:
    st.session_state.uploaded_document_semantic_model = None
if "uploaded_document_stage" not in st.session_state:
    st.session_state.uploaded_document_stage = None
if "uploaded_document_stage_file" not in st.session_state:
    st.session_state.uploaded_document_stage_file = None


def _snowflake_sql_literal(value: str) -> str:
    """Safely convert a Python string into a Snowflake SQL string literal."""
    if value is None:
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


def _document_stage_quoted_name() -> str:
    """Return the fully-qualified named stage used for PDF/DOCX files."""
    return (
        f'"{DOCUMENT_STAGE_DB}"."{DOCUMENT_STAGE_SCHEMA}".'
        f'"{DOCUMENT_STAGE_NAME}"'
    )


def _document_stage_file_reference() -> str:
    """Return the fully-qualified @stage reference required by PUT/TO_FILE."""
    return '@' + _document_stage_quoted_name()


def _ensure_document_stage():
    """Create the persistent, server-encrypted named stage used by AI_COMPLETE.

    AI_COMPLETE document processing requires the referenced FILE to live on an
    accessible internal/external stage. A temporary stage is session-scoped and
    is not reliable for this document-processing path, so use a dedicated named
    internal stage instead.
    """
    stage_name = _document_stage_quoted_name()
    try:
        session.sql(
            f"CREATE STAGE IF NOT EXISTS {stage_name} "
            "ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')"
        ).collect()
    except Exception as exc:
        raise RuntimeError(
            f"Could not create or access document stage {stage_name}. "
            "Run this once with a role that can CREATE STAGE in "
            f"{DOCUMENT_STAGE_DB}.{DOCUMENT_STAGE_SCHEMA}, or grant the Streamlit role "
            "USAGE on the database/schema and READ/WRITE on the stage."
        ) from exc
    return stage_name


def _upload_document_to_stage(uploaded_file) -> str:
    """Upload a PDF/DOCX to the session's temporary Snowflake stage."""
    import os
    import tempfile

    extension = uploaded_file.name.rsplit(".", 1)[-1].lower()
    if extension not in {"pdf", "docx"}:
        raise ValueError("Only PDF and Word (.docx) documents can use document Q&A.")

    stage_name = _ensure_document_stage()
    safe_name = re.sub(r"[^A-Za-z0-9_.-]+", "_", uploaded_file.name)

    # Claude Sonnet 4.6 supports documents up to 22 MB.
    file_size = getattr(uploaded_file, "size", None)
    if file_size is not None and file_size > 22 * 1024 * 1024:
        raise ValueError(
            f"The PDF/Word file is {file_size / (1024 * 1024):.2f} MB. "
            "The selected Claude Sonnet 4.6 document model supports files up to 22 MB."
        )
    if not safe_name.lower().endswith((".pdf", ".docx")):
        safe_name = f"document.{extension}"

    with tempfile.NamedTemporaryFile(delete=False, suffix=f".{extension}") as tmp:
        uploaded_file.seek(0)
        tmp.write(uploaded_file.getvalue())
        local_path = tmp.name

    try:
        # Do not compress: AI_COMPLETE needs the original document extension/content.
        session.file.put(
            local_path,
            _document_stage_file_reference(),
            auto_compress=False,
            overwrite=True,
        )
    finally:
        try:
            os.remove(local_path)
        except OSError:
            pass

    st.session_state.uploaded_document_stage = _document_stage_file_reference()
    st.session_state.uploaded_document_stage_file = safe_name
    return safe_name


def ai_complete_document_question(question: str) -> str:
    """Answer a question directly from the uploaded PDF/DOCX using AI_COMPLETE.

    This is intentionally separate from the working Excel/CSV Cortex Analyst path.
    It does not use the legacy SNOWFLAKE.CORTEX.COMPLETE function.
    """
    stage_name = st.session_state.get("uploaded_document_stage")
    stage_file = st.session_state.get("uploaded_document_stage_file")

    if not stage_name or not stage_file:
        raise RuntimeError(
            "The uploaded PDF/Word document is not available in the Snowflake stage. "
            "Please click Analyze Document again."
        )

    model_literal = _snowflake_sql_literal(DOCUMENT_AI_MODEL)
    question_literal = _snowflake_sql_literal(
        "Answer the user's question using only the uploaded document. "
        "Be precise and concise. If the document does not contain enough information "
        "to answer, say so instead of inventing information. "
        "User question: " + question
    )
    # TO_FILE expects the stage reference as a string such as
    # '@"DATABASE"."SCHEMA"."STAGE"'.
    stage_literal = _snowflake_sql_literal(stage_name)
    file_literal = _snowflake_sql_literal(stage_file)

    sql = f"""
        SELECT AI_COMPLETE(
            MODEL => {model_literal},
            PROMPT => PROMPT(
                {question_literal} || '\n\nDocument to analyze: {{0}}',
                TO_FILE({stage_literal}, {file_literal})
            )
        ) AS RESPONSE
    """

    rows = session.sql(sql).collect()
    if not rows:
        raise RuntimeError("AI_COMPLETE did not return a response.")

    row = rows[0]
    try:
        response = row["RESPONSE"]
    except Exception:
        response = row[0]

    if response is None:
        raise RuntimeError(
            "AI_COMPLETE returned no answer. Check that the SNOWFLAKE.CORTEX_USER "
            "database role is available and that the document is within the model's size limit."
        )

    # Some AI_COMPLETE variants can return an object when error details are requested;
    # this call uses the normal string response, so stringify defensively.
    return str(response)

def _clean_generated_sql(text_value: str) -> str:
    """Extract and validate a read-only SELECT/WITH SQL statement."""
    sql_text = str(text_value or "").strip()

    if "```" in sql_text:
        blocks = re.findall(
            r"```(?:sql|SQL)?\s*(.*?)```", sql_text, flags=re.DOTALL
        )
        if blocks:
            sql_text = blocks[0].strip()

    sql_text = re.sub(
        r"^\s*(SQL\s*:|Query\s*:)\s*", "", sql_text, flags=re.I
    ).strip().rstrip(";").strip()

    if not re.match(r"^(SELECT|WITH)\b", sql_text, flags=re.I):
        raise RuntimeError(
            "Cortex Analyst did not return a valid SELECT/WITH statement."
        )

    forbidden = re.search(
        r"\b(INSERT|UPDATE|DELETE|MERGE|DROP|ALTER|TRUNCATE|CREATE|GRANT|REVOKE|COPY|PUT|REMOVE|CALL)\b",
        sql_text,
        flags=re.I,
    )
    if forbidden:
        raise RuntimeError(
            f"Generated document SQL contains a non-read-only command: {forbidden.group(1)}"
        )

    return sql_text


def _normalize_uploaded_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Make mixed Excel/CSV columns safe for Streamlit and Snowflake.

    Numeric/date/bool columns stay typed. Object columns are normalized to text
    because Excel frequently mixes integers, strings such as 'Grand Total', and
    blanks in the same column.
    """
    if df is None:
        return df

    work_df = df.copy()
    for col in work_df.columns:
        series = work_df[col]
        if pd.api.types.is_object_dtype(series.dtype):
            work_df[col] = series.map(
                lambda value: None if pd.isna(value) else str(value)
            )
    return work_df


def _safe_column_names(df: pd.DataFrame):
    """Create SQL-friendly, unique Snowflake column names."""
    mapping = {}
    used = set()

    for original in df.columns:
        base = re.sub(
            r"[^A-Za-z0-9_]+", "_", str(original)
        ).strip("_").upper()
        if not base:
            base = "COLUMN"
        if base[0].isdigit():
            base = "_" + base

        candidate = base
        n = 2
        while candidate in used:
            candidate = f"{base}_{n}"
            n += 1

        used.add(candidate)
        mapping[str(original)] = candidate

    return mapping


def _snowflake_type_for_pandas(dtype) -> str:
    if pd.api.types.is_bool_dtype(dtype):
        return "BOOLEAN"
    if pd.api.types.is_integer_dtype(dtype):
        return "NUMBER"
    if pd.api.types.is_float_dtype(dtype):
        return "NUMBER"
    if pd.api.types.is_datetime64_any_dtype(dtype):
        return "TIMESTAMP_NTZ"
    return "TEXT"


def _column_synonyms(original_name: str):
    """Create conservative synonyms from the actual uploaded header."""
    text = re.sub(r"[_\-]+", " ", str(original_name)).strip()
    words = text.split()
    synonyms = [text.lower()]

    if text.lower().endswith(" id"):
        synonyms.append(text[:-3].strip().lower() + " identifier")
    if "commercial project" in text.lower() and "id" in text.lower():
        synonyms.extend(["project id", "commercial project"])
    if "jurisdiction" in text.lower():
        synonyms.extend(["jurisdiction", "local jurisdiction"])
    if "contractor" in text.lower():
        synonyms.extend(["contractor", "vendor"])
    if "business name" in text.lower():
        synonyms.extend(["business", "project business"])
    if "close out" in text.lower() or "closeout" in text.lower():
        synonyms.extend([
            "closeout date",
            "close out date",
            "completion date",
            "completed date",
        ])

    # Preserve order and uniqueness.
    result = []
    seen = set()
    for item in synonyms:
        item = item.strip()
        if item and item not in seen:
            result.append(item)
            seen.add(item)
    return result[:8]


def _sample_values(df: pd.DataFrame, original: str, limit: int = 5):
    values = []
    for value in df[original].dropna().head(limit).tolist():
        text = str(value)
        if len(text) > 100:
            text = text[:97] + "..."
        values.append(text)
    return values


def build_uploaded_semantic_model(df: pd.DataFrame, table_name: str) -> str:
    """Build a semantic model directly from the uploaded spreadsheet schema.

    The model is sent inline to the Cortex Analyst REST API. No COMPLETE call
    and no hard-coded question-to-SQL mapping are used.
    """
    mapping = _safe_column_names(df)

    dimensions = []
    time_dimensions = []
    facts = []

    for original, safe in mapping.items():
        dtype = df[original].dtype
        sf_type = _snowflake_type_for_pandas(dtype)
        synonyms = _column_synonyms(original)
        desc = f"Uploaded spreadsheet column '{original}'."

        # Close-out date is commonly the strongest completion indicator in
        # project workbooks. Only add this interpretation when that real column exists.
        original_lower = original.lower()
        if "close out" in original_lower or "closeout" in original_lower:
            desc = (
                f"Uploaded spreadsheet column '{original}'. A non-null value indicates "
                "that the project received close-out approval and can be used as a "
                "completion indicator."
            )

        entry = {
            "name": safe,
            "description": desc,
            "expr": safe,
            "data_type": sf_type,
            "unique": False,
        }
        if synonyms:
            entry["synonyms"] = synonyms
        if pd.api.types.is_datetime64_any_dtype(dtype):
            time_dimensions.append(entry)
        else:
            dimensions.append(entry)

        if pd.api.types.is_numeric_dtype(dtype):
            facts.append({
                "name": safe,
                "description": f"Numeric value from uploaded column '{original}'.",
                "expr": safe,
                "data_type": "NUMBER",
            })

    # A row indicator gives Analyst an explicit way to calculate row/project
    # counts without requiring any hard-coded question mapping.
    facts.append({
        "name": "ROW_INDICATOR",
        "description": "One numeric indicator per uploaded spreadsheet row. SUM this fact to count rows/projects.",
        "expr": "1",
        "data_type": "NUMBER",
    })

    # Add a semantic completion flag only when a real close-out column exists.
    closeout_safe = None
    for original, safe in mapping.items():
        low = original.lower()
        if "close out" in low or "closeout" in low:
            closeout_safe = safe
            break

    if closeout_safe:
        dimensions.append({
            "name": "IS_COMPLETED",
            "description": "True when the close-out approval date is not null; this represents a completed project in this uploaded workbook.",
            "expr": f"{closeout_safe} IS NOT NULL",
            "data_type": "BOOLEAN",
            "unique": False,
            "synonyms": ["completed", "project completed", "completion status"],
        })

    table_definition = {
        "name": "UPLOADED_DATA",
        "description": "One logical table containing the complete uploaded spreadsheet.",
        "base_table": {
            "database": DATABASE,
            "schema": SCHEMA,
            "table": table_name,
        },
        "dimensions": dimensions,
        "facts": facts,
    }
    if time_dimensions:
        table_definition["time_dimensions"] = time_dimensions

    model = {
        "name": "UPLOADED_DOCUMENT_ANALYSIS",
        "description": "Semantic model generated dynamically from one uploaded spreadsheet. Use only this uploaded dataset.",
        "tables": [table_definition],
        "module_custom_instructions": {
            "sql_generation": (
                "Use only the uploaded_data logical table. Query the complete underlying table. "
                "Use ROW_INDICATOR for total row/project counts when appropriate. "
                "For questions asking for the count of an ID column, count non-null values of that ID; "
                "if the ID is explicitly a unique project identifier, COUNT(DISTINCT ID) is appropriate. "
                "If IS_COMPLETED exists, use it when the user asks about completed projects. "
                "Do not invent columns or business definitions."
            ),
            "question_categorization": (
                "Classify questions only from the uploaded table's actual columns and values. "
                "Do not use the Inventory or Sales semantic models for this document question."
            ),
        },
    }

    return yaml.safe_dump(
        model,
        sort_keys=False,
        allow_unicode=True,
        default_flow_style=False,
    )


def _drop_uploaded_table():
    table_name = st.session_state.get("uploaded_document_table")
    if not table_name:
        return
    try:
        # Generated names contain only A-Z, 0-9 and underscore.
        if re.fullmatch(r"UPLOADED_DOCUMENT_[A-Z0-9_]+", str(table_name)):
            session.sql(f'DROP TABLE IF EXISTS "{table_name}"').collect()
    except Exception:
        pass
    st.session_state.uploaded_document_table = None
    st.session_state.uploaded_document_semantic_model = None
    st.session_state.uploaded_document_stage_file = None


def process_uploaded_document(uploaded_file):
    """Read CSV/XLSX/XLS/PDF/DOCX and return display data/text."""
    name = uploaded_file.name
    extension = name.rsplit(".", 1)[-1].lower()

    if extension == "csv":
        uploaded_file.seek(0)
        df = pd.read_csv(uploaded_file)
        df = _normalize_uploaded_dataframe(df)
        return "table", df, "", f"CSV file loaded with {len(df):,} rows."

    if extension in {"xlsx", "xls"}:
        uploaded_file.seek(0)
        excel_file = pd.ExcelFile(uploaded_file)
        sheet_name = excel_file.sheet_names[0]
        df = pd.read_excel(excel_file, sheet_name=sheet_name)
        df = _normalize_uploaded_dataframe(df)
        return (
            "table",
            df,
            "",
            f"Excel file loaded from sheet '{sheet_name}' with {len(df):,} rows.",
        )

    if extension == "pdf":
        try:
            from pypdf import PdfReader
        except ImportError:
            from PyPDF2 import PdfReader

        uploaded_file.seek(0)
        reader = PdfReader(uploaded_file)
        pages = []
        for page in reader.pages:
            pages.append(page.extract_text() or "")
        full_text = "\n\n".join(pages).strip()
        return "text", None, full_text, f"PDF analyzed successfully ({len(reader.pages)} pages)."

    if extension == "docx":
        # DOCX is a ZIP package containing XML. Parse it with Python's standard
        # library so the app does not require the optional python-docx package.
        import zipfile
        import xml.etree.ElementTree as ET

        uploaded_file.seek(0)
        docx_bytes = uploaded_file.read()

        try:
            with zipfile.ZipFile(io.BytesIO(docx_bytes)) as zf:
                xml_bytes = zf.read("word/document.xml")
        except (KeyError, zipfile.BadZipFile) as exc:
            raise ValueError("The uploaded Word file is not a valid .docx document.") from exc

        try:
            root = ET.fromstring(xml_bytes)
        except ET.ParseError as exc:
            raise ValueError("Could not read the Word document content.") from exc

        ns = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}
        paragraphs = []
        for paragraph in root.findall(".//w:p", ns):
            parts = [node.text or "" for node in paragraph.findall(".//w:t", ns)]
            text = "".join(parts).strip()
            if text:
                paragraphs.append(text)

        # Preserve Word tables in a simple row/column text representation.
        table_parts = []
        for table in root.findall(".//w:tbl", ns):
            for row in table.findall("./w:tr", ns):
                cells = []
                for cell in row.findall("./w:tc", ns):
                    cell_parts = [node.text or "" for node in cell.findall(".//w:t", ns)]
                    cells.append(" ".join("".join(cell_parts).split()))
                if any(cells):
                    table_parts.append(" | ".join(cells))

        full_text = "\n".join(paragraphs + table_parts).strip()
        return "text", None, full_text, "DOCX document analyzed successfully."

    raise ValueError("Unsupported document type.")


def prepare_uploaded_table(df: pd.DataFrame) -> str:
    """Create a transient table so Cortex Analyst's REST session can see it."""
    if df is None or df.empty:
        raise ValueError("The uploaded spreadsheet contains no rows.")

    _drop_uploaded_table()

    work_df = _normalize_uploaded_dataframe(df)
    mapping = _safe_column_names(work_df)
    work_df.columns = [mapping[str(c)] for c in work_df.columns]

    table_name = (
        "UPLOADED_DOCUMENT_"
        + datetime.now().strftime("%Y%m%d_%H%M%S_%f").upper()
    )

    # IMPORTANT: do NOT use a TEMPORARY table here. Cortex Analyst REST runs
    # in a separate Snowflake session and cannot see session-scoped temp tables.
    # A TRANSIENT table is visible to the Analyst request and is dropped when
    # the user uploads another document or removes the current document.
    session.write_pandas(
        work_df,
        table_name,
        auto_create_table=True,
        overwrite=True,
        table_type="transient",
    )

    try:
        session.sql(
            f'ALTER TABLE "{table_name}" SET DATA_RETENTION_TIME_IN_DAYS = 0'
        ).collect()
    except Exception:
        pass

    semantic_model = build_uploaded_semantic_model(df, table_name)

    st.session_state.uploaded_document_table = table_name
    st.session_state.uploaded_document_semantic_model = semantic_model

    return semantic_model


def answer_uploaded_table_question(question: str, df: pd.DataFrame):
    """Use Cortex Analyst to generate SQL against the complete uploaded table."""
    if df is None or df.empty:
        raise ValueError("The uploaded spreadsheet has no usable rows.")

    if not st.session_state.uploaded_document_table:
        prepare_uploaded_table(df)

    table_name = st.session_state.uploaded_document_table
    semantic_model = st.session_state.uploaded_document_semantic_model

    if not table_name or not semantic_model:
        raise RuntimeError("The uploaded document semantic model was not created.")

    analyst_json = call_cortex_analyst_with_semantic_model(
        question,
        semantic_model,
    )
    result = extract_analyst_response(analyst_json)

    if result.get("warnings"):
        warning_text = " ".join(
            str(w.get("message", w)) if isinstance(w, dict) else str(w)
            for w in result["warnings"]
        )
        if warning_text:
            st.warning(warning_text)

    if not result.get("sql"):
        raise RuntimeError(
            result.get("text")
            or "Cortex Analyst could not generate SQL for the uploaded document question."
        )

    sql_query = _clean_generated_sql(result["sql"])
    result_df = session.sql(sql_query).to_pandas()

    return result_df, sql_query, result


def _split_document_into_chunks(document_text: str) -> List[str]:
    """Split extracted Word text into useful paragraph/table chunks."""
    chunks = []
    for block in re.split(r"\n{2,}|\n", document_text):
        block = re.sub(r"\s+", " ", block).strip()
        if block:
            chunks.append(block)
    return chunks


def _word_question_answer(question: str, document_text: str) -> str:
    """Answer Word-document questions without Cortex COMPLETE/AI_COMPLETE.

    This is an extractive, trial-safe fallback: it ranks paragraphs/table rows
    by overlap with the question and returns the most relevant document content.
    It does not invent information and therefore works without an LLM entitlement.
    """
    chunks = _split_document_into_chunks(document_text)
    if not chunks:
        raise ValueError("No readable text was extracted from the Word document.")

    stop_words = {
        "what", "is", "are", "the", "a", "an", "of", "for", "to",
        "in", "on", "and", "or", "with", "from", "this", "that",
        "which", "who", "how", "why", "does", "do", "can", "please",
        "tell", "me", "about", "give", "explain", "purpose",
    }
    question_words = [
        w.lower() for w in re.findall(r"[A-Za-z0-9_]+", question)
        if w.lower() not in stop_words and len(w) > 2
    ]

    # Also recognize common phrase variants so questions such as
    # "What is the purpose of PII?" find a paragraph headed "Purpose".
    query_lower = question.lower()
    phrase_terms = []
    if "purpose" in query_lower:
        phrase_terms.extend(["purpose", "objective", "goal", "intended"])
    if "pii" in query_lower:
        phrase_terms.extend(["pii", "personally identifiable information"])
    if "handling" in query_lower:
        phrase_terms.extend(["handling", "protect", "protection", "process"])
    if "approach" in query_lower or "approaches" in query_lower:
        phrase_terms.extend(["approach", "approaches", "method"])

    terms = list(dict.fromkeys(question_words + phrase_terms))
    scored = []
    for idx, chunk in enumerate(chunks):
        low = chunk.lower()
        score = 0
        matched = 0
        for term in terms:
            if term in low:
                matched += 1
                score += 2 if " " in term else 1
        # Prefer shorter focused passages when relevance is similar.
        if matched:
            score += min(len(terms), matched)
            score += 1 if len(chunk) < 500 else 0
            scored.append((score, matched, -len(chunk), idx, chunk))

    if not scored:
        # Safe fallback: show the beginning of the document rather than inventing.
        preview = "\n\n".join(chunks[:3])
        return (
            "I could not find a passage in the Word document that directly matches "
            "your question. Here is the beginning of the extracted document content "
            "so you can refine the question:\n\n" + preview
        )

    scored.sort(reverse=True)
    selected = []
    seen = set()
    for _, _, _, idx, chunk in scored[:5]:
        # Include nearby context when available.
        for pos in (idx - 1, idx, idx + 1):
            if 0 <= pos < len(chunks) and pos not in seen:
                seen.add(pos)
                selected.append(chunks[pos])
        if len(selected) >= 7:
            break

    return (
        "Based on the uploaded Word document, the most relevant content is:\n\n"
        + "\n\n".join(selected[:7])
    )


def answer_uploaded_text_question(question: str, document_text: str):
    """Answer Word questions without changing the working Excel/CSV path.

    DOCX uses local extractive search because AI_COMPLETE/COMPLETE is blocked on
    the current Snowflake trial account. PDF keeps the existing AI_COMPLETE path.
    """
    if not document_text.strip():
        raise ValueError("No readable text was extracted from the uploaded document.")

    if st.session_state.get("uploaded_document_name", "").lower().endswith(".docx"):
        return _word_question_answer(question, document_text)

    return ai_complete_document_question(question)


def render_uploaded_document_preview():
    """Display the analyzed document without interfering with the original UI."""
    doc_type = st.session_state.uploaded_document_type
    doc_name = st.session_state.uploaded_document_name

    if not doc_name:
        return

    st.markdown("---")
    st.markdown(f"### 📄 Uploaded Document: `{doc_name}`")

    if doc_type == "table":
        df = st.session_state.uploaded_document_df
        if df is not None:
            st.dataframe(_normalize_uploaded_dataframe(df), use_container_width=True)
    elif doc_type == "text":
        with st.expander("📖 Extracted Document Content", expanded=False):
            st.text_area(
                "Document text",
                st.session_state.uploaded_document_text,
                height=350,
                disabled=True,
                label_visibility="collapsed",
            )


# ===================================================================
# 2B. PROFESSIONAL APPLICATION PAGES
# ===================================================================

def _set_page(page: str):
    st.session_state.app_page = page
    st.rerun()


def _logout():
    try:
        if st.session_state.get("snowflake_conn"):
            st.session_state.snowflake_conn.close()
    except Exception:
        pass
    st.session_state.authenticated = False
    st.session_state.password = ""
    st.session_state.app_page = "home"
    _new_captcha()
    st.rerun()


def _top_nav():
    """Single continuous highlighted header: brand on left, blue nav buttons on right."""
    st.markdown("""
    <style>
      /* ONE continuous header box across the full content width */
      .st-key-dly_main_header {
          width:100% !important;
          box-sizing:border-box !important;
          background:#e7f5ff !important;
          border:1px solid #b9dcf7 !important;
          border-radius:12px !important;
          padding:11px 14px !important;
          margin:-14px 0 22px 0 !important;
          min-height:60px !important;
          box-shadow:0 5px 18px rgba(35,111,177,.10) !important;
      }
      .st-key-dly_main_header > div,
      .st-key-dly_main_header > div > div {
          box-sizing:border-box !important;
      }
      .st-key-dly_main_header [data-testid="column"] {
          display:flex !important;
          align-items:center !important;
          justify-content:center !important;
      }
      .dly-main-brandline {
          display:flex;
          align-items:center;
          gap:8px;
          min-height:44px;
          white-space:nowrap;
      }
      .dly-main-logo {
          display:inline-flex;
          align-items:center;
          justify-content:center;
          background:#e51f2b;
          color:#fff;
          height:40px;
          min-width:72px;
          padding:0 16px;
          border-radius:3px;
          font-size:.86rem;
          font-weight:900;
          letter-spacing:.2px;
      }
      .dly-main-tagline {
          color:#315f8c;
          font-size:.61rem;
          white-space:nowrap;
      }
      /* BLUE BUTTON BOXES INSIDE THE SAME HEADER */
      .st-key-dly_main_header [data-testid="stButton"] {
          width:100% !important;
      }
      .st-key-dly_main_header [data-testid="stButton"] > button {
          width:100% !important;
          min-height:34px !important;
          height:34px !important;
          padding:0 9px !important;
          margin:0 !important;
          border-radius:7px !important;
          border:1px solid #0878c8 !important;
          background:#0878c8 !important;
          color:#fff !important;
          font-size:.68rem !important;
          font-weight:700 !important;
          line-height:1 !important;
          display:flex !important;
          align-items:center !important;
          justify-content:center !important;
          white-space:nowrap !important;
          box-shadow:0 2px 5px rgba(8,120,200,.16) !important;
      }
      .st-key-dly_main_header [data-testid="stButton"] > button:hover {
          background:#066aaF !important;
          border-color:#066aaF !important;
          color:#fff !important;
      }
      @media(max-width:850px){
          .st-key-dly_main_header { padding:6px !important; }
          .dly-main-tagline { display:none; }
          .dly-main-logo { font-size:.76rem; padding:0 11px; }
          .st-key-dly_main_header [data-testid="stButton"] > button {
              font-size:.56rem !important;
              padding:0 4px !important;
          }
      }
    </style>
    """, unsafe_allow_html=True)

    # IMPORTANT: every item below is rendered INSIDE this single container.
    with st.container(key="dly_main_header"):
        c1, c2, c3, c4, c5 = st.columns(
            [2.8, 1.0, 1.55, 1.25, 1.0],
            gap="small",
            vertical_alignment="center",
        )

        with c1:
            st.markdown(
                '<div class="dly-main-brandline">'
                '<span class="dly-main-logo">DILYTICS</span>'
                '<span class="dly-main-tagline">Data. Insights. Impact.</span>'
                '</div>',
                unsafe_allow_html=True,
            )

        with c2:
            if st.button("⌂ Home", use_container_width=True, key="top_home"):
                _set_page("home")

        with c3:
            if st.button("▣ Document AI Demo", use_container_width=True, key="top_docs"):
                _set_page("document_ai")

        with c4:
            if st.button("ⓘ About DiLytics", use_container_width=True, key="top_about"):
                _set_page("about")

        with c5:
            username = st.session_state.get("username", "User")
            if st.button(f"◯ {username}", use_container_width=True, key="top_profile"):
                st.session_state.show_profile_menu = not st.session_state.get("show_profile_menu", False)

    if st.session_state.get("show_profile_menu"):
        pc = st.columns([8.0, 1.10])
        with pc[1]:
            if st.button("Logout", use_container_width=True, key="top_logout"):
                _logout()


def _module_page(module: str):
    inventory = module == "inventory"
    title = "Inventory Intelligence" if inventory else "Sales Intelligence"
    subtitle = "Turn inventory data into clear, actionable decisions across products, warehouses and stock levels." if inventory else "Turn sales data into clear, actionable decisions across revenue, products, customers, regions and channels."
    points = ([
        "Track total inventory quantity, availability and inventory value.",
        "Compare inventory value across warehouses and product categories.",
        "Identify excess, overstocked, quarantined and out-of-stock inventory.",
        "Find products that need urgent replenishment or reorder attention.",
        "Analyze days of supply and inventory health using the latest snapshot.",
    ] if inventory else [
        "Analyze total sales, orders, discounts, taxes and shipping costs.",
        "Identify top products and understand product-level revenue performance.",
        "Compare sales across customer regions and order channels.",
        "Analyze monthly sales trends and average order value.",
        "Explore completed and cancelled orders to understand sales performance.",
    ])
    icon="▦" if inventory else "▥"
    st.markdown(f"""
    <style>
      .module-hero{{padding:48px 55px;background:linear-gradient(135deg,#fff,#edf7ff);border:1px solid #cfe6ff;border-radius:28px;box-shadow:0 18px 45px rgba(23,91,160,.08)}}
      .module-icon{{font-size:3rem;color:#1769d2}} .module-title{{font-size:3rem;font-weight:900;color:#082d69;margin:.3rem 0}} .module-sub{{font-size:1.1rem;color:#587291;max-width:820px;line-height:1.65}}
      .point{{padding:15px 18px;margin:10px 0;background:#fff;border:1px solid #deedff;border-radius:15px;color:#153f76;box-shadow:0 7px 20px rgba(23,91,160,.05)}}
      .point b{{color:#1769d2;margin-right:10px}}
    </style>
    <div class="module-hero"><div class="module-icon">{icon}</div><div class="module-title">{title}</div><div class="module-sub">{subtitle}</div></div>
    """,unsafe_allow_html=True)
    st.markdown("### What you can analyze")
    for p in points: st.markdown(f'<div class="point"><b>✓</b>{p}</div>',unsafe_allow_html=True)
    st.markdown("### Start exploring")
    a,b=st.columns(2)
    with a:
        if st.button(f"💬 Chat with {title}",use_container_width=True,type="primary"): _set_page("chatbot")
    with b:
        if st.button("⌂  Back to Home",use_container_width=True): _set_page("home")


def _document_ai_page():
    st.markdown("""
    <style>
      .doc-hero{background:linear-gradient(135deg,#f8fcff,#e8f4ff);border:1px solid #cce5ff;border-radius:28px;padding:42px;display:grid;grid-template-columns:1.1fr .9fr;gap:30px;align-items:center}
      .doc-title{font-size:2.8rem;font-weight:900;color:#082d69}.doc-sub{font-size:1.05rem;line-height:1.65;color:#587291}.doc-list{color:#173e73;line-height:2}
      .doc-animation{height:300px;border-radius:28px;background:radial-gradient(circle,#fff,#dff1ff);position:relative;overflow:hidden;border:1px solid #c5e3ff}
      .doc-sheet{position:absolute;width:155px;height:205px;background:#fff;border:4px solid #1769d2;border-radius:12px;left:50%;top:50%;transform:translate(-50%,-50%) rotate(-7deg);box-shadow:0 20px 35px rgba(23,91,160,.15);animation:docFloat 3s ease-in-out infinite}
      .doc-line{height:7px;background:#78b9ef;border-radius:10px;margin:18px 15px 0}.doc-line.short{width:55%}.doc-scan{position:absolute;height:4px;background:#15a9ff;left:20px;right:20px;top:105px;box-shadow:0 0 15px #15a9ff;animation:scan 2.3s linear infinite}
      @keyframes docFloat{50%{transform:translate(-50%,-54%) rotate(7deg)}} @keyframes scan{0%{top:45px}100%{top:220px}}
      @media(max-width:850px){.doc-hero{grid-template-columns:1fr}}
    </style>
    <div class="doc-hero"><div><div class="doc-title">Document AI</div><div class="doc-sub">Upload a business document and move directly into a document-focused conversation. Use the assistant to explore spreadsheets and Word documents, extract relevant content, and ask natural-language questions.</div><div class="doc-list">✓ Excel / CSV analysis<br>✓ Word document content search<br>✓ Data questions and summaries<br>✓ Results displayed inside the familiar chat workspace</div></div><div class="doc-animation"><div class="doc-sheet"><div class="doc-line"></div><div class="doc-line"></div><div class="doc-line short"></div><div class="doc-line"></div><div class="doc-scan"></div></div></div></div>
    """,unsafe_allow_html=True)
    st.markdown("### Analyze your document")
    uploaded=st.file_uploader("Upload CSV, Excel or Word",type=["csv","xlsx","xls","docx"],key="document_ai_uploader")
    c1,c2=st.columns([2,1])
    with c1:
        analyze=st.button("📄 Analyze Your Document",use_container_width=True,type="primary",disabled=uploaded is None)
    with c2:
        if st.button("⌂ Home",use_container_width=True): _set_page("home")
    if analyze and uploaded:
        try:
            with st.spinner("Analyzing document..."):
                doc_type,doc_df,doc_text,doc_message=process_uploaded_document(uploaded)
                _drop_uploaded_table()
                st.session_state.uploaded_document_name=uploaded.name
                st.session_state.uploaded_document_type=doc_type
                st.session_state.uploaded_document_df=doc_df
                st.session_state.uploaded_document_text=doc_text
                st.session_state.uploaded_document=uploaded.name
                st.session_state.uploaded_document_table=None
                st.session_state.uploaded_document_semantic_model=None
                if doc_type=="table": prepare_uploaded_table(doc_df)
            st.session_state.answer_source="Uploaded Document"
            st.session_state.app_page="chatbot"
            st.success(doc_message)
            st.rerun()
        except Exception as e: st.error(f"Document analysis failed: {e}")


def _about_page():
    st.markdown("""
    <style>
      .about-hero{padding:45px;background:linear-gradient(135deg,#fff,#edf7ff);border:1px solid #cfe6ff;border-radius:28px}.about-title{font-size:3rem;font-weight:900;color:#082d69}.about-sub{font-size:1.1rem;color:#587291;line-height:1.7;max-width:900px}.about-card{padding:24px;background:#fff;border:1px solid #dbeeff;border-radius:18px;height:100%;box-shadow:0 10px 28px rgba(23,91,160,.06)}.about-card h3{color:#1769d2}
    </style>
    <div class="about-hero"><div class="about-title">About DiLytics</div><div class="about-sub">DiLytics helps organizations turn complex data into actionable insights through analytics, AI, data engineering and modern cloud platforms. Its mission is to create competitive advantage through outstanding insights using the latest developments in analytics.</div></div>
    """,unsafe_allow_html=True)
    c1,c2,c3=st.columns(3)
    cards=[("Mission","Deliver outstanding insights that create competitive advantage for customers."),("Vision","Be a trusted analytics solution partner that brings immense value to customers and their analytics journeys."),("Insight Solutions","Prebuilt and customizable analytics solutions combine data models, pipelines, dashboards, reports and metrics to accelerate data-driven decisions.")]
    for col,(h,t) in zip([c1,c2,c3],cards):
        with col: st.markdown(f'<div class="about-card"><h3>{h}</h3><p>{t}</p></div>',unsafe_allow_html=True)
    st.markdown("### Why Dilytics for modern analytics")
    points=["End-to-end analytics, data engineering and AI capabilities.","Natural-language access to insights through conversational interfaces.","Modular solutions that can be customized to business processes.","Snowflake, Power BI, Tableau and other modern data-platform expertise."]
    for p in points: st.markdown(f"✓ **{p}**")
    st.markdown("### Key milestones")
    milestones=[("2011","DiLytics was founded in California to deliver enterprise analytics solutions."),("2012","Delivered a complex supply-chain planning analytics solution for a leading biopharmaceutical organization."),("2023–2024","Expanded strategic engagements and implemented DiLytics Insight Solutions for major organizations."),("2025","Delivered Sales, Finance and Planning Analytics solutions for a leading global medical-device manufacturer."),("2026","Expanded analytics delivery across nonprofit behavioral health and other data-driven organizations.")]
    for year,desc in milestones:
        st.markdown(f"**{year}**  —  {desc}")
    if st.button("⌂ Home",use_container_width=False): _set_page("home")


def _open_chat():
    """Open AI chat only after the authentication gate has been satisfied."""
    if not st.session_state.get("authenticated", False):
        st.session_state.app_page = "login"
    else:
        st.session_state.app_page = "chatbot"
    st.rerun()


def _home_page():
    st.markdown("""
    <style>
      .home-wrap{background:#fff;color:#09295f}
      .home-hero{display:grid;grid-template-columns:1fr 1fr;gap:35px;align-items:center;padding:35px 20px 28px}
      .home-eyebrow{letter-spacing:4px;color:#1769d2;font-weight:800}
      .home-title{font-size:4rem;line-height:1.02;font-weight:900;color:#082d69}
      .home-title span{color:#1769d2}
      .home-sub{font-size:1.12rem;color:#587291;line-height:1.6;max-width:600px}
      .home-robot{height:430px;position:relative;display:flex;align-items:center;justify-content:center}
      .orb{width:430px;height:430px;border-radius:50%;background:radial-gradient(circle at 50% 42%,#ffffff,#e8f6ff 55%,#d6edff 100%);display:flex;align-items:center;justify-content:center;box-shadow:0 0 0 25px rgba(31,129,225,.05),0 20px 55px rgba(23,91,160,.08);animation:pulse 4s ease-in-out infinite}
      .home-robot-img{width:390px;max-width:86%;height:auto;object-fit:contain;filter:drop-shadow(0 25px 35px rgba(20,94,170,.18));animation:float 3.5s ease-in-out infinite}
      .bubble{position:absolute;right:5%;top:5%;padding:18px 22px;background:#fff;border:1px solid #d4e8ff;border-radius:20px;color:#1769d2;box-shadow:0 12px 30px rgba(23,91,160,.12);font-weight:700}
      .home-stats{display:flex;gap:35px;margin-top:25px;color:#315a88}

      /* Each intelligence card is ONE container: content + both buttons. */
      .st-key-inventory_card, .st-key-sales_card{
          background:#fff !important;
          border:1px solid #d5eaff !important;
          border-radius:22px !important;
          padding:28px !important;
          box-shadow:0 15px 40px rgba(23,91,160,.07) !important;
          box-sizing:border-box !important;
          height:100% !important;
      }
      .st-key-inventory_card > div, .st-key-sales_card > div{gap:0 !important;}
      .module-card-container{box-sizing:border-box !important;}
      .module-card-container h2{color:#082d69;margin-top:0;margin-bottom:12px}
      .module-card-container p{color:#587291;line-height:1.55}
      .module-card-container li{margin:9px 0;color:#183e70}
      .module-card-actions{margin-top:24px}
      .module-card-actions [data-testid="stButton"] > button{
          border-radius:12px !important;
          min-height:38px !important;
          height:38px !important;
          font-size:.82rem !important;
          font-weight:700 !important;
          color:#fff !important;
          box-shadow:0 4px 10px rgba(8,120,200,.14) !important;
      }
      /* Explore buttons = blue */
      .st-key-home_inv_explore [data-testid="stButton"] > button,
      .st-key-home_sales_explore [data-testid="stButton"] > button{
          border:1px solid #0878c8 !important;
          background:#0878c8 !important;
      }
      .st-key-home_inv_explore [data-testid="stButton"] > button:hover,
      .st-key-home_sales_explore [data-testid="stButton"] > button:hover{
          background:#066aae !important;
          border-color:#066aae !important;
      }
      /* Chat with AI buttons = red */
      .st-key-home_inv_chat [data-testid="stButton"] > button,
      .st-key-home_sales_chat [data-testid="stButton"] > button{
          border:1px solid #e51f2b !important;
          background:#e51f2b !important;
          box-shadow:0 4px 10px rgba(229,31,43,.14) !important;
      }
      .st-key-home_inv_chat [data-testid="stButton"] > button:hover,
      .st-key-home_sales_chat [data-testid="stButton"] > button:hover{
          background:#c91823 !important;
          border-color:#c91823 !important;
      }
      .home-footer{border-top:1px solid #dcecff;margin-top:35px;padding:20px 0;color:#5a7392;text-align:center}
      @keyframes float{50%{transform:translateY(-12px)}}
      @keyframes pulse{50%{transform:scale(1.03)}}
      @media(max-width:850px){
          .home-hero,.module-grid{grid-template-columns:1fr}
          .home-title{font-size:2.8rem}
          .home-robot{height:360px}
          .orb{width:330px;height:330px}
          .home-robot-img{width:310px}
          .bubble{right:0}
      }
    </style>
    <div class="home-hero">
      <div>
        <div class="home-eyebrow">WELCOME TO DILYTICS</div>
        <div class="home-title">Your AI-Powered<br><span>Data Companion</span></div>
        <div class="home-sub">Ask questions, explore insights, and make smarter decisions with the power of your data.</div>
        <div class="home-stats"><span>▮ Insights Made Simple</span><span>⚡ Faster Decisions</span><span>✓ Secure & Compliant</span></div>
      </div>
      <div class="home-robot">
        <div class="orb"><img class="home-robot-img" src="{robot_src}" alt="Dilytics AI assistant" /></div>
        <div class="bubble"><b>Hi!</b><br>How can I help you<br>today?</div>
      </div>
    </div>
    """.replace("{robot_src}", _robot_data_uri()),unsafe_allow_html=True)

    # Each card is a real Streamlit container. The content and both
    # action buttons are rendered inside the same container.
    c1,c2=st.columns(2, gap="medium")

    with c1:
        with st.container(key="inventory_card"):
            st.markdown("""
            <div class="module-card-container">
              <h2>▦ &nbsp; Inventory Intelligence</h2>
              <p>Get real-time insights into stock levels, warehouse capacity and product performance.</p>
              <ul>
                <li>Track inventory levels and availability</li>
                <li>Analyze stock value by warehouse and category</li>
                <li>Identify excess and out-of-stock items</li>
                <li>Find products that need to be reordered</li>
              </ul>
            </div>
            """, unsafe_allow_html=True)
            st.markdown('<div class="module-card-actions">', unsafe_allow_html=True)
            a,b=st.columns(2, gap="small")
            with a:
                if st.button("⌁ Explore Inventory",use_container_width=True,key="home_inv_explore"):
                    _set_page("inventory")
            with b:
                if st.button("◯ Chat with AI",use_container_width=True,key="home_inv_chat"):
                    _open_chat()
            st.markdown('</div>', unsafe_allow_html=True)

    with c2:
        with st.container(key="sales_card"):
            st.markdown("""
            <div class="module-card-container">
              <h2>▥ &nbsp; Sales Intelligence</h2>
              <p>Uncover sales trends, customer insights and revenue opportunities across products, regions and channels.</p>
              <ul>
                <li>Analyze total sales and revenue</li>
                <li>Identify top products and customer segments</li>
                <li>Track sales by region and channel</li>
                <li>Monitor monthly and quarterly trends</li>
              </ul>
            </div>
            """, unsafe_allow_html=True)
            st.markdown('<div class="module-card-actions">', unsafe_allow_html=True)
            a,b=st.columns(2, gap="small")
            with a:
                if st.button("⌁ Explore Sales",use_container_width=True,key="home_sales_explore"):
                    _set_page("sales")
            with b:
                if st.button("◯ Chat with AI",use_container_width=True,key="home_sales_chat"):
                    _open_chat()
            st.markdown('</div>', unsafe_allow_html=True)

    st.markdown('<div class="home-footer">© 2026 DiLytics. All rights reserved. &nbsp; | &nbsp; Powered by Snowflake &nbsp; | &nbsp; Secure & Compliant &nbsp; | &nbsp; Insights Made Simple</div>',unsafe_allow_html=True)


# Initialize route state and render non-chat pages.
if "app_page" not in st.session_state:
    st.session_state.app_page = "home"

if st.session_state.app_page != "chatbot":
    _top_nav()
    page=st.session_state.app_page
    if page=="home": _home_page()
    elif page=="inventory": _module_page("inventory")
    elif page=="sales": _module_page("sales")
    elif page=="document_ai": _document_ai_page()
    elif page=="about": _about_page()
    st.stop()

# Chatbot page keeps the existing working Cortex Analyst/document code below.

# ===================================================================
# 3. CHAT SESSION STATE
# ===================================================================
if "chat_sessions" not in st.session_state:
    st.session_state.chat_sessions = {}

if "current_session_id" not in st.session_state:
    init_id = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    st.session_state.current_session_id = init_id
    st.session_state.chat_sessions[init_id] = {
        "title": "New Conversation",
        "messages": [],
    }

current_id = st.session_state.current_session_id
messages = st.session_state.chat_sessions[current_id]["messages"]


# ===================================================================
# 4. CHART DISPLAY
# ===================================================================
def display_chart_tab(df: pd.DataFrame, key_prefix: str = ""):
    if df is None or df.empty:
        st.info("No data available for charting.")
        return

    if len(df.columns) < 2:
        st.info("Need at least 2 columns to render a chart.")
        return

    all_cols = list(df.columns)
    col1, col2, col3 = st.columns(3)

    x_col = col1.selectbox(
        "Dimension (X-axis)", all_cols, index=0,
        key=f"{key_prefix}_x"
    )

    remaining_cols = [c for c in all_cols if c != x_col]
    if not remaining_cols:
        return

    y_col = col2.selectbox(
        "Metric (Y-axis)", remaining_cols, index=0,
        key=f"{key_prefix}_y"
    )

    chart_type = col3.selectbox(
        "Chart Type",
        ["Bar Chart", "Line Chart", "Area Chart", "Scatter Plot"],
        key=f"{key_prefix}_type",
    )

    chart_df = df.copy()

    if any(k in x_col.lower()
           for k in ["year", "quarter", "month", "day", "date"]):
        chart_df[x_col] = chart_df[x_col].apply(
            lambda x: (
                str(int(x))
                if pd.notnull(x) and isinstance(x, (int, float))
                else str(x)
            )
        )

    try:
        if chart_type == "Bar Chart":
            st.bar_chart(chart_df.set_index(x_col)[y_col])
        elif chart_type == "Line Chart":
            st.line_chart(chart_df.set_index(x_col)[y_col])
        elif chart_type == "Area Chart":
            st.area_chart(chart_df.set_index(x_col)[y_col])
        else:
            st.scatter_chart(chart_df, x=x_col, y=y_col)
    except Exception as e:
        st.info(f"Chart could not be rendered: {e}")


# ===================================================================
# 5. SIDEBAR
# ===================================================================
with st.sidebar:
    st.markdown("### ⚡ Dilytics AI")
    st.markdown(
        '<span class="status-pill">● Cortex Analyst Live</span>',
        unsafe_allow_html=True,
    )
    st.write("")

    if st.button("➕ New Chat", use_container_width=True, type="primary"):
        new_id = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        st.session_state.current_session_id = new_id
        st.session_state.chat_sessions[new_id] = {
            "title": f"Chat {len(st.session_state.chat_sessions) + 1}",
            "messages": [],
        }
        st.rerun()

    st.markdown("---")
    st.markdown("##### 🕒 Recent Conversations")

    for s_id, s_data in reversed(list(st.session_state.chat_sessions.items())):
        is_active = s_id == st.session_state.current_session_id
        label = s_data["title"]
        if len(label) > 20:
            label = label[:18] + "..."

        if st.button(
            f"{'👉 ' if is_active else '🗨️ '}{label}",
            key=f"sess_{s_id}",
            use_container_width=True,
        ):
            st.session_state.current_session_id = s_id
            st.rerun()

    st.markdown("---")

    if st.button("🗑️ Clear All Sessions", use_container_width=True):
        st.session_state.chat_sessions = {}
        init_id = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        st.session_state.current_session_id = init_id
        st.session_state.chat_sessions[init_id] = {
            "title": "New Conversation",
            "messages": [],
        }
        st.rerun()


# ===================================================================
    st.markdown("---")
    st.markdown("##### 📄 Analyze an Uploaded Document")

    uploaded_doc = st.file_uploader(
        "Upload CSV, Excel, PDF or Word",
        type=["csv", "xlsx", "xls", "pdf", "docx"],
        key="document_uploader",
        help="Upload a document, click Analyze, then choose Uploaded Document in the chat.",
    )

    if st.button(
        "🔍 Analyze Document",
        use_container_width=True,
        disabled=uploaded_doc is None,
        key="analyze_uploaded_document",
    ):
        try:
            with st.spinner("Reading and analyzing document..."):
                doc_type, doc_df, doc_text, doc_message = process_uploaded_document(
                    uploaded_doc
                )

                _drop_uploaded_table()
                st.session_state.uploaded_document_name = uploaded_doc.name
                st.session_state.uploaded_document_type = doc_type
                st.session_state.uploaded_document_df = doc_df
                st.session_state.uploaded_document_text = doc_text
                st.session_state.uploaded_document = uploaded_doc.name
                st.session_state.uploaded_document_table = None
                st.session_state.uploaded_document_semantic_model = None

                if doc_type == "table":
                    prepare_uploaded_table(doc_df)
                elif doc_type == "text":
                    # Word (.docx) uses the trial-safe local document Q&A path below.
                    # Keep PDF on the existing AI_COMPLETE path. The working
                    # Excel/CSV Cortex Analyst functionality is untouched.
                    if uploaded_doc.name.lower().endswith(".pdf"):
                        _upload_document_to_stage(uploaded_doc)

            # Keep document analysis inside the current conversation timeline.
            # The upload is an event in the chat, so it appears exactly where
            # it happened instead of being rendered above the old messages.
            # The chat session is initialized here as well because the upload
            # controls are rendered before the main chat-session block below.
            if "chat_sessions" not in st.session_state:
                st.session_state.chat_sessions = {}
            if "current_session_id" not in st.session_state:
                init_id = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
                st.session_state.current_session_id = init_id
                st.session_state.chat_sessions[init_id] = {
                    "title": "New Conversation",
                    "messages": [],
                }
            current_id = st.session_state.current_session_id
            messages_for_event = st.session_state.chat_sessions[current_id]["messages"]
            messages_for_event.append({
                "role": "assistant",
                "content": f"📄 **Document analyzed:** `{uploaded_doc.name}`\n\n{doc_message}",
                "sql": None,
                "data": None,
                "semantic_model": "Uploaded Document",
                "verified_query": None,
                "document_event": True,
                "document_name": uploaded_doc.name,
                "document_type": doc_type,
            })

            st.success(doc_message)
            st.rerun()
        except Exception as e:
            st.error(f"Document analysis failed: {e}")

    if st.session_state.uploaded_document_name:
        st.caption(
            f"Loaded: `{st.session_state.uploaded_document_name}`"
        )
        if st.button(
            "✖ Remove Uploaded Document",
            use_container_width=True,
            key="remove_uploaded_document",
        ):
            _drop_uploaded_table()
            st.session_state.uploaded_document = None
            st.session_state.uploaded_document_name = None
            st.session_state.uploaded_document_df = None
            st.session_state.uploaded_document_text = ""
            st.session_state.uploaded_document_type = None
            st.session_state.uploaded_document_table = None
            st.session_state.uploaded_document_semantic_model = None
            st.session_state.uploaded_document_stage = None
            st.session_state.uploaded_document_stage_file = None
            st.rerun()
# 6. MAIN HEADER
# ===================================================================
# One continuous header container: brand/tagline + navigation all live
# inside the same background box and on the same horizontal line.
st.markdown("""
<style>
/* ================================================================
   Dilytics Chat Header
   One continuous box containing brand + navigation.
   ================================================================ */
.st-key-dly_chat_header {
    width: 100% !important;
    max-width: none !important;
    box-sizing: border-box !important;
    background: #dff1ff !important;
    border: 1px solid #b9dcf7 !important;
    border-radius: 14px !important;
    padding: 10px 12px !important;
    margin: 0 0 18px 0 !important;
    box-shadow: 0 6px 20px rgba(35,111,177,.10) !important;
}

/* Streamlit may place the keyed block inside a wrapper; force the
   highlighted header background to occupy the complete row. */
.st-key-dly_chat_header,
.st-key-dly_chat_header > div,
.st-key-dly_chat_header > div > div {
    box-sizing: border-box !important;
}

.st-key-dly_chat_header [data-testid="column"] {
    display: flex;
    align-items: center;
}

.dly-chat-brandline {
    display: flex;
    align-items: center;
    gap: 10px;
    min-height: 34px;
    white-space: nowrap;
}

.dly-chat-brand {
    font-size: 1.02rem;
    font-weight: 900;
    color: #e51f2b;
    line-height: 1;
    white-space: nowrap;
}

.dly-chat-brand span {
    color: #0a3b78;
}

.dly-chat-tagline {
    color: #52718f;
    font-size: .68rem;
    line-height: 1;
    white-space: nowrap;
    padding-left: 1px;
}

/* Blue navigation button boxes */
.st-key-dly_chat_header [data-testid="stButton"] > button {
    min-height: 32px !important;
    height: 32px !important;
    padding: 0 11px !important;
    border-radius: 8px !important;
    border: 1px solid #0878c8 !important;
    background: #0878c8 !important;
    color: #ffffff !important;
    font-size: .68rem !important;
    font-weight: 700 !important;
    line-height: 1 !important;
    box-shadow: 0 2px 5px rgba(8,120,200,.18) !important;
    white-space: nowrap !important;
}

.st-key-dly_chat_header [data-testid="stButton"] > button:hover {
    background: #075fa3 !important;
    border-color: #075fa3 !important;
    color: #ffffff !important;
}

@media (max-width: 900px) {
    .dly-chat-brandline {
        gap: 6px;
    }
    .dly-chat-brand {
        font-size: .9rem;
    }
    .dly-chat-tagline {
        font-size: .58rem;
    }
    .st-key-dly_chat_header [data-testid="stButton"] > button {
        font-size: .60rem !important;
        padding: 0 6px !important;
    }
}
</style>
""", unsafe_allow_html=True)

with st.container(key="dly_chat_header"):
    # Keep the brand on the left and all navigation inside the SAME highlighted header box.
    h1, h2, h3, h4 = st.columns([2.25, .75, 1.05, .75], gap="small", vertical_alignment="center")
    with h1:
        st.markdown(
            '<div class="dly-chat-brandline">'
            '<div class="dly-chat-brand">DILYTICS <span>Enterprise AI</span></div>'
            '<div class="dly-chat-tagline">Data. Insights. Impact.</div>'
            '</div>',
            unsafe_allow_html=True,
        )
    with h2:
        if st.button("⌂  Home", use_container_width=True, key="chat_home"):
            _set_page("home")
    with h3:
        if st.button("▣  Document AI", use_container_width=True, key="chat_docs"):
            _set_page("document_ai")
    with h4:
        if st.button("↪  Logout", use_container_width=True, key="chat_logout"):
            _logout()

# 7. EXAMPLE QUESTIONS
# These buttons are only examples. They do NOT contain SQL.
# ===================================================================
quick_prompt = None

st.markdown("### Explore your data")
st.caption("Choose a question below or type your own question in the chat.")

tab_inv, tab_sales, tab_supply = st.tabs(
    ["📦 Inventory Intelligence", "💰 Sales Intelligence", "🚚 Supply Chain Intelligence"]
)

with tab_inv:
    with st.expander("💡 What can I ask about Inventory?", expanded=False):
        if st.button(
            "💰 What is the total inventory value?",
            use_container_width=True,
            key="i1",
        ):
            quick_prompt = "What is the total inventory value?"

        if st.button(
            "🏭 What is the inventory value by warehouse?",
            use_container_width=True,
            key="i2",
        ):
            quick_prompt = "What is the inventory value by warehouse?"

        if st.button(
            "📦 Which products have the highest inventory value?",
            use_container_width=True,
            key="i3",
        ):
            quick_prompt = "Which products have the highest inventory value?"

        if st.button(
            "📉 How many products are out of stock?",
            use_container_width=True,
            key="i4",
        ):
            quick_prompt = "How many products are out of stock?"

        if st.button(
            "⚠️ What is the total excess inventory value by warehouse?",
            use_container_width=True,
            key="i5",
        ):
            quick_prompt = "What is the total excess inventory value by warehouse?"

        if st.button(
            "🔄 Which products need to be reordered?",
            use_container_width=True,
            key="i6",
        ):
            quick_prompt = "Which products need to be reordered?"

        if st.button(
            "🏷️ What is the inventory value by product category?",
            use_container_width=True,
            key="i7",
        ):
            quick_prompt = "What is the inventory value by product category?"

with tab_sales:
    with st.expander("💡 What can I ask about Sales?", expanded=False):
        if st.button(
            "💵 What is the total sales amount?",
            use_container_width=True,
            key="s1",
        ):
            quick_prompt = "What is the total sales amount?"

        if st.button(
            "🏆 What are the top products by sales?",
            use_container_width=True,
            key="s2",
        ):
            quick_prompt = "What are the top products by sales?"

        if st.button(
            "🌍 What are total sales by customer region?",
            use_container_width=True,
            key="s3",
        ):
            quick_prompt = "What are total sales by customer region?"

        if st.button(
            "📅 What are total sales by month?",
            use_container_width=True,
            key="s4",
        ):
            quick_prompt = "What are total sales by month?"

        if st.button(
            "📊 What is total sales by order channel?",
            use_container_width=True,
            key="s5",
        ):
            quick_prompt = "What is total sales by order channel?"

        if st.button(
            "💳 What is the average order value?",
            use_container_width=True,
            key="s6",
        ):
            quick_prompt = "What is the average order value?"

        if st.button(
            "🎟️ What is the total discount?",
            use_container_width=True,
            key="s7",
        ):
            quick_prompt = "What is the total discount?"

st.markdown("---")


# ===================================================================
# 8. DISPLAY CHAT HISTORY
# ===================================================================
# IMPORTANT: Uploaded-document events are rendered as normal chat events.
# This preserves chronological order: Question 1 -> ... -> Question 10 ->
# Document uploaded -> Question 11 -> Answer 11.
for idx, msg in enumerate(messages):
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

        if msg.get("document_event"):
            doc_type = msg.get("document_type")
            doc_name = msg.get("document_name")

            if doc_type == "table":
                current_df = st.session_state.uploaded_document_df
                # Show the preview only for the currently loaded document.
                if current_df is not None and doc_name == st.session_state.uploaded_document_name:
                    with st.expander("📊 View uploaded data", expanded=False):
                        st.dataframe(
                            _normalize_uploaded_dataframe(current_df),
                            use_container_width=True,
                        )
            elif doc_type == "text":
                current_text = st.session_state.uploaded_document_text
                if current_text and doc_name == st.session_state.uploaded_document_name:
                    with st.expander("📖 View extracted document content", expanded=False):
                        st.text_area(
                            "Document text",
                            current_text,
                            height=300,
                            disabled=True,
                            label_visibility="collapsed",
                            key=f"doc_preview_{current_id}_{idx}",
                        )

        if msg.get("sql"):
            with st.expander("Generated SQL", expanded=False):
                st.code(msg["sql"], language="sql")

        if msg.get("semantic_model"):
            st.caption(
                f"Semantic model selected: `{msg['semantic_model']}`"
            )

        if msg.get("verified_query"):
            name = msg["verified_query"].get("name")
            if name:
                st.caption(f"Verified Query Used: `{name}`")

        if msg.get("data") is not None:
            tab_data, tab_chart = st.tabs(["Data 📄", "Chart 📈"])
            with tab_data:
                st.dataframe(msg["data"], use_container_width=True)
            with tab_chart:
                display_chart_tab(
                    msg["data"],
                    key_prefix=f"hist_{current_id}_{idx}",
                )


# ===================================================================
# ===================================================================
# 8A. ANSWER SOURCE (ADDED)
# ===================================================================
if st.session_state.uploaded_document_name:
    answer_source = st.radio(
        "Answer from:",
        ["Snowflake Data", "Uploaded Document"],
        horizontal=True,
        key="answer_source",
        help="Choose whether your question should use the existing Inventory/Sales semantic models or the uploaded document.",
    )
else:
    answer_source = "Snowflake Data"
# 9. CHAT INPUT
# ===================================================================
user_prompt = (
    st.chat_input(
        "Ask me anything about inventory, sales, supply chain, customers, or uploaded documents..."
    )
    or quick_prompt
    or st.session_state.get("landing_prompt")
)

# A question entered on the landing page is consumed once after navigation.
if st.session_state.get("landing_prompt"):
    st.session_state.landing_prompt = None


# ===================================================================
# 10. CORTEX ANALYST EXECUTION
# ===================================================================
if user_prompt:
    # ===================================================================
    # UPLOADED DOCUMENT QUESTION PATH (ADDED)
    # This branch is intentionally placed before the original Cortex
    # Analyst block. The original Inventory/Sales path below is unchanged.
    # ===================================================================
    if answer_source == "Uploaded Document":
        if len(messages) == 0:
            st.session_state.chat_sessions[current_id]["title"] = (
                user_prompt[:25] + ("..." if len(user_prompt) > 25 else "")
            )

        messages.append({"role": "user", "content": user_prompt})
        with st.chat_message("user"):
            st.markdown(user_prompt)

        with st.chat_message("assistant"):
            doc_df_result = None
            doc_sql_result = None
            doc_answer = ""

            try:
                with st.spinner("Analyzing your uploaded document..."):
                    if st.session_state.uploaded_document_type == "table":
                        doc_df_result, doc_sql_result, doc_analyst_result = answer_uploaded_table_question(
                            user_prompt,
                            st.session_state.uploaded_document_df,
                        )
                        doc_answer = (
                            "I answered your question using the complete uploaded "
                            "dataset through Cortex Analyst. The SQL below was "
                            "generated dynamically from the uploaded document schema."
                        )
                        if doc_analyst_result.get("semantic_model_selection"):
                            st.caption(
                                "Semantic model selected: "
                                + str(doc_analyst_result["semantic_model_selection"])
                            )
                    else:
                        doc_answer = answer_uploaded_text_question(
                            user_prompt,
                            st.session_state.uploaded_document_text,
                        )

                st.markdown(doc_answer)

                if doc_sql_result:
                    with st.expander("Generated SQL for Uploaded Document", expanded=False):
                        st.code(doc_sql_result, language="sql")

                if doc_df_result is not None:
                    tab_data, tab_chart = st.tabs(["Data 📄", "Chart 📈"])
                    with tab_data:
                        st.dataframe(doc_df_result, use_container_width=True)
                    with tab_chart:
                        display_chart_tab(
                            doc_df_result,
                            key_prefix=f"document_{current_id}_{len(messages)}",
                        )

                messages.append({
                    "role": "assistant",
                    "content": doc_answer,
                    "sql": doc_sql_result,
                    "data": doc_df_result,
                    "semantic_model": "Uploaded Document",
                    "verified_query": None,
                })

            except Exception as e:
                doc_answer = f"Unable to analyze the uploaded document: {e}"
                st.error(doc_answer)
                messages.append({
                    "role": "assistant",
                    "content": doc_answer,
                    "sql": None,
                    "data": None,
                    "semantic_model": "Uploaded Document",
                    "verified_query": None,
                })

        st.rerun()
    if len(messages) == 0:
        st.session_state.chat_sessions[current_id]["title"] = (
            user_prompt[:25] + ("..." if len(user_prompt) > 25 else "")
        )

    messages.append({"role": "user", "content": user_prompt})

    with st.chat_message("user"):
        st.markdown(user_prompt)

    with st.chat_message("assistant"):
        df = None
        sql_query = None
        explanation = ""
        semantic_model = None
        verified_query = None

        try:
            with st.spinner("Cortex Analyst is interpreting your question..."):
                analyst_json = call_cortex_analyst(user_prompt)
                result = extract_analyst_response(analyst_json)

            explanation = result["text"]
            sql_query = result["sql"]
            semantic_model = result["semantic_model_selection"]
            verified_query = result["verified_query_used"]

            for warning in result["warnings"]:
                warning_text = (
                    warning.get("message", str(warning))
                    if isinstance(warning, dict)
                    else str(warning)
                )
                st.warning(warning_text)

            if not sql_query:
                if not explanation:
                    explanation = (
                        "Cortex Analyst could not generate SQL for this "
                        "question from the configured semantic models."
                    )
                st.markdown(explanation)

            else:
                if not explanation:
                    explanation = (
                        "I generated this answer using the Snowflake "
                        "semantic model."
                    )

                st.markdown(explanation)

                if semantic_model:
                    st.caption(
                        f"Semantic model selected: `{semantic_model}`"
                    )

                if verified_query:
                    name = verified_query.get("name")
                    if name:
                        st.caption(f"Verified Query Used: `{name}`")

                with st.expander("Generated SQL", expanded=False):
                    st.code(sql_query, language="sql")

                with st.spinner("Executing generated SQL in Snowflake..."):
                    df = session.sql(sql_query).to_pandas()

                tab_data, tab_chart = st.tabs(["Data 📄", "Chart 📈"])

                with tab_data:
                    st.dataframe(df, use_container_width=True)

                with tab_chart:
                    display_chart_tab(
                        df,
                        key_prefix=f"live_{current_id}_{len(messages)}",
                    )

        except requests.exceptions.Timeout:
            explanation = (
                "Cortex Analyst took too long to respond. Please try again."
            )
            st.error(explanation)

        except requests.exceptions.RequestException as e:
            explanation = f"Could not connect to Cortex Analyst: {e}"
            st.error(explanation)

        except Exception as e:
            explanation = f"Unable to process the question: {e}"
            st.error(explanation)

        messages.append({
            "role": "assistant",
            "content": explanation,
            "sql": sql_query,
            "data": df,
            "semantic_model": semantic_model,
            "verified_query": verified_query,
        })

    st.rerun()
