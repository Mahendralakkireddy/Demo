import io
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

ANALYST_ENDPOINT = f"https://{HOST}/api/v2/cortex/analyst/message"

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
    --dly-navy: #10182d;
    --dly-navy-2: #17213b;
    --dly-blue: #1598e5;
    --dly-cyan: #36c9ff;
    --dly-text: #eef5ff;
    --dly-muted: #9eabc2;
}

.stApp {
    background:
        radial-gradient(circle at 82% 7%, rgba(21,152,229,.16), transparent 27%),
        radial-gradient(circle at 18% 100%, rgba(54,201,255,.08), transparent 30%),
        #0b1222;
}

.main .block-container {
    max-width: 1180px;
    padding-top: 1.25rem;
    padding-bottom: 4rem;
}

/* Header / brand */
.dly-topbar {
    display:flex; align-items:center; justify-content:space-between;
    padding: 12px 18px;
    margin-bottom: 24px;
    border: 1px solid rgba(255,255,255,.08);
    border-radius: 16px;
    background: rgba(15,24,45,.78);
    backdrop-filter: blur(14px);
    box-shadow: 0 10px 35px rgba(0,0,0,.20);
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
    display:flex; gap:22px; color:#b9c4d8; font-size:.78rem;
}
.dly-nav span:first-child { color:#fff; }
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
    color:#55c8ff; font-size:.78rem; font-weight:800;
    text-transform:uppercase; letter-spacing:1.5px;
}
.dly-hero h1 {
    margin: 7px 0 8px; color:#fff;
    font-size: clamp(2rem, 4vw, 3.35rem);
    line-height:1.06; letter-spacing:-1.8px;
}
.dly-hero h1 span { color:#159fe8; }
.dly-hero p {
    max-width:650px; color:#aebbd0; font-size:.92rem; line-height:1.65;
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
    background:rgba(19,31,56,.78);
    border:1px solid rgba(120,150,190,.20);
    color:#eaf4ff;
}
.main div[data-testid="stButton"] > button:hover {
    border-color:rgba(54,201,255,.55);
    color:#fff; transform:translateY(-1px);
    box-shadow:0 7px 22px rgba(0,0,0,.20);
}

/* Intelligence tabs */
.stTabs [data-baseweb="tab-list"] {
    gap:8px; background:transparent;
    border-bottom:1px solid rgba(255,255,255,.08);
}
.stTabs [data-baseweb="tab"] {
    color:#9eabc2; border-radius:10px 10px 0 0; padding:10px 18px;
}
.stTabs [aria-selected="true"] {
    color:#fff !important;
    background:rgba(21,152,229,.12);
}

/* Expander / cards */
[data-testid="stExpander"] {
    background:rgba(17,28,50,.72);
    border:1px solid rgba(120,150,190,.16);
    border-radius:14px;
}

/* Chat bubbles */
[data-testid="stChatMessage"] {
    border:1px solid rgba(120,150,190,.13);
    border-radius:15px;
    background:rgba(17,28,50,.48);
    margin-bottom:10px;
}
[data-testid="stChatMessage"] p { line-height:1.6; }

/* Chat input */
[data-testid="stChatInput"] {
    background:rgba(15,24,45,.92);
}
[data-testid="stChatInput"] > div {
    border:1px solid rgba(54,201,255,.24) !important;
    border-radius:15px !important;
    background:#111c32 !important;
}

/* Sidebar */
section[data-testid="stSidebar"] {
    background:linear-gradient(180deg,#0d1528 0%,#0a1120 100%);
    border-right:1px solid rgba(255,255,255,.06);
}
section[data-testid="stSidebar"] .stMarkdown { color:#d7e2f2; }

/* File uploader */
[data-testid="stFileUploader"] {
    background:rgba(17,28,50,.65);
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
   Dilytics Landing Page — clean blue/green chatbot interface
   VISUAL ONLY: backend/chat/document logic is unchanged.
   ================================================================ */

.dly-landing-wrap {
    min-height: 78vh;
    margin: -1.2rem -2rem 0;
    padding: 1.7rem 5vw 2.8rem;
    position: relative;
    overflow: hidden;
    border-radius: 0 0 28px 28px;
    background:
        radial-gradient(circle at 82% 22%, rgba(31, 220, 178, .22), transparent 25%),
        radial-gradient(circle at 66% 12%, rgba(30, 168, 255, .25), transparent 30%),
        linear-gradient(118deg, #061b3d 0%, #073b72 43%, #087fa0 72%, #16ad78 100%);
    box-shadow: inset 0 0 90px rgba(0,0,0,.12);
}

.dly-landing-wrap::before,
.dly-landing-wrap::after {
    content: "";
    position: absolute;
    pointer-events: none;
    border: 1px solid rgba(106, 225, 255, .16);
    border-radius: 50%;
}

.dly-landing-wrap::before {
    width: 820px;
    height: 820px;
    right: -360px;
    top: -510px;
}

.dly-landing-wrap::after {
    width: 720px;
    height: 720px;
    left: -430px;
    bottom: -560px;
}

.dly-landing-top {
    display: flex;
    align-items: center;
    justify-content: space-between;
    position: relative;
    z-index: 5;
    margin-bottom: 4.5rem;
}

.dly-landing-logo {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    background: #ef2635;
    color: #fff;
    padding: 11px 20px;
    font-size: 1.22rem;
    font-weight: 900;
    letter-spacing: .6px;
    border-radius: 2px;
    box-shadow: 0 8px 25px rgba(0,0,0,.18);
}

.dly-landing-powered {
    color: #d9f7ff;
    font-size: .72rem;
    font-weight: 750;
    letter-spacing: 1.2px;
    text-transform: uppercase;
}

.dly-landing-grid {
    display: grid;
    grid-template-columns: 1.03fr .97fr;
    gap: 2.5rem;
    align-items: center;
    position: relative;
    z-index: 4;
}

.dly-landing-copy {
    padding-left: 1px;
}

.dly-eyebrow {
    color: #45d5ff;
    font-size: .75rem;
    font-weight: 800;
    text-transform: uppercase;
    letter-spacing: 1.8px;
    margin-bottom: 1.25rem;
}

.dly-landing-copy h1 {
    margin: 0;
    color: #fff;
    font-size: clamp(3.4rem, 5.8vw, 5.7rem);
    line-height: .96;
    letter-spacing: -3.5px;
    font-weight: 850;
}

.dly-landing-copy h1 span {
    background: linear-gradient(90deg, #17c7ff 0%, #18d9c6 55%, #24df91 100%);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
}

.dly-landing-copy p {
    color: #d3e8fb;
    font-size: 1.02rem;
    line-height: 1.7;
    max-width: 600px;
    margin: 1.8rem 0 0;
}

.dly-landing-visual {
    min-height: 500px;
    position: relative;
    display: flex;
    align-items: center;
    justify-content: center;
}

/* Soft circular blue/green visual behind the robot */
.dly-robot-orb {
    position: absolute;
    width: 430px;
    height: 430px;
    border-radius: 50%;
    background:
        radial-gradient(circle at 32% 25%, #2bd5ff 0%, #159edc 36%, #0872bd 65%, #087b83 100%);
    box-shadow:
        0 0 85px rgba(20, 206, 255, .24),
        inset -30px -30px 70px rgba(0, 60, 130, .18);
}

/* CSS robot — no external image dependency */
.dly-robot {
    position: relative;
    width: 300px;
    height: 365px;
    z-index: 5;
    margin-top: 50px;
}

.dly-antenna {
    position: absolute;
    width: 10px;
    height: 42px;
    left: 145px;
    top: 0;
    border-radius: 10px;
    background: #14233b;
}

.dly-antenna::before {
    content: "";
    position: absolute;
    width: 29px;
    height: 29px;
    left: -9px;
    top: -20px;
    border-radius: 50%;
    background: radial-gradient(circle at 35% 30%, #fff, #20d8ff 38%, #0878d8 78%);
    box-shadow: 0 0 24px rgba(25, 218, 255, .9);
}

.dly-robot-head {
    position: absolute;
    left: 15px;
    top: 38px;
    width: 270px;
    height: 178px;
    border-radius: 82px;
    background: linear-gradient(145deg, #fff, #d8e6f1);
    border: 5px solid rgba(255,255,255,.82);
    box-shadow: 0 25px 55px rgba(0,0,0,.24);
}

.dly-robot-face {
    position: absolute;
    left: 28px;
    top: 23px;
    width: 205px;
    height: 124px;
    border-radius: 60px;
    background: linear-gradient(145deg, #061329, #111f3a);
    display: flex;
    justify-content: center;
    align-items: center;
    gap: 38px;
    box-shadow: inset 0 0 25px rgba(0,205,255,.10);
}

.dly-eye {
    width: 31px;
    height: 21px;
    border-top: 7px solid #18e2ff;
    border-radius: 50%;
    filter: drop-shadow(0 0 7px #16dfff);
}

.dly-smile {
    position: absolute;
    bottom: 25px;
    left: 87px;
    width: 30px;
    height: 15px;
    border-bottom: 5px solid #19e2ff;
    border-radius: 0 0 24px 24px;
    filter: drop-shadow(0 0 7px #16dfff);
}

.dly-robot-body {
    position: absolute;
    left: 62px;
    top: 225px;
    width: 176px;
    height: 130px;
    border-radius: 52px 52px 30px 30px;
    background: linear-gradient(145deg, #fff, #c8d9e8);
    box-shadow: 0 20px 40px rgba(0,0,0,.22);
}

.dly-body-logo {
    position: absolute;
    top: 42px;
    left: 39px;
    background: #ef2635;
    color: #fff;
    padding: 6px 12px;
    border-radius: 3px;
    font-size: 12px;
    font-weight: 900;
}

.dly-arm {
    position: absolute;
    width: 51px;
    height: 112px;
    top: 6px;
    border-radius: 35px;
    background: linear-gradient(145deg, #fff, #c8d9e8);
}

.dly-arm-left {
    left: -38px;
    transform: rotate(38deg);
}

.dly-arm-right {
    right: -38px;
    transform: rotate(-14deg);
}

.dly-hand {
    position: absolute;
    width: 48px;
    height: 48px;
    bottom: -12px;
    left: 1px;
    border-radius: 50%;
    background: #101c32;
}

.dly-desk {
    position: absolute;
    bottom: 15px;
    left: 50%;
    transform: translateX(-50%);
    width: 590px;
    height: 72px;
    border-radius: 9px;
    background: linear-gradient(180deg, #c89463, #855531);
    box-shadow: 0 25px 45px rgba(0,0,0,.27);
    z-index: 2;
}

.dly-laptop {
    position: absolute;
    width: 255px;
    height: 150px;
    left: 50%;
    bottom: 55px;
    transform: translateX(-50%) perspective(500px) rotateX(-5deg);
    border-radius: 10px 10px 4px 4px;
    background: linear-gradient(145deg, #d0d7df, #788594);
    z-index: 6;
}

.dly-laptop::before {
    content: "D";
    position: absolute;
    top: 42px;
    left: 112px;
    color: rgba(30,40,60,.38);
    font-size: 40px;
    font-weight: 750;
}

.dly-plant {
    position: absolute;
    right: 10px;
    bottom: 72px;
    z-index: 7;
    width: 80px;
    height: 105px;
}

.dly-pot {
    position: absolute;
    bottom: 0;
    left: 7px;
    width: 66px;
    height: 58px;
    border-radius: 8px 8px 20px 20px;
    background: linear-gradient(145deg, #f0f2f4, #bac5ce);
}

.dly-leaf {
    position: absolute;
    width: 31px;
    height: 62px;
    bottom: 42px;
    border-radius: 100% 0 100% 0;
    background: linear-gradient(160deg, #2ad891, #087c73);
}

.dly-leaf-1 { left: 6px; transform: rotate(-32deg); }
.dly-leaf-2 { left: 25px; bottom: 58px; transform: rotate(2deg); }
.dly-leaf-3 { left: 45px; transform: rotate(34deg); }

.dly-chat-bubble {
    position: absolute;
    right: 0;
    top: 8%;
    width: 238px;
    padding: 19px 23px;
    border-radius: 21px 21px 5px 21px;
    background: rgba(241,250,255,.97);
    color: #062e62;
    font-size: 1rem;
    line-height: 1.45;
    box-shadow: 0 15px 40px rgba(0,0,0,.18);
    z-index: 9;
}

.dly-chat-bubble strong {
    display: block;
    font-size: 1.22rem;
    margin-bottom: 5px;
}

.dly-chat-bubble::after {
    content: "";
    position: absolute;
    bottom: -16px;
    left: 42px;
    border-left: 18px solid transparent;
    border-right: 18px solid transparent;
    border-top: 22px solid rgba(241,250,255,.97);
}

/* Bottom feature strip */
.dly-landing-stats {
    display: flex;
    gap: 0;
    margin-top: 3.2rem;
    color: #e4f6ff;
}

.dly-stat {
    display: flex;
    align-items: center;
    gap: 11px;
    padding-right: 28px;
    margin-right: 28px;
    border-right: 1px solid rgba(255,255,255,.20);
}

.dly-stat:last-child {
    border-right: none;
}

.dly-stat-icon {
    width: 45px;
    height: 45px;
    display: flex;
    align-items: center;
    justify-content: center;
    border-radius: 50%;
    font-size: 1.2rem;
    background: rgba(0, 174, 255, .16);
}

.dly-stat:nth-child(2) .dly-stat-icon {
    background: rgba(20, 218, 161, .16);
}

.dly-stat:nth-child(3) .dly-stat-icon {
    background: rgba(255, 195, 50, .16);
}

.dly-stat-text {
    font-size: .82rem;
    line-height: 1.2;
}

.dly-stat-text b {
    display: block;
    color: #fff;
    font-size: .94rem;
    margin-bottom: 2px;
}

/* Streamlit controls used by the existing landing-page logic */
.dly-landing-controls {
    position: relative;
    z-index: 20;
    margin-top: -7.2rem;
    margin-left: 5vw;
    width: min(650px, 48%);
}

.dly-landing-controls [data-testid="stTextInput"] input {
    background: rgba(5, 23, 52, .78) !important;
    color: #fff !important;
    border: 1px solid rgba(71, 211, 255, .35) !important;
    border-radius: 15px !important;
}

.dly-landing-controls [data-testid="stButton"] > button {
    border-radius: 28px !important;
    min-height: 48px !important;
    font-weight: 750 !important;
}

.dly-landing-controls [data-testid="stButton"] button[kind="secondary"] {
    background: rgba(5, 30, 64, .45) !important;
    color: #fff !important;
    border: 1px solid rgba(111, 221, 255, .45) !important;
}

.dly-landing-controls [data-testid="stButton"] button[kind="primary"] {
    background: linear-gradient(90deg, #0daaff, #20d7a3) !important;
    color: #fff !important;
    border: 0 !important;
}

@media (max-width: 1000px) {
    .dly-landing-grid {
        grid-template-columns: 1fr;
    }

    .dly-landing-visual {
        min-height: 430px;
    }

    .dly-landing-controls {
        margin: 0 5vw;
        width: auto;
    }
}

@media (max-width: 650px) {
    .dly-landing-powered {
        display: none;
    }

    .dly-landing-copy h1 {
        font-size: 3.2rem;
    }

    .dly-landing-visual {
        transform: scale(.78);
        margin-top: -45px;
        margin-bottom: -45px;
    }

    .dly-landing-stats {
        display: none;
    }

    .dly-landing-controls {
        margin-top: 0;
    }
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

if not st.session_state.authenticated:
    st.title("Welcome to Dilytics Enterprise AI")
    st.markdown("Please login to connect to your Snowflake Data Warehouse.")

    st.session_state.username = st.text_input(
        "Enter Snowflake Username:", value=st.session_state.username
    )
    st.session_state.password = st.text_input(
        "Enter Password:", type="password"
    )

    if st.button("Login"):
        try:
            with st.spinner("Connecting to Snowflake..."):
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
                st.session_state.snowpark_session = (
                    Session.builder.configs({"connection": conn}).create()
                )
                st.session_state.authenticated = True
                st.rerun()
        except Exception as e:
            st.error(f"Authentication failed: {e}")
    st.stop()

session = st.session_state.snowpark_session
conn = st.session_state.snowflake_conn

# ===================================================================
# LANDING PAGE NAVIGATION
# ===================================================================
if "show_landing_page" not in st.session_state:
    st.session_state.show_landing_page = True
if "landing_prompt" not in st.session_state:
    st.session_state.landing_prompt = None

if st.session_state.show_landing_page:
    st.markdown("""
<style>
    section[data-testid="stSidebar"] { display:none; }
    .main .block-container { max-width: 1450px; padding-top: 0; }
</style>
""", unsafe_allow_html=True)

    st.markdown("""
<div class="dly-landing-wrap">

    <div class="dly-landing-top">
        <div class="dly-landing-logo">DILYTICS</div>
        <div class="dly-landing-powered">Powered by Snowflake Cortex AI</div>
    </div>

    <div class="dly-landing-grid">

        <div class="dly-landing-copy">

            <div class="dly-eyebrow">Dilytics Enterprise AI</div>

            <h1>
                Let's find your<br>
                <span>answers</span>
            </h1>

            <p>
                A friendly AI chatbot, always here for you.
                Ask questions in natural language and get intelligent
                insights from your business data.
            </p>

            <div class="dly-landing-stats">

                <div class="dly-stat">
                    <span class="dly-stat-icon">📊</span>
                    <span class="dly-stat-text">
                        <b>Insights</b>
                        made simple
                    </span>
                </div>

                <div class="dly-stat">
                    <span class="dly-stat-icon">⚡</span>
                    <span class="dly-stat-text">
                        <b>Faster</b>
                        decisions
                    </span>
                </div>

                <div class="dly-stat">
                    <span class="dly-stat-icon">🛡</span>
                    <span class="dly-stat-text">
                        <b>Reliable</b>
                        support
                    </span>
                </div>

            </div>
        </div>


        <div class="dly-landing-visual">

            <div class="dly-robot-orb"></div>

            <div class="dly-chat-bubble">
                <strong>Hi! 👋</strong>
                How can I help you today?
            </div>

            <div class="dly-robot">

                <div class="dly-antenna"></div>

                <div class="dly-robot-head">
                    <div class="dly-robot-face">
                        <div class="dly-eye"></div>
                        <div class="dly-eye"></div>
                        <div class="dly-smile"></div>
                    </div>
                </div>

                <div class="dly-robot-body">

                    <div class="dly-arm dly-arm-left">
                        <div class="dly-hand"></div>
                    </div>

                    <div class="dly-arm dly-arm-right">
                        <div class="dly-hand"></div>
                    </div>

                    <div class="dly-body-logo">DILYTICS</div>

                </div>
            </div>

            <div class="dly-laptop"></div>
            <div class="dly-desk"></div>

            <div class="dly-plant">
                <div class="dly-leaf dly-leaf-1"></div>
                <div class="dly-leaf dly-leaf-2"></div>
                <div class="dly-leaf dly-leaf-3"></div>
                <div class="dly-pot"></div>
            </div>

        </div>

    </div>
</div>
""", unsafe_allow_html=True)

    # Existing landing-page controls retained so functionality is unchanged.
    st.markdown('<div class="dly-landing-controls">', unsafe_allow_html=True)

    landing_question = st.text_input(
        "Ask Me Anything",
        placeholder="Ask me anything about your data...",
        label_visibility="collapsed",
        key="landing_question",
    )

    search_cols = st.columns([1, 1, 3])

    with search_cols[0]:
        explore_clicked = st.button(
            "Explore Data  →",
            use_container_width=True,
            key="landing_explore_btn",
        )

    with search_cols[1]:
        ask_clicked = st.button(
            "🤖  Ask Me Anything",
            use_container_width=True,
            key="landing_ask_btn",
        )

    st.markdown('</div>', unsafe_allow_html=True)

    if explore_clicked:
        st.session_state.show_landing_page = False
        st.rerun()

    if ask_clicked and landing_question.strip():
        st.session_state.landing_prompt = landing_question.strip()
        st.session_state.show_landing_page = False
        st.rerun()

    st.stop()



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
head_col1, head_col2 = st.columns([4.5, 1.2])

with head_col1:
    st.markdown(
        """
        <div class="dly-topbar">
            <div class="dly-brand"><span class="dly-logo">DLY</span> Dilytics Enterprise AI</div>
            <div class="dly-nav"><span>Solutions</span><span>Data Intelligence</span><span>Cortex AI</span><span>Analytics</span></div>
            <div class="dly-status">● Cortex Analyst Live</div>
        </div>
        <div class="dly-hero">
            <div class="dly-eyebrow">Powered by Snowflake Cortex AI</div>
            <h1>Chat with your <span>business data</span></h1>
            <p>Ask questions in natural language and get intelligent insights from Inventory, Sales, Supply Chain, and your connected data sources.</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

with head_col2:
    st.write("")
    st.write("")


# ===================================================================
# 7. EXAMPLE QUESTIONS
# These buttons are only examples. They do NOT contain SQL.
# ===================================================================
quick_prompt = None

st.markdown("### Explore your data")
st.caption("Choose a question below or type your own question in the chat.")

tab_inv, tab_sales = st.tabs(
    ["📦 Inventory Intelligence", "💰 Sales Intelligence"]
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
