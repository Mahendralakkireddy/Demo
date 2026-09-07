import io
import re
import uuid
import zipfile
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests
import snowflake.connector
import streamlit as st
from snowflake.snowpark import Session

try:
    import pypdf
except ImportError:
    pypdf = None


# ===================================================================
# Configuration
# ===================================================================
HOST = "WDSDGTL-XCC29288.snowflakecomputing.com"
ACCOUNT = "WDSDGTL-XCC29288"
DATABASE = "INVENTORY_DW_DEMO"
SCHEMA = "GOLD"
WAREHOUSE = "COMPUTE_WH"
ROLE = "ACCOUNTADMIN"

INVENTORY_YAML_STAGE_PATH = (
    '@"INVENTORY_DW_DEMO"."INVENTORY_SCHEMA"."YAML"/'
    'INV_ANALYST_DEMO_90_VERIFIED_FIXED_1.yaml'
)

SALES_YAML_STAGE_PATH = (
    '@"CORTEX_DEMO"."CORTEX_SCHEMA"."YAML"/'
    'sales_intelligence_model_80_queries_fixed_FINAL.yaml'
)

ANALYST_ENDPOINT = (
    f"https://{HOST}/api/v2/cortex/analyst/message"
)

st.set_page_config(
    page_title="Dilytics Enterprise AI",
    page_icon="📦",
    layout="wide",
)

st.markdown(
    """
    <style>
    .status-pill {
        display:inline-flex;
        align-items:center;
        gap:6px;
        background-color:#ecfdf5;
        color:#065f46;
        border:1px solid #a7f3d0;
        border-radius:20px;
        padding:2px 10px;
        font-size:.75rem;
        font-weight:600;
    }
    div[data-testid="stButton"] > button {
        border-radius:8px;
        font-weight:500;
        transition:all .2s ease-in-out;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


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
        "Enter Snowflake Username:",
        value=st.session_state.username,
    )
    st.session_state.password = st.text_input(
        "Enter Password:",
        type="password",
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
                    Session.builder
                    .configs({"connection": conn})
                    .create()
                )
                st.session_state.authenticated = True
                st.rerun()

        except Exception as e:
            st.error(f"Authentication failed: {e}")

    st.stop()


session = st.session_state.snowpark_session
conn = st.session_state.snowflake_conn


# ===================================================================
# 2. CORTEX ANALYST
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
        "messages": [
            {
                "role": "user",
                "content": [{"type": "text", "text": prompt}],
            }
        ],
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
        "messages": [
            {
                "role": "user",
                "content": [{"type": "text", "text": prompt}],
            }
        ],
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
# 2A. UPLOADED DOCUMENT / TABLE SUPPORT
# ===================================================================
def _quote_ident(name: str) -> str:
    """Safely quote a Snowflake identifier."""
    return '"' + str(name).replace('"', '""') + '"'


def _uploaded_table_name() -> Optional[str]:
    return st.session_state.get("uploaded_document_table")


def _drop_uploaded_table() -> None:
    table_name = _uploaded_table_name()

    if table_name:
        try:
            session.sql(
                f"DROP TABLE IF EXISTS {_quote_ident(table_name)}"
            ).collect()
        except Exception:
            pass

    st.session_state.uploaded_document_table = None
    st.session_state.uploaded_document_semantic_model = None


def _normalize_dataframe_for_snowflake(df: pd.DataFrame) -> pd.DataFrame:
    """Make mixed/object columns safe for write_pandas."""

    result = df.copy()
    result = result.dropna(how="all").reset_index(drop=True)

    # Snowflake column names must be strings and should be unique.
    new_columns = []
    seen = {}

    for idx, col in enumerate(result.columns):
        base = str(col).strip() or f"COL_{idx + 1}"
        base = re.sub(r"[^A-Za-z0-9_]+", "_", base).strip("_")
        if not base:
            base = f"COL_{idx + 1}"

        base = base.upper()

        count = seen.get(base, 0)
        seen[base] = count + 1
        if count:
            base = f"{base}_{count + 1}"

        new_columns.append(base)

    result.columns = new_columns

    for col in result.columns:
        if pd.api.types.is_object_dtype(result[col]):
            result[col] = result[col].map(
                lambda x: (
                    None
                    if pd.isna(x)
                    else x.isoformat()
                    if isinstance(x, (datetime, pd.Timestamp))
                    else str(x)
                )
            )

    return result


def _infer_column_role(series: pd.Series, column_name: str) -> str:
    name = column_name.lower()

    if pd.api.types.is_datetime64_any_dtype(series):
        return "time"

    if pd.api.types.is_bool_dtype(series):
        return "dimension"

    if pd.api.types.is_numeric_dtype(series):
        return "fact"

    date_keywords = [
        "date",
        "time",
        "timestamp",
        "created",
        "updated",
        "month",
        "year",
    ]

    if any(k in name for k in date_keywords):
        converted = pd.to_datetime(series, errors="coerce")
        if converted.notna().mean() >= 0.70:
            return "time"

    return "dimension"


def _yaml_scalar(value: Any) -> str:
    text = str(value).replace("\\", "\\\\").replace('"', '\\"')
    return f'"{text}"'


def build_uploaded_semantic_model(
    df: pd.DataFrame,
    table_name: str,
) -> str:
    """
    Build a simple one-table semantic model for an uploaded spreadsheet.

    The model intentionally avoids sample_values because those can make
    semantic-model validation fail when generated dynamically.
    """

    fq_table = (
        f"{_quote_ident(DATABASE)}."
        f"{_quote_ident(SCHEMA)}."
        f"{_quote_ident(table_name)}"
    )

    dimensions = []
    time_dimensions = []
    facts = []

    for col in df.columns:
        role = _infer_column_role(df[col], str(col))

        if role == "time":
            time_dimensions.append(
                {
                    "name": str(col).lower(),
                    "expr": _quote_ident(col),
                    "data_type": "date",
                    "synonyms": [str(col).replace("_", " ")],
                }
            )
        elif role == "fact":
            facts.append(
                {
                    "name": str(col).lower(),
                    "expr": _quote_ident(col),
                    "data_type": "number",
                    "synonyms": [str(col).replace("_", " ")],
                }
            )
        else:
            dimensions.append(
                {
                    "name": str(col).lower(),
                    "expr": _quote_ident(col),
                    "data_type": "text",
                    "synonyms": [str(col).replace("_", " ")],
                }
            )

    # YAML is generated manually so no PyYAML formatting surprises occur.
    lines = [
        "name: uploaded_document_model",
        "description: Semantic model generated from the uploaded spreadsheet.",
        "tables:",
        "  - name: UPLOADED_DATA",
        f"    base_table:",
        f"      database: {DATABASE}",
        f"      schema: {SCHEMA}",
        f"      table: {table_name}",
        "    dimensions:",
    ]

    if dimensions:
        for d in dimensions:
            lines.extend(
                [
                    f"      - name: {d['name']}",
                    f"        expr: {d['expr']}",
                    f"        data_type: {d['data_type']}",
                    f"        synonyms:",
                    f"          - {_yaml_scalar(d['synonyms'][0])}",
                ]
            )
    else:
        lines.append("      []")

    lines.append("    time_dimensions:")

    if time_dimensions:
        for d in time_dimensions:
            lines.extend(
                [
                    f"      - name: {d['name']}",
                    f"        expr: {d['expr']}",
                    f"        data_type: {d['data_type']}",
                    f"        synonyms:",
                    f"          - {_yaml_scalar(d['synonyms'][0])}",
                ]
            )
    else:
        lines.append("      []")

    lines.append("    facts:")

    if facts:
        for f in facts:
            lines.extend(
                [
                    f"      - name: {f['name']}",
                    f"        expr: {f['expr']}",
                    f"        data_type: {f['data_type']}",
                    f"        synonyms:",
                    f"          - {_yaml_scalar(f['synonyms'][0])}",
                ]
            )
    else:
        lines.append("      []")

    lines.extend(
        [
            "    custom_instructions:",
            "      - Use ROW_COUNT style questions as COUNT(*) when the user asks for the number of rows.",
            "      - Use SUM for additive numeric measures and AVG for average questions.",
            "      - Use COUNT(DISTINCT ...) when the user explicitly asks for unique or distinct values.",
            "      - Use the uploaded table as the complete source of truth for spreadsheet questions.",
        ]
    )

    # Add a row-count fact only when there isn't already an obvious numeric
    # metric. This helps simple "how many records" questions.
    if not facts:
        # Insert before custom_instructions.
        insert_at = len(lines) - 4
        lines[insert_at:insert_at] = [
            "      - name: row_indicator",
            "        expr: 1",
            "        data_type: number",
            "        synonyms:",
            '          - "row count"',
            '          - "records"',
            '          - "projects"',
        ]

    return "\n".join(lines)


def prepare_uploaded_table(df: pd.DataFrame) -> None:
    """Write the uploaded spreadsheet to a transient table and build its model."""

    clean_df = _normalize_dataframe_for_snowflake(df)

    if clean_df.empty:
        raise ValueError("The uploaded spreadsheet does not contain any data rows.")

    # Keep the table name simple and unique so multiple users/runs do not collide.
    table_name = f"UPLOADED_DOCUMENT_{uuid.uuid4().hex[:12].upper()}"

    try:
        session.write_pandas(
            clean_df,
            table_name,
            database=DATABASE,
            schema=SCHEMA,
            auto_create_table=True,
            overwrite=True,
            table_type="transient",
            quote_identifiers=True,
        )
    except TypeError:
        # Compatibility fallback for older Snowpark versions.
        session.write_pandas(
            clean_df,
            table_name,
            database=DATABASE,
            schema=SCHEMA,
            auto_create_table=True,
            overwrite=True,
            quote_identifiers=True,
        )

    st.session_state.uploaded_document_table = table_name
    st.session_state.uploaded_document_semantic_model = (
        build_uploaded_semantic_model(clean_df, table_name)
    )
    st.session_state.uploaded_document_df = clean_df


def answer_uploaded_table_question(
    question: str,
    df: pd.DataFrame,
) -> Tuple[pd.DataFrame, Optional[str], Dict[str, Any]]:
    """Use Cortex Analyst against the uploaded transient table."""

    semantic_model = st.session_state.get(
        "uploaded_document_semantic_model"
    )
    table_name = st.session_state.get("uploaded_document_table")

    if not semantic_model or not table_name:
        raise ValueError(
            "The uploaded spreadsheet has not been loaded into Snowflake."
        )

    analyst_json = call_cortex_analyst_with_semantic_model(
        question,
        semantic_model,
    )
    result = extract_analyst_response(analyst_json)
    sql_query = result.get("sql")

    if not sql_query:
        raise ValueError(
            result.get("text")
            or "Cortex Analyst could not generate SQL for this uploaded spreadsheet."
        )

    # Guard against accidental write/DDL statements.
    first_token = sql_query.strip().split(None, 1)[0].upper()
    if first_token not in {"SELECT", "WITH", "SHOW", "DESCRIBE"}:
        raise ValueError(
            "For safety, uploaded-document questions must generate a read-only SQL query."
        )

    result_df = session.sql(sql_query).to_pandas()
    return result_df, sql_query, result


def extract_df_from_xlsx(file_bytes: bytes) -> pd.DataFrame:
    """Multi-stage robust spreadsheet extraction."""

    try:
        df = pd.read_excel(
            io.BytesIO(file_bytes),
            engine="openpyxl",
        )
        if df is not None and not df.empty:
            return df
    except Exception:
        pass

    try:
        df = pd.read_excel(
            io.BytesIO(file_bytes),
            engine="xlrd",
        )
        if df is not None and not df.empty:
            return df
    except Exception:
        pass

    try:
        df = pd.read_excel(io.BytesIO(file_bytes))
        if df is not None and not df.empty:
            return df
    except Exception:
        pass

    # Deep XLSX ZIP/XML fallback.
    try:
        with zipfile.ZipFile(io.BytesIO(file_bytes)) as z:
            shared_strings = []

            if "xl/sharedStrings.xml" in z.namelist():
                ss_tree = ET.fromstring(
                    z.read("xl/sharedStrings.xml")
                )

                for si in ss_tree.iterfind(
                    "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}si"
                ):
                    t_nodes = si.iterfind(
                        ".//{http://schemas.openxmlformats.org/spreadsheetml/2006/main}t"
                    )
                    shared_strings.append(
                        "".join(n.text or "" for n in t_nodes)
                    )

            sheet_files = [
                n
                for n in z.namelist()
                if n.startswith("xl/worksheets/sheet")
            ]

            if sheet_files:
                sheet_tree = ET.fromstring(
                    z.read(sheet_files[0])
                )
                rows_data = []

                for row in sheet_tree.iterfind(
                    ".//{http://schemas.openxmlformats.org/spreadsheetml/2006/main}row"
                ):
                    row_cells = []

                    for c in row.iterfind(
                        "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}c"
                    ):
                        val_node = c.find(
                            "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}v"
                        )
                        cell_val = (
                            val_node.text
                            if val_node is not None
                            else ""
                        )

                        if (
                            c.attrib.get("t") == "s"
                            and str(cell_val).isdigit()
                        ):
                            idx = int(cell_val)
                            cell_val = (
                                shared_strings[idx]
                                if idx < len(shared_strings)
                                else cell_val
                            )

                        row_cells.append(cell_val)

                    if any(str(cell).strip() for cell in row_cells):
                        rows_data.append(row_cells)

                if rows_data:
                    headers = [
                        str(h).strip()
                        if str(h).strip()
                        else f"Col_{i + 1}"
                        for i, h in enumerate(rows_data[0])
                    ]

                    df = pd.DataFrame(
                        rows_data[1:],
                        columns=headers,
                    )

                    for col in df.columns:
                        try:
                            df[col] = pd.to_numeric(df[col])
                        except (ValueError, TypeError):
                            pass

                    return df

    except Exception:
        pass

    # HTML-table fallback.
    try:
        tables = pd.read_html(io.BytesIO(file_bytes))
        if tables:
            return tables[0]
    except Exception:
        pass

    # Renamed CSV fallback.
    for enc in ["utf-8", "latin1", "cp1252"]:
        for sep in [",", "\t", ";", "|"]:
            try:
                df = pd.read_csv(
                    io.BytesIO(file_bytes),
                    sep=sep,
                    encoding=enc,
                )

                if (
                    df is not None
                    and len(df.columns) > 1
                    and len(df) > 0
                ):
                    return df
            except Exception:
                pass

    raise ValueError(
        "Unable to read this spreadsheet. "
        "Please ensure it is a valid .xlsx, .xls, or .csv file."
    )


def extract_text_from_pdf(file_bytes: bytes) -> str:
    if pypdf is None:
        return (
            "PDF text extraction requires the pypdf library. "
            "Add pypdf to requirements.txt."
        )

    try:
        reader = pypdf.PdfReader(io.BytesIO(file_bytes))
        pages = []

        for page in reader.pages:
            pages.append(page.extract_text() or "")

        return "\n\n".join(pages).strip()

    except Exception as exc:
        return f"Error extracting PDF: {str(exc)}"


def extract_text_from_docx(file_bytes: bytes) -> str:
    """Extract Word paragraphs and table text without python-docx."""

    if not file_bytes:
        return ""

    try:
        with zipfile.ZipFile(io.BytesIO(file_bytes)) as z:
            xml_content = z.read("word/document.xml")
            tree = ET.fromstring(xml_content)

            ns = {
                "w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"
            }

            parts = []

            # Preserve paragraphs.
            for paragraph in tree.findall(".//w:p", ns):
                texts = [
                    node.text or ""
                    for node in paragraph.findall(".//w:t", ns)
                ]
                paragraph_text = "".join(texts).strip()

                if paragraph_text:
                    parts.append(paragraph_text)

            # Preserve tables as simple rows.
            for table in tree.findall(".//w:tbl", ns):
                rows = []

                for row in table.findall(".//w:tr", ns):
                    cells = []

                    for cell in row.findall(".//w:tc", ns):
                        texts = [
                            node.text or ""
                            for node in cell.findall(".//w:t", ns)
                        ]
                        cells.append(
                            " ".join("".join(texts).split())
                        )

                    if any(cells):
                        rows.append(" | ".join(cells))

                if rows:
                    parts.append("\n".join(rows))

            return "\n\n".join(parts).strip()

    except Exception as exc:
        return f"Error extracting Word document: {str(exc)}"


def _split_document_into_chunks(
    text: str,
    chunk_size: int = 1200,
) -> List[str]:
    cleaned = re.sub(r"[ \t]+", " ", text)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned).strip()

    if not cleaned:
        return []

    paragraphs = [
        p.strip()
        for p in re.split(r"\n\s*\n", cleaned)
        if p.strip()
    ]

    chunks = []
    current = ""

    for paragraph in paragraphs:
        if len(current) + len(paragraph) + 2 <= chunk_size:
            current = (
                f"{current}\n\n{paragraph}".strip()
                if current
                else paragraph
            )
        else:
            if current:
                chunks.append(current)
            current = paragraph

    if current:
        chunks.append(current)

    return chunks


def _score_text_chunk(question: str, chunk: str) -> int:
    stop_words = {
        "what",
        "is",
        "the",
        "are",
        "and",
        "for",
        "from",
        "with",
        "this",
        "that",
        "how",
        "why",
        "which",
        "does",
        "about",
        "into",
        "give",
        "show",
        "tell",
        "please",
    }

    q_words = {
        w.lower()
        for w in re.findall(r"\b[a-zA-Z0-9_]+\b", question)
        if len(w) > 2 and w.lower() not in stop_words
    }

    chunk_lower = chunk.lower()
    score = 0

    for word in q_words:
        if word in chunk_lower:
            score += 1

    # Exact phrase is stronger than individual words.
    phrase = question.strip().lower()
    if phrase and phrase in chunk_lower:
        score += 5

    return score


def answer_user_question_on_document(
    question: str,
    doc_context: str,
    filename: str,
    df: Optional[pd.DataFrame] = None,
) -> str:
    """
    Trial-safe document Q&A.

    For PDF/Word this uses extracted document text and returns the most
    relevant passages. It does not call COMPLETE/AI_COMPLETE, so it does
    not hit the trial-account Cortex LLM restriction.
    """

    if not doc_context or not doc_context.strip():
        return (
            f"I could not extract readable text from `{filename}`. "
            "If this is a scanned PDF, OCR may be required."
        )

    chunks = _split_document_into_chunks(doc_context)

    if not chunks:
        return (
            f"I could not find readable content in `{filename}`."
        )

    scored = [
        (_score_text_chunk(question, chunk), chunk)
        for chunk in chunks
    ]

    scored.sort(key=lambda x: x[0], reverse=True)

    best = [
        chunk
        for score, chunk in scored[:3]
        if score > 0
    ]

    if not best:
        # For broad questions, show the beginning of the document rather
        # than falsely claiming that the information does not exist.
        best = chunks[:2]

    answer_parts = [
        f"Based on the uploaded document **`{filename}`**, "
        "the most relevant content is:"
    ]

    for idx, chunk in enumerate(best, start=1):
        answer_parts.append(
            f"\n**Relevant section {idx}:**\n\n{chunk}"
        )

    return "\n".join(answer_parts)


def process_uploaded_document(
    uploaded_file,
) -> Tuple[str, Optional[pd.DataFrame], Optional[str], str]:
    uploaded_file.seek(0)
    filename = uploaded_file.name
    file_bytes = uploaded_file.read()

    if not file_bytes:
        return (
            "The uploaded file is empty.",
            None,
            None,
            "unknown",
        )

    fname_lower = filename.lower()

    try:
        if fname_lower.endswith(".csv"):
            try:
                df = pd.read_csv(io.BytesIO(file_bytes))
            except Exception:
                df = pd.read_csv(
                    io.BytesIO(file_bytes),
                    encoding="latin1",
                )

            clean_df = df.dropna(how="all")

            context_str = (
                f"File: {filename}\n"
                f"Total Rows: {len(clean_df)}\n"
                f"Columns: {', '.join(map(str, clean_df.columns))}\n\n"
                "DATA PREVIEW AND RECORDS:\n"
                f"{clean_df.to_string(max_rows=150)}"
            )

            summary = (
                f"Successfully processed **`{filename}`** with "
                f"**{len(clean_df):,} rows** and "
                f"**{len(clean_df.columns)} columns**.\n\n"
                f"**Columns:** "
                f"{', '.join(f'`{c}`' for c in clean_df.columns)}\n\n"
                "You can now ask questions about the complete uploaded dataset."
            )

            return summary, clean_df, context_str, "table"

        if fname_lower.endswith((".xlsx", ".xls")):
            df = extract_df_from_xlsx(file_bytes)
            clean_df = df.dropna(how="all")

            context_str = (
                f"File: {filename}\n"
                f"Total Rows: {len(clean_df)}\n"
                f"Columns: {', '.join(map(str, clean_df.columns))}\n\n"
                "DATA PREVIEW AND RECORDS:\n"
                f"{clean_df.to_string(max_rows=150)}"
            )

            summary = (
                f"Successfully processed **`{filename}`** with "
                f"**{len(clean_df):,} rows** and "
                f"**{len(clean_df.columns)} columns**.\n\n"
                f"**Columns:** "
                f"{', '.join(f'`{c}`' for c in clean_df.columns)}\n\n"
                "You can now ask questions about the complete uploaded dataset."
            )

            return summary, clean_df, context_str, "table"

        if fname_lower.endswith(".pdf"):
            txt = extract_text_from_pdf(file_bytes)

            if txt.startswith("Error extracting"):
                raise ValueError(txt)

            summary = (
                f"Uploaded PDF **`{filename}`** "
                f"(~{len(txt.split()):,} words). "
                "Ready for questions."
            )

            return summary, None, txt, "text"

        if fname_lower.endswith(".docx"):
            txt = extract_text_from_docx(file_bytes)

            if txt.startswith("Error extracting"):
                raise ValueError(txt)

            summary = (
                f"Uploaded Word Document **`{filename}`** "
                f"(~{len(txt.split()):,} words). "
                "Ready for questions."
            )

            return summary, None, txt, "text"

        return (
            f"Unsupported format for `{filename}`.",
            None,
            None,
            "unknown",
        )

    except Exception as err:
        return (
            f"⚠️ Could not parse `{filename}`: {str(err)}",
            None,
            None,
            "unknown",
        )


def render_uploaded_document_preview() -> None:
    if not st.session_state.get("uploaded_document_name"):
        return

    doc_name = st.session_state.uploaded_document_name
    doc_type = st.session_state.get("uploaded_document_type")

    with st.expander(
        f"📄 Uploaded Document Preview — {doc_name}",
        expanded=False,
    ):
        if doc_type == "table":
            df = st.session_state.get("uploaded_document_df")

            if df is not None and not df.empty:
                st.dataframe(
                    df.head(100),
                    use_container_width=True,
                )
                st.caption(
                    f"Showing up to 100 rows. "
                    f"Complete dataset contains {len(df):,} rows."
                )
            else:
                st.info("No tabular data available.")

        elif doc_type == "text":
            text = st.session_state.get(
                "uploaded_document_text",
                "",
            )

            if text:
                st.text_area(
                    "Extracted document text",
                    text[:20000],
                    height=350,
                )
            else:
                st.warning(
                    "No readable text was extracted from this document."
                )


# ===================================================================
# 3. CHAT SESSION STATE
# ===================================================================
if "chat_sessions" not in st.session_state:
    st.session_state.chat_sessions = {}

if "current_session_id" not in st.session_state:
    init_id = datetime.now().strftime(
        "%Y%m%d_%H%M%S_%f"
    )

    st.session_state.current_session_id = init_id

    st.session_state.chat_sessions[init_id] = {
        "title": "New Conversation",
        "messages": [],
    }

# Document state defaults.
document_defaults = {
    "uploaded_document": None,
    "uploaded_document_name": None,
    "uploaded_document_df": None,
    "uploaded_document_text": "",
    "uploaded_document_type": None,
    "uploaded_document_table": None,
    "uploaded_document_semantic_model": None,
}

for key, value in document_defaults.items():
    if key not in st.session_state:
        st.session_state[key] = value


current_id = st.session_state.current_session_id
messages = st.session_state.chat_sessions[current_id]["messages"]


# ===================================================================
# 4. CHART DISPLAY
# ===================================================================
def display_chart_tab(
    df: pd.DataFrame,
    key_prefix: str = "",
):
    if df is None or df.empty:
        st.info("No data available for charting.")
        return

    if len(df.columns) < 2:
        st.info("Need at least 2 columns to render a chart.")
        return

    all_cols = list(df.columns)
    col1, col2, col3 = st.columns(3)

    x_col = col1.selectbox(
        "Dimension (X-axis)",
        all_cols,
        index=0,
        key=f"{key_prefix}_x",
    )

    remaining_cols = [
        c for c in all_cols
        if c != x_col
    ]

    if not remaining_cols:
        return

    y_col = col2.selectbox(
        "Metric (Y-axis)",
        remaining_cols,
        index=0,
        key=f"{key_prefix}_y",
    )

    chart_type = col3.selectbox(
        "Chart Type",
        [
            "Bar Chart",
            "Line Chart",
            "Area Chart",
            "Scatter Plot",
        ],
        key=f"{key_prefix}_type",
    )

    chart_df = df.copy()

    if any(
        k in str(x_col).lower()
        for k in [
            "year",
            "quarter",
            "month",
            "day",
            "date",
        ]
    ):
        chart_df[x_col] = chart_df[x_col].apply(
            lambda x: (
                str(int(x))
                if pd.notnull(x)
                and isinstance(x, (int, float))
                else str(x)
            )
        )

    try:
        if chart_type == "Bar Chart":
            st.bar_chart(
                chart_df.set_index(x_col)[y_col]
            )

        elif chart_type == "Line Chart":
            st.line_chart(
                chart_df.set_index(x_col)[y_col]
            )

        elif chart_type == "Area Chart":
            st.area_chart(
                chart_df.set_index(x_col)[y_col]
            )

        else:
            st.scatter_chart(
                chart_df,
                x=x_col,
                y=y_col,
            )

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

    if st.button(
        "➕ New Chat",
        use_container_width=True,
        type="primary",
    ):
        new_id = datetime.now().strftime(
            "%Y%m%d_%H%M%S_%f"
        )

        st.session_state.current_session_id = new_id

        st.session_state.chat_sessions[new_id] = {
            "title": (
                f"Chat "
                f"{len(st.session_state.chat_sessions) + 1}"
            ),
            "messages": [],
        }

        st.rerun()

    st.markdown("---")
    st.markdown("##### 🕒 Recent Conversations")

    for s_id, s_data in reversed(
        list(st.session_state.chat_sessions.items())
    ):
        is_active = (
            s_id == st.session_state.current_session_id
        )

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

    if st.button(
        "🗑️ Clear All Sessions",
        use_container_width=True,
    ):
        st.session_state.chat_sessions = {}

        init_id = datetime.now().strftime(
            "%Y%m%d_%H%M%S_%f"
        )

        st.session_state.current_session_id = init_id

        st.session_state.chat_sessions[init_id] = {
            "title": "New Conversation",
            "messages": [],
        }

        st.rerun()

    st.markdown("---")
    st.markdown("##### 📄 Analyze an Uploaded Document")

    uploaded_doc = st.file_uploader(
        "Upload CSV, Excel, PDF or Word",
        type=[
            "csv",
            "xlsx",
            "xls",
            "pdf",
            "docx",
        ],
        key="document_uploader",
        help=(
            "Upload a document, click Analyze, then "
            "choose Uploaded Document in the chat."
        ),
    )

    if st.button(
        "🔍 Analyze Document",
        use_container_width=True,
        disabled=uploaded_doc is None,
        key="analyze_uploaded_document",
    ):
        try:
            with st.spinner(
                "Reading and analyzing document..."
            ):
                # Remove the old transient table only when a new
                # document is actually analyzed.
                _drop_uploaded_table()

                doc_message, doc_df, doc_text, doc_type = (
                    process_uploaded_document(uploaded_doc)
                )

                st.session_state.uploaded_document_name = (
                    uploaded_doc.name
                )
                st.session_state.uploaded_document_type = (
                    doc_type
                )
                st.session_state.uploaded_document_df = doc_df
                st.session_state.uploaded_document_text = (
                    doc_text or ""
                )
                st.session_state.uploaded_document = (
                    uploaded_doc.name
                )

                if doc_type == "table":
                    prepare_uploaded_table(doc_df)

            st.success(doc_message)
            st.rerun()

        except Exception as e:
            st.error(
                f"Document analysis failed: {e}"
            )

    if st.session_state.uploaded_document_name:
        st.caption(
            f"Loaded: "
            f"`{st.session_state.uploaded_document_name}`"
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

            st.rerun()


# ===================================================================
# 6. MAIN HEADER
# ===================================================================
head_col1, head_col2 = st.columns(
    [4.5, 1.2]
)

with head_col1:
    st.title("💬 Dilytics Enterprise AI")

    st.caption(
        "Ask natural-language questions to explore Inventory and Sales."
    )

with head_col2:
    st.write("")

    if st.button(
        "🔄 Reset Thread",
        use_container_width=True,
        help="Clear message history",
    ):
        st.session_state.chat_sessions[
            current_id
        ]["messages"] = []

        st.session_state.chat_sessions[
            current_id
        ]["title"] = "New Conversation"

        st.rerun()


# ===================================================================
# 7. EXAMPLE QUESTIONS
# ===================================================================
quick_prompt = None

tab_inv, tab_sales = st.tabs(
    [
        "📦 Inventory Intelligence",
        "💰 Sales Intelligence",
    ]
)

with tab_inv:
    with st.expander(
        "💡 What can I ask about Inventory?",
        expanded=False,
    ):
        st.markdown(
            """
            Questions are answered by Cortex Analyst using
            `INV_ANALYST_DEMO_90_VERIFIED.yaml`.

            * How many products are out of stock?
            * What is the inventory value by warehouse?
            * Which products have the highest inventory value?
            * Which warehouses have the highest outbound quantity?
            * Which products need to be reordered?
            * What is the inventory value by product category?
            """
        )

    st.markdown(
        "##### 💡 Example Inventory Questions"
    )

    q1, q2, q3, q4, q5 = st.columns(5)

    if q1.button(
        "💰 Inventory Value",
        use_container_width=True,
        key="i1",
    ):
        quick_prompt = (
            "What is the total inventory value?"
        )

    if q2.button(
        "🏭 Value by Warehouse",
        use_container_width=True,
        key="i2",
    ):
        quick_prompt = (
            "What is the inventory value by warehouse?"
        )

    if q3.button(
        "📦 Value by Category",
        use_container_width=True,
        key="i3",
    ):
        quick_prompt = (
            "What is the inventory value by product category?"
        )

    if q4.button(
        "📉 Stockout Count",
        use_container_width=True,
        key="i4",
    ):
        quick_prompt = (
            "How many products are out of stock?"
        )

    if q5.button(
        "⚠️ Excess Stock",
        use_container_width=True,
        key="i5",
    ):
        quick_prompt = (
            "What is the total excess inventory value by warehouse?"
        )


with tab_sales:
    with st.expander(
        "💡 What can I ask about Sales?",
        expanded=False,
    ):
        st.markdown(
            """
            Questions are answered by Cortex Analyst using
            `sales_intelligence_model_80_queries_fixed.yaml`.

            * What is the total sales amount?
            * What are the top products by sales?
            * What are total sales by customer region?
            * What are total sales by month?
            * What is total sales by order channel?
            """
        )

    st.markdown(
        "##### 💡 Example Sales Questions"
    )

    s1, s2, s3, s4, s5 = st.columns(5)

    if s1.button(
        "💵 Total Sales",
        use_container_width=True,
        key="s1",
    ):
        quick_prompt = (
            "What is the total sales amount?"
        )

    if s2.button(
        "🏆 Top Products",
        use_container_width=True,
        key="s2",
    ):
        quick_prompt = (
            "What are the top products by sales?"
        )

    if s3.button(
        "🌍 Sales by Region",
        use_container_width=True,
        key="s3",
    ):
        quick_prompt = (
            "What are total sales by customer region?"
        )

    if s4.button(
        "📅 Monthly Sales",
        use_container_width=True,
        key="s4",
    ):
        quick_prompt = (
            "What are total sales by month?"
        )

    if s5.button(
        "📊 Sales by Channel",
        use_container_width=True,
        key="s5",
    ):
        quick_prompt = (
            "What is total sales by order channel?"
        )


st.markdown("---")


# ===================================================================
# 7A. UPLOADED DOCUMENT PREVIEW
# ===================================================================
render_uploaded_document_preview()


# ===================================================================
# 8. DISPLAY CHAT HISTORY
# ===================================================================
for idx, msg in enumerate(messages):
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

        if msg.get("sql"):
            with st.expander(
                "Generated SQL",
                expanded=False,
            ):
                st.code(
                    msg["sql"],
                    language="sql",
                )

        if msg.get("semantic_model"):
            st.caption(
                "Semantic model selected: "
                f"`{msg['semantic_model']}`"
            )

        if msg.get("verified_query"):
            name = msg["verified_query"].get("name")

            if name:
                st.caption(
                    f"Verified Query Used: `{name}`"
                )

        if msg.get("data") is not None:
            tab_data, tab_chart = st.tabs(
                [
                    "Data 📄",
                    "Chart 📈",
                ]
            )

            with tab_data:
                st.dataframe(
                    msg["data"],
                    use_container_width=True,
                )

            with tab_chart:
                display_chart_tab(
                    msg["data"],
                    key_prefix=(
                        f"hist_{current_id}_{idx}"
                    ),
                )


# ===================================================================
# 8A. ANSWER SOURCE
# ===================================================================
if st.session_state.uploaded_document_name:
    answer_source = st.radio(
        "Answer from:",
        [
            "Snowflake Data",
            "Uploaded Document",
        ],
        horizontal=True,
        key="answer_source",
        help=(
            "Choose whether your question should use "
            "the existing Inventory/Sales semantic models "
            "or the uploaded document."
        ),
    )
else:
    answer_source = "Snowflake Data"


# ===================================================================
# 9. CHAT INPUT
# ===================================================================
user_prompt = (
    st.chat_input(
        "Ask a question about inventory, warehouses, products, sales, customers..."
    )
    or quick_prompt
)


# ===================================================================
# 10. QUESTION EXECUTION
# ===================================================================
if user_prompt:

    # ---------------------------------------------------------------
    # Uploaded document question path
    # ---------------------------------------------------------------
    if answer_source == "Uploaded Document":

        if len(messages) == 0:
            st.session_state.chat_sessions[
                current_id
            ]["title"] = (
                user_prompt[:25]
                + ("..." if len(user_prompt) > 25 else "")
            )

        messages.append(
            {
                "role": "user",
                "content": user_prompt,
            }
        )

        with st.chat_message("user"):
            st.markdown(user_prompt)

        with st.chat_message("assistant"):
            doc_df_result = None
            doc_sql_result = None
            doc_answer = ""

            try:
                with st.spinner(
                    "Analyzing your uploaded document..."
                ):
                    if (
                        st.session_state.uploaded_document_type
                        == "table"
                    ):
                        (
                            doc_df_result,
                            doc_sql_result,
                            doc_analyst_result,
                        ) = answer_uploaded_table_question(
                            user_prompt,
                            st.session_state.uploaded_document_df,
                        )

                        doc_answer = (
                            "I answered your question using the "
                            "complete uploaded dataset through "
                            "Cortex Analyst. The SQL below was "
                            "generated dynamically from the uploaded "
                            "document schema."
                        )

                        if doc_analyst_result.get(
                            "semantic_model_selection"
                        ):
                            st.caption(
                                "Semantic model selected: "
                                + str(
                                    doc_analyst_result[
                                        "semantic_model_selection"
                                    ]
                                )
                            )

                        if doc_analyst_result.get(
                            "verified_query_used"
                        ):
                            vq = doc_analyst_result[
                                "verified_query_used"
                            ]

                            if isinstance(vq, dict):
                                vq_name = vq.get("name")
                                if vq_name:
                                    st.caption(
                                        "Verified Query Used: "
                                        f"`{vq_name}`"
                                    )

                    else:
                        doc_answer = (
                            answer_user_question_on_document(
                                user_prompt,
                                st.session_state.uploaded_document_text,
                                st.session_state.uploaded_document_name,
                            )
                        )

                st.markdown(doc_answer)

                if doc_sql_result:
                    with st.expander(
                        "Generated SQL for Uploaded Document",
                        expanded=False,
                    ):
                        st.code(
                            doc_sql_result,
                            language="sql",
                        )

                if doc_df_result is not None:
                    tab_data, tab_chart = st.tabs(
                        [
                            "Data 📄",
                            "Chart 📈",
                        ]
                    )

                    with tab_data:
                        st.dataframe(
                            doc_df_result,
                            use_container_width=True,
                        )

                    with tab_chart:
                        display_chart_tab(
                            doc_df_result,
                            key_prefix=(
                                f"document_{current_id}_"
                                f"{len(messages)}"
                            ),
                        )

                messages.append(
                    {
                        "role": "assistant",
                        "content": doc_answer,
                        "sql": doc_sql_result,
                        "data": doc_df_result,
                        "semantic_model": (
                            "Uploaded Document"
                        ),
                        "verified_query": None,
                    }
                )

            except Exception as e:
                doc_answer = (
                    "Unable to analyze the uploaded document: "
                    f"{e}"
                )

                st.error(doc_answer)

                messages.append(
                    {
                        "role": "assistant",
                        "content": doc_answer,
                        "sql": None,
                        "data": None,
                        "semantic_model": (
                            "Uploaded Document"
                        ),
                        "verified_query": None,
                    }
                )

        st.rerun()

    # ---------------------------------------------------------------
    # Existing Snowflake Inventory / Sales Cortex Analyst path
    # ---------------------------------------------------------------
    if len(messages) == 0:
        st.session_state.chat_sessions[
            current_id
        ]["title"] = (
            user_prompt[:25]
            + ("..." if len(user_prompt) > 25 else "")
        )

    messages.append(
        {
            "role": "user",
            "content": user_prompt,
        }
    )

    with st.chat_message("user"):
        st.markdown(user_prompt)

    with st.chat_message("assistant"):
        df = None
        sql_query = None
        explanation = ""
        semantic_model = None
        verified_query = None

        try:
            with st.spinner(
                "Cortex Analyst is interpreting your question..."
            ):
                analyst_json = call_cortex_analyst(
                    user_prompt
                )
                result = extract_analyst_response(
                    analyst_json
                )

            explanation = result["text"]
            sql_query = result["sql"]
            semantic_model = result[
                "semantic_model_selection"
            ]
            verified_query = result[
                "verified_query_used"
            ]

            for warning in result["warnings"]:
                warning_text = (
                    warning.get(
                        "message",
                        str(warning),
                    )
                    if isinstance(warning, dict)
                    else str(warning)
                )

                st.warning(warning_text)

            if not sql_query:
                if not explanation:
                    explanation = (
                        "Cortex Analyst could not generate SQL "
                        "for this question from the configured "
                        "semantic models."
                    )

                st.markdown(explanation)

            else:
                if not explanation:
                    explanation = (
                        "I generated this answer using the "
                        "Snowflake semantic model."
                    )

                st.markdown(explanation)

                if semantic_model:
                    st.caption(
                        "Semantic model selected: "
                        f"`{semantic_model}`"
                    )

                if verified_query:
                    name = (
                        verified_query.get("name")
                        if isinstance(
                            verified_query,
                            dict,
                        )
                        else None
                    )

                    if name:
                        st.caption(
                            f"Verified Query Used: `{name}`"
                        )

                with st.expander(
                    "Generated SQL",
                    expanded=False,
                ):
                    st.code(
                        sql_query,
                        language="sql",
                    )

                with st.spinner(
                    "Executing generated SQL in Snowflake..."
                ):
                    df = session.sql(
                        sql_query
                    ).to_pandas()

                tab_data, tab_chart = st.tabs(
                    [
                        "Data 📄",
                        "Chart 📈",
                    ]
                )

                with tab_data:
                    st.dataframe(
                        df,
                        use_container_width=True,
                    )

                with tab_chart:
                    display_chart_tab(
                        df,
                        key_prefix=(
                            f"live_{current_id}_"
                            f"{len(messages)}"
                        ),
                    )

        except requests.exceptions.Timeout:
            explanation = (
                "Cortex Analyst took too long to respond. "
                "Please try again."
            )
            st.error(explanation)

        except requests.exceptions.RequestException as e:
            explanation = (
                f"Could not connect to Cortex Analyst: {e}"
            )
            st.error(explanation)

        except Exception as e:
            explanation = (
                f"Unable to process the question: {e}"
            )
            st.error(explanation)

        messages.append(
            {
                "role": "assistant",
                "content": explanation,
                "sql": sql_query,
                "data": df,
                "semantic_model": semantic_model,
                "verified_query": verified_query,
            }
        )

    st.rerun()
