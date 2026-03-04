#!/usr/bin/env python3
"""
Institution RCR Analyzer - Streamlit App
=========================================
Enter institution names to calculate institution-level iCite / RCR metrics
for all publications from the last 2 full calendar years.

Run with:
    streamlit run institution_app.py

Requirements:
    pip install streamlit openpyxl requests
"""

import io
import re
import time
import statistics
import xml.etree.ElementTree as ET
from datetime import datetime

import requests
import streamlit as st
from openpyxl import Workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
ESEARCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
EFETCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
ICITE_URL = "https://icite.od.nih.gov/api/pubs"
ICITE_BATCH = 200

CURRENT_YEAR = datetime.now().year
ANALYSIS_YEARS = [CURRENT_YEAR - 2, CURRENT_YEAR - 1]  # last 2 full calendar years


# ---------------------------------------------------------------------------
# PubMed search with 10k workaround
# ---------------------------------------------------------------------------
def get_ncbi_delay(api_key):
    return 0.11 if api_key else 0.34


def esearch_count(query, email, api_key=None):
    """Return total count of results for a query without fetching IDs."""
    params = {
        "db": "pubmed", "term": query, "rettype": "count",
        "retmode": "json", "email": email,
    }
    if api_key:
        params["api_key"] = api_key
    resp = requests.get(ESEARCH_URL, params=params, timeout=30)
    resp.raise_for_status()
    return int(resp.json().get("esearchresult", {}).get("count", 0))


def esearch_ids(query, email, api_key=None, retmax=10000):
    """Return up to retmax PMIDs for a query."""
    params = {
        "db": "pubmed", "term": query, "retmax": retmax,
        "retmode": "json", "email": email,
    }
    if api_key:
        params["api_key"] = api_key
    time.sleep(get_ncbi_delay(api_key))
    resp = requests.get(ESEARCH_URL, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json().get("esearchresult", {}).get("idlist", [])


def build_affiliation_query(institution_names, year):
    """Build a PubMed query for institution affiliations in a given year."""
    affil_parts = []
    for name in institution_names:
        name = name.strip()
        if name:
            affil_parts.append(f'"{name}"[Affiliation]')
    affil_query = "(" + " OR ".join(affil_parts) + ")"
    date_query = f"{year}/01/01:{year}/12/31[pdat]"
    return f"{affil_query} AND {date_query}"


def build_monthly_query(institution_names, year, month):
    """Build a PubMed query for a specific month."""
    affil_parts = []
    for name in institution_names:
        name = name.strip()
        if name:
            affil_parts.append(f'"{name}"[Affiliation]')
    affil_query = "(" + " OR ".join(affil_parts) + ")"
    if month == 12:
        end_date = f"{year}/12/31"
    else:
        end_date = f"{year}/{month + 1:02d}/01"
    start_date = f"{year}/{month:02d}/01"
    date_query = f"{start_date}:{end_date}[pdat]"
    return f"{affil_query} AND {date_query}"


def search_institution_pmids(institution_names, year, email, api_key=None, status_fn=None):
    """
    Get all PMIDs for an institution in a given year.
    If > 9,500 results, splits into monthly queries to stay under 10k limit.
    """
    query = build_affiliation_query(institution_names, year)
    count = esearch_count(query, email, api_key)

    if status_fn:
        status_fn(f"  {year}: {count:,} publications found")

    if count == 0:
        return []

    if count <= 9500:
        return esearch_ids(query, email, api_key, retmax=10000)

    # Too many results - split by month
    all_pmids = set()
    for month in range(1, 13):
        monthly_query = build_monthly_query(institution_names, year, month)
        monthly_count = esearch_count(monthly_query, email, api_key)
        if monthly_count == 0:
            continue
        if monthly_count > 9500:
            if status_fn:
                status_fn(f"  {year}/{month:02d}: {monthly_count:,} papers (exceeds 10k limit, results may be incomplete)")
        ids = esearch_ids(monthly_query, email, api_key, retmax=10000)
        all_pmids.update(ids)
        if status_fn:
            status_fn(f"  {year}/{month:02d}: retrieved {len(ids):,} PMIDs (total so far: {len(all_pmids):,})")
    return list(all_pmids)


# ---------------------------------------------------------------------------
# iCite
# ---------------------------------------------------------------------------
def fetch_icite(pmids, status_fn=None):
    """Fetch iCite data for a list of PMIDs. Returns dict keyed by PMID string."""
    result = {}
    total = len(pmids)
    for start in range(0, total, ICITE_BATCH):
        batch = pmids[start:start + ICITE_BATCH]
        params = {
            "pmids": ",".join(batch),
            "fl": "pmid,relative_citation_ratio,nih_percentile,citation_count,"
                  "expected_citations_per_year,citations_per_year,is_research_article,"
                  "year,title,journal,doi,is_clinical,provisional",
        }
        time.sleep(0.5)
        try:
            resp = requests.get(ICITE_URL, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            for pub in data.get("data", []):
                result[str(pub.get("pmid", ""))] = pub
        except Exception:
            pass  # skip failed batches
        if status_fn and (start + ICITE_BATCH) % 1000 < ICITE_BATCH:
            status_fn(f"  iCite: processed {min(start + ICITE_BATCH, total):,} / {total:,} PMIDs")
    return result


# ---------------------------------------------------------------------------
# Metrics calculation
# ---------------------------------------------------------------------------
def compute_institution_metrics(icite_data, year=None):
    """Compute institution-level metrics from iCite data."""
    rcrs = []
    percentiles = []
    citation_counts = []
    research_count = 0
    clinical_count = 0
    provisional_count = 0

    for pmid, pub in icite_data.items():
        if year and pub.get("year") != year:
            continue
        rcr = pub.get("relative_citation_ratio")
        if rcr is not None:
            rcrs.append(rcr)
        pct = pub.get("nih_percentile")
        if pct is not None:
            percentiles.append(pct)
        cc = pub.get("citation_count")
        if cc is not None:
            citation_counts.append(cc)
        if pub.get("is_research_article"):
            research_count += 1
        if pub.get("is_clinical"):
            clinical_count += 1
        if pub.get("provisional"):
            provisional_count += 1

    total_pubs = len(icite_data) if year is None else sum(1 for p in icite_data.values() if p.get("year") == year)

    metrics = {
        "total_publications": total_pubs,
        "pubs_with_rcr": len(rcrs),
        "rcr_sum": round(sum(rcrs), 2) if rcrs else 0,
        "rcr_mean": round(statistics.mean(rcrs), 4) if rcrs else 0,
        "rcr_median": round(statistics.median(rcrs), 4) if rcrs else 0,
        "percentile_mean": round(statistics.mean(percentiles), 2) if percentiles else 0,
        "percentile_median": round(statistics.median(percentiles), 2) if percentiles else 0,
        "citation_total": sum(citation_counts),
        "citation_mean": round(statistics.mean(citation_counts), 2) if citation_counts else 0,
        "research_articles": research_count,
        "clinical_articles": clinical_count,
        "provisional_rcr_count": provisional_count,
    }
    return metrics


# ---------------------------------------------------------------------------
# Excel writer
# ---------------------------------------------------------------------------
HEADER_FILL = PatternFill("solid", fgColor="1B4F72")
HEADER_FONT = Font(name="Arial", bold=True, color="FFFFFF", size=11)
BODY_FONT = Font(name="Arial", size=10)
THIN_BORDER = Border(
    left=Side(style="thin"), right=Side(style="thin"),
    top=Side(style="thin"), bottom=Side(style="thin"),
)
ALT_FILL = PatternFill("solid", fgColor="D6EAF8")


def write_institution_xlsx(institution_label, icite_data, metrics_by_year, combined_metrics):
    wb = Workbook()

    # --- Summary sheet ---
    ws_sum = wb.active
    ws_sum.title = "Summary"

    ws_sum.merge_cells("A1:F1")
    ws_sum["A1"] = f"{institution_label} — Publication Metrics"
    ws_sum["A1"].font = Font(name="Arial", bold=True, size=14, color="1B4F72")
    ws_sum["A1"].alignment = Alignment(horizontal="left", vertical="center")
    ws_sum.row_dimensions[1].height = 30

    ws_sum.merge_cells("A2:F2")
    ws_sum["A2"] = f"Analysis period: {ANALYSIS_YEARS[0]}–{ANALYSIS_YEARS[-1]}"
    ws_sum["A2"].font = Font(name="Arial", italic=True, size=10, color="666666")

    headers = ["Metric"] + [str(y) for y in ANALYSIS_YEARS] + ["Combined"]
    col_widths = [30] + [18] * (len(ANALYSIS_YEARS) + 1)
    header_row = 4

    for col_idx, (header, width) in enumerate(zip(headers, col_widths), start=1):
        cell = ws_sum.cell(row=header_row, column=col_idx, value=header)
        cell.font = HEADER_FONT
        cell.fill = HEADER_FILL
        cell.alignment = Alignment(horizontal="center", vertical="center")
        cell.border = THIN_BORDER
        ws_sum.column_dimensions[get_column_letter(col_idx)].width = width

    metric_rows = [
        ("Total Publications", "total_publications"),
        ("Publications with RCR", "pubs_with_rcr"),
        ("RCR Sum", "rcr_sum"),
        ("Mean RCR", "rcr_mean"),
        ("Median RCR", "rcr_median"),
        ("Mean NIH Percentile", "percentile_mean"),
        ("Median NIH Percentile", "percentile_median"),
        ("Total Citations", "citation_total"),
        ("Mean Citations per Paper", "citation_mean"),
        ("Research Articles", "research_articles"),
        ("Clinical Articles", "clinical_articles"),
        ("Provisional RCR Count", "provisional_rcr_count"),
    ]

    for i, (label, key) in enumerate(metric_rows):
        row = header_row + 1 + i
        fill = ALT_FILL if i % 2 == 0 else PatternFill()
        cell = ws_sum.cell(row=row, column=1, value=label)
        cell.font = Font(name="Arial", bold=True, size=10)
        cell.border = THIN_BORDER
        cell.fill = fill

        for j, year in enumerate(ANALYSIS_YEARS):
            val = metrics_by_year.get(year, {}).get(key, "N/A")
            cell = ws_sum.cell(row=row, column=2 + j, value=val)
            cell.font = BODY_FONT
            cell.alignment = Alignment(horizontal="center")
            cell.border = THIN_BORDER
            cell.fill = fill

        val = combined_metrics.get(key, "N/A")
        cell = ws_sum.cell(row=row, column=2 + len(ANALYSIS_YEARS), value=val)
        cell.font = Font(name="Arial", bold=True, size=10)
        cell.alignment = Alignment(horizontal="center")
        cell.border = THIN_BORDER
        cell.fill = fill

    # --- Publications sheet ---
    ws_pubs = wb.create_sheet("All Publications")

    pub_headers = ["PMID", "Year", "Title", "Journal", "DOI", "RCR", "NIH Percentile", "Citations", "Research", "Clinical"]
    pub_widths = [12, 8, 55, 25, 25, 10, 16, 12, 10, 10]

    for col_idx, (header, width) in enumerate(zip(pub_headers, pub_widths), start=1):
        cell = ws_pubs.cell(row=1, column=col_idx, value=header)
        cell.font = HEADER_FONT
        cell.fill = HEADER_FILL
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        cell.border = THIN_BORDER
        ws_pubs.column_dimensions[get_column_letter(col_idx)].width = width

    sorted_pubs = sorted(icite_data.values(), key=lambda p: (p.get("year", 0), p.get("relative_citation_ratio") or 0), reverse=True)

    for i, pub in enumerate(sorted_pubs):
        row = 2 + i
        rcr = pub.get("relative_citation_ratio")
        pct = pub.get("nih_percentile")
        values = [
            int(pub.get("pmid", 0)),
            pub.get("year", ""),
            pub.get("title", ""),
            pub.get("journal", ""),
            pub.get("doi", ""),
            round(rcr, 2) if rcr is not None else "N/A",
            round(pct, 1) if pct is not None else "N/A",
            pub.get("citation_count", "N/A"),
            "Yes" if pub.get("is_research_article") else "No",
            "Yes" if pub.get("is_clinical") else "No",
        ]
        fill = ALT_FILL if i % 2 == 0 else PatternFill()
        for col_idx, val in enumerate(values, start=1):
            cell = ws_pubs.cell(row=row, column=col_idx, value=val)
            cell.font = BODY_FONT
            cell.border = THIN_BORDER
            cell.fill = fill
            if col_idx in (1, 2, 6, 7, 8, 9, 10):
                cell.alignment = Alignment(horizontal="center", vertical="top")
            else:
                cell.alignment = Alignment(wrap_text=True, vertical="top")

    ws_pubs.freeze_panes = "A2"
    ws_pubs.auto_filter.ref = f"A1:J{1 + len(sorted_pubs)}"

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------
def run_institution_pipeline(institution_names, email, api_key, progress_bar, status_text):
    """Run the full pipeline for one set of institution names."""
    all_pmids = {}  # year -> list of pmid strings

    # Step 1: Search PubMed for each year
    for i, year in enumerate(ANALYSIS_YEARS):
        status_text.text(f"Searching PubMed for {year} publications...")
        pmids = search_institution_pmids(
            institution_names, year, email, api_key,
            status_fn=lambda msg: status_text.text(msg),
        )
        all_pmids[year] = pmids
        progress_bar.progress((i + 1) / (len(ANALYSIS_YEARS) + 2))

    # Step 2: Deduplicate and fetch iCite
    unique_pmids = list(set(p for year_pmids in all_pmids.values() for p in year_pmids))
    status_text.text(f"Fetching iCite data for {len(unique_pmids):,} unique publications...")

    if not unique_pmids:
        return None, None, None, {}

    icite_data = fetch_icite(
        unique_pmids,
        status_fn=lambda msg: status_text.text(msg),
    )
    progress_bar.progress((len(ANALYSIS_YEARS) + 1) / (len(ANALYSIS_YEARS) + 2))

    # Step 3: Compute metrics
    status_text.text("Computing metrics...")
    metrics_by_year = {}
    for year in ANALYSIS_YEARS:
        year_icite = {pmid: icite_data[pmid] for pmid in all_pmids[year] if pmid in icite_data}
        metrics_by_year[year] = compute_institution_metrics(year_icite)

    combined_icite = {pmid: pub for pmid, pub in icite_data.items()}
    combined_metrics = compute_institution_metrics(combined_icite)

    progress_bar.progress(1.0)

    return metrics_by_year, combined_metrics, icite_data, all_pmids


# ===========================================================================
# Streamlit UI
# ===========================================================================
st.set_page_config(page_title="Institution RCR Analyzer", page_icon="\U0001f3db", layout="wide")

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Libre+Baskerville:wght@400;700&family=Nunito+Sans:wght@400;500;600;700&display=swap');

    .stApp { background-color: #fafbfc; }

    .main-header {
        font-family: 'Libre Baskerville', Georgia, serif;
        font-size: 2.4rem; font-weight: 700; color: #1B4F72;
        margin-bottom: 0.2rem; letter-spacing: -0.01em;
    }
    .sub-header {
        font-family: 'Nunito Sans', sans-serif;
        font-size: 1.05rem; color: #5D6D7E; margin-bottom: 0.3rem; font-weight: 400;
    }
    .detail-text {
        font-family: 'Nunito Sans', sans-serif;
        font-size: 0.88rem; color: #85929E; margin-bottom: 2rem; line-height: 1.5;
    }
    .section-label {
        font-family: 'Nunito Sans', sans-serif;
        font-size: 0.75rem; font-weight: 700; text-transform: uppercase;
        letter-spacing: 0.08em; color: #85929E; margin-bottom: 0.5rem;
    }
    .stat-card {
        background: white; border: 1px solid #D5DBDB; border-radius: 12px;
        padding: 1.25rem 1.5rem; text-align: center;
    }
    .stat-value {
        font-family: 'Libre Baskerville', Georgia, serif;
        font-size: 1.8rem; font-weight: 700; color: #1B4F72;
    }
    .stat-label {
        font-family: 'Nunito Sans', sans-serif;
        font-size: 0.78rem; color: #85929E; text-transform: uppercase;
        letter-spacing: 0.05em; margin-top: 0.25rem;
    }
    div[data-testid="stDataFrame"] { border: 1px solid #D5DBDB; border-radius: 8px; }

    button[data-baseweb="tab"] {
        font-family: 'Nunito Sans', sans-serif !important;
        font-size: 1rem !important; font-weight: 500 !important;
        color: #5D6D7E !important; padding: 0.75rem 1.25rem !important;
    }
    button[data-baseweb="tab"][aria-selected="true"] {
        color: #1B4F72 !important; font-weight: 700 !important;
    }
    button[data-baseweb="tab"]:hover { color: #1B4F72 !important; }
    div[data-baseweb="tab-list"] { gap: 0.5rem; }
</style>
""", unsafe_allow_html=True)

st.markdown('<div class="main-header">Institution RCR Analyzer</div>', unsafe_allow_html=True)
st.markdown('<div class="sub-header">Analyze publication impact metrics for academic medical centers, hospitals, and schools of medicine.</div>', unsafe_allow_html=True)
st.markdown(
    f'<div class="detail-text">'
    f'This site queries PubMed for all publications listing the institution under any author\'s affiliation '
    f'during the last two full calendar years ({ANALYSIS_YEARS[0]} and {ANALYSIS_YEARS[1]}). '
    f'It then retrieves NIH iCite data to calculate institution-level metrics including the total RCR sum, '
    f'mean RCR per publication, and median RCR per publication.'
    f'</div>',
    unsafe_allow_html=True,
)

# --- Sidebar ---
with st.sidebar:
    st.markdown('<div class="section-label">NCBI Credentials</div>', unsafe_allow_html=True)
    st.caption("Required by NCBI for E-utilities access. Your credentials are not stored.")
    email = st.text_input("Email address", placeholder="you@university.edu")
    api_key = st.text_input("NCBI API key (optional)", type="password", placeholder="Paste your key here")
    if api_key:
        st.success("API key provided \u2014 requests at 10/sec")
    else:
        st.info("No API key \u2014 requests limited to 3/sec")
    st.markdown("---")
    st.markdown('<div class="section-label">How to get an API key</div>', unsafe_allow_html=True)
    st.caption("1. Sign in at [ncbi.nlm.nih.gov](https://www.ncbi.nlm.nih.gov/myncbi/)\n2. Go to Account Settings\n3. Scroll to API Key Management\n4. Click Create API Key")
    st.markdown("---")
    st.markdown('<div class="section-label">About the metrics</div>', unsafe_allow_html=True)
    st.caption(
        "**RCR (Relative Citation Ratio)**: A field-normalized metric where 1.0 = average for the field. "
        "An RCR of 2.0 means the paper is cited twice as much as expected.\n\n"
        "**NIH Percentile**: Where the paper falls among all NIH-funded publications of the same age.\n\n"
        "**Provisional**: Newer papers may have provisional RCR scores that can change as citations accumulate."
    )

# --- Session state ---
if "institutions" not in st.session_state:
    st.session_state.institutions = []

# --- Input ---
st.markdown('<div class="section-label">ADD INSTITUTIONS</div>', unsafe_allow_html=True)

st.caption(
    "Enter institution names exactly as they appear in PubMed author affiliations. "
    "Multiple name variants for the **same institution** should be entered together (they will be ORed). "
    "Use \"Add institution\" to add separate institutions for comparison."
)

col_name, col_variants = st.columns([1, 2])
with col_name:
    inst_label = st.text_input("Institution label", placeholder="e.g. Utopia Medical Center", key="inst_label")
with col_variants:
    inst_variants = st.text_input(
        "Affiliation name(s) \u2014 comma-separated variants",
        placeholder="e.g. Utopia Medical Center, Utopia School of Medicine, Utopia Hospital",
        key="inst_variants",
    )

if st.button("\u2795 Add institution"):
    if inst_label.strip() and inst_variants.strip():
        variants = [v.strip() for v in inst_variants.split(",") if v.strip()]
        st.session_state.institutions.append({"label": inst_label.strip(), "variants": variants})
        st.rerun()
    else:
        st.warning("Please enter both a label and at least one affiliation name.")

st.markdown("")
st.caption("**Tip:** Paste multiple institutions at once \u2014 one per line. Format: `Label; Variant1, Variant2, ...`")
bulk_text = st.text_area(
    "Bulk entry (optional)",
    placeholder="Utopia Medical Center; Utopia Medical Center, Utopia School of Medicine\nSpringfield General Hospital; Springfield General, Springfield Medical School",
    height=100, key="bulk_input",
)
if st.button("\u2795 Add all from bulk entry"):
    added = 0
    for line in bulk_text.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        if ";" in line:
            parts = line.split(";", 1)
            label = parts[0].strip()
            variants = [v.strip() for v in parts[1].split(",") if v.strip()]
        else:
            label = line
            variants = [line]
        if label and variants:
            st.session_state.institutions.append({"label": label, "variants": variants})
            added += 1
    if added:
        st.rerun()

# --- Institution list ---
institutions = st.session_state.institutions

if institutions:
    st.markdown("---")
    st.markdown(f'<div class="section-label">INSTITUTIONS TO ANALYZE \u2014 {len(institutions)} TOTAL</div>', unsafe_allow_html=True)

    preview_data = []
    for i, inst in enumerate(institutions):
        preview_data.append({
            "#": i + 1,
            "Label": inst["label"],
            "Affiliation Variants": ", ".join(inst["variants"]),
        })
    st.dataframe(preview_data, use_container_width=True, hide_index=True)

    col_clear, col_run = st.columns([1, 3])
    with col_clear:
        if st.button("\U0001f5d1\ufe0f Clear list", use_container_width=True):
            st.session_state.institutions = []
            st.rerun()
    with col_run:
        run_disabled = not email
        if not email:
            st.warning("Enter your email in the sidebar to run.")
        run_clicked = st.button(
            "\U0001f680 Run Analysis", type="primary",
            use_container_width=True, disabled=run_disabled,
        )

    if run_clicked and email:
        st.markdown("---")

        all_results = []

        for inst_idx, inst in enumerate(institutions):
            st.markdown(f'<div class="section-label">ANALYZING: {inst["label"]}</div>', unsafe_allow_html=True)
            progress_bar = st.progress(0)
            status_text = st.empty()

            metrics_by_year, combined_metrics, icite_data, pmids_by_year = run_institution_pipeline(
                inst["variants"], email, api_key.strip() if api_key else None,
                progress_bar, status_text,
            )

            if combined_metrics is None:
                st.warning(f"No publications found for {inst['label']}.")
                continue

            status_text.text(f"{inst['label']}: Done!")

            all_results.append({
                "label": inst["label"],
                "variants": inst["variants"],
                "metrics_by_year": metrics_by_year,
                "combined": combined_metrics,
                "icite_data": icite_data,
                "pmids_by_year": pmids_by_year,
            })

        if all_results:
            # --- Results display ---
            st.markdown("---")
            st.markdown('<div class="main-header" style="font-size:1.6rem;">Results</div>', unsafe_allow_html=True)
            st.markdown(
                f'<div class="detail-text">Analysis period: {ANALYSIS_YEARS[0]}\u2013{ANALYSIS_YEARS[-1]}</div>',
                unsafe_allow_html=True,
            )

            # Comparison table if multiple institutions
            if len(all_results) > 1:
                st.markdown('<div class="section-label">INSTITUTION COMPARISON</div>', unsafe_allow_html=True)
                comparison = []
                for r in all_results:
                    comparison.append({
                        "Institution": r["label"],
                        "Total Pubs": r["combined"]["total_publications"],
                        "RCR Sum": r["combined"]["rcr_sum"],
                        "Mean RCR": r["combined"]["rcr_mean"],
                        "Median RCR": r["combined"]["rcr_median"],
                        "Mean Percentile": r["combined"]["percentile_mean"],
                        "Total Citations": r["combined"]["citation_total"],
                    })
                st.dataframe(comparison, use_container_width=True, hide_index=True)
                st.markdown("")

            # Per-institution details
            for r in all_results:
                st.markdown(f'### {r["label"]}')

                # Stat cards
                cm = r["combined"]
                c1, c2, c3, c4, c5 = st.columns(5)
                with c1:
                    st.markdown(f'<div class="stat-card"><div class="stat-value">{cm["total_publications"]:,}</div><div class="stat-label">Publications</div></div>', unsafe_allow_html=True)
                with c2:
                    st.markdown(f'<div class="stat-card"><div class="stat-value">{cm["rcr_sum"]:,.1f}</div><div class="stat-label">RCR Sum</div></div>', unsafe_allow_html=True)
                with c3:
                    st.markdown(f'<div class="stat-card"><div class="stat-value">{cm["rcr_mean"]:.2f}</div><div class="stat-label">Mean RCR</div></div>', unsafe_allow_html=True)
                with c4:
                    st.markdown(f'<div class="stat-card"><div class="stat-value">{cm["rcr_median"]:.2f}</div><div class="stat-label">Median RCR</div></div>', unsafe_allow_html=True)
                with c5:
                    st.markdown(f'<div class="stat-card"><div class="stat-value">{cm["percentile_mean"]:.1f}</div><div class="stat-label">Mean Percentile</div></div>', unsafe_allow_html=True)

                st.markdown("")

                # Year breakdown table
                st.markdown('<div class="section-label">YEAR-BY-YEAR BREAKDOWN</div>', unsafe_allow_html=True)
                year_rows = []
                for year in ANALYSIS_YEARS:
                    ym = r["metrics_by_year"].get(year, {})
                    year_rows.append({
                        "Year": year,
                        "Publications": ym.get("total_publications", 0),
                        "With RCR": ym.get("pubs_with_rcr", 0),
                        "RCR Sum": ym.get("rcr_sum", 0),
                        "Mean RCR": ym.get("rcr_mean", 0),
                        "Median RCR": ym.get("rcr_median", 0),
                        "Mean Percentile": ym.get("percentile_mean", 0),
                        "Provisional": ym.get("provisional_rcr_count", 0),
                    })
                # Add combined row
                year_rows.append({
                    "Year": "Combined",
                    "Publications": cm["total_publications"],
                    "With RCR": cm["pubs_with_rcr"],
                    "RCR Sum": cm["rcr_sum"],
                    "Mean RCR": cm["rcr_mean"],
                    "Median RCR": cm["rcr_median"],
                    "Mean Percentile": cm["percentile_mean"],
                    "Provisional": cm["provisional_rcr_count"],
                })
                st.dataframe(year_rows, use_container_width=True, hide_index=True)

                st.markdown("")

            # --- Downloads ---
            st.markdown("---")
            st.markdown('<div class="section-label">DOWNLOADS</div>', unsafe_allow_html=True)

            cols = st.columns(min(len(all_results), 3))
            for i, r in enumerate(all_results):
                with cols[i % 3]:
                    xlsx_buf = write_institution_xlsx(
                        r["label"], r["icite_data"],
                        r["metrics_by_year"], r["combined"],
                    )
                    safe_name = re.sub(r'[^\w\s-]', '', r["label"]).strip().replace(' ', '_')
                    st.download_button(
                        label=f"\U0001f4e5 {r['label']}",
                        data=xlsx_buf,
                        file_name=f"{safe_name}_RCR_Report.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key=f"dl_inst_{i}",
                    )
