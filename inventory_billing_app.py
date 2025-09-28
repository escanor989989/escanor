# inventory_billing_app.py
# Streamlit + SQLite Inventory & Billing System
# - Seeds items & rates from XLSM (INVOICE sheet bottom list)
# - Dedupe by item + UPSERT on seed (no UNIQUE errors)
# - Loader accepts numeric-text rates (fixes "blank items" issue)
# - Billing & Inventory pages PRE-POPULATE rows for ALL master items
# - Report export bug fixed (writer.book + buffer.getvalue())
# - UI: background image / animated gradient, subtle animations, balloons/snow
# - Streamlit deprecation fixed: use width="stretch" instead of use_container_width

import os, io, base64, sqlite3, datetime as dt
from typing import List, Tuple
import pandas as pd
import streamlit as st

# ---------------- Config ----------------
DB_PATH   = os.environ.get("INV_BILL_DB",  "inventory_billing.db")
XLSM_PATH = os.environ.get("INV_BILL_XLSM", "DAY REPORT 28.09.2025.xlsm")

# Optional background image: put file next to script or set BACKGROUND_IMAGE_URL
BACKGROUND_IMAGE_URL = os.environ.get("BACKGROUND_IMAGE_URL", "")
LOCAL_BG_CANDIDATES  = ["bg.jpg", "bg.jpeg", "bg.png", "background.jpg", "background.png"]

st.set_page_config(page_title="Inventory & Billing System", page_icon="🧮", layout="wide")

# ---------------- Helpers: Background & CSS ----------------
def _local_bg_b64() -> str:
    for fname in LOCAL_BG_CANDIDATES:
        if os.path.exists(fname):
            with open(fname, "rb") as f:
                return base64.b64encode(f.read()).decode()
    return ""

def inject_css():
    b64 = _local_bg_b64()
    if BACKGROUND_IMAGE_URL:
        bg_css = f"background: url('{BACKGROUND_IMAGE_URL}') no-repeat center center/cover fixed;"
    elif b64:
        bg_css = f"background: url('data:image/jpg;base64,{b64}') no-repeat center center/cover fixed;"
    else:
        bg_css = ("background: linear-gradient(120deg, #111827 0%, #0f172a 50%, #111827 100%) fixed;"
                  "background-size: 400% 400%; animation: bgshift 22s ease infinite;")

    st.markdown(f"""
    <style>
    .stApp {{ {bg_css} }}
    .block-container {{
        backdrop-filter: blur(6px);
        background: rgba(17, 24, 39, 0.35);
        border-radius: 14px;
        padding: 1.25rem 1.25rem 2rem;
        animation: fadeInUp 600ms ease 40ms both;
    }}
    h1.title {{
        font-size: 1.9rem; margin: .25rem 0 .5rem 0; letter-spacing: .4px;
        text-shadow: 0 2px 12px rgba(14,165,233,.35); animation: float 7s ease-in-out infinite;
    }}
    .subtitle {{ color:#cbd5e1; margin-top:-6px; animation: fadeIn 900ms ease both; }}
    .kpi {{ padding:10px 14px; border:1px solid rgba(229,231,235,.2); border-radius:14px;
            background: rgba(250,250,250,.08); transition:.2s; animation:pulse 1800ms ease-in-out infinite; }}
    .kpi:hover {{ transform: translateY(-2px); box-shadow: 0 10px 25px rgba(0,0,0,.25); }}
    .stButton>button {{
        border-radius: 12px; border:1px solid rgba(14,165,233,.35); box-shadow:0 8px 18px rgba(14,165,233,.15);
        transition:.12s; backdrop-filter: blur(2px);
    }}
    .stButton>button:hover {{ transform: translateY(-1px) scale(1.01); box-shadow:0 12px 28px rgba(14,165,233,.25);
                              background: rgba(2,132,199,.12); }}
    .stApp:before {{
        content:""; position: fixed; inset:0; pointer-events:none; z-index:0;
        background: radial-gradient(600px 200px at 10% 10%, rgba(14,165,233,.18), transparent 60%),
                    radial-gradient(500px 300px at 90% 30%, rgba(99,102,241,.15), transparent 60%);
        animation: float 12s ease-in-out infinite;
    }}
    @keyframes bgshift {{ 0%{{background-position:0% 50%}} 50%{{background-position:100% 50%}} 100%{{background-position:0% 50%}} }}
    @keyframes fadeInUp {{ 0%{{opacity:0;transform:translateY(10px)}} 100%{{opacity:1;transform:translateY(0)}} }}
    @keyframes fadeIn {{ from{{opacity:0}} to{{opacity:1}} }}
    @keyframes float {{ 0%{{transform:translateY(0)}} 50%{{transform:translateY(-4px)}} 100%{{transform:translateY(0)}} }}
    @keyframes pulse {{ 0%{{box-shadow:0 0 0 rgba(14,165,233,.10)}} 50%{{box-shadow:0 0 20px rgba(14,165,233,.25)}} 100%{{box-shadow:0 0 0 rgba(14,165,233,.10)}} }}
    </style>
    """, unsafe_allow_html=True)

inject_css()

# ---------------- DB ----------------
def get_conn():
    conn = sqlite3.connect(DB_PATH, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn

def init_db():
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS items(
            item TEXT PRIMARY KEY,
            rate REAL NOT NULL
        );""")
        cur.execute("""
        CREATE TABLE IF NOT EXISTS inventory_movements(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date DATE NOT NULL,
            item TEXT NOT NULL REFERENCES items(item) ON UPDATE CASCADE ON DELETE RESTRICT,
            opening_balance REAL DEFAULT 0,
            stock_in REAL DEFAULT 0,
            stock_out REAL DEFAULT 0,
            stock_returning_today REAL DEFAULT 0,
            closing_balance AS (opening_balance + stock_in - stock_out + stock_returning_today) STORED,
            stock_remaining AS (closing_balance) STORED
        );""")
        cur.execute("""
        CREATE TABLE IF NOT EXISTS invoices(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date DATE NOT NULL,
            person_name TEXT NOT NULL,
            total_amount REAL NOT NULL DEFAULT 0,
            collection_amount REAL NOT NULL DEFAULT 0,
            due_amount REAL NOT NULL DEFAULT 0,
            notes TEXT
        );""")
        cur.execute("""
        CREATE TABLE IF NOT EXISTS invoice_lines(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            invoice_id INTEGER NOT NULL REFERENCES invoices(id) ON DELETE CASCADE,
            line_no INTEGER NOT NULL,
            item TEXT NOT NULL REFERENCES items(item),
            unit_price REAL NOT NULL,
            qty REAL NOT NULL,
            units TEXT,
            amount AS (unit_price * qty) STORED
        );""")
        cur.execute("""
        CREATE TABLE IF NOT EXISTS collections(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date DATE NOT NULL,
            amount REAL NOT NULL,
            note TEXT
        );""")
        conn.commit()

# ---------------- Import from XLSM ----------------
def load_items_from_xlsm(xlsm_path: str) -> pd.DataFrame:
    """Parse items & rates from INVOICE sheet; accept numeric text; dedupe by item."""
    try:
        xls = pd.ExcelFile(xlsm_path)
        df = pd.read_excel(xls, sheet_name="INVOICE", header=None)

        items = df[[2, 3]].dropna(how="all")
        items = items[items[2].apply(lambda x: isinstance(x, str) and x.strip() != "")]

        def parse_rate(val):
            try:
                if pd.isna(val): return None
                if isinstance(val, str):
                    s = val.strip().replace(",", "")
                    if s == "": return None
                    return float(s)
                return float(val)
            except Exception:
                return None

        items = items.rename(columns={2: "item", 3: "rate"})
        items["item"] = items["item"].astype(str).str.strip()
        items["rate"] = items["rate"].apply(parse_rate)

        items = items.dropna(subset=["item", "rate"])
        items = items[items["item"].str.len() > 0]
        items = items.groupby("item", as_index=False).agg(rate=("rate", "last"))
        return items.reset_index(drop=True)
    except Exception as e:
        st.warning(f"Couldn't auto-import items from XLSM: {e}")
        return pd.DataFrame(columns=["item", "rate"])

def ensure_seed_items():
    with get_conn() as conn:
        n = conn.execute("SELECT COUNT(*) FROM items;").fetchone()[0]
        if n == 0 and os.path.exists(XLSM_PATH):
            items = load_items_from_xlsm(XLSM_PATH)
            if len(items):
                for _, r in items.iterrows():
                    conn.execute(
                        "INSERT INTO items(item, rate) VALUES(?, ?) "
                        "ON CONFLICT(item) DO UPDATE SET rate=excluded.rate;",
                        (str(r["item"]).strip(), float(r["rate"]))
                    )
                conn.commit()
                st.success(f"Loaded {len(items)} items & rates from '{XLSM_PATH}'.")
        elif n == 0:
            st.info("No items found. Add in 'Master Data' or set INV_BILL_XLSM to auto-import.")

def upsert_item(item: str, rate: float):
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO items(item, rate) VALUES(?, ?) "
            "ON CONFLICT(item) DO UPDATE SET rate=excluded.rate;",
            (item.strip(), float(rate))
        )
        conn.commit()

def get_items_df() -> pd.DataFrame:
    with get_conn() as conn:
        return pd.read_sql_query("SELECT item, rate FROM items ORDER BY item;", conn)

# ---------------- Business ops ----------------
def create_invoice(date: dt.date, person_name: str, lines: List[dict],
                   collection_amount: float, notes: str = "") -> int:
    total = sum((float(l.get("unit_price", 0)) * float(l.get("qty", 0))) for l in lines)
    due = total - float(collection_amount or 0)

    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO invoices(date, person_name, total_amount, collection_amount, due_amount, notes) "
            "VALUES(?,?,?,?,?,?);",
            (date.isoformat(), person_name.strip(), total, collection_amount, due, notes)
        )
        inv_id = cur.lastrowid

        for i, line in enumerate(lines, start=1):
            if not line.get("item"): continue
            cur.execute(
                "INSERT INTO invoice_lines(invoice_id, line_no, item, unit_price, qty, units) "
                "VALUES(?,?,?,?,?,?);",
                (inv_id, i, line["item"], float(line.get("unit_price") or 0),
                 float(line.get("qty") or 0), line.get("units", ""))
            )

        if collection_amount and collection_amount != 0:
            cur.execute(
                "INSERT INTO collections(date, amount, note) VALUES(?,?,?);",
                (date.isoformat(), float(collection_amount), f"Invoice #{inv_id} - {person_name}")
            )
        conn.commit()
    return inv_id

def add_inventory_movement(date: dt.date, rows: List[dict]):
    with get_conn() as conn:
        cur = conn.cursor()
        for r in rows:
            if not r.get("item"): continue
            cur.execute(
                "INSERT INTO inventory_movements(date, item, opening_balance, stock_in, stock_out, stock_returning_today) "
                "VALUES(?,?,?,?,?,?);",
                (date.isoformat(), r.get("item"),
                 float(r.get("opening_balance") or 0.0),
                 float(r.get("stock_in") or 0.0),
                 float(r.get("stock_out") or 0.0),
                 float(r.get("stock_returning_today") or 0.0))
            )
        conn.commit()

def df_to_csv_download(df: pd.DataFrame, filename: str) -> Tuple[bytes, str]:
    return df.to_csv(index=False).encode("utf-8"), filename

def df_to_excel_download(df: pd.DataFrame, filename: str) -> Tuple[bytes, str]:
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="Data")
    return buf.getvalue(), filename

# ---------------- UI ----------------
st.markdown("<h1 class='title'>🧮 Inventory & Billing System</h1>", unsafe_allow_html=True)
st.markdown("<div class='subtitle'>SQLite-backed • Single-file • Mirrors XLSM structure • With animations ✨</div>", unsafe_allow_html=True)

init_db()
ensure_seed_items()

PAGES = ["Billing", "Inventory", "Data Extraction", "Reports", "Master Data", "About"]
page = st.sidebar.radio("Navigation", PAGES)

# ---------------- Billing (pre-populated) ----------------
if page == "Billing":
    st.subheader("Create Bill / Invoice")
    c1, c2, c3 = st.columns([1, 1, 2])
    with c1:
        bill_date = st.date_input("Date", value=dt.date.today())
    with c2:
        person_name = st.text_input("Person Name", placeholder="Enter name...")
    with c3:
        notes = st.text_input("Notes (optional)", placeholder="Any remarks...")

    items_df = get_items_df()
    item_names = items_df["item"].tolist()
    rate_map = dict(zip(items_df["item"], items_df["rate"]))

    st.markdown("#### Line Items (auto-listed from Master Data)")
    if len(items_df) > 0:
        default_rows = [
            {"item": row["item"], "unit_price": float(row["rate"] or 0), "qty": 0.0, "units": "UNITS"}
            for _, row in items_df.iterrows()
        ]
    else:
        default_rows = [{"item": "", "unit_price": 0.0, "qty": 0.0, "units": "UNITS"} for _ in range(10)]

    edited = st.data_editor(
        pd.DataFrame(default_rows),
        column_config={
            "item": st.column_config.SelectboxColumn("ITEM NAME", options=item_names, required=False, width="large"),
            "unit_price": st.column_config.NumberColumn("UNIT PRICE", step=0.01, format="%.2f"),
            "qty": st.column_config.NumberColumn("QTY", step=1.0, format="%.2f"),
            "units": st.column_config.TextColumn("UNIT"),
        },
        num_rows="dynamic",
        width="stretch",            # <— updated
        key="billing_editor",
    )

    def autofill_prices(df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        for idx, row in out.iterrows():
            it = str(row.get("item") or "").strip()
            if it and (not row.get("unit_price") or float(row["unit_price"]) == 0.0):
                if it in rate_map:
                    out.at[idx, "unit_price"] = float(rate_map[it])
        return out

    edited = autofill_prices(edited)
    edited["amount"] = (edited["unit_price"].astype(float).fillna(0) * edited["qty"].astype(float).fillna(0)).round(2)
    st.dataframe(edited, width="stretch")   # <— updated

    total_amount = float(edited["amount"].sum())
    cA, cB, cC = st.columns(3)
    with cA: st.metric("Total Amount", f"{total_amount:,.2f}")
    with cB:
        collection_amount = st.number_input("Collection Amount (today)", min_value=0.0, value=0.0, step=1.0)
    with cC: st.metric("Due Amount", f"{max(total_amount - collection_amount, 0.0):,.2f}")

    if st.button("✅ Save Invoice"):
        if not person_name.strip():
            st.error("Person Name is required.")
        else:
            lines = []
            for _, r in edited.iterrows():
                it = str(r.get("item") or "").strip()
                qty = float(r.get("qty") or 0)
                if not it or qty == 0:
                    continue
                lines.append({
                    "item": it,
                    "unit_price": float(r.get("unit_price") or 0),
                    "qty": qty,
                    "units": str(r.get("units") or ""),
                })
            if len(lines) == 0:
                st.error("Enter quantity for at least one item.")
            else:
                inv_id = create_invoice(bill_date, person_name, lines, collection_amount, notes)
                st.success(f"Invoice #{inv_id} saved for {person_name}.")
                st.balloons()
                inv_df = pd.DataFrame(lines)
                inv_df["person_name"] = person_name
                inv_df["date"] = bill_date
                inv_df["total"] = total_amount
                csv_bytes, fname = df_to_csv_download(inv_df, f"invoice_{inv_id}.csv")
                st.download_button("⬇️ Download Invoice CSV", data=csv_bytes, file_name=fname, mime="text/csv")

# ---------------- Inventory (pre-populated) ----------------
elif page == "Inventory":
    st.subheader("Inventory Movements")
    date_val = st.date_input("Date", value=dt.date.today(), key="inv_date")

    items_df = get_items_df()
    item_names = items_df["item"].tolist()

    st.markdown("Enter movements; rows auto-listed from Master Data. Closing/Remaining auto-computed.")
    if len(items_df) > 0:
        default_rows = [{
            "item": row["item"],
            "opening_balance": 0.0,
            "stock_in": 0.0,
            "stock_out": 0.0,
            "stock_returning_today": 0.0,
        } for _, row in items_df.iterrows()]
    else:
        default_rows = [{
            "item": "",
            "opening_balance": 0.0,
            "stock_in": 0.0,
            "stock_out": 0.0,
            "stock_returning_today": 0.0,
        } for _ in range(10)]

    inv_edit = st.data_editor(
        pd.DataFrame(default_rows),
        column_config={
            "item": st.column_config.SelectboxColumn("ITEM", options=item_names, required=False, width="large"),
            "opening_balance": st.column_config.NumberColumn("OPENING STOCK BALANCE", step=1.0, format="%.2f"),
            "stock_in": st.column_config.NumberColumn("STOCK IN", step=1.0, format="%.2f"),
            "stock_out": st.column_config.NumberColumn("STOCK OUT", step=1.0, format="%.2f"),
            "stock_returning_today": st.column_config.NumberColumn("STOCK RETURNING TODAY", step=1.0, format="%.2f"),
        },
        num_rows="dynamic",
        width="stretch",           # <— updated
        key="inv_editor",
    )

    inv_preview = inv_edit.copy()
    for col in ["opening_balance", "stock_in", "stock_out", "stock_returning_today"]:
        inv_preview[col] = inv_preview[col].astype(float).fillna(0.0)
    inv_preview["closing_balance"] = (
        inv_preview["opening_balance"] + inv_preview["stock_in"] - inv_preview["stock_out"] + inv_preview["stock_returning_today"]
    ).round(2)
    inv_preview["stock_remaining"] = inv_preview["closing_balance"]
    st.dataframe(inv_preview, width="stretch")   # <— updated

    if st.button("💾 Save Movements"):
        rows = []
        for _, r in inv_edit.iterrows():
            it = str(r.get("item") or "").strip()
            if not it: continue
            rows.append({
                "item": it,
                "opening_balance": float(r.get("opening_balance") or 0),
                "stock_in": float(r.get("stock_in") or 0),
                "stock_out": float(r.get("stock_out") or 0),
                "stock_returning_today": float(r.get("stock_returning_today") or 0),
            })
        if len(rows) == 0:
            st.error("Add at least one item row.")
        else:
            add_inventory_movement(date_val, rows)
            st.success(f"Saved {len(rows)} movement rows for {date_val}.")
            st.snow()

# ---------------- Data Extraction ----------------
elif page == "Data Extraction":
    st.subheader("Data Extraction")
    tab1, tab2, tab3 = st.tabs(["Invoices & Lines", "Inventory", "Collections"])

    with tab1:
        c1, c2 = st.columns(2)
        with c1: d1 = st.date_input("From Date", value=dt.date.today().replace(day=1))
        with c2: d2 = st.date_input("To Date", value=dt.date.today())
        cols = st.multiselect("Columns",
                              ["id","date","person_name","total_amount","collection_amount","due_amount","notes"],
                              default=["id","date","person_name","total_amount","collection_amount","due_amount"])
        with get_conn() as conn:
            q = "SELECT " + ", ".join(cols) + " FROM invoices WHERE date BETWEEN ? AND ? ORDER BY date, id;"
            df = pd.read_sql_query(q, conn, params=(d1.isoformat(), d2.isoformat()))
        st.dataframe(df, width="stretch")   # <— updated
        csvg, fn = df_to_csv_download(df, "invoices.csv")
        st.download_button("⬇️ Download CSV", csvg, file_name=fn, mime="text/csv")
        xlsxg, fnx = df_to_excel_download(df, "invoices.xlsx")
        st.download_button("⬇️ Download Excel", xlsxg, file_name=fnx)

        st.markdown("**Invoice Lines**")
        with get_conn() as conn:
            ql = (
                "SELECT il.invoice_id, i.date, i.person_name, il.line_no, il.item, il.unit_price, il.qty, il.amount "
                "FROM invoice_lines il JOIN invoices i ON i.id = il.invoice_id "
                "WHERE i.date BETWEEN ? AND ? ORDER BY il.invoice_id, il.line_no;"
            )
            dfl = pd.read_sql_query(ql, conn, params=(d1.isoformat(), d2.isoformat()))
        st.dataframe(dfl, width="stretch")  # <— updated
        csvg2, fn2 = df_to_csv_download(dfl, "invoice_lines.csv")
        st.download_button("⬇️ Download Lines CSV", csvg2, file_name=fn2, mime="text/csv")

    with tab2:
        c1, c2 = st.columns(2)
        with c1: d1 = st.date_input("From Date ", value=dt.date.today().replace(day=1), key="inv_from")
        with c2: d2 = st.date_input("To Date  ", value=dt.date.today(), key="inv_to")
        inv_cols = st.multiselect(
            "Columns",
            ["date","item","opening_balance","stock_in","stock_out","stock_returning_today","closing_balance","stock_remaining"],
            default=["date","item","opening_balance","stock_in","stock_out","stock_returning_today","closing_balance","stock_remaining"])
        with get_conn() as conn:
            q = "SELECT " + ", ".join(inv_cols) + " FROM inventory_movements WHERE date BETWEEN ? AND ? ORDER BY date, item;"
            df = pd.read_sql_query(q, conn, params=(d1.isoformat(), d2.isoformat()))
        st.dataframe(df, width="stretch")   # <— updated
        csvg, fn = df_to_csv_download(df, "inventory.csv")
        st.download_button("⬇️ Download CSV", csvg, file_name=fn, mime="text/csv")
        xlsxg, fnx = df_to_excel_download(df, "inventory.xlsx")
        st.download_button("⬇️ Download Excel", xlsxg, file_name=fnx)

    with tab3:
        c1, c2 = st.columns(2)
        with c1: d1 = st.date_input("From Date  ", value=dt.date.today().replace(day=1), key="col_from")
        with c2: d2 = st.date_input("To Date    ", value=dt.date.today(), key="col_to")
        with get_conn() as conn:
            df = pd.read_sql_query(
                "SELECT date, amount, note FROM collections WHERE date BETWEEN ? AND ? ORDER BY date;",
                conn, params=(d1.isoformat(), d2.isoformat()))
        st.dataframe(df, width="stretch")   # <— updated
        csvg, fn = df_to_csv_download(df, "collections.csv")
        st.download_button("⬇️ Download CSV", csvg, file_name=fn, mime="text/csv")

# ---------------- Reports ----------------
elif page == "Reports":
    st.subheader("Reports")
    rep_date = st.date_input("Report Date", value=dt.date.today())

    with get_conn() as conn:
        inv = pd.read_sql_query(
            "SELECT id, person_name, total_amount, collection_amount, due_amount "
            "FROM invoices WHERE date = ? ORDER BY id;",
            conn, params=(rep_date.isoformat(),)
        )
        inv_total = inv["total_amount"].sum() if not inv.empty else 0.0
        inv_coll  = inv["collection_amount"].sum() if not inv.empty else 0.0
        inv_due   = inv["due_amount"].sum() if not inv.empty else 0.0

        inv_mov = pd.read_sql_query(
            "SELECT m.item, m.date, m.closing_balance "
            "FROM inventory_movements m "
            "WHERE date <= ? ORDER BY m.item, m.date;",
            conn, params=(rep_date.isoformat(),)
        )
        latest = (inv_mov.sort_values(["item", "date"])
                        .groupby("item", as_index=False).tail(1)
                        .sort_values("item"))

    c1, c2, c3 = st.columns(3)
    with c1: st.metric("Total Billed (₹)", f"{inv_total:,.2f}")
    with c2: st.metric("Collected Today (₹)", f"{inv_coll:,.2f}")
    with c3: st.metric("Total Due (₹)", f"{inv_due:,.2f}")

    st.markdown("#### Bills Today")
    st.dataframe(inv, width="stretch")      # <— updated

    st.markdown("#### Inventory Snapshot (last closing up to date)")
    st.dataframe(latest.rename(columns={"closing_balance": "stock_remaining"}), width="stretch")  # <— updated

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
        inv.to_excel(writer, index=False, sheet_name="Bills")
        latest.to_excel(writer, index=False, sheet_name="Inventory")
        writer.book.set_properties({'title': f"Report {rep_date}"})
    data = buf.getvalue()
    st.download_button("⬇️ Download Daily Report (Excel)", data=data, file_name=f"report_{rep_date}.xlsx")

# ---------------- Master Data ----------------
elif page == "Master Data":
    st.subheader("Items & Rates")
    st.caption("Seeded from XLSM (INVOICE sheet) on first run if DB was empty. You can add/edit here.")
    df_items = get_items_df()
    st.dataframe(df_items, width="stretch")   # <— updated

    with st.expander("Add / Update Item"):
        c1, c2 = st.columns([3, 1])
        with c1: item = st.text_input("Item Name")
        with c2: rate = st.number_input("Rate", min_value=0.0, step=0.1)
        if st.button("Save Item"):
            if not item.strip():
                st.error("Item name is required.")
            else:
                upsert_item(item, rate)
                st.success(f"Saved rate {rate} for item '{item}'.")
                st.experimental_rerun()

    with st.expander("Import Items from XLSM again"):
        path = st.text_input("XLSM Path", value=XLSM_PATH)
        if st.button("Import Now"):
            if not os.path.exists(path):
                st.error("File not found.")
            else:
                items = load_items_from_xlsm(path)
                if items.empty:
                    st.warning("No items found in the XLSM.")
                else:
                    with get_conn() as conn:
                        for _, r in items.iterrows():
                            conn.execute(
                                "INSERT INTO items(item, rate) VALUES(?, ?) "
                                "ON CONFLICT(item) DO UPDATE SET rate=excluded.rate;",
                                (str(r["item"]).strip(), float(r["rate"]))
                            )
                        conn.commit()
                    st.success(f"Imported/updated {len(items)} items.")

# ---------------- About ----------------
else:
    st.subheader("About")
    st.write("""
- **Billing**: pre-populated with all Master Data items (qty starts at 0; only non-zero qty lines are saved).
- **Inventory**: pre-populated rows for every item; closing/remaining auto.
- **Data Extraction**: date range + column pickers + CSV/Excel exports.
- **Reports**: daily totals + inventory snapshot (export fixed).
- **UI**: background/gradient, animations, balloons/snow on save.
""")
