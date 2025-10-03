# inventory_billing_app.py
# Inventory + Billing (Streamlit + SQLite) — with auto-migrations
# Features:
# - Billing: multiple Collections (Cash/UPI), Dues by Shop, line-item Highlight, strict totals validation
# - Inventory movements
# - Data Extraction & Reports (include highlight, collections, dues)
# - Admin login (env ADMIN_USER/ADMIN_PASS) with CRUD + optional email on login (SMTP_* envs)
# - XLSM import for master items, UPSERT seeding
# - Streamlit width API (width="stretch"), background/animations
# - **NEW:** DB migration: add 'highlight' column & ensure new tables exist

import os, io, base64, sqlite3, smtplib, ssl, datetime as dt
from email.message import EmailMessage
from typing import List, Tuple
import pandas as pd
import streamlit as st

# ---------------- Config ----------------
DB_PATH   = os.environ.get("INV_BILL_DB",  "inventory_billing.db")
XLSM_PATH = os.environ.get("INV_BILL_XLSM", "DAY REPORT 28.09.2025.xlsm")

ADMIN_USER = os.environ.get("ADMIN_USER", "admin")
ADMIN_PASS = os.environ.get("ADMIN_PASS", "admin123")

SMTP_HOST   = os.environ.get("SMTP_HOST", "")
SMTP_PORT   = int(os.environ.get("SMTP_PORT", "587"))  # TLS
SMTP_USER   = os.environ.get("SMTP_USER", "")
SMTP_PASS   = os.environ.get("SMTP_PASS", "")
SMTP_SENDER = os.environ.get("SMTP_SENDER", SMTP_USER or "noreply@example.com")
ADMIN_NOTIFY_EMAIL = "escanor989989@gmail.com"

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
    }}
    h1.title {{
        font-size: 1.9rem; margin: .25rem 0 .5rem 0; letter-spacing: .4px;
        text-shadow: 0 2px 12px rgba(14,165,233,.35);
        animation: float 7s ease-in-out infinite;
    }}
    .subtitle {{ color:#cbd5e1; margin-top:-6px; }}
    @keyframes float {{ 0%{{transform:translateY(0)}} 50%{{transform:translateY(-4px)}} 100%{{transform:translateY(0)}} }}
    </style>
    """, unsafe_allow_html=True)

inject_css()

# ---------------- DB core ----------------
def get_conn():
    conn = sqlite3.connect(DB_PATH, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn

def table_has_column(conn: sqlite3.Connection, table: str, column: str) -> bool:
    cur = conn.execute(f"PRAGMA table_info({table});")
    cols = [r[1] for r in cur.fetchall()]
    return column in cols

def init_db_schema():
    """Create base tables (idempotent)."""
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
            highlight INTEGER NOT NULL DEFAULT 0,
            amount AS (unit_price * qty) STORED
        );""")
        cur.execute("""
        CREATE TABLE IF NOT EXISTS invoice_collections(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            invoice_id INTEGER NOT NULL REFERENCES invoices(id) ON DELETE CASCADE,
            method TEXT NOT NULL CHECK(method IN ('Cash','UPI')),
            amount REAL NOT NULL
        );""")
        cur.execute("""
        CREATE TABLE IF NOT EXISTS invoice_dues(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            invoice_id INTEGER NOT NULL REFERENCES invoices(id) ON DELETE CASCADE,
            shop_no TEXT NOT NULL,
            amount REAL NOT NULL
        );""")
        cur.execute("""
        CREATE TABLE IF NOT EXISTS collections(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date DATE NOT NULL,
            amount REAL NOT NULL,
            note TEXT
        );""")
        conn.commit()

def migrate_db():
    """Add new columns/tables to old databases."""
    with get_conn() as conn:
        # Ensure 'highlight' on invoice_lines
        if not table_has_column(conn, "invoice_lines", "highlight"):
            conn.execute("ALTER TABLE invoice_lines ADD COLUMN highlight INTEGER NOT NULL DEFAULT 0;")

        # Ensure collections/dues tables exist (CREATE IF NOT EXISTS is enough)
        conn.execute("""
        CREATE TABLE IF NOT EXISTS invoice_collections(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            invoice_id INTEGER NOT NULL REFERENCES invoices(id) ON DELETE CASCADE,
            method TEXT NOT NULL CHECK(method IN ('Cash','UPI')),
            amount REAL NOT NULL
        );""")
        conn.execute("""
        CREATE TABLE IF NOT EXISTS invoice_dues(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            invoice_id INTEGER NOT NULL REFERENCES invoices(id) ON DELETE CASCADE,
            shop_no TEXT NOT NULL,
            amount REAL NOT NULL
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
def create_invoice(
    date: dt.date,
    person_name: str,
    lines: List[dict],
    collections: List[dict],  # [{'method': 'Cash'|'UPI', 'amount': float}]
    dues: List[dict],         # [{'shop_no': str, 'amount': float}]
    notes: str = "",
) -> Tuple[bool, str, int]:
    """Validates price math; returns (ok, message, invoice_id)."""
    total = sum((float(l.get("unit_price", 0)) * float(l.get("qty", 0))) for l in lines)
    coll_total = sum(float(c.get("amount", 0)) for c in collections)
    due_total  = sum(float(d.get("amount", 0)) for d in dues)
    ok = abs(total - (coll_total + due_total)) < 0.005

    if not ok:
        msg = f"Total ({total:.2f}) must equal Collections ({coll_total:.2f}) + Dues ({due_total:.2f})."
        return False, msg, -1

    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO invoices(date, person_name, total_amount, collection_amount, due_amount, notes) "
            "VALUES(?,?,?,?,?,?);",
            (date.isoformat(), person_name.strip(), total, coll_total, due_total, notes)
        )
        inv_id = cur.lastrowid

        for i, line in enumerate(lines, start=1):
            if not line.get("item"): continue
            cur.execute(
                "INSERT INTO invoice_lines(invoice_id, line_no, item, unit_price, qty, units, highlight) "
                "VALUES(?,?,?,?,?,?,?);",
                (inv_id, i, line["item"], float(line.get("unit_price") or 0),
                 float(line.get("qty") or 0), line.get("units",""), 1 if line.get("highlight") else 0)
            )

        for c in collections:
            if float(c.get("amount",0)) <= 0: continue
            method = "Cash" if c.get("method") not in ("Cash","UPI") else c["method"]
            cur.execute(
                "INSERT INTO invoice_collections(invoice_id, method, amount) VALUES(?,?,?);",
                (inv_id, method, float(c["amount"]))
            )

        for d in dues:
            if float(d.get("amount",0)) <= 0: continue
            cur.execute(
                "INSERT INTO invoice_dues(invoice_id, shop_no, amount) VALUES(?,?,?);",
                (inv_id, str(d["shop_no"]).strip(), float(d["amount"]))
            )

        # Optional daily aggregate record
        if coll_total != 0:
            cur.execute(
                "INSERT INTO collections(date, amount, note) VALUES(?,?,?);",
                (date.isoformat(), float(coll_total), f"Invoice #{inv_id} - {person_name}")
            )

        conn.commit()
    return True, f"Invoice #{inv_id} saved.", inv_id

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

# ---------------- Auth / Email ----------------
def send_admin_email(subject: str, body: str):
    if not SMTP_HOST or not SMTP_USER or not SMTP_PASS:
        st.warning("SMTP not configured; skipping email notification.")
        return
    try:
        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = SMTP_SENDER
        msg["To"] = ADMIN_NOTIFY_EMAIL
        msg.set_content(body)
        context = ssl.create_default_context()
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls(context=context)
            server.login(SMTP_USER, SMTP_PASS)
            server.send_message(msg)
        st.success("Admin login notification email sent.")
    except Exception as e:
        st.warning(f"Unable to send email: {e}")

def require_admin_auth() -> bool:
    if "authed" not in st.session_state:
        st.session_state.authed = False
    if st.session_state.authed:
        return True
    st.subheader("Admin Login")
    u = st.text_input("User ID")
    p = st.text_input("Password", type="password")
    if st.button("Login"):
        if u == ADMIN_USER and p == ADMIN_PASS:
            st.session_state.authed = True
            st.success("Authenticated.")
            send_admin_email("Admin Login", f"Admin user '{u}' logged in at {dt.datetime.now()}.")
            return True
        else:
            st.error("Invalid credentials.")
    return False

# ---------------- Startup ----------------
def init_all():
    init_db_schema()
    migrate_db()          # <-- run migrations for older DBs
    ensure_seed_items()

st.markdown("<h1 class='title'>🧮 Inventory & Billing System</h1>", unsafe_allow_html=True)
st.markdown("<div class='subtitle'>SQLite-backed • With validations, admin & reports</div>", unsafe_allow_html=True)

init_all()

PAGES = ["Billing", "Inventory", "Data Extraction", "Reports", "Master Data", "Admin"]
page = st.sidebar.radio("Navigation", PAGES)

# ---------------- Billing ----------------
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

    st.markdown("#### Line Items (auto-listed from Master Data) — tick **Highlight** for emphasis")
    if len(items_df) > 0:
        default_rows = [
            {"item": row["item"], "unit_price": float(row["rate"] or 0), "qty": 0.0, "units": "UNITS", "highlight": False}
            for _, row in items_df.iterrows()
        ]
    else:
        default_rows = [{"item": "", "unit_price": 0.0, "qty": 0.0, "units": "UNITS", "highlight": False} for _ in range(10)]

    edited = st.data_editor(
        pd.DataFrame(default_rows),
        column_config={
            "item": st.column_config.SelectboxColumn("ITEM NAME", options=item_names, required=False, width="large"),
            "unit_price": st.column_config.NumberColumn("UNIT PRICE", step=0.01, format="%.2f"),
            "qty": st.column_config.NumberColumn("QTY", step=1.0, format="%.2f"),
            "units": st.column_config.TextColumn("UNIT"),
            "highlight": st.column_config.CheckboxColumn("HIGHLIGHT"),
        },
        num_rows="dynamic",
        width="stretch",
        key="billing_editor",
    )

    # Auto-fill price if user changes item to another one with zero price
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
    st.dataframe(edited, width="stretch")

    total_amount = float(edited["amount"].sum())
    st.metric("Total Amount", f"{total_amount:,.2f}")

    # ---- Collections ----
    st.markdown("### Collections (multiple)")
    coll_default = [{"method": "Cash", "amount": 0.0}]
    coll_df = st.data_editor(
        pd.DataFrame(coll_default),
        column_config={
            "method": st.column_config.SelectboxColumn("METHOD", options=["Cash","UPI"], width="small"),
            "amount": st.column_config.NumberColumn("AMOUNT", step=1.0, format="%.2f"),
        },
        num_rows="dynamic",
        width="stretch",
        key="collections_editor",
    )
    coll_total = float(pd.to_numeric(coll_df["amount"], errors="coerce").fillna(0).sum())
    st.metric("Collections Total", f"{coll_total:,.2f}")

    # ---- Dues ----
    st.markdown("### Dues by Shop (must equal Total − Collections)")
    due_default = [{"shop_no": "", "amount": 0.0}]
    dues_df = st.data_editor(
        pd.DataFrame(due_default),
        column_config={
            "shop_no": st.column_config.TextColumn("SHOP NO."),
            "amount": st.column_config.NumberColumn("AMOUNT", step=1.0, format="%.2f"),
        },
        num_rows="dynamic",
        width="stretch",
        key="dues_editor",
    )
    dues_total = float(pd.to_numeric(dues_df["amount"], errors="coerce").fillna(0).sum())
    st.metric("Dues Total", f"{dues_total:,.2f}")

    expected_due = round(total_amount - coll_total, 2)
    if abs(dues_total - expected_due) > 0.005:
        st.error(f"⚠️ Dues by shop ({dues_total:.2f}) must equal Total − Collections ({expected_due:.2f}).")

    # ---- Save Invoice ----
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
                    "highlight": bool(r.get("highlight", False)),
                })

            colls = []
            for _, r in coll_df.iterrows():
                amt = float(r.get("amount") or 0)
                if amt <= 0: continue
                method = "Cash" if r.get("method") not in ("Cash","UPI") else r["method"]
                colls.append({"method": method, "amount": amt})

            dues = []
            for _, r in dues_df.iterrows():
                amt = float(r.get("amount") or 0)
                shop = str(r.get("shop_no") or "").strip()
                if amt <= 0 or not shop: continue
                dues.append({"shop_no": shop, "amount": amt})

            ok, msg, inv_id = create_invoice(bill_date, person_name, lines, colls, dues, notes)
            if not ok:
                st.error(f"❌ {msg}")
            else:
                st.success(msg)
                st.balloons()

                export_df = pd.DataFrame(lines)
                export_df["person_name"] = person_name
                export_df["date"] = bill_date
                export_df["total"] = total_amount
                csv_bytes, fname = df_to_csv_download(export_df, f"invoice_{inv_id}.csv")
                st.download_button("⬇️ Download Invoice CSV", data=csv_bytes, file_name=fname, mime="text/csv")

# ---------------- Inventory ----------------
elif page == "Inventory":
    st.subheader("Inventory Movements")
    date_val = st.date_input("Date", value=dt.date.today(), key="inv_date")

    items_df = get_items_df()
    item_names = items_df["item"].tolist()

    st.markdown("Enter movements; rows auto-listed from Master Data. Closing/Remaining auto-computed.")
    if len(items_df) > 0:
        default_rows = [{
            "item": row["item"],
            "opening_balance": 0.0, "stock_in": 0.0, "stock_out": 0.0, "stock_returning_today": 0.0,
        } for _, row in items_df.iterrows()]
    else:
        default_rows = [{"item": "", "opening_balance": 0.0, "stock_in": 0.0, "stock_out": 0.0, "stock_returning_today": 0.0}
                        for _ in range(10)]

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
        width="stretch",
        key="inv_editor",
    )

    inv_preview = inv_edit.copy()
    for col in ["opening_balance","stock_in","stock_out","stock_returning_today"]:
        inv_preview[col] = inv_preview[col].astype(float).fillna(0.0)
    inv_preview["closing_balance"] = (
        inv_preview["opening_balance"] + inv_preview["stock_in"] - inv_preview["stock_out"] + inv_preview["stock_returning_today"]
    ).round(2)
    inv_preview["stock_remaining"] = inv_preview["closing_balance"]
    st.dataframe(inv_preview, width="stretch")

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
    tab1, tab2, tab3 = st.tabs(["Invoices & Lines", "Inventory", "Collections & Dues"])

    with tab1:
        c1, c2 = st.columns(2)
        with c1: d1 = st.date_input("From Date", value=dt.date.today().replace(day=1))
        with c2: d2 = st.date_input("To Date", value=dt.date.today())
        cols = st.multiselect(
            "Invoice Columns",
            ["id","date","person_name","total_amount","collection_amount","due_amount","notes"],
            default=["id","date","person_name","total_amount","collection_amount","due_amount"]
        )
        with get_conn() as conn:
            q = "SELECT " + ", ".join(cols) + " FROM invoices WHERE date BETWEEN ? AND ? ORDER BY date, id;"
            df = pd.read_sql_query(q, conn, params=(d1.isoformat(), d2.isoformat()))
        st.dataframe(df, width="stretch")
        st.download_button("⬇️ CSV (Invoices)", *df_to_csv_download(df, "invoices.csv"), mime="text/csv")

        st.markdown("**Invoice Lines (includes highlight flag)**")
        with get_conn() as conn:
            ql = (
                "SELECT il.invoice_id, i.date, i.person_name, il.line_no, il.item, il.unit_price, il.qty, il.amount, il.highlight "
                "FROM invoice_lines il JOIN invoices i ON i.id = il.invoice_id "
                "WHERE i.date BETWEEN ? AND ? ORDER BY il.invoice_id, il.line_no;"
            )
            dfl = pd.read_sql_query(ql, conn, params=(d1.isoformat(), d2.isoformat()))
        st.dataframe(dfl, width="stretch")
        st.download_button("⬇️ CSV (Lines)", *df_to_csv_download(dfl, "invoice_lines.csv"), mime="text/csv")

    with tab2:
        c1, c2 = st.columns(2)
        with c1: d1 = st.date_input("From Date ", value=dt.date.today().replace(day=1), key="inv_from")
        with c2: d2 = st.date_input("To Date  ", value=dt.date.today(), key="inv_to")
        inv_cols = st.multiselect(
            "Inventory Columns",
            ["date","item","opening_balance","stock_in","stock_out","stock_returning_today","closing_balance","stock_remaining"],
            default=["date","item","opening_balance","stock_in","stock_out","stock_returning_today","closing_balance","stock_remaining"])
        with get_conn() as conn:
            q = "SELECT " + ", ".join(inv_cols) + " FROM inventory_movements WHERE date BETWEEN ? AND ? ORDER BY date, item;"
            df = pd.read_sql_query(q, conn, params=(d1.isoformat(), d2.isoformat()))
        st.dataframe(df, width="stretch")
        st.download_button("⬇️ CSV (Inventory)", *df_to_csv_download(df, "inventory.csv"), mime="text/csv")

    with tab3:
        c1, c2 = st.columns(2)
        with c1: d1 = st.date_input("From Date  ", value=dt.date.today().replace(day=1), key="col_from")
        with c2: d2 = st.date_input("To Date    ", value=dt.date.today(), key="col_to")
        with get_conn() as conn:
            dcf = pd.read_sql_query(
                "SELECT ic.invoice_id, i.date, i.person_name, ic.method, ic.amount "
                "FROM invoice_collections ic JOIN invoices i ON i.id = ic.invoice_id "
                "WHERE i.date BETWEEN ? AND ? ORDER BY ic.invoice_id;", conn, params=(d1.isoformat(), d2.isoformat()))
            ddf = pd.read_sql_query(
                "SELECT d.invoice_id, i.date, i.person_name, d.shop_no, d.amount "
                "FROM invoice_dues d JOIN invoices i ON i.id = d.invoice_id "
                "WHERE i.date BETWEEN ? AND ? ORDER BY d.invoice_id;", conn, params=(d1.isoformat(), d2.isoformat()))
        st.markdown("**Collections (Cash/UPI)**")
        st.dataframe(dcf, width="stretch")
        st.download_button("⬇️ CSV (Collections)", *df_to_csv_download(dcf, "invoice_collections.csv"), mime="text/csv")
        st.markdown("**Dues by Shop**")
        st.dataframe(ddf, width="stretch")
        st.download_button("⬇️ CSV (Dues)", *df_to_csv_download(ddf, "invoice_dues.csv"), mime="text/csv")

# ---------------- Reports ----------------
elif page == "Reports":
    st.subheader("Reports")
    rep_date = st.date_input("Report Date", value=dt.date.today())

    with get_conn() as conn:
        inv = pd.read_sql_query(
            "SELECT id, person_name, total_amount, collection_amount, due_amount FROM invoices WHERE date = ? ORDER BY id;",
            conn, params=(rep_date.isoformat(),)
        )
        coll = pd.read_sql_query(
            "SELECT ic.invoice_id, ic.method, ic.amount FROM invoice_collections ic "
            "JOIN invoices i ON i.id = ic.invoice_id WHERE i.date = ?;", conn, params=(rep_date.isoformat(),))
        dues = pd.read_sql_query(
            "SELECT d.invoice_id, d.shop_no, d.amount FROM invoice_dues d "
            "JOIN invoices i ON i.id = d.invoice_id WHERE i.date = ?;", conn, params=(rep_date.isoformat(),))
        lines = pd.read_sql_query(
            "SELECT l.invoice_id, l.line_no, l.item, l.qty, l.unit_price, l.amount, l.highlight "
            "FROM invoice_lines l JOIN invoices i ON i.id = l.invoice_id WHERE i.date = ?;",
            conn, params=(rep_date.isoformat(),)
        )

    inv_total = inv["total_amount"].sum() if not inv.empty else 0.0
    inv_coll  = inv["collection_amount"].sum() if not inv.empty else 0.0
    inv_due   = inv["due_amount"].sum() if not inv.empty else 0.0

    c1, c2, c3 = st.columns(3)
    with c1: st.metric("Total Billed (₹)", f"{inv_total:,.2f}")
    with c2: st.metric("Collected (₹)", f"{inv_coll:,.2f}")
    with c3: st.metric("Total Due (₹)", f"{inv_due:,.2f}")

    st.markdown("#### Bills Today")
    st.dataframe(inv, width="stretch")

    st.markdown("#### Collections Breakdown (Cash vs UPI)")
    if not coll.empty:
        grp = coll.groupby("method", as_index=False)["amount"].sum()
        st.dataframe(grp, width="stretch")
    else:
        st.info("No collections.")

    st.markdown("#### Dues by Shop")
    if not dues.empty:
        g2 = dues.groupby("shop_no", as_index=False)["amount"].sum()
        st.dataframe(g2, width="stretch")
    else:
        st.info("No dues.")

    st.markdown("#### Highlighted Items")
    if not lines.empty:
        hi = lines[lines["highlight"] == 1]
        if hi.empty:
            st.info("No highlighted items today.")
        else:
            st.dataframe(hi.sort_values(["invoice_id","line_no"]), width="stretch")
    else:
        st.info("No line items today.")

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
        inv.to_excel(writer, index=False, sheet_name="Bills")
        coll.to_excel(writer, index=False, sheet_name="Collections")
        dues.to_excel(writer, index=False, sheet_name="Dues")
        lines.to_excel(writer, index=False, sheet_name="Lines")
        writer.book.set_properties({'title': f"Report {rep_date}"})
    st.download_button("⬇️ Download Daily Report (Excel)", data=buf.getvalue(), file_name=f"report_{rep_date}.xlsx")

# ---------------- Master Data ----------------
elif page == "Master Data":
    st.subheader("Items & Rates")
    st.caption("Seeded from XLSM (INVOICE sheet) on first run if DB was empty. You can add/edit here.")
    df_items = get_items_df()
    st.dataframe(df_items, width="stretch")

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
                    st.experimental_rerun()

# ---------------- Admin (login + CRUD) ----------------
else:
    if not require_admin_auth():
        st.stop()

    st.subheader("Admin • Manage Records")

    with st.expander("Invoices"):
        with get_conn() as conn:
            df = pd.read_sql_query("SELECT * FROM invoices ORDER BY id DESC LIMIT 500;", conn)
        st.dataframe(df, width="stretch")
        del_id = st.number_input("Delete invoice by ID", step=1, min_value=0)
        if st.button("Delete Invoice"):
            with get_conn() as conn:
                conn.execute("DELETE FROM invoices WHERE id = ?;", (int(del_id),))
                conn.commit()
            st.success(f"Deleted invoice {int(del_id)} (if existed).")

    with st.expander("Invoice Lines"):
        with get_conn() as conn:
            df = pd.read_sql_query("SELECT * FROM invoice_lines ORDER BY invoice_id DESC, line_no;", conn)
        st.dataframe(df, width="stretch")
        ln_id = st.number_input("Delete line by ID", step=1, min_value=0, key="del_line")
        if st.button("Delete Line"):
            with get_conn() as conn:
                conn.execute("DELETE FROM invoice_lines WHERE id = ?;", (int(ln_id),))
                conn.commit()
            st.success(f"Deleted line {int(ln_id)} (if existed).")

    with st.expander("Invoice Collections"):
        with get_conn() as conn:
            df = pd.read_sql_query("SELECT * FROM invoice_collections ORDER BY invoice_id DESC;", conn)
        st.dataframe(df, width="stretch")
        cid = st.number_input("Delete collection by ID", step=1, min_value=0, key="del_col")
        if st.button("Delete Collection"):
            with get_conn() as conn:
                conn.execute("DELETE FROM invoice_collections WHERE id = ?;", (int(cid),))
                conn.commit()
            st.success(f"Deleted collection {int(cid)} (if existed).")

    with st.expander("Invoice Dues"):
        with get_conn() as conn:
            df = pd.read_sql_query("SELECT * FROM invoice_dues ORDER BY invoice_id DESC;", conn)
        st.dataframe(df, width="stretch")
        did = st.number_input("Delete due by ID", step=1, min_value=0, key="del_due")
        if st.button("Delete Due"):
            with get_conn() as conn:
                conn.execute("DELETE FROM invoice_dues WHERE id = ?;", (int(did),))
                conn.commit()
            st.success(f"Deleted due {int(did)} (if existed).")

    with st.expander("Inventory Movements"):
        with get_conn() as conn:
            df = pd.read_sql_query("SELECT * FROM inventory_movements ORDER BY date DESC, item;", conn)
        st.dataframe(df, width="stretch")
        mid = st.number_input("Delete movement by ID", step=1, min_value=0, key="del_mov")
        if st.button("Delete Movement"):
            with get_conn() as conn:
                conn.execute("DELETE FROM inventory_movements WHERE id = ?;", (int(mid),))
                conn.commit()
            st.success(f"Deleted movement {int(mid)} (if existed).")
