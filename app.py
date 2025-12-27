from flask import Flask, render_template, request, redirect
from flask_login import (
    LoginManager,
    login_user,
    login_required,
    logout_user,
    UserMixin,
    current_user,
)
from werkzeug.security import check_password_hash
from pathlib import Path
import sqlite3
from datetime import date, timedelta
import math
import re

app = Flask(__name__, instance_relative_config=True)
app.secret_key = "change-this-secret-key"
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = "login_form"

Path(app.instance_path).mkdir(parents=True, exist_ok=True)
DATABASE = Path(app.instance_path) / "inventory_control.db"
app.config["DATABASE"] = DATABASE


class User(UserMixin):
    def __init__(self, id, name, email, role, store_id):
        self.id = id
        self.name = name
        self.email = email
        self.role = role
        self.store_id = store_id


@login_manager.user_loader
def load_user(user_id):
    conn = get_db()
    row = conn.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()
    conn.close()

    if row:
        return User(row["id"], row["name"], row["email"], row["role"], row["store_id"])
    return None


# ---------------------------------------------
# トップページ
# ---------------------------------------------
@app.route("/")
def index():
    return render_template("index.html")


# ---------------------------------------------
# ログインフォーム表示
# ---------------------------------------------
@app.route("/login", methods=["GET"])
def login_form():
    return render_template("login.html")


@app.route("/login", methods=["POST"])
def login():
    email = request.form["email"]
    password = request.form["password"]

    conn = get_db()
    user = conn.execute("SELECT * FROM users WHERE email = ?", (email,)).fetchone()
    conn.close()

    if user and check_password_hash(user["password_hash"], password):
        login_user(
            User(user["id"], user["name"], user["email"], user["role"], user["store_id"])
        )
        return redirect("/")

    return "ログイン失敗：メールアドレスまたはパスワードが違います"


@app.route("/logout")
@login_required
def logout():
    logout_user()
    return redirect("/login")


# ---------------------------------------------
# DB接続用の関数（毎回これでDBを開く）
# ---------------------------------------------
def get_db():
    conn = sqlite3.connect(app.config["DATABASE"])
    conn.row_factory = sqlite3.Row  # ← 辞書のように列名でアクセスできる
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def is_admin_user():
    return getattr(current_user, "role", None) == "admin"


def parse_float(value):
    if value is None:
        return None
    value = str(value).strip()
    if value == "":
        return None
    try:
        return float(value)
    except ValueError:
        return None


def parse_int(value):
    if value is None:
        return None
    value = str(value).strip()
    if value == "":
        return None
    try:
        return int(value)
    except ValueError:
        return None


def ensure_material_store_minimums_table():
    """Ensure the table for per-store minimum stock thresholds exists."""
    conn = get_db()
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS material_store_minimums (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            material_id INTEGER NOT NULL,
            store_id INTEGER NOT NULL,
            minimum_stock REAL,
            UNIQUE (material_id, store_id),
            FOREIGN KEY (material_id) REFERENCES materials(id) ON DELETE CASCADE,
            FOREIGN KEY (store_id) REFERENCES stores(id) ON DELETE CASCADE
        )
        """
    )
    conn.commit()
    conn.close()


ensure_material_store_minimums_table()


def ensure_daily_reports_tables():
    """Ensure the tables for daily reports exist."""
    conn = get_db()
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS daily_reports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            store_id INTEGER NOT NULL,
            date TEXT NOT NULL,
            sales REAL,
            wasted_takoyaki INTEGER,
            production_sets INTEGER,
            working_hours REAL,
            next_material_delivery TEXT,
            remarks TEXT,
            FOREIGN KEY (store_id) REFERENCES stores(id)
        )
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_daily_reports_store_date
        ON daily_reports(store_id, date)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS daily_report_orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            daily_report_id INTEGER NOT NULL,
            material_id INTEGER NOT NULL,
            quantity INTEGER NOT NULL,
            FOREIGN KEY (daily_report_id) REFERENCES daily_reports(id) ON DELETE CASCADE,
            FOREIGN KEY (material_id) REFERENCES materials(id)
        )
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_daily_report_orders_unique_material
        ON daily_report_orders(daily_report_id, material_id)
        """
    )
    conn.commit()
    conn.close()


ensure_daily_reports_tables()


def ensure_stocktake_tables():
    """Ensure the tables for stocktakes exist."""
    conn = get_db()
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS stocktake_sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_id INTEGER,
            store_id INTEGER NOT NULL,
            count_date TEXT NOT NULL,
            session_type TEXT DEFAULT 'ad_hoc',
            count_month TEXT,
            status TEXT DEFAULT 'draft',
            notes TEXT,
            confirmed_at TEXT,
            FOREIGN KEY (company_id) REFERENCES companies(id),
            FOREIGN KEY (store_id) REFERENCES stores(id)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS stocktake_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id INTEGER NOT NULL,
            material_id INTEGER NOT NULL,
            counted_quantity REAL NOT NULL,
            FOREIGN KEY (session_id) REFERENCES stocktake_sessions(id) ON DELETE CASCADE,
            FOREIGN KEY (material_id) REFERENCES materials(id)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS stocktake_order_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id INTEGER NOT NULL,
            material_id INTEGER NOT NULL,
            quantity INTEGER NOT NULL,
            FOREIGN KEY (session_id) REFERENCES stocktake_sessions(id) ON DELETE CASCADE,
            FOREIGN KEY (material_id) REFERENCES materials(id)
        )
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_stocktake_items_unique
        ON stocktake_items(session_id, material_id)
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_stocktake_order_items_unique
        ON stocktake_order_items(session_id, material_id)
        """
    )

    session_columns = {
        row["name"]
        for row in conn.execute("PRAGMA table_info(stocktake_sessions)").fetchall()
    }
    if "session_type" not in session_columns:
        conn.execute(
            "ALTER TABLE stocktake_sessions ADD COLUMN session_type TEXT DEFAULT 'ad_hoc'"
        )
    if "count_month" not in session_columns:
        conn.execute("ALTER TABLE stocktake_sessions ADD COLUMN count_month TEXT")

    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_stocktake_sessions_type_month
        ON stocktake_sessions(session_type, count_month)
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_stocktake_sessions_monthly_unique
        ON stocktake_sessions(store_id, count_month)
        WHERE session_type = 'monthly'
        """
    )
    conn.commit()
    conn.close()


ensure_stocktake_tables()


# ---------------------------------------------
# 材料一覧
# ---------------------------------------------
@app.route("/materials")
@login_required
def materials_list():
    conn = get_db()
    materials = conn.execute("SELECT * FROM materials").fetchall()
    stores = conn.execute("SELECT id, name FROM stores").fetchall()
    store_minimum_rows = conn.execute(
        "SELECT material_id, store_id, minimum_stock FROM material_store_minimums"
    ).fetchall()

    per_store_minimums = {}
    for row in store_minimum_rows:
        material_id = row["material_id"]
        store_id = row["store_id"]
        per_store_minimums.setdefault(material_id, {})[store_id] = row["minimum_stock"]

    store_param = request.args.get("store_id", "all")
    selected_store_id = None
    selected_store = None
    if store_param != "all":
        try:
            store_id_int = int(store_param)
        except ValueError:
            conn.close()
            return redirect("/materials")
        selected_store = next((store for store in stores if store["id"] == store_id_int), None)
        if not selected_store:
            conn.close()
            return redirect("/materials")
        selected_store_id = store_id_int

    stock_rows = conn.execute(
        """
        SELECT im.material_id,
               im.store_id,
               COALESCE(
                   SUM(
                       CASE
                           WHEN mt.name IN ('出庫', '廃棄') THEN -im.quantity
                           ELSE im.quantity
                       END
                   ),
                   0
               ) AS store_stock
        FROM inventory_movements im
        JOIN movement_types mt ON im.movement_type_id = mt.id
        GROUP BY im.material_id, im.store_id
        """
    ).fetchall()
    stock_levels = {}
    per_store_stock = {}
    for row in stock_rows:
        material_id = row["material_id"]
        store_id = row["store_id"]
        qty = row["store_stock"]
        stock_levels[material_id] = stock_levels.get(material_id, 0) + qty
        if material_id not in per_store_stock:
            per_store_stock[material_id] = {}
        per_store_stock[material_id][store_id] = qty

    def classify_stock(quantity, minimum_stock):
        """Return the CSS class that represents current stock vs. minimum."""
        if minimum_stock is None:
            return "stock-unknown"
        try:
            minimum_value = float(minimum_stock)
        except (TypeError, ValueError):
            return "stock-unknown"

        if minimum_value <= 0:
            minimum_value = 0

        quantity = quantity or 0
        if minimum_value and quantity < minimum_value:
            return "stock-low"
        if minimum_value and quantity > minimum_value * 2:
            return "stock-high"
        return "stock-ok"

    stock_statuses = {}
    for material in materials:
        min_stock = material["minimum_stock"]
        material_id = material["id"]
        stock_statuses[material_id] = {
            "total": classify_stock(stock_levels.get(material_id, 0), min_stock),
            "stores": {},
        }
        for store in stores:
            per_store = per_store_stock.get(material_id, {}).get(store["id"], 0)
            store_minimum = per_store_minimums.get(material_id, {}).get(store["id"])
            applicable_min = store_minimum if store_minimum is not None else min_stock
            stock_statuses[material_id]["stores"][store["id"]] = classify_stock(
                per_store, applicable_min
            )

    category_rows = conn.execute(
        "SELECT id, category_name FROM material_categories"
    ).fetchall()

    categories = {}
    for category in category_rows:
        categories[category["id"]] = {
            "id": category["id"],
            "category_name": category["category_name"],
        }

    conn.close()
    return render_template(
        "materials_list.html",
        materials=materials,
        categories=categories,
        stock_levels=stock_levels,
        per_store_stock=per_store_stock,
        per_store_minimums=per_store_minimums,
        stock_statuses=stock_statuses,
        stores=stores,
        selected_store_id=selected_store_id,
        selected_store=selected_store,
    )


# ---------------------------------------------
# 材料登録フォームの表示
# ---------------------------------------------
@app.route("/materials/add", methods=["GET"])
@login_required
def add_material_form():
    conn = get_db()
    categories = conn.execute(
        "SELECT id, category_name FROM material_categories"
    ).fetchall()
    stores = conn.execute("SELECT id, name FROM stores").fetchall()
    conn.close()
    return render_template("add_material.html", categories=categories, stores=stores)


# ---------------------------------------------
# 材料登録（POST）
# ---------------------------------------------
@app.route("/materials/add", methods=["POST"])
@login_required
def add_material():
    name = request.form["name"]
    unit = request.form["unit"]
    price = request.form["price"]
    minimum_stock = request.form["minimum_stock"]
    category_id = request.form.get("category_id")
    memo = request.form.get("memo", "")

    if category_id:
        category_id = int(category_id)
    else:
        category_id = None

    conn = get_db()
    cursor = conn.execute(
        """
        INSERT INTO materials 
        (name, unit, price_per_unit, minimum_stock, category_id, memo)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (name, unit, price, minimum_stock, category_id, memo),
    )
    material_id = cursor.lastrowid

    store_rows = conn.execute("SELECT id FROM stores").fetchall()
    for store in store_rows:
        field_name = f"minimum_stock_store_{store['id']}"
        raw_value = request.form.get(field_name)
        if raw_value is None or raw_value == "":
            continue
        try:
            min_value = float(raw_value)
        except ValueError:
            continue
        conn.execute(
            """
            INSERT INTO material_store_minimums (material_id, store_id, minimum_stock)
            VALUES (?, ?, ?)
            """,
            (material_id, store["id"], min_value),
        )

    conn.commit()
    conn.close()

    return redirect("/materials")


# ---------------------------------------------
# 材料編集フォームの表示（GET）
# ---------------------------------------------
@app.route("/materials/<int:material_id>/edit", methods=["GET"])
@login_required
def edit_material_form(material_id):
    conn = get_db()

    material = conn.execute(
        "SELECT * FROM materials WHERE id = ?", (material_id,)
    ).fetchone()

    categories = conn.execute(
        "SELECT id, category_name FROM material_categories"
    ).fetchall()

    stores = conn.execute("SELECT id, name FROM stores").fetchall()
    store_minimum_rows = conn.execute(
        "SELECT store_id, minimum_stock FROM material_store_minimums WHERE material_id = ?",
        (material_id,),
    ).fetchall()
    store_minimums = {row["store_id"]: row["minimum_stock"] for row in store_minimum_rows}

    conn.close()

    return render_template(
        "edit_material.html",
        material=material,
        categories=categories,
        stores=stores,
        store_minimums=store_minimums,
    )


# ---------------------------------------------
# 材料編集の更新処理（POST）
# ---------------------------------------------
@app.route("/materials/<int:material_id>/edit", methods=["POST"])
@login_required
def edit_material(material_id):
    name = request.form["name"]
    unit = request.form["unit"]
    price = request.form["price"]
    minimum_stock = request.form["minimum_stock"]
    category_id = request.form.get("category_id")
    memo = request.form.get("memo", "")

    if category_id:
        category_id = int(category_id)
    else:
        category_id = None

    conn = get_db()
    conn.execute(
        """
        UPDATE materials
        SET name=?, unit=?, price_per_unit=?, minimum_stock=?,
            category_id=?, memo=?
        WHERE id=?
        """,
        (name, unit, price, minimum_stock, category_id, memo, material_id),
    )
    conn.execute(
        "DELETE FROM material_store_minimums WHERE material_id = ?", (material_id,)
    )

    store_rows = conn.execute("SELECT id FROM stores").fetchall()
    for store in store_rows:
        field_name = f"minimum_stock_store_{store['id']}"
        raw_value = request.form.get(field_name)
        if raw_value is None or raw_value == "":
            continue
        try:
            min_value = float(raw_value)
        except ValueError:
            continue
        conn.execute(
            """
            INSERT INTO material_store_minimums (material_id, store_id, minimum_stock)
            VALUES (?, ?, ?)
            """,
            (material_id, store["id"], min_value),
        )

    conn.commit()
    conn.close()

    return redirect("/materials")


# ---------------------------------------------
# 材料削除の確認画面（GET）
# ---------------------------------------------
@app.route("/materials/<int:material_id>/delete", methods=["GET"])
@login_required
def delete_material_confirm(material_id):
    conn = get_db()
    material = conn.execute(
        "SELECT * FROM materials WHERE id=?", (material_id,)
    ).fetchone()
    conn.close()

    return render_template("delete_material.html", material=material)


# ---------------------------------------------
# 材料削除（POST）
# ---------------------------------------------
@app.route("/materials/<int:material_id>/delete", methods=["POST"])
@login_required
def delete_material(material_id):
    conn = get_db()
    conn.execute("DELETE FROM materials WHERE id = ?", (material_id,))
    conn.commit()
    conn.close()

    return redirect("/materials")


# ---------------------------------------------
# 入出庫一覧表示
# ---------------------------------------------
@app.route("/movements")
@login_required
def movement_list():
    conn = get_db()
    movements = conn.execute(
        """
        SELECT im.id, im.quantity, im.datetime, im.memo,
               m.name AS material_name,
               mt.name AS movement_type_name,
               s.name AS store_name
        FROM inventory_movements im
        JOIN materials m ON im.material_id = m.id
        JOIN movement_types mt ON im.movement_type_id = mt.id
        JOIN stores s ON im.store_id = s.id
        ORDER BY im.datetime DESC
        """
    ).fetchall()
    conn.close()
    return render_template("movement_list.html", movements=movements)


# ---------------------------------------------
# 入出庫フォーム表示（GET）
# ---------------------------------------------
@app.route("/movements/add", methods=["GET"])
@login_required
def movement_add_form():
    conn = get_db()

    materials = conn.execute("SELECT id, name FROM materials").fetchall()
    movement_types = conn.execute("SELECT id, name FROM movement_types").fetchall()
    stores = conn.execute("SELECT id, name FROM stores").fetchall()

    conn.close()

    return render_template(
        "movement_add.html",
        materials=materials,
        movement_types=movement_types,
        stores=stores,
    )


# ---------------------------------------------
# 入出庫登録処理（POST）
# ---------------------------------------------
@app.route("/movements/add", methods=["POST"])
@login_required
def movement_add():
    store_id = request.form.get("store_id")
    material_id = request.form.get("material_id")
    movement_type_id = request.form.get("movement_type_id")
    quantity = request.form.get("quantity")
    memo = request.form.get("memo", "")
    datetime_value = request.form.get("datetime")

    if not (store_id and material_id and movement_type_id and quantity and datetime_value):
        return "必要な項目が未入力です。", 400

    conn = get_db()
    conn.execute(
        """
        INSERT INTO inventory_movements 
        (store_id, material_id, movement_type_id, quantity, datetime, memo)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            store_id,
            material_id,
            movement_type_id,
            quantity,
            datetime_value,
            memo,
        ),
    )
    conn.commit()
    conn.close()

    return redirect("/movements")


# ---------------------------------------------
# 日報一覧
# ---------------------------------------------
@app.route("/daily_reports")
@login_required
def daily_reports_list():
    conn = get_db()
    stores = conn.execute("SELECT id, name FROM stores ORDER BY id").fetchall()

    selected_store_id = current_user.store_id
    store_param = request.args.get("store_id")
    if is_admin_user() and store_param:
        parsed_store_id = parse_int(store_param)
        if parsed_store_id and any(store["id"] == parsed_store_id for store in stores):
            selected_store_id = parsed_store_id

    reports = conn.execute(
        """
        SELECT dr.*, s.name AS store_name
        FROM daily_reports dr
        JOIN stores s ON dr.store_id = s.id
        WHERE dr.store_id = ?
        ORDER BY dr.date DESC, dr.id DESC
        """,
        (selected_store_id,),
    ).fetchall()
    conn.close()

    selected_store = next(
        (store for store in stores if store["id"] == selected_store_id), None
    )
    return render_template(
        "daily_reports_list.html",
        reports=reports,
        stores=stores,
        selected_store=selected_store,
        selected_store_id=selected_store_id,
        is_admin=is_admin_user(),
    )


def fetch_store_stock_levels(conn, store_id):
    rows = conn.execute(
        """
        SELECT im.material_id,
               COALESCE(
                   SUM(
                       CASE
                           WHEN mt.name IN ('出庫', '廃棄') THEN -im.quantity
                           ELSE im.quantity
                       END
                   ),
                   0
               ) AS store_stock
        FROM inventory_movements im
        JOIN movement_types mt ON im.movement_type_id = mt.id
        WHERE im.store_id = ?
        GROUP BY im.material_id
        """,
        (store_id,),
    ).fetchall()
    return {row["material_id"]: row["store_stock"] for row in rows}


def build_daily_report_line_message(report, store_name, orders):
    def fmt_yen(value):
        if value is None:
            return "未入力"
        try:
            return f"{int(round(float(value))):,}円"
        except (TypeError, ValueError):
            return str(value)

    def fmt_hours(value):
        if value is None:
            return "未入力"
        try:
            return f"{float(value):g}h"
        except (TypeError, ValueError):
            return str(value)

    def fmt_dt(value):
        if not value:
            return "未入力"
        return str(value).replace("T", " ")

    sales = report["sales"]
    production_sets = report["production_sets"]
    wasted_takoyaki = report["wasted_takoyaki"]
    working_hours = report["working_hours"]

    def fmt_number(value):
        if value is None:
            return "未入力"
        try:
            return f"{float(value):g}"
        except (TypeError, ValueError):
            return str(value)
    productivity_sets = None
    productivity_sales = None
    if working_hours and working_hours > 0:
        if production_sets is not None:
            productivity_sets = production_sets / working_hours
        if sales is not None:
            productivity_sales = sales / working_hours

    lines = [
        f"【日報】{store_name} {report['date']}",
        f"売上: {fmt_yen(sales)}",
        f"販売セット数: {fmt_number(production_sets)}",
        f"処分たこ焼き数: {fmt_number(wasted_takoyaki)}",
        f"営業時間: {fmt_hours(working_hours)}",
    ]
    if productivity_sets is not None or productivity_sales is not None:
        prod_parts = []
        if productivity_sets is not None:
            prod_parts.append(f"{productivity_sets:.2f}セット/h")
        if productivity_sales is not None:
            prod_parts.append(f"{int(round(productivity_sales)):,}円/h")
        lines.append(f"生産性: {', '.join(prod_parts)}")

    lines.append(f"次回材料受け取り: {fmt_dt(report['next_material_delivery'])}")

    if orders:
        lines.append("発注（不足在庫）:")
        for order in orders:
            lines.append(
                f"- {order['material_name']}: {int(order['quantity'])} {order['unit']}"
            )
    else:
        lines.append("発注（不足在庫）: なし")

    remarks = (report["remarks"] or "").strip()
    lines.append("所感・気付き・困りごと:")
    lines.append(remarks if remarks else "（未入力）")

    return "\n".join(lines)


def build_stocktake_line_message(session, store_name, order_items):
    lines = [
        f"【FC本部発注】{store_name} {session['count_date']}",
    ]
    if order_items:
        for item in order_items:
            lines.append(
                f"- {item['material_name']}: {int(item['quantity'])} {item['unit']}"
            )
    else:
        lines.append("発注: なし")

    notes = (session["notes"] or "").strip()
    if notes:
        lines.append("備考:")
        lines.append(notes)

    return "\n".join(lines)


# ---------------------------------------------
# 日報作成フォーム（GET）
# ---------------------------------------------
@app.route("/daily_reports/add", methods=["GET"])
@login_required
def daily_report_add_form():
    conn = get_db()
    stores = conn.execute("SELECT id, name FROM stores ORDER BY id").fetchall()
    materials = conn.execute(
        """
        SELECT m.id, m.name, m.unit, m.minimum_stock
        FROM materials m
        ORDER BY m.name
        """
    ).fetchall()

    selected_store_id = current_user.store_id
    store_param = request.args.get("store_id")
    if is_admin_user() and store_param:
        parsed_store_id = parse_int(store_param)
        if parsed_store_id and any(store["id"] == parsed_store_id for store in stores):
            selected_store_id = parsed_store_id

    store_stock = fetch_store_stock_levels(conn, selected_store_id)
    store_minimum_rows = conn.execute(
        "SELECT material_id, minimum_stock FROM material_store_minimums WHERE store_id = ?",
        (selected_store_id,),
    ).fetchall()
    store_minimums = {row["material_id"]: row["minimum_stock"] for row in store_minimum_rows}

    material_rows = []
    for material in materials:
        material_id = material["id"]
        stock = store_stock.get(material_id, 0) or 0
        minimum = store_minimums.get(material_id)
        if minimum is None:
            minimum = material["minimum_stock"]
        shortage = None
        recommended_order = 0
        if minimum is not None:
            try:
                minimum_value = float(minimum)
            except (TypeError, ValueError):
                minimum_value = None
            if minimum_value is not None and minimum_value > 0:
                shortage = minimum_value - float(stock)
                if shortage > 0:
                    recommended_order = int(math.ceil(shortage))

        material_rows.append(
            {
                "id": material_id,
                "name": material["name"],
                "unit": material["unit"],
                "stock": stock,
                "minimum": minimum,
                "recommended_order": recommended_order,
                "is_low": recommended_order > 0,
            }
        )

    conn.close()

    selected_store = next(
        (store for store in stores if store["id"] == selected_store_id), None
    )
    default_date = request.args.get("date") or date.today().isoformat()

    return render_template(
        "daily_report_add.html",
        stores=stores,
        selected_store=selected_store,
        selected_store_id=selected_store_id,
        is_admin=is_admin_user(),
        material_rows=material_rows,
        default_date=default_date,
    )


# ---------------------------------------------
# 日報作成（POST）
# ---------------------------------------------
@app.route("/daily_reports/add", methods=["POST"])
@login_required
def daily_report_add():
    report_date = (request.form.get("date") or "").strip()
    if not report_date:
        return "日付が未入力です。", 400

    conn = get_db()
    stores = conn.execute("SELECT id FROM stores").fetchall()
    valid_store_ids = {store["id"] for store in stores}

    store_id = current_user.store_id
    if is_admin_user():
        store_id = parse_int(request.form.get("store_id")) or store_id
    if store_id not in valid_store_ids:
        conn.close()
        return "店舗が不正です。", 400

    sales = parse_float(request.form.get("sales"))
    wasted_takoyaki = parse_int(request.form.get("wasted_takoyaki"))
    production_sets = parse_float(request.form.get("production_sets"))
    working_hours = parse_float(request.form.get("working_hours"))
    next_material_delivery = (request.form.get("next_material_delivery") or "").strip() or None
    remarks = (request.form.get("remarks") or "").strip() or None

    existing = conn.execute(
        "SELECT id FROM daily_reports WHERE store_id = ? AND date = ?",
        (store_id, report_date),
    ).fetchone()

    if existing:
        daily_report_id = existing["id"]
        conn.execute(
            """
            UPDATE daily_reports
            SET sales = ?, wasted_takoyaki = ?, production_sets = ?, working_hours = ?,
                next_material_delivery = ?, remarks = ?
            WHERE id = ?
            """,
            (
                sales,
                wasted_takoyaki,
                production_sets,
                working_hours,
                next_material_delivery,
                remarks,
                daily_report_id,
            ),
        )
    else:
        cursor = conn.execute(
            """
            INSERT INTO daily_reports
            (store_id, date, sales, wasted_takoyaki, production_sets, working_hours, next_material_delivery, remarks)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                store_id,
                report_date,
                sales,
                wasted_takoyaki,
                production_sets,
                working_hours,
                next_material_delivery,
                remarks,
            ),
        )
        daily_report_id = cursor.lastrowid

    materials = conn.execute("SELECT id FROM materials").fetchall()
    material_ids = {material["id"] for material in materials}

    order_items = []
    for key, value in request.form.items():
        if not key.startswith("order_qty_"):
            continue
        material_id = parse_int(key.replace("order_qty_", "", 1))
        if not material_id or material_id not in material_ids:
            continue
        quantity = parse_int(value)
        if quantity is None or quantity <= 0:
            continue
        order_items.append((daily_report_id, material_id, quantity))

    conn.execute(
        "DELETE FROM daily_report_orders WHERE daily_report_id = ?",
        (daily_report_id,),
    )
    if order_items:
        conn.executemany(
            """
            INSERT INTO daily_report_orders (daily_report_id, material_id, quantity)
            VALUES (?, ?, ?)
            """,
            order_items,
        )

    conn.commit()
    conn.close()

    return redirect(f"/daily_reports/{daily_report_id}")


# ---------------------------------------------
# 日報詳細
# ---------------------------------------------
@app.route("/daily_reports/<int:daily_report_id>")
@login_required
def daily_report_detail(daily_report_id):
    conn = get_db()
    report = conn.execute(
        """
        SELECT dr.*, s.name AS store_name
        FROM daily_reports dr
        JOIN stores s ON dr.store_id = s.id
        WHERE dr.id = ?
        """,
        (daily_report_id,),
    ).fetchone()
    if not report:
        conn.close()
        return "日報が見つかりません。", 404

    if not is_admin_user() and report["store_id"] != current_user.store_id:
        conn.close()
        return "権限がありません。", 403

    orders = conn.execute(
        """
        SELECT dro.quantity, m.name AS material_name, m.unit
        FROM daily_report_orders dro
        JOIN materials m ON dro.material_id = m.id
        WHERE dro.daily_report_id = ?
        ORDER BY m.name
        """,
        (daily_report_id,),
    ).fetchall()

    line_message = build_daily_report_line_message(report, report["store_name"], orders)
    conn.close()

    return render_template(
        "daily_report_detail.html",
        report=report,
        orders=orders,
        line_message=line_message,
    )


# ---------------------------------------------
# 日報編集フォーム（GET）
# ---------------------------------------------
@app.route("/daily_reports/<int:daily_report_id>/edit", methods=["GET"])
@login_required
def daily_report_edit_form(daily_report_id):
    conn = get_db()
    report = conn.execute(
        """
        SELECT dr.*, s.name AS store_name
        FROM daily_reports dr
        JOIN stores s ON dr.store_id = s.id
        WHERE dr.id = ?
        """,
        (daily_report_id,),
    ).fetchone()
    if not report:
        conn.close()
        return "日報が見つかりません。", 404

    if not is_admin_user() and report["store_id"] != current_user.store_id:
        conn.close()
        return "権限がありません。", 403

    stores = conn.execute("SELECT id, name FROM stores ORDER BY id").fetchall()
    materials = conn.execute(
        """
        SELECT m.id, m.name, m.unit, m.minimum_stock
        FROM materials m
        ORDER BY m.name
        """
    ).fetchall()

    orders = conn.execute(
        """
        SELECT dro.material_id, dro.quantity, m.name AS material_name, m.unit
        FROM daily_report_orders dro
        JOIN materials m ON dro.material_id = m.id
        WHERE dro.daily_report_id = ?
        """,
        (daily_report_id,),
    ).fetchall()
    order_quantities = {row["material_id"]: row["quantity"] for row in orders}

    store_stock = fetch_store_stock_levels(conn, report["store_id"])
    store_minimum_rows = conn.execute(
        "SELECT material_id, minimum_stock FROM material_store_minimums WHERE store_id = ?",
        (report["store_id"],),
    ).fetchall()
    store_minimums = {row["material_id"]: row["minimum_stock"] for row in store_minimum_rows}

    material_rows = []
    for material in materials:
        material_id = material["id"]
        stock = store_stock.get(material_id, 0) or 0
        minimum = store_minimums.get(material_id)
        if minimum is None:
            minimum = material["minimum_stock"]
        recommended_order = 0
        if minimum is not None:
            try:
                minimum_value = float(minimum)
            except (TypeError, ValueError):
                minimum_value = None
            if minimum_value is not None and minimum_value > 0:
                shortage = minimum_value - float(stock)
                if shortage > 0:
                    recommended_order = int(math.ceil(shortage))

        existing_order = order_quantities.get(material_id)
        material_rows.append(
            {
                "id": material_id,
                "name": material["name"],
                "unit": material["unit"],
                "stock": stock,
                "minimum": minimum,
                "recommended_order": recommended_order,
                "is_low": recommended_order > 0,
                "order_qty": existing_order,
            }
        )

    conn.close()

    return render_template(
        "daily_report_edit.html",
        report=report,
        stores=stores,
        is_admin=is_admin_user(),
        material_rows=material_rows,
    )


# ---------------------------------------------
# 日報更新（POST）
# ---------------------------------------------
@app.route("/daily_reports/<int:daily_report_id>/edit", methods=["POST"])
@login_required
def daily_report_edit(daily_report_id):
    conn = get_db()
    report = conn.execute(
        "SELECT * FROM daily_reports WHERE id = ?",
        (daily_report_id,),
    ).fetchone()
    if not report:
        conn.close()
        return "日報が見つかりません。", 404

    if not is_admin_user() and report["store_id"] != current_user.store_id:
        conn.close()
        return "権限がありません。", 403

    store_id = report["store_id"]
    if is_admin_user():
        store_id = parse_int(request.form.get("store_id")) or store_id

    report_date = (request.form.get("date") or "").strip()
    if not report_date:
        conn.close()
        return "日付が未入力です。", 400

    conflict = conn.execute(
        """
        SELECT id FROM daily_reports
        WHERE store_id = ? AND date = ? AND id != ?
        """,
        (store_id, report_date, daily_report_id),
    ).fetchone()
    if conflict:
        conn.close()
        return "同じ店舗・同じ日付の日報が既に存在します。", 400

    sales = parse_float(request.form.get("sales"))
    wasted_takoyaki = parse_int(request.form.get("wasted_takoyaki"))
    production_sets = parse_float(request.form.get("production_sets"))
    working_hours = parse_float(request.form.get("working_hours"))
    next_material_delivery = (request.form.get("next_material_delivery") or "").strip() or None
    remarks = (request.form.get("remarks") or "").strip() or None

    conn.execute(
        """
        UPDATE daily_reports
        SET store_id = ?, date = ?, sales = ?, wasted_takoyaki = ?, production_sets = ?,
            working_hours = ?, next_material_delivery = ?, remarks = ?
        WHERE id = ?
        """,
        (
            store_id,
            report_date,
            sales,
            wasted_takoyaki,
            production_sets,
            working_hours,
            next_material_delivery,
            remarks,
            daily_report_id,
        ),
    )

    materials = conn.execute("SELECT id FROM materials").fetchall()
    material_ids = {material["id"] for material in materials}

    order_items = []
    for key, value in request.form.items():
        if not key.startswith("order_qty_"):
            continue
        material_id = parse_int(key.replace("order_qty_", "", 1))
        if not material_id or material_id not in material_ids:
            continue
        quantity = parse_int(value)
        if quantity is None or quantity <= 0:
            continue
        order_items.append((daily_report_id, material_id, quantity))

    conn.execute(
        "DELETE FROM daily_report_orders WHERE daily_report_id = ?",
        (daily_report_id,),
    )
    if order_items:
        conn.executemany(
            """
            INSERT INTO daily_report_orders (daily_report_id, material_id, quantity)
            VALUES (?, ?, ?)
            """,
            order_items,
        )

    conn.commit()
    conn.close()
    return redirect(f"/daily_reports/{daily_report_id}")


# ---------------------------------------------
# 日報削除（POST）
# ---------------------------------------------
@app.route("/daily_reports/<int:daily_report_id>/delete", methods=["POST"])
@login_required
def daily_report_delete(daily_report_id):
    conn = get_db()
    report = conn.execute(
        "SELECT * FROM daily_reports WHERE id = ?",
        (daily_report_id,),
    ).fetchone()
    if not report:
        conn.close()
        return "日報が見つかりません。", 404
    if not is_admin_user() and report["store_id"] != current_user.store_id:
        conn.close()
        return "権限がありません。", 403

    conn.execute(
        "DELETE FROM daily_report_orders WHERE daily_report_id = ?",
        (daily_report_id,),
    )
    conn.execute("DELETE FROM daily_reports WHERE id = ?", (daily_report_id,))
    conn.commit()
    conn.close()

    return redirect("/daily_reports")


# ---------------------------------------------
# 棚卸一覧
# ---------------------------------------------
@app.route("/stocktakes")
@login_required
def stocktake_list():
    conn = get_db()
    stores = conn.execute("SELECT id, name FROM stores ORDER BY id").fetchall()

    selected_store_id = current_user.store_id
    store_param = request.args.get("store_id")
    if is_admin_user() and store_param:
        parsed_store_id = parse_int(store_param)
        if parsed_store_id and any(store["id"] == parsed_store_id for store in stores):
            selected_store_id = parsed_store_id

    sessions = conn.execute(
        """
        SELECT ss.*, s.name AS store_name
        FROM stocktake_sessions ss
        JOIN stores s ON ss.store_id = s.id
        WHERE ss.store_id = ?
        ORDER BY ss.count_date DESC, ss.id DESC
        """,
        (selected_store_id,),
    ).fetchall()
    conn.close()

    selected_store = next(
        (store for store in stores if store["id"] == selected_store_id), None
    )
    return render_template(
        "stocktake_list.html",
        sessions=sessions,
        stores=stores,
        selected_store=selected_store,
        selected_store_id=selected_store_id,
        is_admin=is_admin_user(),
    )


# ---------------------------------------------
# 棚卸作成フォーム（GET）
# ---------------------------------------------
@app.route("/stocktakes/add", methods=["GET"])
@login_required
def stocktake_add_form():
    conn = get_db()
    stores = conn.execute("SELECT id, name FROM stores ORDER BY id").fetchall()
    materials = conn.execute(
        """
        SELECT m.id, m.name, m.unit, m.minimum_stock
        FROM materials m
        ORDER BY m.name
        """
    ).fetchall()

    selected_store_id = current_user.store_id
    store_param = request.args.get("store_id")
    if is_admin_user() and store_param:
        parsed_store_id = parse_int(store_param)
        if parsed_store_id and any(store["id"] == parsed_store_id for store in stores):
            selected_store_id = parsed_store_id

    store_stock = fetch_store_stock_levels(conn, selected_store_id)
    store_minimum_rows = conn.execute(
        "SELECT material_id, minimum_stock FROM material_store_minimums WHERE store_id = ?",
        (selected_store_id,),
    ).fetchall()
    store_minimums = {row["material_id"]: row["minimum_stock"] for row in store_minimum_rows}

    material_rows = []
    for material in materials:
        material_id = material["id"]
        system_stock = store_stock.get(material_id, 0) or 0
        minimum = store_minimums.get(material_id)
        if minimum is None:
            minimum = material["minimum_stock"]
        recommended_order = 0
        if minimum is not None:
            try:
                minimum_value = float(minimum)
            except (TypeError, ValueError):
                minimum_value = None
            if minimum_value is not None and minimum_value > 0:
                shortage = minimum_value - float(system_stock)
                if shortage > 0:
                    recommended_order = int(math.ceil(shortage))

        material_rows.append(
            {
                "id": material_id,
                "name": material["name"],
                "unit": material["unit"],
                "system_stock": system_stock,
                "minimum": minimum,
                "recommended_order": recommended_order,
                "is_low": recommended_order > 0,
            }
        )

    conn.close()

    selected_store = next(
        (store for store in stores if store["id"] == selected_store_id), None
    )
    default_date = request.args.get("date") or date.today().isoformat()

    session_type = (request.args.get("type") or "ad_hoc").strip()
    if session_type not in ("ad_hoc", "monthly"):
        session_type = "ad_hoc"
    count_month = (request.args.get("month") or "").strip()
    if session_type == "monthly":
        if not re.match(r"^\d{4}-\d{2}$", count_month):
            count_month = date.today().strftime("%Y-%m")
        try:
            year, month = map(int, count_month.split("-", 1))
            if not (1 <= month <= 12):
                raise ValueError
            first_day = date(year, month, 1)
            if month == 12:
                next_month = date(year + 1, 1, 1)
            else:
                next_month = date(year, month + 1, 1)
            default_date = (next_month - timedelta(days=1)).isoformat()
        except ValueError:
            session_type = "ad_hoc"
            count_month = ""

    return render_template(
        "stocktake_add.html",
        stores=stores,
        selected_store=selected_store,
        selected_store_id=selected_store_id,
        is_admin=is_admin_user(),
        material_rows=material_rows,
        default_date=default_date,
        session_type=session_type,
        count_month=count_month,
    )


# ---------------------------------------------
# 棚卸作成（POST）
# ---------------------------------------------
@app.route("/stocktakes/add", methods=["POST"])
@login_required
def stocktake_add():
    count_date = (request.form.get("date") or "").strip()
    if not count_date:
        return "棚卸日が未入力です。", 400

    session_type = (request.form.get("session_type") or "ad_hoc").strip()
    if session_type not in ("ad_hoc", "monthly"):
        session_type = "ad_hoc"
    count_month = (request.form.get("count_month") or "").strip() or None
    if session_type == "monthly":
        if count_month is None:
            count_month = count_date[:7]
        if not re.match(r"^\d{4}-\d{2}$", count_month):
            return "月次棚卸しの対象月が不正です。", 400
        try:
            year, month = map(int, count_month.split("-", 1))
            if not (1 <= month <= 12):
                raise ValueError
        except ValueError:
            return "月次棚卸しの対象月が不正です。", 400
    else:
        count_month = None

    conn = get_db()
    stores = conn.execute("SELECT id FROM stores").fetchall()
    valid_store_ids = {store["id"] for store in stores}

    store_id = current_user.store_id
    if is_admin_user():
        store_id = parse_int(request.form.get("store_id")) or store_id
    if store_id not in valid_store_ids:
        conn.close()
        return "店舗が不正です。", 400

    notes = (request.form.get("notes") or "").strip() or None

    try:
        cursor = conn.execute(
            """
            INSERT INTO stocktake_sessions
            (company_id, store_id, count_date, session_type, count_month, status, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (None, store_id, count_date, session_type, count_month, "draft", notes),
        )
        session_id = cursor.lastrowid
    except sqlite3.IntegrityError:
        conn.close()
        if session_type == "monthly":
            return "この店舗の月次棚卸しは既に作成されています。", 400
        raise

    materials = conn.execute("SELECT id FROM materials").fetchall()
    material_ids = {material["id"] for material in materials}

    items = []
    for material_id in material_ids:
        key = f"count_qty_{material_id}"
        value = request.form.get(key)
        counted = parse_float(value)
        if counted is None:
            conn.close()
            return "全ての材料の棚卸数を入力してください。", 400
        items.append((session_id, material_id, counted))

    conn.executemany(
        """
        INSERT INTO stocktake_items (session_id, material_id, counted_quantity)
        VALUES (?, ?, ?)
        """,
        items,
    )

    order_items = []
    for key, value in request.form.items():
        if not key.startswith("order_qty_"):
            continue
        material_id = parse_int(key.replace("order_qty_", "", 1))
        if not material_id or material_id not in material_ids:
            continue
        quantity = parse_int(value)
        if quantity is None or quantity <= 0:
            continue
        order_items.append((session_id, material_id, quantity))

    if order_items:
        conn.executemany(
            """
            INSERT INTO stocktake_order_items (session_id, material_id, quantity)
            VALUES (?, ?, ?)
            """,
            order_items,
        )

    conn.commit()
    conn.close()

    return redirect(f"/stocktakes/{session_id}")


# ---------------------------------------------
# 月次棚卸し（一覧・集計）
# ---------------------------------------------
@app.route("/monthly_stocktakes")
@login_required
def monthly_stocktakes():
    month = (request.args.get("month") or date.today().strftime("%Y-%m")).strip()
    if not re.match(r"^\d{4}-\d{2}$", month):
        month = date.today().strftime("%Y-%m")

    conn = get_db()
    stores = conn.execute("SELECT id, name FROM stores ORDER BY id").fetchall()
    materials = conn.execute(
        "SELECT id, name, unit FROM materials ORDER BY name"
    ).fetchall()

    sessions = conn.execute(
        """
        SELECT ss.*, s.name AS store_name
        FROM stocktake_sessions ss
        JOIN stores s ON ss.store_id = s.id
        WHERE ss.session_type = 'monthly' AND ss.count_month = ?
        ORDER BY ss.store_id
        """,
        (month,),
    ).fetchall()
    sessions_by_store = {row["store_id"]: row for row in sessions}

    count_rows = conn.execute(
        """
        SELECT ss.store_id, si.material_id, si.counted_quantity
        FROM stocktake_sessions ss
        JOIN stocktake_items si ON si.session_id = ss.id
        WHERE ss.session_type = 'monthly' AND ss.count_month = ?
        """,
        (month,),
    ).fetchall()
    counts = {}
    for row in count_rows:
        counts.setdefault(row["material_id"], {})[row["store_id"]] = row[
            "counted_quantity"
        ]

    conn.close()
    return render_template(
        "monthly_stocktakes.html",
        month=month,
        stores=stores,
        materials=materials,
        sessions_by_store=sessions_by_store,
        counts=counts,
        is_admin=is_admin_user(),
        current_store_id=current_user.store_id,
    )


# ---------------------------------------------
# 棚卸詳細
# ---------------------------------------------
@app.route("/stocktakes/<int:session_id>")
@login_required
def stocktake_detail(session_id):
    conn = get_db()
    session = conn.execute(
        """
        SELECT ss.*, s.name AS store_name
        FROM stocktake_sessions ss
        JOIN stores s ON ss.store_id = s.id
        WHERE ss.id = ?
        """,
        (session_id,),
    ).fetchone()
    if not session:
        conn.close()
        return "棚卸が見つかりません。", 404

    if not is_admin_user() and session["store_id"] != current_user.store_id:
        conn.close()
        return "権限がありません。", 403

    items = conn.execute(
        """
        SELECT si.counted_quantity, m.name AS material_name, m.unit, m.id AS material_id
        FROM stocktake_items si
        JOIN materials m ON si.material_id = m.id
        WHERE si.session_id = ?
        ORDER BY m.name
        """,
        (session_id,),
    ).fetchall()

    order_items = conn.execute(
        """
        SELECT soi.quantity, m.name AS material_name, m.unit, m.id AS material_id
        FROM stocktake_order_items soi
        JOIN materials m ON soi.material_id = m.id
        WHERE soi.session_id = ?
        ORDER BY m.name
        """,
        (session_id,),
    ).fetchall()

    line_message = build_stocktake_line_message(
        session, session["store_name"], order_items
    )
    conn.close()

    return render_template(
        "stocktake_detail.html",
        session=session,
        items=items,
        order_items=order_items,
        line_message=line_message,
    )


# ---------------------------------------------
# 棚卸編集フォーム（GET）
# ---------------------------------------------
@app.route("/stocktakes/<int:session_id>/edit", methods=["GET"])
@login_required
def stocktake_edit_form(session_id):
    conn = get_db()
    session = conn.execute(
        """
        SELECT ss.*, s.name AS store_name
        FROM stocktake_sessions ss
        JOIN stores s ON ss.store_id = s.id
        WHERE ss.id = ?
        """,
        (session_id,),
    ).fetchone()
    if not session:
        conn.close()
        return "棚卸が見つかりません。", 404
    if not is_admin_user() and session["store_id"] != current_user.store_id:
        conn.close()
        return "権限がありません。", 403
    if session["status"] == "confirmed":
        conn.close()
        return "確定済みの棚卸は編集できません。", 403

    stores = conn.execute("SELECT id, name FROM stores ORDER BY id").fetchall()
    materials = conn.execute(
        """
        SELECT m.id, m.name, m.unit, m.minimum_stock
        FROM materials m
        ORDER BY m.name
        """
    ).fetchall()

    item_rows = conn.execute(
        """
        SELECT material_id, counted_quantity
        FROM stocktake_items
        WHERE session_id = ?
        """,
        (session_id,),
    ).fetchall()
    counted_map = {row["material_id"]: row["counted_quantity"] for row in item_rows}

    order_rows = conn.execute(
        """
        SELECT material_id, quantity
        FROM stocktake_order_items
        WHERE session_id = ?
        """,
        (session_id,),
    ).fetchall()
    order_map = {row["material_id"]: row["quantity"] for row in order_rows}

    store_stock = fetch_store_stock_levels(conn, session["store_id"])
    store_minimum_rows = conn.execute(
        "SELECT material_id, minimum_stock FROM material_store_minimums WHERE store_id = ?",
        (session["store_id"],),
    ).fetchall()
    store_minimums = {row["material_id"]: row["minimum_stock"] for row in store_minimum_rows}

    material_rows = []
    for material in materials:
        material_id = material["id"]
        system_stock = store_stock.get(material_id, 0) or 0
        minimum = store_minimums.get(material_id)
        if minimum is None:
            minimum = material["minimum_stock"]
        recommended_order = 0
        if minimum is not None:
            try:
                minimum_value = float(minimum)
            except (TypeError, ValueError):
                minimum_value = None
            if minimum_value is not None and minimum_value > 0:
                shortage = minimum_value - float(counted_map.get(material_id, system_stock) or 0)
                if shortage > 0:
                    recommended_order = int(math.ceil(shortage))

        material_rows.append(
            {
                "id": material_id,
                "name": material["name"],
                "unit": material["unit"],
                "system_stock": system_stock,
                "minimum": minimum,
                "counted_qty": counted_map.get(material_id, system_stock),
                "recommended_order": recommended_order,
                "order_qty": order_map.get(material_id),
            }
        )

    conn.close()
    return render_template(
        "stocktake_edit.html",
        session=session,
        stores=stores,
        is_admin=is_admin_user(),
        material_rows=material_rows,
    )


# ---------------------------------------------
# 棚卸更新（POST）
# ---------------------------------------------
@app.route("/stocktakes/<int:session_id>/edit", methods=["POST"])
@login_required
def stocktake_edit(session_id):
    conn = get_db()
    session = conn.execute(
        "SELECT * FROM stocktake_sessions WHERE id = ?",
        (session_id,),
    ).fetchone()
    if not session:
        conn.close()
        return "棚卸が見つかりません。", 404
    if not is_admin_user() and session["store_id"] != current_user.store_id:
        conn.close()
        return "権限がありません。", 403
    if session["status"] == "confirmed":
        conn.close()
        return "確定済みの棚卸は編集できません。", 403

    store_id = session["store_id"]
    if is_admin_user():
        store_id = parse_int(request.form.get("store_id")) or store_id

    count_date = (request.form.get("date") or "").strip()
    if not count_date:
        conn.close()
        return "棚卸日が未入力です。", 400

    notes = (request.form.get("notes") or "").strip() or None

    session_type = (session["session_type"] or "ad_hoc").strip()
    count_month = None
    if session_type == "monthly":
        count_month = count_date[:7]
        if not re.match(r"^\d{4}-\d{2}$", count_month):
            conn.close()
            return "月次棚卸しの棚卸日が不正です。", 400

    try:
        conn.execute(
            """
            UPDATE stocktake_sessions
            SET store_id = ?, count_date = ?, count_month = ?, notes = ?
            WHERE id = ?
            """,
            (store_id, count_date, count_month, notes, session_id),
        )
    except sqlite3.IntegrityError:
        conn.close()
        if session_type == "monthly":
            return "この店舗の月次棚卸しは既に作成されています。", 400
        raise

    materials = conn.execute("SELECT id FROM materials").fetchall()
    material_ids = {material["id"] for material in materials}

    items = []
    for material_id in material_ids:
        key = f"count_qty_{material_id}"
        value = request.form.get(key)
        counted = parse_float(value)
        if counted is None:
            conn.close()
            return "全ての材料の棚卸数を入力してください。", 400
        items.append((session_id, material_id, counted))

    conn.execute("DELETE FROM stocktake_items WHERE session_id = ?", (session_id,))
    conn.executemany(
        """
        INSERT INTO stocktake_items (session_id, material_id, counted_quantity)
        VALUES (?, ?, ?)
        """,
        items,
    )

    order_items = []
    for key, value in request.form.items():
        if not key.startswith("order_qty_"):
            continue
        material_id = parse_int(key.replace("order_qty_", "", 1))
        if not material_id or material_id not in material_ids:
            continue
        quantity = parse_int(value)
        if quantity is None or quantity <= 0:
            continue
        order_items.append((session_id, material_id, quantity))

    conn.execute(
        "DELETE FROM stocktake_order_items WHERE session_id = ?",
        (session_id,),
    )
    if order_items:
        conn.executemany(
            """
            INSERT INTO stocktake_order_items (session_id, material_id, quantity)
            VALUES (?, ?, ?)
            """,
            order_items,
        )

    conn.commit()
    conn.close()
    return redirect(f"/stocktakes/{session_id}")


# ---------------------------------------------
# 棚卸削除（POST）
# ---------------------------------------------
@app.route("/stocktakes/<int:session_id>/delete", methods=["POST"])
@login_required
def stocktake_delete(session_id):
    conn = get_db()
    session = conn.execute(
        "SELECT * FROM stocktake_sessions WHERE id = ?",
        (session_id,),
    ).fetchone()
    if not session:
        conn.close()
        return "棚卸が見つかりません。", 404
    if not is_admin_user() and session["store_id"] != current_user.store_id:
        conn.close()
        return "権限がありません。", 403
    if session["status"] == "confirmed":
        conn.close()
        return "確定済みの棚卸は削除できません。", 403

    conn.execute("DELETE FROM stocktake_order_items WHERE session_id = ?", (session_id,))
    conn.execute("DELETE FROM stocktake_items WHERE session_id = ?", (session_id,))
    conn.execute("DELETE FROM stocktake_sessions WHERE id = ?", (session_id,))
    conn.commit()
    conn.close()

    return redirect("/stocktakes")


# ---------------------------------------------
# 棚卸確定（差分調整）
# ---------------------------------------------
@app.route("/stocktakes/<int:session_id>/confirm", methods=["POST"])
@login_required
def stocktake_confirm(session_id):
    conn = get_db()
    session = conn.execute(
        "SELECT * FROM stocktake_sessions WHERE id = ?",
        (session_id,),
    ).fetchone()
    if not session:
        conn.close()
        return "棚卸が見つかりません。", 404
    if not is_admin_user() and session["store_id"] != current_user.store_id:
        conn.close()
        return "権限がありません。", 403
    if session["status"] == "confirmed":
        conn.close()
        return redirect(f"/stocktakes/{session_id}")

    items = conn.execute(
        """
        SELECT si.material_id, si.counted_quantity, m.name AS material_name
        FROM stocktake_items si
        JOIN materials m ON si.material_id = m.id
        WHERE si.session_id = ?
        """,
        (session_id,),
    ).fetchall()

    system_stock = fetch_store_stock_levels(conn, session["store_id"])
    movement_type = conn.execute(
        "SELECT id FROM movement_types WHERE name = '棚卸調整'"
    ).fetchone()
    movement_type_id = movement_type["id"] if movement_type else None
    if not movement_type_id:
        conn.close()
        return "棚卸調整の入出庫種別が見つかりません。", 500

    adjustments = []
    for item in items:
        material_id = item["material_id"]
        counted = item["counted_quantity"]
        current = system_stock.get(material_id, 0) or 0
        diff = float(counted) - float(current)
        if abs(diff) < 1e-9:
            continue
        memo = f"棚卸調整 (stocktake:{session_id})"
        adjustments.append(
            (
                session["store_id"],
                material_id,
                movement_type_id,
                diff,
                session["count_date"],
                memo,
            )
        )

    if adjustments:
        conn.executemany(
            """
            INSERT INTO inventory_movements
            (store_id, material_id, movement_type_id, quantity, datetime, memo)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            adjustments,
        )

    conn.execute(
        """
        UPDATE stocktake_sessions
        SET status = 'confirmed', confirmed_at = datetime('now')
        WHERE id = ?
        """,
        (session_id,),
    )
    conn.commit()
    conn.close()

    return redirect(f"/stocktakes/{session_id}")


# ---------------------------------------------
# 入出庫編集フォーム表示（GET）
# ---------------------------------------------
@app.route("/movements/<int:movement_id>/edit", methods=["GET"])
@login_required
def edit_movement_form(movement_id):
    conn = get_db()

    movement = conn.execute(
        """
        SELECT * FROM inventory_movements
        WHERE id = ?
        """,
        (movement_id,),
    ).fetchone()

    materials = conn.execute("SELECT id, name FROM materials").fetchall()
    movement_types = conn.execute("SELECT id, name FROM movement_types").fetchall()
    stores = conn.execute("SELECT id, name FROM stores").fetchall()

    conn.close()

    return render_template(
        "edit_movement.html",
        movement=movement,
        materials=materials,
        movement_types=movement_types,
        stores=stores,
    )


# ---------------------------------------------
# 入出庫更新処理（POST）
# ---------------------------------------------
@app.route("/movements/<int:movement_id>/edit", methods=["POST"])
@login_required
def edit_movement(movement_id):
    store_id = request.form["store_id"]
    material_id = request.form["material_id"]
    movement_type_id = request.form["movement_type_id"]
    quantity = request.form["quantity"]
    datetime_value = request.form["datetime"]
    memo = request.form.get("memo", "")

    conn = get_db()
    conn.execute(
        """
        UPDATE inventory_movements
        SET store_id = ?, material_id = ?, movement_type_id = ?, 
            quantity = ?, datetime = ?, memo = ?
        WHERE id = ?
        """,
        (
            store_id,
            material_id,
            movement_type_id,
            quantity,
            datetime_value,
            memo,
            movement_id,
        ),
    )
    conn.commit()
    conn.close()

    return redirect("/movements")


# ---------------------------------------------
# 入出庫削除（POST）
# ---------------------------------------------
@app.route("/movements/<int:movement_id>/delete", methods=["POST"])
@login_required
def delete_movement(movement_id):
    conn = get_db()
    conn.execute("DELETE FROM inventory_movements WHERE id = ?", (movement_id,))
    conn.commit()
    conn.close()

    return redirect("/movements")


if __name__ == "__main__":
    app.run(debug=True)
