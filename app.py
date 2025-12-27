import os
import math
import re
from datetime import date, timedelta

from flask import Flask, render_template, request, redirect
from flask_login import (
    LoginManager,
    login_user,
    login_required,
    logout_user,
    UserMixin,
    current_user,
)
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import text, inspect
from sqlalchemy.exc import IntegrityError
from werkzeug.security import check_password_hash

app = Flask(__name__, instance_relative_config=True)
app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", "dev-secret-key")

db_url = os.environ.get("DATABASE_URL")
if db_url:
    if db_url.startswith("postgres://"):
        db_url = db_url.replace("postgres://", "postgresql://", 1)
    if db_url.startswith("postgresql://"):
        db_url = db_url.replace("postgresql://", "postgresql+psycopg://", 1)
    app.config["SQLALCHEMY_DATABASE_URI"] = db_url
else:
    os.makedirs(app.instance_path, exist_ok=True)
    app.config["SQLALCHEMY_DATABASE_URI"] = (
        f"sqlite:///{os.path.join(app.instance_path, 'inventory_control.db')}"
    )

app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

db = SQLAlchemy(app)
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = "login_form"


class Company(db.Model):
    __tablename__ = "companies"

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String, nullable=False)
    type = db.Column(db.String)


class Store(db.Model):
    __tablename__ = "stores"

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String, nullable=False)
    company_id = db.Column(db.Integer, db.ForeignKey("companies.id"))


class User(db.Model, UserMixin):
    __tablename__ = "users"

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String, nullable=False)
    email = db.Column(db.String, unique=True, nullable=False)
    password_hash = db.Column(db.String, nullable=False)
    role = db.Column(db.String, nullable=False)
    store_id = db.Column(db.Integer, db.ForeignKey("stores.id"))


class MaterialCategory(db.Model):
    __tablename__ = "material_categories"

    id = db.Column(db.Integer, primary_key=True)
    category_name = db.Column(db.String, nullable=False)
    is_perishable = db.Column(db.Integer, default=0)


class Material(db.Model):
    __tablename__ = "materials"

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String, nullable=False)
    unit = db.Column(db.String)
    price_per_unit = db.Column(db.Float)
    minimum_stock = db.Column(db.Float)
    category_id = db.Column(db.Integer, db.ForeignKey("material_categories.id"))
    perishable = db.Column(db.Integer, default=0)
    memo = db.Column(db.Text)


class MovementType(db.Model):
    __tablename__ = "movement_types"

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String, nullable=False)


class InventoryMovement(db.Model):
    __tablename__ = "inventory_movements"

    id = db.Column(db.Integer, primary_key=True)
    store_id = db.Column(db.Integer, db.ForeignKey("stores.id"), nullable=False)
    material_id = db.Column(db.Integer, db.ForeignKey("materials.id"), nullable=False)
    movement_type_id = db.Column(
        db.Integer, db.ForeignKey("movement_types.id"), nullable=False
    )
    quantity = db.Column(db.Float, nullable=False)
    total_price = db.Column(db.Float)
    datetime = db.Column(db.String, nullable=False)
    memo = db.Column(db.Text)


class ForecastOrder(db.Model):
    __tablename__ = "forecast_orders"

    id = db.Column(db.Integer, primary_key=True)
    store_id = db.Column(db.Integer, db.ForeignKey("stores.id"), nullable=False)
    material_id = db.Column(db.Integer, db.ForeignKey("materials.id"), nullable=False)
    quantity = db.Column(db.Float, nullable=False)
    order_date = db.Column(db.String, nullable=False)
    status = db.Column(db.String, nullable=False)


class StockCount(db.Model):
    __tablename__ = "stock_counts"

    id = db.Column(db.Integer, primary_key=True)
    store_id = db.Column(db.Integer, db.ForeignKey("stores.id"), nullable=False)
    material_id = db.Column(db.Integer, db.ForeignKey("materials.id"), nullable=False)
    counted_quantity = db.Column(db.Float, nullable=False)
    count_date = db.Column(db.String, nullable=False)
    type = db.Column(db.String)


class MaterialStoreMinimum(db.Model):
    __tablename__ = "material_store_minimums"
    __table_args__ = (
        db.UniqueConstraint("material_id", "store_id", name="idx_material_store_minimums_unique"),
    )

    id = db.Column(db.Integer, primary_key=True)
    material_id = db.Column(db.Integer, db.ForeignKey("materials.id"), nullable=False)
    store_id = db.Column(db.Integer, db.ForeignKey("stores.id"), nullable=False)
    minimum_stock = db.Column(db.Float)


class DailyReport(db.Model):
    __tablename__ = "daily_reports"
    __table_args__ = (
        db.UniqueConstraint("store_id", "date", name="idx_daily_reports_store_date"),
    )

    id = db.Column(db.Integer, primary_key=True)
    store_id = db.Column(db.Integer, db.ForeignKey("stores.id"), nullable=False)
    date = db.Column(db.String, nullable=False)
    sales = db.Column(db.Float)
    wasted_takoyaki = db.Column(db.Integer)
    production_sets = db.Column(db.Integer)
    working_hours = db.Column(db.Float)
    next_material_delivery = db.Column(db.String)
    remarks = db.Column(db.Text)


class DailyReportOrder(db.Model):
    __tablename__ = "daily_report_orders"
    __table_args__ = (
        db.UniqueConstraint(
            "daily_report_id",
            "material_id",
            name="idx_daily_report_orders_unique_material",
        ),
    )

    id = db.Column(db.Integer, primary_key=True)
    daily_report_id = db.Column(
        db.Integer, db.ForeignKey("daily_reports.id"), nullable=False
    )
    material_id = db.Column(db.Integer, db.ForeignKey("materials.id"), nullable=False)
    quantity = db.Column(db.Float, nullable=False)


class StocktakeSession(db.Model):
    __tablename__ = "stocktake_sessions"

    id = db.Column(db.Integer, primary_key=True)
    company_id = db.Column(db.Integer, db.ForeignKey("companies.id"))
    store_id = db.Column(db.Integer, db.ForeignKey("stores.id"), nullable=False)
    count_date = db.Column(db.String, nullable=False)
    session_type = db.Column(db.String, server_default="ad_hoc")
    count_month = db.Column(db.String)
    status = db.Column(db.String, server_default="draft")
    notes = db.Column(db.Text)
    confirmed_at = db.Column(db.String)


class StocktakeItem(db.Model):
    __tablename__ = "stocktake_items"
    __table_args__ = (
        db.UniqueConstraint("session_id", "material_id", name="idx_stocktake_items_unique"),
    )

    id = db.Column(db.Integer, primary_key=True)
    session_id = db.Column(db.Integer, db.ForeignKey("stocktake_sessions.id"), nullable=False)
    material_id = db.Column(db.Integer, db.ForeignKey("materials.id"), nullable=False)
    counted_quantity = db.Column(db.Float, nullable=False)


class StocktakeOrderItem(db.Model):
    __tablename__ = "stocktake_order_items"
    __table_args__ = (
        db.UniqueConstraint(
            "session_id",
            "material_id",
            name="idx_stocktake_order_items_unique",
        ),
    )

    id = db.Column(db.Integer, primary_key=True)
    session_id = db.Column(db.Integer, db.ForeignKey("stocktake_sessions.id"), nullable=False)
    material_id = db.Column(db.Integer, db.ForeignKey("materials.id"), nullable=False)
    quantity = db.Column(db.Float, nullable=False)


@login_manager.user_loader
def load_user(user_id):
    if not user_id:
        return None
    return db.session.get(User, int(user_id))


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

    user = User.query.filter_by(email=email).first()
    if user and check_password_hash(user.password_hash, password):
        login_user(user)
        return redirect("/")

    return "ログイン失敗：メールアドレスまたはパスワードが違います"


@app.route("/logout")
@login_required
def logout():
    logout_user()
    return redirect("/login")


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


def normalize_params(sql, params):
    if params is None or params == () or params == []:
        return sql, {}
    if isinstance(params, dict):
        return sql, params
    out = []
    index = 0
    for ch in sql:
        if ch == "?":
            index += 1
            out.append(f":p{index}")
        else:
            out.append(ch)
    bind_params = {f"p{i}": params[i - 1] for i in range(1, index + 1)}
    return "".join(out), bind_params


def db_execute(sql, params=None):
    sql, bind_params = normalize_params(sql, params)
    return db.session.execute(text(sql), bind_params)


def db_fetchall(sql, params=None):
    return db_execute(sql, params).mappings().all()


def db_fetchone(sql, params=None):
    return db_execute(sql, params).mappings().first()


def db_fetchscalar(sql, params=None):
    return db_execute(sql, params).scalar()


def ensure_schema():
    db.create_all()

    inspector = inspect(db.engine)
    if "stocktake_sessions" in inspector.get_table_names():
        columns = {column["name"] for column in inspector.get_columns("stocktake_sessions")}
        if "session_type" not in columns:
            db.session.execute(
                text(
                    "ALTER TABLE stocktake_sessions ADD COLUMN session_type TEXT DEFAULT 'ad_hoc'"
                )
            )
        if "count_month" not in columns:
            db.session.execute(
                text("ALTER TABLE stocktake_sessions ADD COLUMN count_month TEXT")
            )

    db.session.execute(
        text(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_material_store_minimums_unique
            ON material_store_minimums(material_id, store_id)
            """
        )
    )
    db.session.execute(
        text(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_daily_reports_store_date
            ON daily_reports(store_id, date)
            """
        )
    )
    db.session.execute(
        text(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_daily_report_orders_unique_material
            ON daily_report_orders(daily_report_id, material_id)
            """
        )
    )
    db.session.execute(
        text(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_stocktake_items_unique
            ON stocktake_items(session_id, material_id)
            """
        )
    )
    db.session.execute(
        text(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_stocktake_order_items_unique
            ON stocktake_order_items(session_id, material_id)
            """
        )
    )
    db.session.execute(
        text(
            """
            CREATE INDEX IF NOT EXISTS idx_stocktake_sessions_type_month
            ON stocktake_sessions(session_type, count_month)
            """
        )
    )
    db.session.execute(
        text(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_stocktake_sessions_monthly_unique
            ON stocktake_sessions(store_id, count_month)
            WHERE session_type = 'monthly'
            """
        )
    )
    db.session.commit()


with app.app_context():
    ensure_schema()


# ---------------------------------------------
# 材料一覧
# ---------------------------------------------
@app.route("/materials")
@login_required
def materials_list():
    materials = db_fetchall("SELECT * FROM materials")
    stores = db_fetchall("SELECT id, name FROM stores")
    store_minimum_rows = db_fetchall(
        "SELECT material_id, store_id, minimum_stock FROM material_store_minimums"
    )

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
            return redirect("/materials")
        selected_store = next((store for store in stores if store["id"] == store_id_int), None)
        if not selected_store:
            return redirect("/materials")
        selected_store_id = store_id_int

    stock_rows = db_fetchall(
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
    )
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

    category_rows = db_fetchall("SELECT id, category_name FROM material_categories")

    categories = {}
    for category in category_rows:
        categories[category["id"]] = {
            "id": category["id"],
            "category_name": category["category_name"],
        }

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
    categories = db_fetchall("SELECT id, category_name FROM material_categories")
    stores = db_fetchall("SELECT id, name FROM stores")
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

    result = db_execute(
        """
        INSERT INTO materials 
        (name, unit, price_per_unit, minimum_stock, category_id, memo)
        VALUES (?, ?, ?, ?, ?, ?)
        RETURNING id
        """,
        (name, unit, price, minimum_stock, category_id, memo),
    )
    material_id = result.scalar()

    store_rows = db_fetchall("SELECT id FROM stores")
    for store in store_rows:
        field_name = f"minimum_stock_store_{store['id']}"
        raw_value = request.form.get(field_name)
        if raw_value is None or raw_value == "":
            continue
        try:
            min_value = float(raw_value)
        except ValueError:
            continue
        db_execute(
            """
            INSERT INTO material_store_minimums (material_id, store_id, minimum_stock)
            VALUES (?, ?, ?)
            """,
            (material_id, store["id"], min_value),
        )

    db.session.commit()

    return redirect("/materials")


# ---------------------------------------------
# 材料編集フォームの表示（GET）
# ---------------------------------------------
@app.route("/materials/<int:material_id>/edit", methods=["GET"])
@login_required
def edit_material_form(material_id):
    material = db_fetchone("SELECT * FROM materials WHERE id = ?", (material_id,))

    categories = db_fetchall("SELECT id, category_name FROM material_categories")

    stores = db_fetchall("SELECT id, name FROM stores")
    store_minimum_rows = db_fetchall(
        "SELECT store_id, minimum_stock FROM material_store_minimums WHERE material_id = ?",
        (material_id,),
    )
    store_minimums = {row["store_id"]: row["minimum_stock"] for row in store_minimum_rows}

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

    db_execute(
        """
        UPDATE materials
        SET name=?, unit=?, price_per_unit=?, minimum_stock=?,
            category_id=?, memo=?
        WHERE id=?
        """,
        (name, unit, price, minimum_stock, category_id, memo, material_id),
    )
    db_execute(
        "DELETE FROM material_store_minimums WHERE material_id = ?", (material_id,)
    )

    store_rows = db_fetchall("SELECT id FROM stores")
    for store in store_rows:
        field_name = f"minimum_stock_store_{store['id']}"
        raw_value = request.form.get(field_name)
        if raw_value is None or raw_value == "":
            continue
        try:
            min_value = float(raw_value)
        except ValueError:
            continue
        db_execute(
            """
            INSERT INTO material_store_minimums (material_id, store_id, minimum_stock)
            VALUES (?, ?, ?)
            """,
            (material_id, store["id"], min_value),
        )

    db.session.commit()

    return redirect("/materials")


# ---------------------------------------------
# 材料削除の確認画面（GET）
# ---------------------------------------------
@app.route("/materials/<int:material_id>/delete", methods=["GET"])
@login_required
def delete_material_confirm(material_id):
    material = db_fetchone("SELECT * FROM materials WHERE id=?", (material_id,))

    return render_template("delete_material.html", material=material)


# ---------------------------------------------
# 材料削除（POST）
# ---------------------------------------------
@app.route("/materials/<int:material_id>/delete", methods=["POST"])
@login_required
def delete_material(material_id):
    db_execute("DELETE FROM materials WHERE id = ?", (material_id,))
    db.session.commit()

    return redirect("/materials")


# ---------------------------------------------
# 入出庫一覧表示
# ---------------------------------------------
@app.route("/movements")
@login_required
def movement_list():
    movements = db_fetchall(
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
    )
    return render_template("movement_list.html", movements=movements)


# ---------------------------------------------
# 入出庫フォーム表示（GET）
# ---------------------------------------------
@app.route("/movements/add", methods=["GET"])
@login_required
def movement_add_form():
    materials = db_fetchall("SELECT id, name FROM materials")
    movement_types = db_fetchall("SELECT id, name FROM movement_types")
    stores = db_fetchall("SELECT id, name FROM stores")

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

    db_execute(
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
    db.session.commit()

    return redirect("/movements")


# ---------------------------------------------
# 日報一覧
# ---------------------------------------------
@app.route("/daily_reports")
@login_required
def daily_reports_list():
    stores = db_fetchall("SELECT id, name FROM stores ORDER BY id")

    selected_store_id = current_user.store_id
    store_param = request.args.get("store_id")
    if is_admin_user() and store_param:
        parsed_store_id = parse_int(store_param)
        if parsed_store_id and any(store["id"] == parsed_store_id for store in stores):
            selected_store_id = parsed_store_id

    reports = db_fetchall(
        """
        SELECT dr.*, s.name AS store_name
        FROM daily_reports dr
        JOIN stores s ON dr.store_id = s.id
        WHERE dr.store_id = ?
        ORDER BY dr.date DESC, dr.id DESC
        """,
        (selected_store_id,),
    )

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


def fetch_store_stock_levels(store_id):
    rows = db_fetchall(
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
    )
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
    stores = db_fetchall("SELECT id, name FROM stores ORDER BY id")
    materials = db_fetchall(
        """
        SELECT m.id, m.name, m.unit, m.minimum_stock
        FROM materials m
        ORDER BY m.name
        """
    )

    selected_store_id = current_user.store_id
    store_param = request.args.get("store_id")
    if is_admin_user() and store_param:
        parsed_store_id = parse_int(store_param)
        if parsed_store_id and any(store["id"] == parsed_store_id for store in stores):
            selected_store_id = parsed_store_id

    store_stock = fetch_store_stock_levels(selected_store_id)
    store_minimum_rows = db_fetchall(
        "SELECT material_id, minimum_stock FROM material_store_minimums WHERE store_id = ?",
        (selected_store_id,),
    )
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

    stores = db_fetchall("SELECT id FROM stores")
    valid_store_ids = {store["id"] for store in stores}

    store_id = current_user.store_id
    if is_admin_user():
        store_id = parse_int(request.form.get("store_id")) or store_id
    if store_id not in valid_store_ids:
        return "店舗が不正です。", 400

    sales = parse_float(request.form.get("sales"))
    wasted_takoyaki = parse_int(request.form.get("wasted_takoyaki"))
    production_sets = parse_float(request.form.get("production_sets"))
    working_hours = parse_float(request.form.get("working_hours"))
    next_material_delivery = (request.form.get("next_material_delivery") or "").strip() or None
    remarks = (request.form.get("remarks") or "").strip() or None

    existing = db_fetchone(
        "SELECT id FROM daily_reports WHERE store_id = ? AND date = ?",
        (store_id, report_date),
    )

    if existing:
        daily_report_id = existing["id"]
        db_execute(
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
        result = db_execute(
            """
            INSERT INTO daily_reports
            (store_id, date, sales, wasted_takoyaki, production_sets, working_hours, next_material_delivery, remarks)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            RETURNING id
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
        daily_report_id = result.scalar()

    materials = db_fetchall("SELECT id FROM materials")
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

    db_execute(
        "DELETE FROM daily_report_orders WHERE daily_report_id = ?",
        (daily_report_id,),
    )
    if order_items:
        for order_item in order_items:
            db_execute(
                """
                INSERT INTO daily_report_orders (daily_report_id, material_id, quantity)
                VALUES (?, ?, ?)
                """,
                order_item,
            )

    db.session.commit()

    return redirect(f"/daily_reports/{daily_report_id}")


# ---------------------------------------------
# 日報詳細
# ---------------------------------------------
@app.route("/daily_reports/<int:daily_report_id>")
@login_required
def daily_report_detail(daily_report_id):
    report = db_fetchone(
        """
        SELECT dr.*, s.name AS store_name
        FROM daily_reports dr
        JOIN stores s ON dr.store_id = s.id
        WHERE dr.id = ?
        """,
        (daily_report_id,),
    )
    if not report:
        return "日報が見つかりません。", 404

    if not is_admin_user() and report["store_id"] != current_user.store_id:
        return "権限がありません。", 403

    orders = db_fetchall(
        """
        SELECT dro.quantity, m.name AS material_name, m.unit
        FROM daily_report_orders dro
        JOIN materials m ON dro.material_id = m.id
        WHERE dro.daily_report_id = ?
        ORDER BY m.name
        """,
        (daily_report_id,),
    )

    line_message = build_daily_report_line_message(report, report["store_name"], orders)

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
    report = db_fetchone(
        """
        SELECT dr.*, s.name AS store_name
        FROM daily_reports dr
        JOIN stores s ON dr.store_id = s.id
        WHERE dr.id = ?
        """,
        (daily_report_id,),
    )
    if not report:
        return "日報が見つかりません。", 404

    if not is_admin_user() and report["store_id"] != current_user.store_id:
        return "権限がありません。", 403

    stores = db_fetchall("SELECT id, name FROM stores ORDER BY id")
    materials = db_fetchall(
        """
        SELECT m.id, m.name, m.unit, m.minimum_stock
        FROM materials m
        ORDER BY m.name
        """
    )

    orders = db_fetchall(
        """
        SELECT dro.material_id, dro.quantity, m.name AS material_name, m.unit
        FROM daily_report_orders dro
        JOIN materials m ON dro.material_id = m.id
        WHERE dro.daily_report_id = ?
        """,
        (daily_report_id,),
    )
    order_quantities = {row["material_id"]: row["quantity"] for row in orders}

    store_stock = fetch_store_stock_levels(report["store_id"])
    store_minimum_rows = db_fetchall(
        "SELECT material_id, minimum_stock FROM material_store_minimums WHERE store_id = ?",
        (report["store_id"],),
    )
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
    report = db_fetchone("SELECT * FROM daily_reports WHERE id = ?", (daily_report_id,))
    if not report:
        return "日報が見つかりません。", 404

    if not is_admin_user() and report["store_id"] != current_user.store_id:
        return "権限がありません。", 403

    store_id = report["store_id"]
    if is_admin_user():
        store_id = parse_int(request.form.get("store_id")) or store_id

    report_date = (request.form.get("date") or "").strip()
    if not report_date:
        return "日付が未入力です。", 400

    conflict = db_fetchone(
        """
        SELECT id FROM daily_reports
        WHERE store_id = ? AND date = ? AND id != ?
        """,
        (store_id, report_date, daily_report_id),
    )
    if conflict:
        return "同じ店舗・同じ日付の日報が既に存在します。", 400

    sales = parse_float(request.form.get("sales"))
    wasted_takoyaki = parse_int(request.form.get("wasted_takoyaki"))
    production_sets = parse_float(request.form.get("production_sets"))
    working_hours = parse_float(request.form.get("working_hours"))
    next_material_delivery = (request.form.get("next_material_delivery") or "").strip() or None
    remarks = (request.form.get("remarks") or "").strip() or None

    db_execute(
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

    materials = db_fetchall("SELECT id FROM materials")
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

    db_execute(
        "DELETE FROM daily_report_orders WHERE daily_report_id = ?",
        (daily_report_id,),
    )
    if order_items:
        for order_item in order_items:
            db_execute(
                """
                INSERT INTO daily_report_orders (daily_report_id, material_id, quantity)
                VALUES (?, ?, ?)
                """,
                order_item,
            )

    db.session.commit()
    return redirect(f"/daily_reports/{daily_report_id}")


# ---------------------------------------------
# 日報削除（POST）
# ---------------------------------------------
@app.route("/daily_reports/<int:daily_report_id>/delete", methods=["POST"])
@login_required
def daily_report_delete(daily_report_id):
    report = db_fetchone("SELECT * FROM daily_reports WHERE id = ?", (daily_report_id,))
    if not report:
        return "日報が見つかりません。", 404
    if not is_admin_user() and report["store_id"] != current_user.store_id:
        return "権限がありません。", 403

    db_execute(
        "DELETE FROM daily_report_orders WHERE daily_report_id = ?",
        (daily_report_id,),
    )
    db_execute("DELETE FROM daily_reports WHERE id = ?", (daily_report_id,))
    db.session.commit()

    return redirect("/daily_reports")


# ---------------------------------------------
# 棚卸一覧
# ---------------------------------------------
@app.route("/stocktakes")
@login_required
def stocktake_list():
    stores = db_fetchall("SELECT id, name FROM stores ORDER BY id")

    selected_store_id = current_user.store_id
    store_param = request.args.get("store_id")
    if is_admin_user() and store_param:
        parsed_store_id = parse_int(store_param)
        if parsed_store_id and any(store["id"] == parsed_store_id for store in stores):
            selected_store_id = parsed_store_id

    sessions = db_fetchall(
        """
        SELECT ss.*, s.name AS store_name
        FROM stocktake_sessions ss
        JOIN stores s ON ss.store_id = s.id
        WHERE ss.store_id = ?
        ORDER BY ss.count_date DESC, ss.id DESC
        """,
        (selected_store_id,),
    )

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
    stores = db_fetchall("SELECT id, name FROM stores ORDER BY id")
    materials = db_fetchall(
        """
        SELECT m.id, m.name, m.unit, m.minimum_stock
        FROM materials m
        ORDER BY m.name
        """
    )

    selected_store_id = current_user.store_id
    store_param = request.args.get("store_id")
    if is_admin_user() and store_param:
        parsed_store_id = parse_int(store_param)
        if parsed_store_id and any(store["id"] == parsed_store_id for store in stores):
            selected_store_id = parsed_store_id

    store_stock = fetch_store_stock_levels(selected_store_id)
    store_minimum_rows = db_fetchall(
        "SELECT material_id, minimum_stock FROM material_store_minimums WHERE store_id = ?",
        (selected_store_id,),
    )
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

    stores = db_fetchall("SELECT id FROM stores")
    valid_store_ids = {store["id"] for store in stores}

    store_id = current_user.store_id
    if is_admin_user():
        store_id = parse_int(request.form.get("store_id")) or store_id
    if store_id not in valid_store_ids:
        return "店舗が不正です。", 400

    notes = (request.form.get("notes") or "").strip() or None

    try:
        result = db_execute(
            """
            INSERT INTO stocktake_sessions
            (company_id, store_id, count_date, session_type, count_month, status, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            RETURNING id
            """,
            (None, store_id, count_date, session_type, count_month, "draft", notes),
        )
        session_id = result.scalar()
    except IntegrityError:
        db.session.rollback()
        if session_type == "monthly":
            return "この店舗の月次棚卸しは既に作成されています。", 400
        raise

    materials = db_fetchall("SELECT id FROM materials")
    material_ids = {material["id"] for material in materials}

    items = []
    for material_id in material_ids:
        key = f"count_qty_{material_id}"
        value = request.form.get(key)
        counted = parse_float(value)
        if counted is None:
            db.session.rollback()
            return "全ての材料の棚卸数を入力してください。", 400
        items.append((session_id, material_id, counted))

    for item in items:
        db_execute(
            """
            INSERT INTO stocktake_items (session_id, material_id, counted_quantity)
            VALUES (?, ?, ?)
            """,
            item,
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
        for order_item in order_items:
            db_execute(
                """
                INSERT INTO stocktake_order_items (session_id, material_id, quantity)
                VALUES (?, ?, ?)
                """,
                order_item,
            )

    db.session.commit()

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

    stores = db_fetchall("SELECT id, name FROM stores ORDER BY id")
    materials = db_fetchall("SELECT id, name, unit FROM materials ORDER BY name")

    sessions = db_fetchall(
        """
        SELECT ss.*, s.name AS store_name
        FROM stocktake_sessions ss
        JOIN stores s ON ss.store_id = s.id
        WHERE ss.session_type = 'monthly' AND ss.count_month = ?
        ORDER BY ss.store_id
        """,
        (month,),
    )
    sessions_by_store = {row["store_id"]: row for row in sessions}

    count_rows = db_fetchall(
        """
        SELECT ss.store_id, si.material_id, si.counted_quantity
        FROM stocktake_sessions ss
        JOIN stocktake_items si ON si.session_id = ss.id
        WHERE ss.session_type = 'monthly' AND ss.count_month = ?
        """,
        (month,),
    )
    counts = {}
    for row in count_rows:
        counts.setdefault(row["material_id"], {})[row["store_id"]] = row[
            "counted_quantity"
        ]

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
    session = db_fetchone(
        """
        SELECT ss.*, s.name AS store_name
        FROM stocktake_sessions ss
        JOIN stores s ON ss.store_id = s.id
        WHERE ss.id = ?
        """,
        (session_id,),
    )
    if not session:
        return "棚卸が見つかりません。", 404

    if not is_admin_user() and session["store_id"] != current_user.store_id:
        return "権限がありません。", 403

    items = db_fetchall(
        """
        SELECT si.counted_quantity, m.name AS material_name, m.unit, m.id AS material_id
        FROM stocktake_items si
        JOIN materials m ON si.material_id = m.id
        WHERE si.session_id = ?
        ORDER BY m.name
        """,
        (session_id,),
    )

    order_items = db_fetchall(
        """
        SELECT soi.quantity, m.name AS material_name, m.unit, m.id AS material_id
        FROM stocktake_order_items soi
        JOIN materials m ON soi.material_id = m.id
        WHERE soi.session_id = ?
        ORDER BY m.name
        """,
        (session_id,),
    )

    line_message = build_stocktake_line_message(
        session, session["store_name"], order_items
    )
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
    session = db_fetchone(
        """
        SELECT ss.*, s.name AS store_name
        FROM stocktake_sessions ss
        JOIN stores s ON ss.store_id = s.id
        WHERE ss.id = ?
        """,
        (session_id,),
    )
    if not session:
        return "棚卸が見つかりません。", 404
    if not is_admin_user() and session["store_id"] != current_user.store_id:
        return "権限がありません。", 403
    if session["status"] == "confirmed":
        return "確定済みの棚卸は編集できません。", 403

    stores = db_fetchall("SELECT id, name FROM stores ORDER BY id")
    materials = db_fetchall(
        """
        SELECT m.id, m.name, m.unit, m.minimum_stock
        FROM materials m
        ORDER BY m.name
        """
    )

    item_rows = db_fetchall(
        """
        SELECT material_id, counted_quantity
        FROM stocktake_items
        WHERE session_id = ?
        """,
        (session_id,),
    )
    counted_map = {row["material_id"]: row["counted_quantity"] for row in item_rows}

    order_rows = db_fetchall(
        """
        SELECT material_id, quantity
        FROM stocktake_order_items
        WHERE session_id = ?
        """,
        (session_id,),
    )
    order_map = {row["material_id"]: row["quantity"] for row in order_rows}

    store_stock = fetch_store_stock_levels(session["store_id"])
    store_minimum_rows = db_fetchall(
        "SELECT material_id, minimum_stock FROM material_store_minimums WHERE store_id = ?",
        (session["store_id"],),
    )
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
    session = db_fetchone("SELECT * FROM stocktake_sessions WHERE id = ?", (session_id,))
    if not session:
        return "棚卸が見つかりません。", 404
    if not is_admin_user() and session["store_id"] != current_user.store_id:
        return "権限がありません。", 403
    if session["status"] == "confirmed":
        return "確定済みの棚卸は編集できません。", 403

    store_id = session["store_id"]
    if is_admin_user():
        store_id = parse_int(request.form.get("store_id")) or store_id

    count_date = (request.form.get("date") or "").strip()
    if not count_date:
        return "棚卸日が未入力です。", 400

    notes = (request.form.get("notes") or "").strip() or None

    session_type = (session["session_type"] or "ad_hoc").strip()
    count_month = None
    if session_type == "monthly":
        count_month = count_date[:7]
        if not re.match(r"^\d{4}-\d{2}$", count_month):
            return "月次棚卸しの棚卸日が不正です。", 400

    try:
        db_execute(
            """
            UPDATE stocktake_sessions
            SET store_id = ?, count_date = ?, count_month = ?, notes = ?
            WHERE id = ?
            """,
            (store_id, count_date, count_month, notes, session_id),
        )
    except IntegrityError:
        db.session.rollback()
        if session_type == "monthly":
            return "この店舗の月次棚卸しは既に作成されています。", 400
        raise

    materials = db_fetchall("SELECT id FROM materials")
    material_ids = {material["id"] for material in materials}

    items = []
    for material_id in material_ids:
        key = f"count_qty_{material_id}"
        value = request.form.get(key)
        counted = parse_float(value)
        if counted is None:
            db.session.rollback()
            return "全ての材料の棚卸数を入力してください。", 400
        items.append((session_id, material_id, counted))

    db_execute("DELETE FROM stocktake_items WHERE session_id = ?", (session_id,))
    for item in items:
        db_execute(
            """
            INSERT INTO stocktake_items (session_id, material_id, counted_quantity)
            VALUES (?, ?, ?)
            """,
            item,
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

    db_execute(
        "DELETE FROM stocktake_order_items WHERE session_id = ?",
        (session_id,),
    )
    if order_items:
        for order_item in order_items:
            db_execute(
                """
                INSERT INTO stocktake_order_items (session_id, material_id, quantity)
                VALUES (?, ?, ?)
                """,
                order_item,
            )

    db.session.commit()
    return redirect(f"/stocktakes/{session_id}")


# ---------------------------------------------
# 棚卸削除（POST）
# ---------------------------------------------
@app.route("/stocktakes/<int:session_id>/delete", methods=["POST"])
@login_required
def stocktake_delete(session_id):
    session = db_fetchone("SELECT * FROM stocktake_sessions WHERE id = ?", (session_id,))
    if not session:
        return "棚卸が見つかりません。", 404
    if not is_admin_user() and session["store_id"] != current_user.store_id:
        return "権限がありません。", 403
    if session["status"] == "confirmed":
        return "確定済みの棚卸は削除できません。", 403

    db_execute("DELETE FROM stocktake_order_items WHERE session_id = ?", (session_id,))
    db_execute("DELETE FROM stocktake_items WHERE session_id = ?", (session_id,))
    db_execute("DELETE FROM stocktake_sessions WHERE id = ?", (session_id,))
    db.session.commit()

    return redirect("/stocktakes")


# ---------------------------------------------
# 棚卸確定（差分調整）
# ---------------------------------------------
@app.route("/stocktakes/<int:session_id>/confirm", methods=["POST"])
@login_required
def stocktake_confirm(session_id):
    session = db_fetchone("SELECT * FROM stocktake_sessions WHERE id = ?", (session_id,))
    if not session:
        return "棚卸が見つかりません。", 404
    if not is_admin_user() and session["store_id"] != current_user.store_id:
        return "権限がありません。", 403
    if session["status"] == "confirmed":
        return redirect(f"/stocktakes/{session_id}")

    items = db_fetchall(
        """
        SELECT si.material_id, si.counted_quantity, m.name AS material_name
        FROM stocktake_items si
        JOIN materials m ON si.material_id = m.id
        WHERE si.session_id = ?
        """,
        (session_id,),
    )

    system_stock = fetch_store_stock_levels(session["store_id"])
    movement_type = db_fetchone(
        "SELECT id FROM movement_types WHERE name = '棚卸調整'"
    )
    movement_type_id = movement_type["id"] if movement_type else None
    if not movement_type_id:
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
        for adjustment in adjustments:
            db_execute(
                """
                INSERT INTO inventory_movements
                (store_id, material_id, movement_type_id, quantity, datetime, memo)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                adjustment,
            )

    db_execute(
        """
        UPDATE stocktake_sessions
        SET status = 'confirmed', confirmed_at = CURRENT_TIMESTAMP
        WHERE id = ?
        """,
        (session_id,),
    )
    db.session.commit()

    return redirect(f"/stocktakes/{session_id}")


# ---------------------------------------------
# 入出庫編集フォーム表示（GET）
# ---------------------------------------------
@app.route("/movements/<int:movement_id>/edit", methods=["GET"])
@login_required
def edit_movement_form(movement_id):
    movement = db_fetchone(
        """
        SELECT * FROM inventory_movements
        WHERE id = ?
        """,
        (movement_id,),
    )

    materials = db_fetchall("SELECT id, name FROM materials")
    movement_types = db_fetchall("SELECT id, name FROM movement_types")
    stores = db_fetchall("SELECT id, name FROM stores")

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

    db_execute(
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
    db.session.commit()

    return redirect("/movements")


# ---------------------------------------------
# 入出庫削除（POST）
# ---------------------------------------------
@app.route("/movements/<int:movement_id>/delete", methods=["POST"])
@login_required
def delete_movement(movement_id):
    db_execute("DELETE FROM inventory_movements WHERE id = ?", (movement_id,))
    db.session.commit()

    return redirect("/movements")


if __name__ == "__main__":
    app.run(debug=True)
