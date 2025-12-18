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
    category_columns = conn.execute("PRAGMA table_info(material_categories)").fetchall()
    has_category_perishable = any(col["name"] == "is_perishable" for col in category_columns)

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

    category_select = "id, category_name"
    if has_category_perishable:
        category_select += ", is_perishable"

    category_rows = conn.execute(
        f"SELECT {category_select} FROM material_categories"
    ).fetchall()

    categories = {}
    for category in category_rows:
        categories[category["id"]] = {
            "id": category["id"],
            "category_name": category["category_name"],
            "is_perishable": (
                category["is_perishable"] if has_category_perishable else None
            ),
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
