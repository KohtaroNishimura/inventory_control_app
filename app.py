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
import sqlite3

app = Flask(__name__)
app.secret_key = "change-this-secret-key"
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = "login_form"

DATABASE = "inventory_control.db"


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
    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row  # ← 辞書のように列名でアクセスできる
    return conn


# ---------------------------------------------
# 材料一覧
# ---------------------------------------------
@app.route("/materials")
@login_required
def materials_list():
    conn = get_db()
    materials = conn.execute("SELECT * FROM materials").fetchall()
    category_columns = conn.execute("PRAGMA table_info(material_categories)").fetchall()
    has_category_perishable = any(col["name"] == "is_perishable" for col in category_columns)

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
    conn.close()
    return render_template("add_material.html", categories=categories)


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
    conn.execute(
        """
        INSERT INTO materials 
        (name, unit, price_per_unit, minimum_stock, category_id, memo)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (name, unit, price, minimum_stock, category_id, memo),
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

    conn.close()

    return render_template(
        "edit_material.html",
        material=material,
        categories=categories
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
        SELECT im.id, im.quantity, im.total_price, im.datetime, im.memo,
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
    total_price = request.form.get("total_price")
    memo = request.form.get("memo", "")
    datetime_value = request.form.get("datetime")

    if not (store_id and material_id and movement_type_id and quantity and datetime_value):
        return "必要な項目が未入力です。", 400

    conn = get_db()
    conn.execute(
        """
        INSERT INTO inventory_movements 
        (store_id, material_id, movement_type_id, quantity, total_price, datetime, memo)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            store_id,
            material_id,
            movement_type_id,
            quantity,
            total_price,
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
    total_price = request.form.get("total_price", None)
    datetime_value = request.form["datetime"]
    memo = request.form.get("memo", "")

    conn = get_db()
    conn.execute(
        """
        UPDATE inventory_movements
        SET store_id = ?, material_id = ?, movement_type_id = ?, 
            quantity = ?, total_price = ?, datetime = ?, memo = ?
        WHERE id = ?
        """,
        (
            store_id,
            material_id,
            movement_type_id,
            quantity,
            total_price,
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
