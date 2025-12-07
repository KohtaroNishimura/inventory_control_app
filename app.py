from flask import Flask, render_template, request, redirect
import sqlite3

app = Flask(__name__)

DATABASE = "inventory_control.db"


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
def materials_list():
    conn = get_db()
    materials = conn.execute("SELECT * FROM materials").fetchall()
    conn.close()
    return render_template("materials_list.html", materials=materials)


# ---------------------------------------------
# 材料登録フォームの表示
# ---------------------------------------------
@app.route("/materials/add", methods=["GET"])
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
def add_material():
    name = request.form["name"]
    unit = request.form["unit"]
    price = request.form["price"]
    minimum_stock = request.form["minimum_stock"]
    perishable = request.form.get("perishable", 0)
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
        (name, unit, price_per_unit, minimum_stock, perishable, category_id, memo)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (name, unit, price, minimum_stock, perishable, category_id, memo),
    )
    conn.commit()
    conn.close()

    return redirect("/materials")


# ---------------------------------------------
# 材料編集フォームの表示（GET）
# ---------------------------------------------
@app.route("/materials/<int:material_id>/edit", methods=["GET"])
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
def edit_material(material_id):
    name = request.form["name"]
    unit = request.form["unit"]
    price = request.form["price"]
    minimum_stock = request.form["minimum_stock"]
    perishable = request.form.get("perishable", 0)
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
            perishable=?, category_id=?, memo=?
        WHERE id=?
        """,
        (name, unit, price, minimum_stock, perishable, category_id, memo, material_id),
    )
    conn.commit()
    conn.close()

    return redirect("/materials")


if __name__ == "__main__":
    app.run(debug=True)
