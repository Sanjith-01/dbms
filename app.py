from flask import Flask, render_template, request, redirect, url_for, flash
import mysql.connector
from mysql.connector import Error
import os
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", "default_secret")

# Database connection
def get_connection():
    return mysql.connector.connect(
        host=os.getenv("DB_HOST", "localhost"),
        user=os.getenv("DB_USER", "root"),
        password=os.getenv("DB_PASSWORD", "SaNjith10"),
        database=os.getenv("DB_NAME", "wastemanagementnew")
    )

# Helper to get all tables
#cursor = conn.cursor(dictionary=True)

def get_all_tables(cursor):
    cursor.execute("""
        SELECT table_name FROM information_schema.tables 
        WHERE table_schema = DATABASE()
        ORDER BY table_name
    """)
    results = cursor.fetchall()
    return [row['TABLE_NAME'] for row in results]
 # ✅ works only with dictionary=True

# Helper to get table columns
def get_table_columns(cursor, table_name):
    cursor.execute("""
        SELECT column_name, data_type, column_key, extra, is_nullable 
        FROM information_schema.columns 
        WHERE table_schema = DATABASE() AND table_name = %s
        ORDER BY ordinal_position
    """, (table_name,))
    columns = cursor.fetchall()
    cursor.close()
    return columns

# Home
@app.route('/')
def home():
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    tables = get_all_tables(cursor)
    cursor.close()
    conn.close()
    return render_template('home.html', tables=tables)

# View Table
@app.route('/table/<table_name>')
def view_table(table_name):
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)

    tables = get_all_tables(cursor)
    if table_name not in tables:
        flash(f"Table `{table_name}` does not exist.", "danger")
        return redirect(url_for('home'))

    columns = get_table_columns(cursor, table_name)
    cursor.execute(f"SELECT * FROM `{table_name}`")
    rows = cursor.fetchall()

    # Get primary key column name
    pk_col = next((col['column_name'] for col in columns if col['column_key'] == 'PRI'), None)

    cursor.close()
    conn.close()
    return render_template('table_view.html', table_name=table_name, columns=columns, rows=rows, pk_col=pk_col)

# Add Row
@app.route('/table/<table_name>/add', methods=['GET', 'POST'])
def add_row(table_name):
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)

    if table_name not in get_all_tables(cursor):
        flash(f"Table `{table_name}` does not exist.", "danger")
        return redirect(url_for('home'))

    columns = get_table_columns(cursor, table_name)
    form_columns = [col for col in columns if 'auto_increment' not in col['extra'].lower()]

    if request.method == 'POST':
        col_names = [col['column_name'] for col in form_columns]
        values = [request.form.get(col) or None for col in col_names]
        placeholders = ', '.join(['%s'] * len(col_names))
        col_str = ', '.join([f"`{col}`" for col in col_names])
        try:
            cursor.execute(f"INSERT INTO `{table_name}` ({col_str}) VALUES ({placeholders})", tuple(values))
            conn.commit()
            flash("Row added successfully!", "success")
            return redirect(url_for('view_table', table_name=table_name))
        except Error as e:
            flash(f"Error adding row: {e}", "danger")

    cursor.close()
    conn.close()
    return render_template('table_add.html', table_name=table_name, columns=form_columns)

# Edit Row
@app.route('/table/<table_name>/edit/<pk_value>', methods=['GET', 'POST'])
def edit_row(table_name, pk_value):
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)

    if table_name not in get_all_tables(cursor):
        flash(f"Table `{table_name}` does not exist.", "danger")
        return redirect(url_for('home'))

    columns = get_table_columns(cursor, table_name)
    pk_col = next((col['column_name'] for col in columns if col['column_key'] == 'PRI'), None)

    if not pk_col:
        flash("No primary key defined for this table, edit not supported.", "warning")
        return redirect(url_for('view_table', table_name=table_name))

    if request.method == 'POST':
        update_cols = [col['column_name'] for col in columns if col['column_name'] != pk_col]
        values = [request.form.get(col) or None for col in update_cols]
        set_clause = ", ".join([f"`{col}`=%s" for col in update_cols])
        try:
            cursor.execute(f"UPDATE `{table_name}` SET {set_clause} WHERE `{pk_col}`=%s", tuple(values) + (pk_value,))
            conn.commit()
            flash("Row updated successfully!", "success")
            return redirect(url_for('view_table', table_name=table_name))
        except Error as e:
            flash(f"Error updating row: {e}", "danger")

    cursor.execute(f"SELECT * FROM `{table_name}` WHERE `{pk_col}`=%s", (pk_value,))
    row = cursor.fetchone()

    cursor.close()
    conn.close()
    if not row:
        flash("Row not found.", "warning")
        return redirect(url_for('view_table', table_name=table_name))

    return render_template('table_edit.html', table_name=table_name, columns=columns, row=row, pk_col=pk_col)

# Delete Row
@app.route('/table/<table_name>/delete/<pk_value>', methods=['POST'])
def delete_row(table_name, pk_value):
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)

    if table_name not in get_all_tables(cursor):
        flash(f"Table `{table_name}` does not exist.", "danger")
        return redirect(url_for('home'))

    columns = get_table_columns(cursor, table_name)
    pk_col = next((col['column_name'] for col in columns if col['column_key'] == 'PRI'), None)

    if not pk_col:
        flash("No primary key defined for this table, delete not supported.", "warning")
        return redirect(url_for('view_table', table_name=table_name))

    try:
        cursor.execute(f"DELETE FROM `{table_name}` WHERE `{pk_col}`=%s", (pk_value,))
        conn.commit()
        flash("Row deleted successfully!", "success")
    except Error as e:
        flash(f"Error deleting row: {e}", "danger")

    cursor.close()
    conn.close()
    return redirect(url_for('view_table', table_name=table_name))

if __name__ == '__main__':
    app.run(debug=True)
