from flask import Flask, render_template, request, redirect, url_for, flash
import mysql.connector
from mysql.connector import Error
import os
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", "default_secret")


# ----------------- Database Connection -----------------
def get_connection():
    return mysql.connector.connect(
        host=os.getenv("DB_HOST", "localhost"),
        user=os.getenv("DB_USER", "root"),
        password=os.getenv("DB_PASSWORD", ""),
        database=os.getenv("DB_NAME", "wastemanagementnew2")
    )


# ----------------- Helpers -----------------
def get_all_tables(cursor):
    cursor.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = DATABASE()
        ORDER BY table_name
    """)
    results = cursor.fetchall()
    return [row['TABLE_NAME'] for row in results]  # MySQL returns uppercase keys


def get_table_columns(cursor, table_name):
    cursor.execute("""
        SELECT column_name, data_type, column_key, extra, is_nullable,
               column_default
        FROM information_schema.columns
        WHERE table_schema = DATABASE() AND table_name = %s
        ORDER BY ordinal_position
    """, (table_name,))
    columns = cursor.fetchall()
    # Normalize keys to lowercase
    return [{k.lower(): v for k, v in col.items()} for col in columns]


def get_primary_key(cursor, table_name):
    cursor.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = DATABASE() 
          AND table_name = %s
          AND column_key = 'PRI'
    """, (table_name,))
    result = cursor.fetchone()
    return result['COLUMN_NAME'] if result else None


def get_foreign_keys(cursor, table_name):
    """Return dict of {column_name: (referenced_table, referenced_column)}"""
    cursor.execute("""
        SELECT column_name, referenced_table_name, referenced_column_name
        FROM information_schema.key_column_usage
        WHERE table_schema = DATABASE()
          AND table_name = %s
          AND referenced_table_name IS NOT NULL
    """, (table_name,))
    fks = cursor.fetchall()
    return {row['COLUMN_NAME']: (row['REFERENCED_TABLE_NAME'], row['REFERENCED_COLUMN_NAME']) for row in fks}


def get_fk_options(cursor, table_name):
    """Return dict {fk_col: list of dicts} for dropdowns"""
    fks = get_foreign_keys(cursor, table_name)
    options = {}
    for col, (ref_table, ref_col) in fks.items():
        cursor.execute(f"SELECT {ref_col} FROM {ref_table}")
        options[col] = [r[ref_col] for r in cursor.fetchall()]
    return options


# ----------------- Routes -----------------
@app.route('/')
def home():
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    tables = get_all_tables(cursor)
    cursor.close()
    conn.close()
    return render_template('home.html', tables=tables)


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
    pk_col = next((col['column_name'] for col in columns if col.get('column_key') == 'PRI'), None)

    cursor.close()
    conn.close()
    return render_template('table_view.html', table_name=table_name, columns=columns, rows=rows, pk_col=pk_col)


@app.route('/table/<table_name>/add', methods=['GET', 'POST'])
def add_row(table_name):
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    columns = get_table_columns(cursor, table_name)
    fk_options = get_fk_options(cursor, table_name)

    if request.method == 'POST':
        insert_cols = [col['column_name'] for col in columns if 'auto_increment' not in col['extra'].lower()]
        values = [request.form.get(col) for col in insert_cols]
        placeholders = ', '.join(['%s'] * len(insert_cols))
        col_names = ', '.join(insert_cols)
        sql = f"INSERT INTO {table_name} ({col_names}) VALUES ({placeholders})"
        cursor.execute(sql, values)
        conn.commit()
        cursor.close()
        conn.close()
        return redirect(url_for('view_table', table_name=table_name))

    cursor.close()
    conn.close()
    return render_template('table_add.html', table_name=table_name, columns=columns, fk_options=fk_options)


@app.route('/table/<table_name>/edit/<pk_value>', methods=['GET', 'POST'])
def edit_row(table_name, pk_value):
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    columns = get_table_columns(cursor, table_name)
    pk_col = get_primary_key(cursor, table_name)
    fk_options = get_fk_options(cursor, table_name)

    if request.method == 'POST':
        update_cols = [col['column_name'] for col in columns if col['column_name'] != pk_col]
        set_clause = ', '.join([f"{col} = %s" for col in update_cols])
        values = [request.form.get(col) for col in update_cols]
        sql = f"UPDATE {table_name} SET {set_clause} WHERE {pk_col} = %s"
        values.append(pk_value)
        cursor.execute(sql, values)
        conn.commit()
        cursor.close()
        conn.close()
        return redirect(url_for('view_table', table_name=table_name))

    cursor.execute(f"SELECT * FROM {table_name} WHERE {pk_col} = %s", (pk_value,))
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    return render_template('table_edit.html', table_name=table_name, columns=columns, row=row, pk_col=pk_col, fk_options=fk_options)


@app.route('/table/<table_name>/delete/<pk_value>', methods=['POST'])
def delete_row(table_name, pk_value):
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    tables = get_all_tables(cursor)
    if table_name not in tables:
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


# ----------------- Additional Views -----------------
@app.route('/workers_with_roles')
def workers_with_roles():
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT w.WorkerID, w.Name AS WorkerName, r.RoleName, w.Contact, w.HireDate
        FROM Worker w
        LEFT JOIN WorkerRole r ON w.RoleID = r.RoleID
        ORDER BY w.Name;
    """)
    workers = cursor.fetchall()
    cursor.close()
    conn.close()
    return render_template('worker_roles.html', workers=workers)


@app.route('/plants_info')
def plants_info():
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT tp.PlantID, tp.Name AS PlantName, tp.Location, tp.Type, tp.Capacity,
               w.WorkerID, w.Name AS ManagerName
        FROM TreatmentPlant tp
        LEFT JOIN Worker w ON tp.Manager = w.Name
        ORDER BY tp.Name;
    """)
    plants = cursor.fetchall()
    cursor.close()
    conn.close()
    return render_template('plants_info.html', plants=plants)


@app.route('/batch_details')
def batch_details():
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT pb.BatchID, pb.StartDate, pb.EndDate, pb.BatchType, pb.OutputQuantity,
               cu.UsageID, ct.TypeName AS ChemicalName, cu.QuantityUsed, cu.DateApplied,
               wl.WorkerID, w.Name AS WorkerName, wl.HoursWorked, wl.TaskDescription,
               op.ProductID, pt.TypeName AS ProductType, op.Quantity, qg.GradeName, op.DispatchDate
        FROM ProcessBatch pb
        LEFT JOIN ChemicalUsage cu ON pb.BatchID = cu.BatchID
        LEFT JOIN ChemicalType ct ON cu.ChemicalTypeID = ct.ChemicalTypeID
        LEFT JOIN WorkerLog wl ON pb.BatchID = wl.BatchID
        LEFT JOIN Worker w ON wl.WorkerID = w.WorkerID
        LEFT JOIN OutputProduct op ON pb.BatchID = op.BatchID
        LEFT JOIN ProductType pt ON op.ProductTypeID = pt.ProductTypeID
        LEFT JOIN QualityGrade qg ON op.GradeID = qg.GradeID
        ORDER BY pb.StartDate DESC;
    """)
    batch_records = cursor.fetchall()
    cursor.close()
    conn.close()
    return render_template('batch_detail.html', batch_records=batch_records)


if __name__ == '__main__':
    app.run(debug=True)
