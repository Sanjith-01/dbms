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


# ----------------- Utility helpers (robust to dict/tuple cursor rows) -----------------
def _row_get(row, key_name):
    """Return case-insensitive value from row which may be dict or tuple."""
    if row is None:
        return None
    if isinstance(row, dict):
        for k, v in row.items():
            if k.lower() == key_name.lower():
                return v
        return None
    else:  # tuple/list
        # When tuple, caller should access by index normally; but for single-col selects we can return row[0]
        return row[0] if len(row) >= 1 else None


# ----------------- Schema helpers -----------------
def get_all_tables(cursor):
    cursor.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = DATABASE()
        ORDER BY table_name
    """)
    results = cursor.fetchall()
    tables = []
    for r in results:
        v = _row_get(r, 'table_name')
        if v:
            tables.append(v)
    return tables


def get_table_columns(cursor, table_name):
    cursor.execute("""
        SELECT column_name, data_type, column_key, extra, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_schema = DATABASE() AND table_name = %s
        ORDER BY ordinal_position
    """, (table_name,))
    results = cursor.fetchall()

    cols = []
    for c in results:
        if isinstance(c, dict):
            cols.append({k.lower(): v for k, v in c.items()})
        else:
            # SELECT order: column_name, data_type, column_key, extra, is_nullable, column_default
            cols.append({
                'column_name': c[0],
                'data_type': c[1],
                'column_key': c[2],
                'extra': c[3] or '',
                'is_nullable': c[4],
                'column_default': c[5]
            })
    return cols


def get_primary_key(cursor, table_name):
    cursor.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = DATABASE()
          AND table_name = %s
          AND column_key = 'PRI'
    """, (table_name,))
    result = cursor.fetchone()
    return _row_get(result, 'column_name')


def get_foreign_keys(cursor, table_name):
    """
    Returns dict {fk_column_name: (referenced_table_name, referenced_column_name)}
    """
    cursor.execute("""
        SELECT column_name, referenced_table_name, referenced_column_name
        FROM information_schema.key_column_usage
        WHERE table_schema = DATABASE()
          AND table_name = %s
          AND referenced_table_name IS NOT NULL
    """, (table_name,))
    results = cursor.fetchall()
    fks = {}
    for r in results:
        if isinstance(r, dict):
            col = r.get(next(k for k in r if k.lower() == 'column_name' ), None)
            ref_table = r.get(next(k for k in r if k.lower() == 'referenced_table_name' ), None)
            ref_col = r.get(next(k for k in r if k.lower() == 'referenced_column_name' ), None)
            fks[col] = (ref_table, ref_col)
        else:
            # tuple: column_name, referenced_table_name, referenced_column_name
            fks[r[0]] = (r[1], r[2])
    return fks


def get_fk_options(cursor, table_name):
    """Return dict {fk_col: [possible_values]}"""
    fks = get_foreign_keys(cursor, table_name)
    options = {}
    for col, (ref_table, ref_col) in fks.items():
        # get all available keys from referenced table
        cursor.execute(f"SELECT {ref_col} FROM `{ref_table}`")
        rows = cursor.fetchall()
        vals = [_row_get(rr, ref_col) for rr in rows]
        options[col] = vals
    return options


def get_table_row_count(cursor, table_name):
    """Return integer count of rows in table"""
    try:
        cursor.execute(f"SELECT COUNT(*) AS cnt FROM `{table_name}`")
        r = cursor.fetchone()
        # r may be dict or tuple
        cnt = None
        if isinstance(r, dict):
            # find the key ignoring case
            cnt = next((v for k, v in r.items() if k.lower() == 'cnt' or k.lower().endswith('count(*)')), None)
        else:
            cnt = r[0] if r else 0
        return int(cnt or 0)
    except Exception:
        return 0


def get_missing_references(cursor, table_name):
    """
    Return list of referenced table names that have zero rows,
    i.e., prerequisites that must be populated before inserting into table_name.
    """
    fks = get_foreign_keys(cursor, table_name)
    missing = []
    for (ref_table, ref_col) in fks.values():
        cnt = get_table_row_count(cursor, ref_table)
        if cnt == 0:
            missing.append(ref_table)
    return missing  # list of table names (strings)

# ----------------- Table Categories -----------------
REFERENCE_TABLES = ["MaterialType", "ChemicalType", "WorkerRole", "ProductType", "QualityGrade"]
PARENT_TABLES = ["TreatmentPlant", "Worker", "ProcessBatch", "MaterialInput"]
CHILD_TABLES = ["WorkerLog", "ChemicalUsage", "OutputProduct"]

def get_table_category(table_name):
    if table_name in REFERENCE_TABLES:
        return "reference"
    elif table_name in PARENT_TABLES:
        return "parent"
    elif table_name in CHILD_TABLES:
        return "child"
    return "general"

# ----------------- Routes -----------------
@app.route('/')
def home():
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)

    all_tables = get_all_tables(cursor)

    # Build table metadata: name, count, missing_refs
    tables_info = []
    for t in all_tables:
        cnt = get_table_row_count(cursor, t)
        missing = get_missing_references(cursor, t)
        tables_info.append({'name': t, 'count': cnt, 'missing': missing})

    cursor.close()
    conn.close()

    # Categorize by known types (match case-insensitively)
    ref_names = {'materialtype', 'chemicaltype', 'workerrole', 'producttype', 'qualitygrade'}
    parent_names = {'treatmentplant'}
    child_names = {'materialinput', 'processbatch', 'chemicalusage', 'worker', 'workerlog', 'outputproduct'}

    reference_tables = [t for t in tables_info if t['name'].lower() in ref_names]
    parent_tables = [t for t in tables_info if t['name'].lower() in parent_names]
    child_tables = [t for t in tables_info if t['name'].lower() in child_names]
    other_tables = [t for t in tables_info if t not in reference_tables + parent_tables + child_tables]

    return render_template('home.html',
                           reference_tables=reference_tables,
                           parent_tables=parent_tables,
                           child_tables=child_tables,
                           other_tables=other_tables)


@app.route('/table/<table_name>')
def view_table(table_name):
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)

    tables = get_all_tables(cursor)
    if table_name not in tables:
        flash(f"Table `{table_name}` does not exist.", "danger")
        cursor.close()
        conn.close()
        return redirect(url_for('home'))

    columns = get_table_columns(cursor, table_name)
    cursor.execute(f"SELECT * FROM `{table_name}`")
    rows = cursor.fetchall()

    pk_col = get_primary_key(cursor, table_name)
    missing_refs = get_missing_references(cursor, table_name)
    fk_options = get_fk_options(cursor, table_name)

    cursor.close()
    conn.close()

    return render_template(
    'table_view.html',
    table_name=table_name,
    columns=columns,
    rows=rows,
    pk_col=pk_col,
    category=get_table_category(table_name)
)


@app.route('/table/<table_name>/add', methods=['GET', 'POST'])
def add_row(table_name):
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)

    # verify table exists
    tables = get_all_tables(cursor)
    if table_name not in tables:
        flash(f"Table `{table_name}` does not exist.", "danger")
        cursor.close()
        conn.close()
        return redirect(url_for('home'))

    columns = get_table_columns(cursor, table_name)
    fk_options = get_fk_options(cursor, table_name)
    missing_refs = get_missing_references(cursor, table_name)

    if request.method == 'POST':
        # if prerequisites missing, prevent insert and show message
        if missing_refs:
            flash(f"Cannot add to {table_name}. Missing prerequisite data in: {', '.join(missing_refs)}", "warning")
            cursor.close()
            conn.close()
            return render_template('table_add.html',
                                   table_name=table_name,
                                   columns=columns,
                                   fk_options=fk_options,
                                   missing_refs=missing_refs)

        # Build insert (skip auto_increment)
        insert_cols = [col['column_name'] for col in columns if 'auto_increment' not in (col.get('extra') or '').lower()]
        values = []
        for col in insert_cols:
            v = request.form.get(col)
            if v == '':
                v = None
            values.append(v)

        placeholders = ', '.join(['%s'] * len(insert_cols))
        col_names = ', '.join([f"`{c}`" for c in insert_cols])
        sql = f"INSERT INTO `{table_name}` ({col_names}) VALUES ({placeholders})"
        try:
            cursor.execute(sql, values)
            conn.commit()
            flash("Row added successfully!", "success")
        except Error as e:
            flash(f"Insert error: {e}", "danger")
        cursor.close()
        conn.close()
        return redirect(url_for('view_table', table_name=table_name))

    cursor.close()
    conn.close()
    return render_template('table_add.html',
                           table_name=table_name,
                           columns=columns,
                           fk_options=fk_options,
                           missing_refs=missing_refs)


@app.route('/table/<table_name>/edit/<pk_value>', methods=['GET', 'POST'])
def edit_row(table_name, pk_value):
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)

    tables = get_all_tables(cursor)
    if table_name not in tables:
        flash(f"Table `{table_name}` does not exist.", "danger")
        cursor.close()
        conn.close()
        return redirect(url_for('home'))

    columns = get_table_columns(cursor, table_name)
    pk_col = get_primary_key(cursor, table_name)
    fk_options = get_fk_options(cursor, table_name)

    # fetch row for prefill
    cursor.execute(f"SELECT * FROM `{table_name}` WHERE `{pk_col}` = %s", (pk_value,))
    row = cursor.fetchone()

    if request.method == 'POST':
        update_cols = [col['column_name'] for col in columns if col['column_name'] != pk_col]
        values = []
        for c in update_cols:
            v = request.form.get(c)
            if v == '':
                v = None
            values.append(v)
        set_clause = ', '.join([f"`{c}` = %s" for c in update_cols])
        sql = f"UPDATE `{table_name}` SET {set_clause} WHERE `{pk_col}` = %s"
        try:
            cursor.execute(sql, tuple(values) + (pk_value,))
            conn.commit()
            flash("Row updated.", "success")
        except Error as e:
            flash(f"Update error: {e}", "danger")
        cursor.close()
        conn.close()
        return redirect(url_for('view_table', table_name=table_name))

    cursor.close()
    conn.close()
    return render_template('table_edit.html',
                           table_name=table_name,
                           columns=columns,
                           row=row,
                           pk_col=pk_col,
                           fk_options=fk_options)


@app.route('/table/<table_name>/delete/<pk_value>', methods=['POST'])
def delete_row(table_name, pk_value):
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    tables = get_all_tables(cursor)
    if table_name not in tables:
        flash(f"Table `{table_name}` does not exist.", "danger")
        cursor.close()
        conn.close()
        return redirect(url_for('home'))

    columns = get_table_columns(cursor, table_name)
    pk_col = get_primary_key(cursor, table_name)

    if not pk_col:
        flash("No primary key defined for this table, delete not supported.", "warning")
        cursor.close()
        conn.close()
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


# Keep your combined views as they are (no changes required):
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
