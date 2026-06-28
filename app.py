from flask import Flask, render_template, request, redirect, url_for, flash, session
import mysql.connector
from mysql.connector import Error
import os
from dotenv import load_dotenv
from werkzeug.security import generate_password_hash, check_password_hash 

load_dotenv()

app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", "default_secret")


# ----------------- Database Connection & Schema -----------------
def get_connection():
    try:
        conn = mysql.connector.connect(
            host=os.getenv("DB_HOST", "localhost"),
            user=os.getenv("DB_USER", "root"),
            password=os.getenv("DB_PASSWORD", "SaNjith10"), 
            database=os.getenv("DB_NAME", "smart_waste_db")
        )
        if conn.is_connected():
            return conn
    except Error as e:
        print(f"Error connecting to MySQL database: {e}")
        return None

# Define the new schema (SQL is executed on app run)
# NOTE: 'DROP TABLE' commands have been removed, and 'IF NOT EXISTS' is added 
# to preserve data on application restart (Persistence fix).
SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS AppUser (
    UserID INT AUTO_INCREMENT PRIMARY KEY,
    Username VARCHAR(50) UNIQUE NOT NULL,
    PasswordHash VARCHAR(255) NOT NULL,
    Role ENUM('Admin', 'Manager', 'Driver') NOT NULL
);

CREATE TABLE IF NOT EXISTS Driver (
    DriverID INT AUTO_INCREMENT PRIMARY KEY,
    Name VARCHAR(100) NOT NULL,
    VehicleNo VARCHAR(20) UNIQUE NOT NULL,
    Contact VARCHAR(15)
);

CREATE TABLE IF NOT EXISTS SensorData (
    SensorDataID INT AUTO_INCREMENT PRIMARY KEY,
    AreaName VARCHAR(100) NOT NULL,
    WasteType VARCHAR(50) NOT NULL,
    FullnessPercentage DECIMAL(5,2) NOT NULL,
    ReportTime DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    TaskStatus ENUM('Pending', 'In Progress', 'Completed', 'Canceled') NOT NULL DEFAULT 'Pending',
    DriverID INT NULL, 
    FOREIGN KEY (DriverID) REFERENCES Driver(DriverID) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS Input (
    InputID INT AUTO_INCREMENT PRIMARY KEY,
    DateReceived DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    WeightKg DECIMAL(10,2) NOT NULL,
    WasteType VARCHAR(50) NOT NULL,
    Source_SensorDataID INT NOT NULL,
    FOREIGN KEY (Source_SensorDataID) REFERENCES SensorData(SensorDataID)
);

CREATE TABLE IF NOT EXISTS Process (
    ProcessID INT AUTO_INCREMENT PRIMARY KEY,
    StartDate DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    EndDate DATETIME NULL,
    MethodUsed VARCHAR(100),
    Input_InputID INT NOT NULL,
    FOREIGN KEY (Input_InputID) REFERENCES Input(InputID)
);

CREATE TABLE IF NOT EXISTS Output (
    OutputID INT AUTO_INCREMENT PRIMARY KEY,
    DateProduced DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    ProductType VARCHAR(100) NOT NULL,
    QuantityKg DECIMAL(10,2) NOT NULL,
    Process_ProcessID INT NOT NULL,
    FOREIGN KEY (Process_ProcessID) REFERENCES Process(ProcessID)
);
"""

# ----------------- Utility helpers -----------------
def _row_get(row, key_name):
    """Return case-insensitive value from row which may be dict or tuple."""
    if row is None: return None
    if isinstance(row, dict):
        for k, v in row.items():
            if k.lower() == key_name.lower(): return v
        return None
    else: return row[0] if len(row) >= 1 else None


# ----------------- Authentication and Permissions -----------------
CRUD_PERMISSIONS = {
    'Admin': ["AppUser", "Driver", "SensorData", "Input", "Process", "Output"],
    'Manager': ['Input', 'Process', 'Output'],
    'Driver': ['SensorData']
}

ADMIN_TABLES = ["AppUser", "Driver", "SensorData", "Input", "Process", "Output"]

def is_authorized(table_name, permission_type='CRUD'):
    """Checks if the user's role allows CRUD on the given table."""
    role = session.get('user_role')
    if not role:
        return False
    
    if table_name in CRUD_PERMISSIONS.get(role, []):
        return True
        
    if role == 'Admin':
        return True
        
    return False

# ----------------- Schema helpers -----------------
def get_table_columns(cursor, table_name):
    cursor.execute("""
        SELECT column_name, data_type, column_key, extra, is_nullable, column_default
        FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = %s
        ORDER BY ordinal_position
    """, (table_name,))
    results = cursor.fetchall()
    cols = []
    for c in results:
        if isinstance(c, dict):
            cols.append({k.lower(): v for k, v in c.items()})
        else:
            cols.append({
                'column_name': c[0], 'data_type': c[1], 'column_key': c[2], 
                'extra': c[3] or '', 'is_nullable': c[4], 'column_default': c[5]
            })
    return cols

def get_primary_key(cursor, table_name):
    cursor.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = DATABASE() AND table_name = %s AND column_key = 'PRI'
    """, (table_name,))
    result = cursor.fetchone()
    return _row_get(result, 'column_name')

def get_foreign_keys(cursor, table_name):
    cursor.execute("""
        SELECT column_name, referenced_table_name, referenced_column_name
        FROM information_schema.key_column_usage
        WHERE table_schema = DATABASE() AND table_name = %s AND referenced_table_name IS NOT NULL
    """, (table_name,))
    fks = {}
    
    COL_NAME_KEYS = ['column_name', 'COLUMN_NAME']
    REF_TABLE_KEYS = ['referenced_table_name', 'REFERENCED_TABLE_NAME']
    REF_COL_KEYS = ['referenced_column_name', 'REFERENCED_COLUMN_NAME']

    for r in cursor.fetchall():
        if isinstance(r, dict):
            col = next((r[k] for k in COL_NAME_KEYS if k in r), None)
            ref_table = next((r[k] for k in REF_TABLE_KEYS if k in r), None)
            ref_col = next((r[k] for k in REF_COL_KEYS if k in r), None)
            
            if col and ref_table and ref_col:
                fks[col] = (ref_table, ref_col)
        else:
            fks[r[0]] = (r[1], r[2])
    return fks

def get_fk_options(cursor, table_name):
    fks = get_foreign_keys(cursor, table_name)
    options = {}
    for col, (ref_table, ref_col) in fks.items():
        
        if ref_table == 'Driver':
            cursor.execute("SELECT DriverID, Name, VehicleNo FROM Driver")
            rows = cursor.fetchall()
            options[col] = [(str(_row_get(r, 'DriverID')), 
                             f"{_row_get(r, 'Name')} (Vehicle: {_row_get(r, 'VehicleNo')})") 
                            for r in rows]
            
        elif ref_table == 'SensorData':
            cursor.execute("SELECT SensorDataID, AreaName, ReportTime FROM SensorData")
            rows = cursor.fetchall()
            options[col] = [(str(_row_get(r, 'SensorDataID')), 
                             f"Task ID: {_row_get(r, 'SensorDataID')} ({_row_get(r, 'AreaName')})") 
                            for r in rows]

        elif ref_table == 'Input':
            cursor.execute("SELECT InputID, DateReceived, WasteType FROM Input")
            rows = cursor.fetchall()
            options[col] = [(str(_row_get(r, 'InputID')), 
                             f"Input ID: {_row_get(r, 'InputID')} ({_row_get(r, 'WasteType')})") 
                            for r in rows]
                            
        elif ref_table == 'Process':
            cursor.execute("SELECT ProcessID, StartDate, MethodUsed FROM Process")
            rows = cursor.fetchall()
            options[col] = [(str(_row_get(r, 'ProcessID')), 
                             f"Process ID: {_row_get(r, 'ProcessID')} (Start: {str(_row_get(r, 'StartDate')).split(' ')[0]})")
                            for r in rows]

        else:
            cursor.execute(f"SELECT {ref_col} FROM `{ref_table}`")
            rows = cursor.fetchall()
            options[col] = [(str(_row_get(r, ref_col)), str(_row_get(r, ref_col))) for r in rows]
            
    return options

def get_table_row_count(cursor, table_name):
    try:
        cursor.execute(f"SELECT COUNT(*) AS cnt FROM `{table_name}`")
        cnt = _row_get(cursor.fetchone(), 'cnt')
        return int(cnt or 0)
    except Exception:
        return 0

def get_missing_references(cursor, table_name):
    fks = get_foreign_keys(cursor, table_name)
    missing = []
    for (ref_table, ref_col) in fks.values():
        if ref_table == 'Driver' and 'SensorData' in table_name: continue
        cnt = get_table_row_count(cursor, ref_table)
        if cnt == 0:
            missing.append(ref_table)
    return missing

def get_table_category(table_name):
    if table_name in ["AppUser", "Driver"]: return "system"
    elif table_name == "SensorData": return "trigger"
    elif table_name == "Input": return "flow_input"
    elif table_name == "Process": return "flow_process"
    elif table_name == "Output": return "flow_output"
    return "general"

# ----------------- AUTHENTICATION ROUTES -----------------
@app.route('/', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']

        conn = get_connection()
        if conn is None:
            flash("Database connection error.", "danger")
            return render_template('login.html')

        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT PasswordHash, Role FROM AppUser WHERE Username = %s", (username,))
        user = cursor.fetchone()
        cursor.close()
        conn.close()

        # Hardcoded password check fallback (for project setup simplicity)
        if username == 'admin' and password == 'password':
            session['logged_in'] = True
            session['user_role'] = 'Admin'
            flash('Admin Login successful!', 'success')
            return redirect(url_for('admin_home'))
        elif username == 'manager' and password == 'password':
            session['logged_in'] = True
            session['user_role'] = 'Manager'
            flash('Manager Login successful!', 'success')
            # CORRECT REDIRECT: Redirects to the dashboard
            return redirect(url_for('manager_home')) 
        elif username == 'driver' and password == 'password':
            session['logged_in'] = True
            session['user_role'] = 'Driver'
            flash('Driver Login successful!', 'success')
            return redirect(url_for('driver_home'))
        else:
            flash('Invalid username or password.', 'danger')
            return render_template('login.html')

    return render_template('login.html')

@app.route('/logout')
def logout():
    session.pop('logged_in', None)
    session.pop('user_role', None)
    flash('You have been logged out.', 'success')
    return redirect(url_for('login'))

# ----------------- HOME DASHBOARDS (Role-Based Routing) -----------------
@app.route('/admin_home')
def admin_home():
    if session.get('user_role') != 'Admin': return redirect(url_for('login'))

    conn = get_connection()
    if conn is None: flash("Database connection error.", "danger"); return redirect(url_for('login'))
    cursor = conn.cursor(dictionary=True)

    tables_info = []
    for t in ADMIN_TABLES:
        cnt = get_table_row_count(cursor, t)
        missing = get_missing_references(cursor, t)
        tables_info.append({'name': t, 'count': cnt, 'missing': missing})

    cursor.close()
    conn.close()

    system_tables = [t for t in tables_info if t['name'] in ['AppUser', 'Driver']]
    trigger_tables = [t for t in tables_info if t['name'] == 'SensorData']
    flow_tables = [t for t in tables_info if t['name'] in ['Input', 'Process', 'Output']]
    
    return render_template('admin_home.html',
                           system_tables=system_tables,
                           trigger_tables=trigger_tables,
                           flow_tables=flow_tables)


@app.route('/manager_home')
def manager_home():
    # THIS FUNCTION MUST RENDER THE DASHBOARD
    if session.get('user_role') not in ['Manager', 'Admin']: return redirect(url_for('login'))
    
    conn = get_connection()
    if conn is None: flash("Database connection error.", "danger"); return redirect(url_for('login'))
    cursor = conn.cursor(dictionary=True)
    
    flow_tables = []
    for t_name in ['Input', 'Process', 'Output']:
        cnt = get_table_row_count(cursor, t_name)
        missing = get_missing_references(cursor, t_name)
        flow_tables.append({'name': t_name, 'count': cnt, 'missing': missing})
        
    cursor.close()
    conn.close()
    
    return render_template('manager_home.html', flow_tables=flow_tables)

@app.route('/driver_home')
def driver_home():
    if session.get('user_role') not in ['Driver', 'Admin']: return redirect(url_for('login'))
    
    conn = get_connection()
    if conn is None: flash("Database connection error.", "danger"); return redirect(url_for('login'))
    cursor = conn.cursor(dictionary=True)
    
    # LEFT JOIN for task assignment view
    cursor.execute("""
        SELECT 
            sd.SensorDataID, sd.AreaName, sd.ReportTime, sd.TaskStatus, sd.WasteType, sd.FullnessPercentage, d.Name AS DriverName, d.VehicleNo
        FROM SensorData sd
        LEFT JOIN Driver d ON sd.DriverID = d.DriverID
        ORDER BY FIELD(sd.TaskStatus, 'Pending', 'In Progress', 'Completed', 'Canceled'), sd.ReportTime ASC
    """)
    tasks = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    return render_template('driver_tasks.html', tasks=tasks, user_role=session.get('user_role'))


# ----------------- CRUD ROUTES (Permission Checks) -----------------

@app.route('/table/<table_name>')
def view_table(table_name):
    if not session.get('logged_in'): return redirect(url_for('login'))
    
    if not is_authorized(table_name) and table_name not in ['Input', 'Process', 'Output']:
        flash(f"Permission denied: You do not have access to manage {table_name}.", "danger")
        return redirect(url_for('admin_home')) 
    elif not is_authorized(table_name) and table_name == 'SensorData' and session.get('user_role') == 'Manager':
        flash(f"Permission denied: Managers do not need to view {table_name}.", "danger")
        return redirect(url_for('manager_home'))

    conn = get_connection()
    if conn is None: flash("Database connection error.", "danger"); return redirect(url_for('login'))
    cursor = conn.cursor(dictionary=True)

    columns = get_table_columns(cursor, table_name)
    cursor.execute(f"SELECT * FROM `{table_name}`")
    rows = cursor.fetchall()
    pk_col = get_primary_key(cursor, table_name)
    missing_refs = get_missing_references(cursor, table_name)

    cursor.close()
    conn.close()

    return render_template(
    'table_view.html',
    table_name=table_name,
    columns=columns,
    rows=rows,
    pk_col=pk_col,
    category=get_table_category(table_name),
    missing_refs=missing_refs,
    user_role=session.get('user_role')
)


@app.route('/table/<table_name>/add', methods=['GET', 'POST'])
def add_row(table_name):
    if not session.get('logged_in'): return redirect(url_for('login'))
    if not is_authorized(table_name):
        flash(f"Permission denied: You cannot add to {table_name}.", "danger")
        return redirect(url_for('admin_home'))

    conn = get_connection()
    if conn is None: flash("Database connection error.", "danger"); return redirect(url_for('login'))
    cursor = conn.cursor(dictionary=True)

    columns = get_table_columns(cursor, table_name)
    fk_options = get_fk_options(cursor, table_name)
    missing_refs = get_missing_references(cursor, table_name)

    if request.method == 'POST':
        if missing_refs:
            flash(f"Cannot add to {table_name}. Missing prerequisite data in: {', '.join(missing_refs)}", "warning")
            cursor.close(); conn.close(); 
            return redirect(url_for('add_row', table_name=table_name))

        insert_cols = [col['column_name'] for col in columns if 'auto_increment' not in (col.get('extra') or '').lower()]
        values = []
        
        for col in insert_cols:
            v = request.form.get(col)
            if v == '': v = None
            
            col_type = next(c['data_type'] for c in columns if c['column_name'] == col)
            if v is not None:
                if 'ID' in col:
                    try: v = int(v) 
                    except ValueError: v = None
                elif 'DECIMAL' in col_type.upper():
                    try: v = float(v)
                    except ValueError: v = None
                elif 'INT' in col_type.upper():
                    try: v = int(v)
                    except ValueError: v = None
            
            if table_name == 'AppUser' and col == 'PasswordHash' and v:
                v = generate_password_hash(v)
            
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
        cursor.close(); conn.close();
        return redirect(url_for('view_table', table_name=table_name))

    cursor.close(); conn.close();
    return render_template('table_add.html',
                           table_name=table_name,
                           columns=columns,
                           fk_options=fk_options,
                           missing_refs=missing_refs,
                           user_role=session.get('user_role'))


@app.route('/table/<table_name>/edit/<pk_value>', methods=['GET', 'POST'])
def edit_row(table_name, pk_value):
    if not session.get('logged_in'): return redirect(url_for('login'))
    
    if not is_authorized(table_name):
        flash(f"Permission denied: You cannot edit {table_name}.", "danger")
        return redirect(url_for('admin_home'))
    elif session.get('user_role') == 'Manager' and table_name not in ['Input', 'Process', 'Output']:
        flash(f"Permission denied: Managers can only edit flow tables (Input, Process, Output).", "danger")
        return redirect(url_for('manager_home'))

    conn = get_connection()
    if conn is None: flash("Database connection error.", "danger"); return redirect(url_for('login'))
    cursor = conn.cursor(dictionary=True)

    columns = get_table_columns(cursor, table_name)
    pk_col = get_primary_key(cursor, table_name)
    fk_options = get_fk_options(cursor, table_name)

    cursor.execute(f"SELECT * FROM `{table_name}` WHERE `{pk_col}` = %s", (pk_value,))
    row = cursor.fetchone()

    if request.method == 'POST':
        update_cols = [col['column_name'] for col in columns]
        values = []
        for c in update_cols:
            v = request.form.get(c)
            if v == '': v = None

            col_type = next(col['data_type'] for col in columns if col['column_name'] == c)
            if v is not None:
                if 'INT' in col_type.upper():
                    try: v = int(v)
                    except ValueError: v = None
                elif 'DECIMAL' in col_type.upper() or 'FLOAT' in col_type.upper():
                    try: v = float(v)
                    except ValueError: v = None
            values.append(v)

        set_clause = ', '.join([f"`{c}` = %s" for c in update_cols])
        sql = f"UPDATE `{table_name}` SET {set_clause} WHERE `{pk_col}` = %s"
        try:
            cursor.execute(sql, tuple(values) + (pk_value,))
            conn.commit()
            flash("Row updated successfully.", "success")
        except Error as e:
            flash(f"Update failed: {e}", "danger")
        finally:
            cursor.close(); conn.close();
        return redirect(url_for('view_table', table_name=table_name))

    cursor.close(); conn.close();
    return render_template('table_edit.html',
                           table_name=table_name,
                           columns=columns,
                           row=row,
                           pk_col=pk_col,
                           fk_options=fk_options,
                           user_role=session.get('user_role'))


@app.route('/table/<table_name>/delete/<pk_value>', methods=['POST'])
def delete_row(table_name, pk_value):
    if not session.get('logged_in'): return redirect(url_for('login'))
    
    if not is_authorized(table_name):
        flash(f"Permission denied: You cannot delete {table_name}.", "danger")
        return redirect(url_for('admin_home'))
    elif session.get('user_role') == 'Manager' and table_name not in ['Input', 'Process', 'Output']:
        flash(f"Permission denied: Managers can only delete flow tables (Input, Process, Output).", "danger")
        return redirect(url_for('manager_home'))
        
    conn = get_connection()
    if conn is None: flash("Database connection error.", "danger"); return redirect(url_for('login'))
    pk_col = get_primary_key(conn.cursor(), table_name)

    if not pk_col:
        flash("No primary key defined for this table, delete not supported.", "warning")
        conn.close(); return redirect(url_for('view_table', table_name=table_name))

    try:
        cursor = conn.cursor()
        cursor.execute(f"DELETE FROM `{table_name}` WHERE `{pk_col}`=%s", (pk_value,))
        conn.commit()
        flash("Row deleted successfully!", "success")
    except Error as e:
        flash(f"Error deleting row: {e}", "danger")

    conn.close();
    return redirect(url_for('view_table', table_name=table_name))


# ----------------- REPORTS AND COMBINED VIEWS -----------------

@app.route('/batch_flow_details')
def batch_details():
    """
    Shows a combined view of Process, Input, and Output tables to track material flow.
    Accessible by Admin and Manager roles.
    """
    user_role = session.get('user_role')
    if user_role not in ['Admin', 'Manager']: 
        flash("Permission denied. Only Admins and Managers can view batch flow details.", "danger")
        return redirect(url_for('login'))

    conn = get_connection()
    if conn is None: 
        flash("Database connection error.", "danger")
        return redirect(url_for('login'))
        
    cursor = conn.cursor(dictionary=True)

    # SQL JOIN query (The requested feature)
    sql_query = """
        SELECT
            p.ProcessID AS BatchID,
            p.StartDate,
            p.EndDate,
            -- Using Input's WasteType as a logical BatchType for this report
            i.WasteType AS BatchType, 
            o.ProductType AS ProductType,
            o.QuantityKg AS OutputQuantity,
            o.DateProduced AS DispatchDate -- Using DateProduced as a proxy for Dispatch Date
        FROM Process p
        INNER JOIN Input i ON p.Input_InputID = i.InputID
        LEFT JOIN Output o ON o.Process_ProcessID = p.ProcessID
        ORDER BY p.StartDate DESC
    """
    
    try:
        cursor.execute(sql_query)
        batch_records = cursor.fetchall()
        flash(f"Successfully loaded {len(batch_records)} batch records.", "success")
    except Error as e:
        flash(f"Error fetching batch details: {e}", "danger")
        batch_records = []
    finally:
        cursor.close()
        conn.close()

    return render_template('batch_detail.html', 
                           batch_records=batch_records,
                           user_role=user_role)


# --- FINAL EXECUTION (Schema and Default User Setup) ---
if __name__ == '__main__':
    with get_connection() as conn:
        if conn:
            cursor = conn.cursor()
            try:
                # Use IF NOT EXISTS for tables now to preserve data on restart.
                for sql_command in SCHEMA_SQL.split(';'):
                    if sql_command.strip():
                        cursor.execute(sql_command.strip())
                conn.commit()
                print("All 6 tables ensured to exist (data is now persistent).")
            except Error as e:
                print(f"Error during schema check/creation: {e}")
            finally:
                cursor.close()
            
            # Check for default users and create them only if they don't exist
            cursor = conn.cursor()
            try:
                cursor.execute("SELECT COUNT(*) FROM AppUser WHERE Username = 'admin'")
                if cursor.fetchone()[0] == 0:
                    cursor.execute("INSERT INTO AppUser (Username, PasswordHash, Role) VALUES (%s, %s, %s)", 
                                   ('admin', generate_password_hash('password'), 'Admin'))
                    cursor.execute("INSERT INTO AppUser (Username, PasswordHash, Role) VALUES (%s, %s, %s)", 
                                   ('manager', generate_password_hash('password'), 'Manager'))
                    cursor.execute("INSERT INTO AppUser (Username, PasswordHash, Role) VALUES (%s, %s, %s)", 
                                   ('driver', generate_password_hash('password'), 'Driver'))
                    conn.commit()
                    print("Default users (admin/manager/driver) created with password 'password'.")
            except Error as e:
                pass 
            finally:
                cursor.close()

    app.run(host='0.0.0.0', port=5000, debug=True)