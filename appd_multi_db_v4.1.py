import urllib.request
import urllib.error
import smtplib
import json
import time
import base64
import sys
import ssl
import csv
import io
import signal
import os
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

# ==============================================================================
# CONFIGURATION
# ==============================================================================
CONTROLLER = "https://{Controller_URL}"
USERNAME   = "admin"
ACCOUNT    = "customer1"
PASSWORD   = "PASSWORD"  # <--- Updates automatically with Basic Auth

# Monitoring Targets
DATABASES = {
    "Don-PRD": 21,
    "Shared-PRD": 31,
    "Shared-Postgres": 61
}

# Settings
DURATION_MINUTES = 60   # Script runs for this long, then emails report
MIN_DURATION_MS  = 50   # Ignore queries faster than this (Noise filter)

# Email Settings
SMTP_SERVER   = "smtp.office365.com"
SMTP_PORT     = 587
SMTP_USER     = "apps-alerts@example.com"
SMTP_PASS     = "SMTP-PASSWORD"
EMAIL_TO      = ["AppdENG@example.com"] 
EMAIL_SUBJECT = "AppD Peak Performance Report (Live Monitor)"
# ==============================================================================

# Global State
observed_peaks = {name: {} for name in DATABASES}
start_time_log = time.time()

# Ignore SSL Certificate Errors (Standard Library way)
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

def get_auth_headers():
    """Generates the Basic Auth Header"""
    auth_str = f"{USERNAME}@{ACCOUNT}:{PASSWORD}"
    auth_bytes = auth_str.encode('ascii')
    auth_b64 = base64.b64encode(auth_bytes).decode('ascii')
    return {
        'Authorization': f'Basic {auth_b64}',
        'Content-Type': 'application/json;charset=UTF-8'
    }

def make_request(url, payload):
    """Zero-dependency HTTP POST"""
    try:
        data = json.dumps(payload).encode('utf-8')
        req = urllib.request.Request(url, data=data, headers=get_auth_headers(), method='POST')
        
        with urllib.request.urlopen(req, context=ctx) as response:
            if response.status == 200:
                return json.loads(response.read().decode('utf-8'))
            return None
            
    except urllib.error.HTTPError as e:
        print(f" [HTTP ERROR] {e.code}: {e.reason}")
        if e.code == 401:
            print(" [CRITICAL] Authentication Failed. Check Password.")
            sys.exit(1)
        return None
    except Exception as e:
        print(f" [ERROR] Connection Request Failed: {e}")
        return None

def fetch_live_batch(db_id):
    """Fetches last 1 minute of data"""
    end_ts = int(time.time() * 1000)
    start_ts = end_ts - (60 * 1000)
    
    payload = {
        "dbConfigId": -1, "dbServerId": db_id, "field": "query-id", "size": 100,
        "filterBy": "time", "startTime": start_ts, "endTime": end_ts,
        "useTimeBasedCorrelation": False, "waitStateIds": []
    }
    
    url = f"{CONTROLLER}/controller/databasesui/databases/queryListData"
    data = make_request(url, payload)
    
    if data:
        return data.get('data', {}).get('data', []) if 'data' in data else data
    return []

def process_batch(db_name, rows):
    """Updates the High Water Mark for queries"""
    updates = 0
    now_str = datetime.now().strftime('%H:%M')
    
    for r in rows:
        count = r.get('executionCount') or r.get('hits') or 0
        dur = r.get('timeSpent') or r.get('duration') or 0
        if count == 0: continue
        
        avg_now = int(dur / count)
        if avg_now < MIN_DURATION_MS: continue
        
        sql_full = (r.get('queryText') or r.get('name') or "Unknown").strip()
        sql_hash = str(hash(sql_full))
        
        db_records = observed_peaks[db_name]
        
        # Logic: If we haven't seen it, OR if this is a new worst-case time
        if sql_hash not in db_records:
            db_records[sql_hash] = {"sql": sql_full, "max_avg": avg_now, "count": count, "peak_time": now_str}
            updates += 1
        else:
            if avg_now > db_records[sql_hash]['max_avg']:
                db_records[sql_hash].update({"max_avg": avg_now, "count": count, "peak_time": now_str})
                updates += 1
    return updates

# ------------------------------------------------------------------
# REPORTING ENGINE
# ------------------------------------------------------------------
def generate_csv_string():
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["DB_NAME", "PEAK_TIME", "MAX_DURATION_MS", "EXECUTION_COUNT", "SQL_TEXT"])
    
    for db, recs in observed_peaks.items():
        # Sort by duration (slowest first)
        sorted_rows = sorted(recs.values(), key=lambda x: x['max_avg'], reverse=True)
        for r in sorted_rows:
            writer.writerow([db, r['peak_time'], r['max_avg'], r['count'], r['sql']])
            
    return output.getvalue()

def generate_html_report(csv_data):
    """Builds a clean HTML Email"""
    db_sections = ""
    
    for db_name in DATABASES:
        rows = list(observed_peaks[db_name].values())
        if not rows:
            db_sections += f"<div style='margin-bottom:20px; padding:15px; background:#fff; border:1px solid #ddd;'><h3>{db_name}</h3><p>No queries > {MIN_DURATION_MS}ms captured.</p></div>"
            continue
            
        # Top 5 Table
        sorted_rows = sorted(rows, key=lambda x: x['max_avg'], reverse=True)[:5]
        t_rows = ""
        for r in sorted_rows:
            color = "#dc3545" if r['max_avg'] > 1000 else "#333"
            t_rows += f"""
            <tr style="border-bottom:1px solid #eee;">
                <td style="padding:8px; font-family:monospace; font-size:12px; color:#555;">{r['sql'][:80]}...</td>
                <td style="padding:8px; text-align:center;">{r['peak_time']}</td>
                <td style="padding:8px; text-align:center; color:{color}; font-weight:bold;">{r['max_avg']} ms</td>
            </tr>"""
            
        db_sections += f"""
        <div style="margin-bottom:20px; background:#fff; border:1px solid #ddd; border-radius:5px; overflow:hidden;">
            <div style="background:#f1f3f5; padding:10px 15px; border-bottom:1px solid #ddd;">
                <h3 style="margin:0; color:#005073;">{db_name}</h3>
            </div>
            <table style="width:100%; border-collapse:collapse;">
                <thead><tr style="background:#fff;"><th style="text-align:left; padding:8px;">Top Spikes Detected</th><th>Time</th><th>Max Duration</th></tr></thead>
                <tbody>{t_rows}</tbody>
            </table>
        </div>"""

    return f"""
    <html><body style="font-family:'Segoe UI', sans-serif; background:#f4f7f6; padding:20px;">
        <div style="max-width:800px; margin:0 auto;">
            <div style="background:#005073; color:#fff; padding:20px; text-align:center; border-radius:5px 5px 0 0;">
                <h2 style="margin:0;">AppDynamics Peak Report</h2>
                <p style="margin:5px 0 0 0;">Duration: {int((time.time()-start_time_log)/60)} Minutes</p>
            </div>
            {db_sections}
            <div style="text-align:center; color:#777; font-size:12px; margin-top:20px;">
                Full raw data attached in CSV. Generated by AppD Universal Monitor.
            </div>
        </div>
    </body></html>"""

def send_email(html, csv_str):
    print(" [SENDING] Connecting to SMTP...")
    msg = MIMEMultipart()
    msg['From'] = SMTP_USER
    msg['To'] = ", ".join(EMAIL_TO)
    msg['Subject'] = EMAIL_SUBJECT
    
    msg.attach(MIMEText(html, 'html'))
    
    # Attach CSV
    part = MIMEApplication(csv_str.encode('utf-8'))
    part.add_header('Content-Disposition', 'attachment', filename='peaks.csv')
    msg.attach(part)
    
    try:
        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        server.starttls()
        server.login(SMTP_USER, SMTP_PASS)
        server.sendmail(SMTP_USER, EMAIL_TO, msg.as_string())
        server.quit()
        print(" [SUCCESS] Email Sent Successfully.")
    except Exception as e:
        print(f" [ERROR] SMTP Failed: {e}")

def finish_up(sig=None, frame=None):
    print("\n\n[STOP] Generating Final Report...")
    csv_str = generate_csv_string()
    html = generate_html_report(csv_str)
    send_email(html, csv_str)
    print("[DONE] Exiting.")
    sys.exit(0)

# ==============================================================================
# MAIN
# ==============================================================================
if __name__ == "__main__":
    signal.signal(signal.SIGINT, finish_up)
    
    print(f"[*] AppD Universal Monitor Started (PID: {os.getpid()})")
    print(f"[*] Mode: Basic Auth ({USERNAME})")
    print(f"[*] Duration: {DURATION_MINUTES} Minutes")
    
    while True:
        # Check Timer
        elapsed = (time.time() - start_time_log) / 60
        if elapsed >= DURATION_MINUTES:
            print("\n[TIMER] Time limit reached.")
            finish_up()

        sys.stdout.write(f"\r[{datetime.now().strftime('%H:%M')}] Polling... ")
        
        for name, db_id in DATABASES.items():
            data = fetch_live_batch(db_id)
            new_peaks = process_batch(name, data)
            sys.stdout.write(f"{name}:{len(observed_peaks[name])} (New:{new_peaks}) | ")
            
        sys.stdout.flush()
        time.sleep(60)
