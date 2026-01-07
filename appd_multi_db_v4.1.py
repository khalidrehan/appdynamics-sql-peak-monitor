import requests
import smtplib
import json
import time
import base64
import io
import sys
import signal
import csv
import matplotlib.pyplot as plt
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.application import MIMEApplication

# ==============================================================================
# CONFIGURATION
# ==============================================================================
JSESSIONID = "PASTE_JSESSIONID_HERE"
CSRF_TOKEN = "PASTE_CSRF_TOKEN_HERE"

CONTROLLER = "[https://your-controller.saas.appdynamics.com](https://your-controller.saas.appdynamics.com)"

# 1. MONITORING TARGETS
DATABASES = {
    "Production-Primary": 21,
    "Analytics-DB": 31
}

# 2. AUTO-STOP TIMER (Minutes)
DURATION_MINUTES = 60

# 3. EMAIL SETTINGS
SMTP_SERVER   = "smtp.office365.com"
SMTP_PORT     = 587
SMTP_USER     = "apps-alerts@example.com"
SMTP_PASS     = "Password"
EMAIL_TO      = ["email1@example.com"] 
EMAIL_SUBJECT = "AppD Multi-DB Peak Performance Report"

# Threshold (ms)
MIN_DURATION_MS = 50 
# ==============================================================================

observed_peaks = {name: {} for name in DATABASES}
start_time_log = time.time()

def fetch_live_batch(db_id):
    """Gets the last 1 minute of data for a specific DB"""
    end_ts = int(time.time() * 1000)
    start_ts = end_ts - (60 * 1000)

    headers = {
        'Content-Type': 'application/json;charset=UTF-8',
        'Cookie': f'JSESSIONID={JSESSIONID}',
        'X-CSRF-TOKEN': CSRF_TOKEN
    }
    payload = {
        "dbConfigId": -1, "dbServerId": db_id, "field": "query-id", "size": 100,
        "filterBy": "time", "startTime": start_ts, "endTime": end_ts,
        "useTimeBasedCorrelation": False, "waitStateIds": []
    }
    try:
        r = requests.post(f"{CONTROLLER}/controller/databasesui/databases/queryListData", json=payload, headers=headers, verify=False)
        if r.status_code != 200: return []
        data = r.json()
        return data.get('data', {}).get('data', []) if 'data' in data else data
    except:
        return []

def process_batch(db_name, rows):
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

        if sql_hash not in db_records:
            db_records[sql_hash] = {
                "sql": sql_full,
                "max_avg": avg_now,
                "count": count,
                "peak_time": now_str
            }
            updates += 1
        else:
            if avg_now > db_records[sql_hash]['max_avg']:
                db_records[sql_hash]['max_avg'] = avg_now
                db_records[sql_hash]['count'] = count
                db_records[sql_hash]['peak_time'] = now_str
                updates += 1
    return updates

# ==============================================================================
# REPORT GENERATORS
# ==============================================================================

def generate_csv_string():
    """Creates the CSV content in memory"""
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["DB_NAME", "PEAK_TIME", "MAX_DURATION_MS", "EXECUTION_COUNT", "SQL_TEXT"])
    
    for db_name, records in observed_peaks.items():
        # Sort by slowest
        sorted_rows = sorted(records.values(), key=lambda x: x['max_avg'], reverse=True)
        for r in sorted_rows:
            writer.writerow([db_name, r['peak_time'], r['max_avg'], r['count'], r['sql']])
            
    return output.getvalue()

def generate_overall_chart():
    """Generates a summary chart comparing the WORST query from each DB"""
    db_names = []
    max_times = []
    
    for db in DATABASES:
        records = observed_peaks[db].values()
        if records:
            worst = max(records, key=lambda x: x['max_avg'])
            max_times.append(worst['max_avg'])
        else:
            max_times.append(0)
        db_names.append(db)
        
    plt.figure(figsize=(8, 4), dpi=100)
    plt.style.use('ggplot')
    bars = plt.bar(db_names, max_times, color=['#005073', '#17a2b8', '#28a745'])
    
    plt.title('Overall Health: Highest Latency Spike per DB', fontweight='bold', pad=15)
    plt.ylabel('Peak Duration (ms)')
    
    for bar in bars:
        height = bar.get_height()
        if height > 0:
            plt.text(bar.get_x() + bar.get_width()/2., height,
                     f'{int(height)} ms', ha='center', va='bottom', fontweight='bold')
                     
    plt.tight_layout()
    buf = io.BytesIO()
    plt.savefig(buf, format='png', bbox_inches='tight')
    buf.seek(0)
    img = base64.b64encode(buf.read()).decode('utf-8')
    plt.close()
    return img

def generate_db_chart(db_name, rows):
    if not rows: return None
    top_slow = sorted(rows, key=lambda x: x['max_avg'], reverse=True)[:5]
    names = [(x['sql'][:40] + "...") for x in top_slow]
    times = [x['max_avg'] for x in top_slow]
    
    plt.figure(figsize=(8, 4), dpi=100) 
    plt.style.use('ggplot')
    plt.rcParams.update({'font.size': 10})
    bars = plt.barh(names, times, color='#dc3545')
    plt.xlabel('Peak Response Time (ms)', fontweight='bold')
    plt.title(f'{db_name}: Top 5 Slowest Spikes', color='#333', fontweight='bold')
    plt.gca().invert_yaxis()
    for bar in bars:
        width = bar.get_width()
        plt.text(width + (max(times)*0.01), bar.get_y() + bar.get_height()/2, 
                 f'{int(width)} ms', va='center', color='black', fontsize=9, fontweight='bold')
    plt.tight_layout()
    buf = io.BytesIO()
    plt.savefig(buf, format='png', bbox_inches='tight')
    buf.seek(0)
    img = base64.b64encode(buf.read()).decode('utf-8')
    plt.close()
    return img

def generate_full_html(overall_chart_b64):
    print(" [3] Crafting HTML Report...")
    
    db_sections = ""
    for db_name in DATABASES:
        rows = list(observed_peaks[db_name].values())
        if not rows:
            db_sections += f"<div style='background:#fff; padding:15px; margin-bottom:15px;'><h3>{db_name}</h3><p style='color:#777'>No queries captured (> {MIN_DURATION_MS}ms)</p></div>"
            continue

        chart_b64 = generate_db_chart(db_name, rows)
        sorted_rows = sorted(rows, key=lambda x: x['max_avg'], reverse=True)[:10]
        
        t_rows = ""
        for i, r in enumerate(sorted_rows):
            bg = "#f9f9f9" if i % 2 == 0 else "#ffffff"
            style = "color:#dc3545; font-weight:bold;" if r['max_avg'] > 1000 else "color:#333;"
            t_rows += f"""<tr style="background-color: {bg};">
                <td style="padding:8px; border-bottom:1px solid #ddd; font-family:monospace; font-size:12px;">{r['sql'][:80]}...</td>
                <td style="padding:8px; border-bottom:1px solid #ddd; text-align:center;">{r['peak_time']}</td>
                <td style="padding:8px; border-bottom:1px solid #ddd; text-align:center; {style}">{r['max_avg']} ms</td>
            </tr>"""

        chart_html = ""
        if chart_b64:
            chart_html = f'<img src="data:image/png;base64,{chart_b64}" style="width:100%; max-width:600px; border:1px solid #eee; margin-bottom:15px;" />'

        db_sections += f"""
        <div style="background:#fff; padding:20px; border-radius:8px; margin-bottom:20px; box-shadow:0 2px 5px rgba(0,0,0,0.05);">
            <h3 style="color:#005073; border-bottom:2px solid #eee; padding-bottom:10px; margin-top:0;">{db_name}</h3>
            <div style="text-align:center;">{chart_html}</div>
            <table style="width:100%; border-collapse:collapse;">
                <thead><tr style="background:#e9ecef;"><th style="padding:8px; text-align:left;">Top Spikes</th><th style="padding:8px; text-align:center;">Time</th><th style="padding:8px; text-align:center;">Peak Duration</th></tr></thead>
                <tbody>{t_rows}</tbody>
            </table>
        </div>
        """

    html = f"""
    <html>
    <body style="font-family: 'Segoe UI', sans-serif; background-color: #eaeff2; padding: 20px;">
        <div style="max-width: 900px; margin: 0 auto;">
            <div style="background-color: #005073; color: #ffffff; padding: 20px; text-align: center; border-radius: 8px 8px 0 0;">
                <h2 style="margin: 0;">Multi-Database Peak Report</h2>
                <p style="margin: 5px 0 0 0;">Monitoring Window: {int((time.time()-start_time_log)/60)} Minutes</p>
            </div>
            
            <div style="background:#fff; padding:20px; margin-bottom:20px; text-align:center;">
                <h3 style="color:#555; margin-top:0;">Overall Performance Comparison</h3>
                <img src="data:image/png;base64,{overall_chart_b64}" style="width:100%; max-width:600px;" />
            </div>

            {db_sections}
            
            <div style="text-align:center; color:#888; font-size:11px; margin-top:20px;">
                <b>Note:</b> Full query details are attached in the CSV file.
            </div>
        </div>
    </body>
    </html>
    """
    return html

def send_email_with_csv(html_content, csv_string):
    print(" [4] Sending Email with CSV...")
    msg = MIMEMultipart()
    msg['From'] = SMTP_USER
    msg['To'] = ", ".join(EMAIL_TO)
    msg['Subject'] = EMAIL_SUBJECT
    
    # 1. Attach HTML Body
    msg.attach(MIMEText(html_content, 'html'))
    
    # 2. Attach CSV File
    attachment = MIMEApplication(csv_string.encode('utf-8'))
    attachment.add_header('Content-Disposition', 'attachment', filename='AppD_Peak_Report.csv')
    msg.attach(attachment)
    
    try:
        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        server.starttls()
        server.login(SMTP_USER, SMTP_PASS)
        server.sendmail(SMTP_USER, EMAIL_TO, msg.as_string())
        server.quit()
        print(" [SUCCESS] Report Sent!")
    except Exception as e:
        print(f" [ERROR] SMTP Failed: {e}")

def finish_up(sig=None, frame=None):
    print(f"\n\n[STOP] Generating Report...")
    
    # Generate Assets
    csv_data = generate_csv_string()
    overall_chart = generate_overall_chart()
    html_body = generate_full_html(overall_chart)
    
    # Send
    send_email_with_csv(html_body, csv_data)
    sys.exit(0)

# ==============================================================================
# MAIN
# ==============================================================================
if __name__ == "__main__":
    requests.packages.urllib3.disable_warnings()
    signal.signal(signal.SIGINT, finish_up)
    
    print(f"[*] Starting Multi-DB Observer ({DURATION_MINUTES} mins)...")
    print(f"[*] Targets: {list(DATABASES.keys())}")
    
    start_time = time.time()
    
    while True:
        elapsed_mins = (time.time() - start_time) / 60
        if elapsed_mins >= DURATION_MINUTES:
            print("\n[TIMER] Time limit reached.")
            finish_up()

        sys.stdout.write(f"\r[{datetime.now().strftime('%H:%M')}] Polling... ")
        for name, db_id in DATABASES.items():
            data = fetch_live_batch(db_id)
            new = process_batch(name, data)
            sys.stdout.write(f"{name}:{len(observed_peaks[name])} ")
        
        sys.stdout.flush()
        time.sleep(60)