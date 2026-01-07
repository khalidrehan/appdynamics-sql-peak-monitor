# AppDynamics SQL Peak Monitor

A Python-based observability tool that bridges the gap between "Average Performance" and "Peak Experience."

Standard AppDynamics reports often aggregate data over time, smoothing out critical performance spikes. This tool connects to the AppDynamics Controller API, polls database performance in real-time (1-minute resolution), and captures the **exact worst-case execution time** for every query.

## 🚀 Key Features

* **Live Peak Detection:** Polls the Controller every 60 seconds to capture transient performance spikes that vanish in hourly averages.
* **Multi-Database Support:** Monitors multiple database instances (SQL Server, Oracle, Postgres, etc.) simultaneously.
* **Smart Deduplication:** Tracks unique SQL statements and updates their "High Water Mark" (Max Duration) in memory.
* **Automated Reporting:**
    * **Email Scorecard:** Sends a beautifully formatted HTML email with health status and embedded charts.
    * **CSV Attachment:** Attaches a raw data file with full SQL text for deep analysis.
    * **Visualizations:** Generates "Top 5 Slowest Spikes" graphs for each database using `matplotlib`.

## 🛠️ Prerequisites

* Python 3.6+
* AppDynamics Controller Access (User/Pass or Session Tokens)
* SMTP Server (for email reports)

### Dependencies
```bash
pip install requests matplotlib
