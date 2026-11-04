import time
import requests
import psycopg2

SLACK_WEBHOOK = "https://hooks.slack.com/services/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"
conn = psycopg2.connect(host="localhost", port=5433, dbname="postgres", user="postgres", password="postgres")

seen_alerts = set()

while True:
    cur = conn.cursor()
    cur.execute("""
        SELECT account_id, total_usage, plan_limit, updated_at
        FROM account_usage
        WHERE is_overage = true
        ORDER BY updated_at DESC LIMIT 20
    """)
    for account_id, total_usage, plan_limit, updated_at in cur.fetchall():
        key = f"{account_id}-{updated_at}"
        if key not in seen_alerts:
            seen_alerts.add(key)
            requests.post(SLACK_WEBHOOK, json={
                "text": f":rotating_light: *Overage detected* — `{account_id}` used {total_usage} (limit {plan_limit}) as of {updated_at}"
            })
    cur.close()
    time.sleep(10)