
import smtplib
from email.mime.text import MIMEText
import pandas as pd
import json
import os
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def send_emails(clusters, reports, price_optimization_data=None, segmentation_stats=None, db=None):
    smtp_server = "smtp.gmail.com"
    smtp_port = 587
    sender = "nhaal160@gmail.com"
    password = "shanazeer"

    templates = {
        "high": {
            "id": "1",
            "name": "Premium Welcome Series",
            "subject": "Welcome to our VIP Experience",
            "segmentId": "high",
            "segmentName": "High Customers",
            "content": "Dear High-Value Customer,\n\nThank you for your significant contributions! Your segment report: {report}\nCluster Stats: {customer_count} customers, ${total_revenue:.2f} revenue, top categories: {top_categories}.\n{price_optimization}\nBest regards,\nAgentBI Team",
            "status": "active",
            "openRate": 0.0,
            "clickRate": 0.0,
            "lastSent": datetime.now().strftime("%Y-%m-%d_%H:%M")
        },
        "mid": {
            "id": "2",
            "name": "Weekly Deals Newsletter",
            "subject": "This Week's Best Offers",
            "segmentId": "mid",
            "segmentName": "Mid Customers",
            "content": "Dear Mid-Value Customer,\n\nWe appreciate your support! Your segment report: {report}\nCluster Stats: {customer_count} customers, ${total_revenue:.2f} revenue, top categories: {top_categories}.\n{price_optimization}\nBest regards,\nAgentBI Team",
            "status": "active",
            "openRate": 0.0,
            "clickRate": 0.0,
            "lastSent": datetime.now().strftime("%Y-%m-%d_%H:%M")
        },
        "low": {
            "id": "3",
            "name": "Flash Sale Alert",
            "subject": "⚡ 48-Hour Flash Sale - Up to 60% Off",
            "segmentId": "low",
            "segmentName": "Low Customers",
            "content": "Dear Customer,\n\nWe’re here to help you grow! Your segment report: {report}\nCluster Stats: {customer_count} customers, ${total_revenue:.2f} revenue, top categories: {top_categories}.\n{price_optimization}\nBest regards,\nAgentBI Team",
            "status": "active",
            "openRate": 0.0,
            "clickRate": 0.0,
            "lastSent": datetime.now().strftime("%Y-%m-%d_%H:%M")
        }
    }

    stats = segmentation_stats or []
    email_file = "Backend/output/emails.csv"
    customer_emails = {}
    if os.path.exists(email_file):
        email_df = pd.read_csv(email_file)
        customer_emails = email_df[['Customer ID', 'email']].drop_duplicates().set_index('Customer ID')['email'].to_dict()
    else:
        logger.warning("No email list found at %s. Using placeholder.", email_file)

    server = smtplib.SMTP(smtp_server, smtp_port)
    server.starttls()
    server.login(sender, password)

    emails_sent = []
    for cluster in clusters[:50]:
        cluster_label = cluster['id']
        report = next((r for r in reports if r.startswith(f"{cluster_label}-value")), "No report available")
        cluster_stat = next((s for s in stats if s['id'] == cluster_label), {})
        customer_count = cluster_stat.get('count', 0)
        total_revenue = cluster_stat.get('value', 0.0)
        top_categories = cluster_stat.get('characteristics', [])
        price_optimization = price_optimization_data.get("llm_report", "No price optimization data available.") if price_optimization_data else ""
        template = templates.get(cluster_label, templates["low"])
        template["recipients"] = customer_count
        email_content = template["content"].format(
            report=report,
            customer_count=customer_count,
            total_revenue=total_revenue,
            top_categories=', '.join(top_categories),
            price_optimization=price_optimization
        )
        customer_id = cluster.get('customer_id', f"customer_{cluster_label}")
        email = customer_emails.get(customer_id, f"{cluster_label}@example.com")
        msg = MIMEText(email_content)
        msg['Subject'] = template["subject"]
        msg['From'] = sender
        msg['To'] = email
        server.sendmail(sender, email, msg.as_string())
        emails_sent.append({
            "to": email,
            "subject": template["subject"],
            "body": email_content
        })

    server.quit()

    output_dir = os.path.join(os.path.dirname(__file__), f"output_{datetime.now().strftime('%Y-%m-%d_%H:%M')}")
    os.makedirs(output_dir, exist_ok=True)
    with open(f"{output_dir}/emails_sent.json", "w") as f:
        json.dump(emails_sent, f, indent=2)
    logger.info("Emails sent and saved to %s", output_dir)

    return {
        "task_id": 10,
        "pipeline_id": "AgentBI-Demo",
        "schema_version": "v0.6.2",
        "timestamp": datetime.now().strftime("%Y-%m-%d_%H:%M"),
        "status": "success",
        "emails_sent": len(emails_sent),
        "message": f"Sent {len(emails_sent)} emails"
    }