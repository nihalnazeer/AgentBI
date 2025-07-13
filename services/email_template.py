import smtplib
from email.mime.text import MIMEText
import pandas as pd
import json
import os
from services.utils import load_sales_data
import logging

logger = logging.getLogger(__name__)

def send_emails(clusters, reports, price_optimization_data=None):
    smtp_server = "smtp.gmail.com"
    smtp_port = 587
    sender = "your_email@gmail.com"
    password = "your_app_password"

    # Load stats from file
    with open("Backend/output/stats.json", "r") as f:
        stats = json.load(f)["stats"]

    # Define segment-specific templates
    templates = {
        "High": "Dear High-Value Customer,\n\nThank you for your significant contributions! Your segment report: {report}\nCluster Stats: {customer_count} customers, ${total_revenue:.2f} revenue, top categories: {top_categories}.\n{price_optimization}\nBest regards,\nAgentBI Team",
        "Mid": "Dear Mid-Value Customer,\n\nWe appreciate your support! Your segment report: {report}\nCluster Stats: {customer_count} customers, ${total_revenue:.2f} revenue, top categories: {top_categories}.\n{price_optimization}\nBest regards,\nAgentBI Team",
        "Low": "Dear Customer,\n\nWe’re here to help you grow! Your segment report: {report}\nCluster Stats: {customer_count} customers, ${total_revenue:.2f} revenue, top categories: {top_categories}.\n{price_optimization}\nBest regards,\nAgentBI Team"
    }

    # Optional: Load uploaded email list
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

    for cluster in clusters[:50]:
        cluster_label = cluster['cluster']
        report = next((r for r in reports if r.startswith(f"{cluster_label}-value")), "No report available")
        cluster_stat = next((s for s in stats if s['cluster_label'] == cluster_label), {})
        customer_count = cluster_stat.get('customer_count', 0)
        total_revenue = cluster_stat.get('total_revenue', 0.0)
        top_categories = cluster_stat.get('top_categories', [])
        price_optimization = price_optimization_data.get(cluster_label, "No price optimization data available.") if price_optimization_data else ""
        template = templates.get(cluster_label, templates["Low"])
        email_content = template.format(
            report=report,
            customer_count=customer_count,
            total_revenue=total_revenue,
            top_categories=', '.join(top_categories),
            price_optimization=price_optimization
        )
        customer_id = cluster.get('customer_id', f"customer_{cluster_label}")
        email = customer_emails.get(customer_id, f"{cluster_label.lower()}@example.com")
        msg = MIMEText(email_content)
        msg['Subject'] = f"AgentBI Segmentation Report - {cluster_label}"
        msg['From'] = sender
        msg['To'] = email
        server.sendmail(sender, email, msg.as_string())

    server.quit()