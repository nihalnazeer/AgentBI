
import logging
import json
import os
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def generate_notifications(trigger_results, db=None):
    try:
        notifications = [
            {
                "id": email["id"],
                "title": email["subject"],
                "message": email["body"],
                "type": "automation" if "Alert" in email["subject"] or "Escalation" in email["subject"] else "info",
                "priority": email["priority"],
                "timestamp": email["lastTriggered"],
                "read": False,
                "pipeline_id": "AgentBI-Demo",
                "schema_version": "v0.6.2",
                "task_id": 9
            } for email in trigger_results.get("emails", []) if "admin@example.com" in email["to"]
        ]

        output_dir = os.path.join(os.path.dirname(__file__), f"output_{datetime.now().strftime('%Y-%m-%d_%H:%M')}")
        os.makedirs(output_dir, exist_ok=True)
        output = {
            "notifications": notifications,
            "timestamp": datetime.now().strftime("%Y-%m-%d_%H:%M"),
            "pipeline_id": "AgentBI-Demo",
            "schema_version": "v0.6.2",
            "task_id": 9
        }
        with open(f"{output_dir}/notifications.json", "w") as f:
            json.dump(output, f, indent=2)
        logger.info("Notifications saved to %s", output_dir)

        return output
    except Exception as e:
        logger.error("Notification generation failed: %s", str(e))
        return {
            "error": str(e),
            "pipeline_id": "AgentBI-Demo",
            "schema_version": "v0.6.2",
            "task_id": 9
        }