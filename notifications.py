# notifications.py - APNs push notification sending for favorite restaurant updates

import time
import logging
import jwt
import httpx
from db_manager import DatabaseConnection
from psycopg.rows import dict_row
from config import APNsConfig

logger = logging.getLogger(__name__)

APNS_PROD_URL = "https://api.push.apple.com"
APNS_SANDBOX_URL = "https://api.sandbox.push.apple.com"

# Cache for APNs JWT token (valid for up to 1 hour)
_apns_token_cache = {"token": None, "issued_at": 0}
_APNS_TOKEN_DURATION = 3500  # Refresh before 1-hour expiry


def _get_apns_token():
    """Generate or return cached APNs JWT token for token-based auth."""
    now = int(time.time())
    if _apns_token_cache["token"] and (now - _apns_token_cache["issued_at"]) < _APNS_TOKEN_DURATION:
        return _apns_token_cache["token"]

    if not all([APNsConfig.KEY_ID, APNsConfig.TEAM_ID, APNsConfig.KEY_CONTENT]):
        logger.error("APNs credentials not configured (APNS_KEY_ID, APNS_TEAM_ID, APNS_KEY_CONTENT)")
        return None

    key_content = APNsConfig.KEY_CONTENT.replace("\\n", "\n")

    payload = {"iss": APNsConfig.TEAM_ID, "iat": now}
    headers = {"alg": "ES256", "kid": APNsConfig.KEY_ID}

    token = jwt.encode(payload, key_content, algorithm="ES256", headers=headers)
    _apns_token_cache["token"] = token
    _apns_token_cache["issued_at"] = now
    logger.info("Generated new APNs JWT token")
    return token


def _send_apns_notification(device_token, title, body, data=None):
    """Send a single push notification via APNs HTTP/2."""
    apns_token = _get_apns_token()
    if not apns_token:
        return False

    base_url = APNS_SANDBOX_URL if APNsConfig.USE_SANDBOX else APNS_PROD_URL
    url = f"{base_url}/3/device/{device_token}"

    payload = {
        "aps": {
            "alert": {"title": title, "body": body},
            "sound": "default",
        }
    }
    if data:
        payload.update(data)

    headers = {
        "authorization": f"bearer {apns_token}",
        "apns-topic": APNsConfig.BUNDLE_ID,
        "apns-push-type": "alert",
        "apns-priority": "5",
    }

    try:
        with httpx.Client(http2=True) as client:
            response = client.post(url, json=payload, headers=headers, timeout=10)

        if response.status_code == 200:
            return True
        elif response.status_code == 410:
            logger.info(f"Device token expired (410), removing: {device_token[:16]}...")
            _remove_device_token(device_token)
            return False
        else:
            logger.error(f"APNs error {response.status_code}: {response.text}")
            return False
    except Exception as e:
        logger.error(f"Failed to send APNs notification: {e}")
        return False


def _remove_device_token(device_token):
    """Remove an expired/invalid device token from the database."""
    try:
        with DatabaseConnection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM user_push_tokens WHERE device_token = %s", (device_token,))
            conn.commit()
    except Exception as e:
        logger.error(f"Failed to remove device token: {e}")


def _record_notification(user_id, camis, notification_type, message):
    """Record a sent notification for deduplication."""
    try:
        with DatabaseConnection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """INSERT INTO notification_history (user_id, restaurant_camis, notification_type, message)
                       VALUES (%s, %s, %s, %s)""",
                    (user_id, camis, notification_type, message),
                )
            conn.commit()
    except Exception as e:
        logger.error(f"Failed to record notification history: {e}")


def _was_recently_notified(cursor, user_id, camis, notification_type, hours=24):
    """Check if user was already notified about this event recently."""
    cursor.execute(
        """SELECT 1 FROM notification_history
           WHERE user_id = %s AND restaurant_camis = %s
             AND notification_type = %s
             AND sent_at >= NOW() - make_interval(hours => %s)
           LIMIT 1""",
        (user_id, camis, notification_type, hours),
    )
    return cursor.fetchone() is not None


def send_notifications_for_updates(grade_updates, new_violations, reopened_camis_list):
    """
    Main entry point: called after update_database_batch completes.
    Checks which updated restaurants are favorited by users and sends notifications.

    Args:
        grade_updates: list of tuples (camis, previous_grade, new_grade, update_type, inspection_date)
        new_violations: list of tuples (camis, inspection_date, violation_code, violation_description)
        reopened_camis_list: list of camis strings for restaurants that just reopened
    """
    if not any([grade_updates, new_violations, reopened_camis_list]):
        return

    all_changed_camis = set()
    for gu in grade_updates:
        all_changed_camis.add(gu[0])
    for v in new_violations:
        all_changed_camis.add(v[0])
    for c in reopened_camis_list:
        all_changed_camis.add(c)

    if not all_changed_camis:
        return

    # Find users who favorited these restaurants and have push tokens
    try:
        with DatabaseConnection() as conn:
            with conn.cursor(row_factory=dict_row) as cursor:
                cursor.execute(
                    """SELECT f.user_id, f.restaurant_camis, r.dba, pt.device_token
                       FROM favorites f
                       JOIN user_push_tokens pt ON f.user_id = pt.user_id
                       JOIN (
                           SELECT DISTINCT ON (camis) camis, dba
                           FROM restaurants ORDER BY camis, inspection_date DESC
                       ) r ON f.restaurant_camis = r.camis
                       WHERE f.restaurant_camis = ANY(%s)""",
                    (list(all_changed_camis),),
                )
                subscriptions = cursor.fetchall()
    except Exception as e:
        logger.error(f"Failed to query favorite subscriptions: {e}")
        return

    if not subscriptions:
        logger.info("No users have favorited the updated restaurants - no notifications to send.")
        return

    # Build lookup: camis -> list of subscribers
    camis_subscribers = {}
    for sub in subscriptions:
        camis = sub["restaurant_camis"]
        if camis not in camis_subscribers:
            camis_subscribers[camis] = []
        camis_subscribers[camis].append({
            "user_id": sub["user_id"],
            "device_token": sub["device_token"],
            "dba": sub["dba"],
        })

    sent_count = 0

    try:
        with DatabaseConnection() as conn:
            with conn.cursor(row_factory=dict_row) as cursor:

                # 1. Grade change notifications
                for camis, prev_grade, new_grade, update_type, inspection_date in grade_updates:
                    if camis not in camis_subscribers:
                        continue
                    for sub in camis_subscribers[camis]:
                        if _was_recently_notified(cursor, sub["user_id"], camis, "grade_change"):
                            continue
                        name = sub["dba"] or "A restaurant you follow"
                        if update_type == "finalized":
                            title = "Grade Update"
                            body = f"{name} received a grade of {new_grade}"
                        else:
                            title = "New Inspection Result"
                            body = f"{name} was inspected and received a {new_grade}"
                        if _send_apns_notification(sub["device_token"], title, body, {"camis": camis}):
                            _record_notification(sub["user_id"], camis, "grade_change", body)
                            sent_count += 1

                # 2. New violation notifications (one per restaurant)
                violation_camis_seen = set()
                for camis, inspection_date, v_code, v_desc in new_violations:
                    if camis in violation_camis_seen or camis not in camis_subscribers:
                        continue
                    violation_camis_seen.add(camis)
                    for sub in camis_subscribers[camis]:
                        if _was_recently_notified(cursor, sub["user_id"], camis, "new_violation"):
                            continue
                        name = sub["dba"] or "A restaurant you follow"
                        title = "New Violation Found"
                        body = f"{name} received a new violation during a recent inspection"
                        if _send_apns_notification(sub["device_token"], title, body, {"camis": camis}):
                            _record_notification(sub["user_id"], camis, "new_violation", body)
                            sent_count += 1

                # 3. Reopened notifications
                for camis in reopened_camis_list:
                    if camis not in camis_subscribers:
                        continue
                    for sub in camis_subscribers[camis]:
                        if _was_recently_notified(cursor, sub["user_id"], camis, "reopened"):
                            continue
                        name = sub["dba"] or "A restaurant you follow"
                        title = "Restaurant Reopened"
                        body = f"{name} has been re-opened"
                        if _send_apns_notification(sub["device_token"], title, body, {"camis": camis}):
                            _record_notification(sub["user_id"], camis, "reopened", body)
                            sent_count += 1

    except Exception as e:
        logger.error(f"Error during notification sending: {e}")

    logger.info(f"Push notifications sent: {sent_count}")
