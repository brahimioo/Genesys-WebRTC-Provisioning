import os
import time
import logging
from datetime import datetime
import requests

# ========= Config =========
ENVIRONMENT = os.getenv("GENESYS_ENVIRONMENT", "mypurecloud.de")
CLIENT_ID = os.getenv("GENESYS_CLIENT_ID")
CLIENT_SECRET = os.getenv("GENESYS_CLIENT_SECRET")

# Template phone name contains (case-insensitive)
TEMPLATE_PHONE_NAME_CONTAINS = os.getenv("TEMPLATE_PHONE_NAME_CONTAINS", "WebRTC - Genesys Test User 1")

# Skill + language
TARGET_SKILL_NAME = os.getenv("TARGET_SKILL_NAME", "_Voice")
TARGET_LANGUAGE_NAME = os.getenv("TARGET_LANGUAGE_NAME", "Nederlands")

# Proficiency: 0 sterren
TARGET_SKILL_PROFICIENCY = float(os.getenv("TARGET_SKILL_PROFICIENCY", "0"))
TARGET_LANGUAGE_PROFICIENCY = float(os.getenv("TARGET_LANGUAGE_PROFICIENCY", "0"))

REQUEST_DELAY = float(os.getenv("REQUEST_DELAY", "0.2"))
HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "30"))

DEFAULT_STATION_VERIFY_RETRIES = int(os.getenv("DEFAULT_STATION_VERIFY_RETRIES", "8"))
DEFAULT_STATION_VERIFY_SLEEP = float(os.getenv("DEFAULT_STATION_VERIFY_SLEEP", "0.6"))

# Safety: stop after N users (0 = no limit)
MAX_USERS = int(os.getenv("MAX_USERS", "0"))

# ========= Logging =========
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ========= Helpers =========
def auth_headers(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}

def safe_get_json(resp: requests.Response) -> dict:
    try:
        return resp.json()
    except Exception:
        return {}

def require_env():
    if not CLIENT_ID or not CLIENT_SECRET:
        raise SystemExit(
            "Missing env vars. Set GENESYS_CLIENT_ID and GENESYS_CLIENT_SECRET as GitHub Secrets."
        )

# ========= OAuth =========
def get_access_token() -> str | None:
    url = f"https://login.{ENVIRONMENT}/oauth/token"
    data = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET
    }
    try:
        r = requests.post(url, data=data, timeout=HTTP_TIMEOUT)
        if r.status_code == 200:
            return r.json().get("access_token")
        logging.error("Auth failed: %s - %s", r.status_code, r.text)
        return None
    except requests.RequestException as e:
        logging.error("Auth request error: %s", e)
        return None

# ========= Users (paged) =========
def get_all_active_users(token: str) -> list[dict]:
    users = []
    page = 1
    headers = auth_headers(token)

    while True:
        url = f"https://api.{ENVIRONMENT}/api/v2/users?pageSize=100&pageNumber={page}"
        r = requests.get(url, headers=headers, timeout=HTTP_TIMEOUT)
        if r.status_code != 200:
            logging.error("Users fetch failed page %s: %s - %s", page, r.status_code, r.text)
            break

        data = safe_get_json(r)
        entities = data.get("entities", []) or []

        for u in entities:
            if u.get("state") != "active":
                continue
            users.append({
                "ID": u.get("id"),
                "Naam": u.get("name", ""),
                "Email": u.get("email"),
                "Afdeling": u.get("department"),
                "Titel": u.get("title")
            })

        if page >= data.get("pageCount", 1):
            break

        page += 1
        time.sleep(REQUEST_DELAY)

        if MAX_USERS and len(users) >= MAX_USERS:
            users = users[:MAX_USERS]
            break

    return users

# ========= WebRTC station =========
def get_webrtc_station_for_user(token: str, user_id: str) -> tuple[bool, str | None]:
    headers = auth_headers(token)
    url = f"https://api.{ENVIRONMENT}/api/v2/stations?webRtcUserId={user_id}"
    r = requests.get(url, headers=headers, timeout=HTTP_TIMEOUT)
    if r.status_code != 200:
        logging.warning("Stations lookup failed for %s: %s - %s", user_id, r.status_code, r.text)
        return False, None

    data = safe_get_json(r)
    entities = data.get("entities", []) or []
    if not entities:
        return False, None
    return True, entities[0].get("id")

def get_user_station_state(token: str, user_id: str) -> dict | None:
    headers = auth_headers(token)
    url = f"https://api.{ENVIRONMENT}/api/v2/users/{user_id}/station"
    r = requests.get(url, headers=headers, timeout=HTTP_TIMEOUT)
    if r.status_code != 200:
        logging.warning("User station state fetch failed for %s: %s - %s", user_id, r.status_code, r.text)
        return None
    return safe_get_json(r)

def is_default_station_set(state: dict | None, expected_station_id: str | None) -> bool:
    if not state or not expected_station_id:
        return False

    candidates = []
    for key in ("defaultStationId", "defaultStation", "stationId", "associatedStationId", "associatedStation"):
        v = state.get(key)
        if isinstance(v, str):
            candidates.append(v)
        elif isinstance(v, dict) and isinstance(v.get("id"), str):
            candidates.append(v["id"])

    station = state.get("station")
    if isinstance(station, dict) and isinstance(station.get("id"), str):
        candidates.append(station["id"])

    return expected_station_id in [c for c in candidates if c]

def set_default_station(token: str, user_id: str, station_id: str) -> bool:
    headers = auth_headers(token)
    url = f"https://api.{ENVIRONMENT}/api/v2/users/{user_id}/station/defaultstation/{station_id}"
    r = requests.put(url, headers=headers, timeout=HTTP_TIMEOUT)

    if r.status_code not in (200, 202, 204):
        logging.warning("Default station PUT failed for %s: %s - %s", user_id, r.status_code, r.text)
        return False

    for attempt in range(1, DEFAULT_STATION_VERIFY_RETRIES + 1):
        state = get_user_station_state(token, user_id)
        if is_default_station_set(state, station_id):
            return True
        time.sleep(DEFAULT_STATION_VERIFY_SLEEP)

    logging.warning("Default station accepted but not verified for %s (station=%s).", user_id, station_id)
    return True

# ========= Template phone =========
def find_phone_id_by_name_contains(token: str, name_contains: str) -> str | None:
    headers = auth_headers(token)
    needle = (name_contains or "").strip().lower()
    page = 1

    while True:
        url = f"https://api.{ENVIRONMENT}/api/v2/telephony/providers/edges/phones?pageSize=100&pageNumber={page}"
        r = requests.get(url, headers=headers, timeout=HTTP_TIMEOUT)
        if r.status_code != 200:
            logging.error("Phones fetch failed: %s - %s", r.status_code, r.text)
            return None

        data = safe_get_json(r)
        entities = data.get("entities", []) or []
        for p in entities:
            pname = (p.get("name") or "").lower()
            if needle and needle in pname:
                return p.get("id")

        if page >= data.get("pageCount", 1):
            break

        page += 1
        time.sleep(REQUEST_DELAY)

    return None

def get_phone_details(token: str, phone_id: str) -> dict | None:
    headers = auth_headers(token)
    url = f"https://api.{ENVIRONMENT}/api/v2/telephony/providers/edges/phones/{phone_id}"
    r = requests.get(url, headers=headers, timeout=HTTP_TIMEOUT)
    if r.status_code != 200:
        logging.error("Phone details fetch failed: %s - %s", r.status_code, r.text)
        return None
    return safe_get_json(r)

def build_payload_from_template(template_phone: dict, user: dict) -> dict:
    site_id = template_phone["site"]["id"]
    phone_base_settings_id = template_phone["phoneBaseSettings"]["id"]

    lines = template_phone.get("lines", []) or []
    if not lines:
        raise ValueError("Template phone has no lines[]")

    lbs = lines[0].get("lineBaseSettings", {}) or {}
    line_base_settings_id = lbs.get("id")
    if not line_base_settings_id:
        raise ValueError("Template phone lines[0].lineBaseSettings.id missing")

    return {
        "name": f"WebRTC - {user.get('Naam')}",
        "site": {"id": site_id},
        "phoneBaseSettings": {"id": phone_base_settings_id},
        "webRtcUser": {"id": user.get("ID")},
        "lines": [{"lineBaseSettings": {"id": line_base_settings_id}}]
    }

# ========= Skill/Language lookup =========
def find_routing_skill_id_by_name(token: str, skill_name_exact: str) -> str | None:
    headers = auth_headers(token)
    needle = (skill_name_exact or "").strip().lower()
    page = 1

    while True:
        url = f"https://api.{ENVIRONMENT}/api/v2/routing/skills?pageSize=100&pageNumber={page}"
        r = requests.get(url, headers=headers, timeout=HTTP_TIMEOUT)
        if r.status_code != 200:
            logging.error("Skills fetch failed: %s - %s", r.status_code, r.text)
            return None

        data = safe_get_json(r)
        entities = data.get("entities", []) or []
        for s in entities:
            if (s.get("name") or "").strip().lower() == needle:
                return s.get("id")

        if page >= data.get("pageCount", 1):
            break

        page += 1
        time.sleep(REQUEST_DELAY)

    return None

def find_routing_language_id_by_name(token: str, language_name_exact: str) -> str | None:
    headers = auth_headers(token)
    needle = (language_name_exact or "").strip().lower()
    page = 1

    while True:
        url = f"https://api.{ENVIRONMENT}/api/v2/routing/languages?pageSize=100&pageNumber={page}"
        r = requests.get(url, headers=headers, timeout=HTTP_TIMEOUT)
        if r.status_code != 200:
            logging.error("Languages fetch failed: %s - %s", r.status_code, r.text)
            return None

        data = safe_get_json(r)
        entities = data.get("entities", []) or []
        for lang in entities:
            if (lang.get("name") or "").strip().lower() == needle:
                return lang.get("id")

        if page >= data.get("pageCount", 1):
            break

        page += 1
        time.sleep(REQUEST_DELAY)

    return None

# ========= User has skill/language (paged) =========
def user_has_skill(token: str, user_id: str, skill_id: str) -> bool:
    headers = auth_headers(token)
    page = 1
    while True:
        url = f"https://api.{ENVIRONMENT}/api/v2/users/{user_id}/routingskills?pageSize=100&pageNumber={page}"
        r = requests.get(url, headers=headers, timeout=HTTP_TIMEOUT)
        if r.status_code != 200:
            logging.warning("User skills fetch failed for %s: %s - %s", user_id, r.status_code, r.text)
            return False

        data = safe_get_json(r)
        entities = data.get("entities", []) or []
        if any(e.get("id") == skill_id for e in entities):
            return True

        if page >= data.get("pageCount", 1):
            break

        page += 1
        time.sleep(REQUEST_DELAY)

    return False

def user_has_language(token: str, user_id: str, language_id: str) -> bool:
    headers = auth_headers(token)
    page = 1
    while True:
        url = f"https://api.{ENVIRONMENT}/api/v2/users/{user_id}/routinglanguages?pageSize=100&pageNumber={page}"
        r = requests.get(url, headers=headers, timeout=HTTP_TIMEOUT)
        if r.status_code != 200:
            logging.warning("User languages fetch failed for %s: %s - %s", user_id, r.status_code, r.text)
            return False

        data = safe_get_json(r)
        entities = data.get("entities", []) or []
        if any(e.get("id") == language_id for e in entities):
            return True

        if page >= data.get("pageCount", 1):
            break

        page += 1
        time.sleep(REQUEST_DELAY)

    return False

# ========= Assign skill/language (bulk PATCH) =========
def ensure_user_skill(token: str, user: dict, skill_id: str, proficiency: float) -> bool:
    user_id = user["ID"]
    if user_has_skill(token, user_id, skill_id):
        return True

    headers = {**auth_headers(token), "Content-Type": "application/json"}
    url = f"https://api.{ENVIRONMENT}/api/v2/users/{user_id}/routingskills/bulk"
    body = [{"id": skill_id, "proficiency": float(proficiency)}]

    r = requests.patch(url, headers=headers, json=body, timeout=HTTP_TIMEOUT)
    if r.status_code not in (200, 201, 202, 204):
        logging.error("Skill assign failed for %s (%s): %s - %s", user.get("Naam"), user_id, r.status_code, r.text)
        return False
    return True

def ensure_user_language(token: str, user: dict, language_id: str, proficiency: float) -> bool:
    user_id = user["ID"]
    if user_has_language(token, user_id, language_id):
        return True

    headers = {**auth_headers(token), "Content-Type": "application/json"}
    url = f"https://api.{ENVIRONMENT}/api/v2/users/{user_id}/routinglanguages/bulk"
    body = [{"id": language_id, "proficiency": float(proficiency)}]

    r = requests.patch(url, headers=headers, json=body, timeout=HTTP_TIMEOUT)
    if r.status_code not in (200, 201, 202, 204):
        logging.error("Language assign failed for %s (%s): %s - %s", user.get("Naam"), user_id, r.status_code, r.text)
        return False
    return True

def ensure_user_skill_and_language(token: str, user: dict, skill_id: str, language_id: str) -> bool:
    ok1 = ensure_user_skill(token, user, skill_id, TARGET_SKILL_PROFICIENCY)
    time.sleep(REQUEST_DELAY)
    ok2 = ensure_user_language(token, user, language_id, TARGET_LANGUAGE_PROFICIENCY)
    return ok1 and ok2

# ========= Create WebRTC phone =========
def create_webrtc_phone_for_user(token: str, user: dict, template_phone_details: dict, skill_id: str, language_id: str) -> bool:
    payload = build_payload_from_template(template_phone_details, user)

    headers = {**auth_headers(token), "Content-Type": "application/json"}
    url = f"https://api.{ENVIRONMENT}/api/v2/telephony/providers/edges/phones"

    r = requests.post(url, headers=headers, json=payload, timeout=HTTP_TIMEOUT)
    if r.status_code not in (200, 201):
        logging.error("WebRTC phone create failed for %s (%s): %s - %s", user.get("Naam"), user.get("ID"), r.status_code, r.text)
        return False

    time.sleep(max(REQUEST_DELAY, 0.6))
    for _ in range(1, 7):
        has_webrtc, station_id = get_webrtc_station_for_user(token, user["ID"])
        if has_webrtc and station_id:
            set_default_station(token, user["ID"], station_id)
            break
        time.sleep(0.7)

    return ensure_user_skill_and_language(token, user, skill_id, language_id)

# ========= Filter: users without WebRTC =========
def get_users_without_webrtc(token: str, users: list[dict]) -> list[dict]:
    result = []
    for idx, u in enumerate(users, start=1):
        has_webrtc, _ = get_webrtc_station_for_user(token, u["ID"])
        if not has_webrtc:
            result.append(u)

        if idx % 50 == 0:
            logging.info("WebRTC scan progress: %s/%s (without=%s)", idx, len(users), len(result))

        time.sleep(REQUEST_DELAY)
    return result

# ========= Main job =========
def run():
    require_env()

    token = get_access_token()
    if not token:
        raise SystemExit("Could not obtain access token")

    skill_id = find_routing_skill_id_by_name(token, TARGET_SKILL_NAME)
    if not skill_id:
        raise SystemExit(f"Skill not found: {TARGET_SKILL_NAME}")

    language_id = find_routing_language_id_by_name(token, TARGET_LANGUAGE_NAME)
    if not language_id:
        raise SystemExit(f"Language not found: {TARGET_LANGUAGE_NAME}")

    template_phone_id = find_phone_id_by_name_contains(token, TEMPLATE_PHONE_NAME_CONTAINS)
    if not template_phone_id:
        raise SystemExit(f"Template phone not found (name contains): {TEMPLATE_PHONE_NAME_CONTAINS}")

    template_phone_details = get_phone_details(token, template_phone_id)
    if not template_phone_details:
        raise SystemExit("Could not fetch template phone details")

    users_all = get_all_active_users(token)
    logging.info("Active users fetched: %s", len(users_all))

    users = get_users_without_webrtc(token, users_all)
    logging.info("Users without WebRTC: %s", len(users))

    if not users:
        logging.info("Nothing to do.")
        return

    ok = 0
    fail = 0

    for idx, u in enumerate(users, start=1):
        try:
            success = create_webrtc_phone_for_user(token, u, template_phone_details, skill_id, language_id)
            if success:
                ok += 1
            else:
                fail += 1
        except Exception as e:
            fail += 1
            logging.exception("Unhandled error for user %s (%s): %s", u.get("Naam"), u.get("ID"), e)

        if idx % 25 == 0:
            logging.info("Provision progress: %s/%s (ok=%s fail=%s)", idx, len(users), ok, fail)

        time.sleep(REQUEST_DELAY)

    logging.info("DONE. ok=%s fail=%s total=%s", ok, fail, len(users))

if __name__ == "__main__":
    logging.info("Genesys job started (%s)", datetime.now().isoformat())
    run()

