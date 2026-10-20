import threading
import time
from email.utils import parsedate_to_datetime
from datetime import datetime, timezone

import requests

from .errors import ProvisioningError


def retry_delay(value):
    try:
        return max(1, min(int(float(value)), 86400))
    except (TypeError, ValueError):
        try:
            return max(1, min(int(
                (parsedate_to_datetime(value) - datetime.now(timezone.utc)).total_seconds()
            ), 86400))
        except (TypeError, ValueError, OverflowError):
            return 60


class Genesys:
    """No automatic retries for uncertain writes. Reconciliation decides the next action."""

    def __init__(self, settings, rate_limiter, session_factory=requests.Session):
        self.settings = settings
        self.rate = rate_limiter
        self.session_factory = session_factory
        self.local = threading.local()
        self.token = ""
        self.token_expires = 0
        self.token_lock = threading.Lock()

    @property
    def session(self):
        if not hasattr(self.local, "session"):
            self.local.session = self.session_factory()
        return self.local.session

    def authenticate(self):
        with self.token_lock:
            if self.token and time.monotonic() < self.token_expires:
                return self.token
            try:
                response = self.session.post(
                    "https://login." + self.settings.environment + "/oauth/token",
                    auth=(self.settings.client_id, self.settings.client_secret),
                    data={"grant_type": "client_credentials"}, timeout=(5, 20),
                )
            except requests.RequestException:
                raise ProvisioningError("oauth_network_error", True, 60) from None
            if response.status_code != 200:
                raise ProvisioningError("oauth_rejected", response.status_code >= 500, 300,
                                        response.status_code)
            data = response.json()
            if not data.get("access_token"):
                raise ProvisioningError("oauth_invalid_response")
            self.token = data["access_token"]
            self.token_expires = time.monotonic() + max(1, int(data.get("expires_in", 300)) - 60)
            return self.token

    def request(self, method, path, body=None, params=None, allow_404=False):
        for attempt in range(2):
            token = self.authenticate()
            self.rate.acquire()
            try:
                response = self.session.request(
                    method, "https://api." + self.settings.environment + "/api/v2" + path,
                    headers={"Authorization": "Bearer " + token, "Accept": "application/json"},
                    json=body, params=params, timeout=(5, 20),
                )
            except requests.RequestException:
                raise ProvisioningError("api_network_outcome_unknown", True, 60) from None
            status = response.status_code
            if status == 401 and attempt == 0:
                with self.token_lock:
                    if self.token == token:
                        self.token = ""
                continue
            if status == 429:
                delay = retry_delay(response.headers.get("Retry-After"))
                self.rate.block(delay)
                raise ProvisioningError("api_throttled", True, delay, status)
            if allow_404 and status == 404:
                return None
            if status >= 400:
                raise ProvisioningError(
                    "api_http_" + str(status), status in {408, 409, 425} or status >= 500,
                    60, status,
                )
            if status == 204 or not response.content:
                return {}
            try:
                return response.json()
            except ValueError:
                raise ProvisioningError("api_invalid_json", True, 60) from None
        raise ProvisioningError("api_unauthorized", False, 60, 401)

    def all(self, path, params=None):
        """Return a complete listing or raise; never silently return a partial inventory."""
        result = []
        for page in range(1, self.settings.scan_max_pages + 1):
            query = dict(params or {}, pageSize=100, pageNumber=page)
            data = self.request("GET", path, params=query)
            entities = data.get("entities")
            if not isinstance(entities, list):
                raise ProvisioningError("api_invalid_listing")
            result.extend(entities)
            page_count = data.get("pageCount")
            if page_count is not None:
                if page >= int(page_count):
                    return result
            elif len(entities) < 100:
                return result
        raise ProvisioningError("inventory_page_limit")

    def members(self):
        return self.all("/groups/" + self.settings.group_id + "/members")

    def user(self, user_id):
        return self.request("GET", "/users/" + user_id, allow_404=True)

    def phone(self, user_id):
        phones = self.all(
            "/telephony/providers/edges/phones",
            {"webRtcUser.id": user_id, "fields": "webRtcUser",
             "expand": "site,phoneBaseSettings", "sortBy": "id"},
        )
        # Refuse ambiguous or incorrectly filtered results.
        matches = [p for p in phones if (p.get("webRtcUser") or {}).get("id") == user_id]
        if len(matches) != len(phones) or len(matches) > 1:
            raise ProvisioningError("ambiguous_phone_lookup")
        return matches[0] if matches else None

    def station(self, user_id):
        stations = self.all("/stations", {"webRtcUserId": user_id})
        if len(stations) > 1:
            raise ProvisioningError("ambiguous_station_lookup")
        return stations[0].get("id") if stations else None

    def station_state(self, user_id):
        return self.request("GET", "/users/" + user_id + "/station")

    def routing(self, user_id, kind):
        return self.all("/users/" + user_id + "/" + kind)

    def create_phone(self, user):
        return self.request("POST", "/telephony/providers/edges/phones", {
            "name": "WebRTC - " + str(user.get("name") or user["id"])[:80] + " - " + user["id"],
            "site": {"id": self.settings.site_id},
            "phoneBaseSettings": {"id": self.settings.phone_base_id},
            "webRtcUser": {"id": user["id"]},
            "lines": [{"lineBaseSettings": {"id": self.settings.line_base_id}}],
        })

    def set_default(self, user_id, station_id):
        return self.request(
            "PUT", "/users/" + user_id + "/station/defaultstation/" + station_id
        )

    def add_routing(self, user_id, kind, resource_id, proficiency):
        return self.request(
            "PATCH", "/users/" + user_id + "/" + kind + "/bulk",
            [{"id": resource_id, "proficiency": proficiency}],
        )

    def set_preferred(self, user, value):
        return self.request("PATCH", "/users/" + user["id"], {
            "version": user["version"], "preferredName": value,
        })

    def preflight(self):
        for kind, resource_id in (
            ("sites", self.settings.site_id),
            ("phonebasesettings", self.settings.phone_base_id),
            ("linebasesettings", self.settings.line_base_id),
        ):
            self.request("GET", "/telephony/providers/edges/" + kind + "/" + resource_id)
        if self.settings.skill_id:
            self.request("GET", "/routing/skills/" + self.settings.skill_id)
        if self.settings.language_id:
            # The language listing is authoritative; individual GET availability varies.
            languages = self.all("/routing/languages")
            if not any(x.get("id") == self.settings.language_id for x in languages):
                raise ProvisioningError("configured_language_not_found")
