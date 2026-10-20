import json
import logging


def emit(name, **fields):
    logger = logging.getLogger("webrtc")
    level = logging.ERROR if name in {"scan_failed", "user_failed", "poison_message"} else logging.INFO
    logger.log(level, "webrtc_event %s", json.dumps({"event": name, **fields}, separators=(",", ":")))
