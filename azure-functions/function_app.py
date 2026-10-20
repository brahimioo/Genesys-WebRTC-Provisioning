import json
import logging
import time
from functools import lru_cache
from uuid import UUID

import azure.functions as func

from webrtc.auth import require_admin
from webrtc.errors import ProvisioningError
from webrtc.service import Service
from webrtc.telemetry import emit

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)
logging.getLogger("azure").setLevel(logging.WARNING)


@lru_cache(maxsize=1)
def runtime():
    return Service()


def response(body, status=200):
    return func.HttpResponse(json.dumps(body, default=str), status_code=status,
                             mimetype="application/json")


def admin_request(req, handler):
    try:
        service = runtime()
        actor = require_admin(req.headers, service.settings)
        return handler(service, actor)
    except ProvisioningError as error:
        return response({"error": error.code}, error.status or (409 if error.retryable else 400))
    except (ValueError, TypeError, KeyError):
        return response({"error": "invalid_request_or_configuration"}, 400)
    except Exception as error:
        emit("admin_request_failed", code=type(error).__name__)
        return response({"error": "service_unavailable"}, 503)


@app.timer_trigger(schedule="%SCAN_SCHEDULE%", arg_name="timer",
                   run_on_startup=False, use_monitor=True)
def scan_users(timer: func.TimerRequest):
    runtime().scan()


@app.timer_trigger(schedule="%RECOVERY_SCHEDULE%", arg_name="timer",
                   run_on_startup=False, use_monitor=True)
def recover_work(timer: func.TimerRequest):
    runtime().maintenance()


@app.timer_trigger(schedule="%RECONCILE_SCHEDULE%", arg_name="timer",
                   run_on_startup=False, use_monitor=True)
def reconcile_users(timer: func.TimerRequest):
    # Completed users are rechecked when RECONCILE_HOURS has elapsed.
    runtime().scan()


@app.queue_trigger(arg_name="message", queue_name="webrtc-scans",
                   connection="ProvisioningStorage")
def requested_scan(message: func.QueueMessage):
    runtime().scan()


@app.queue_trigger(arg_name="message", queue_name="webrtc-jobs",
                   connection="ProvisioningStorage")
def provision_user(message: func.QueueMessage):
    job = json.loads(message.get_body().decode("utf-8"))
    if job.get("schemaVersion") != 1:
        raise ValueError("Unsupported queue schema")
    job["userId"] = str(UUID(job["userId"]))
    UUID(job["generation"])
    runtime().provisioner.process(job)


@app.queue_trigger(arg_name="message", queue_name="webrtc-jobs-poison",
                   connection="ProvisioningStorage")
def poison_user(message: func.QueueMessage):
    try:
        job = json.loads(message.get_body().decode("utf-8"))
        user_id = str(UUID(job["userId"]))
        service = runtime()
        doc, etag = service.store.get_user(user_id)
        if (doc and doc.get("generation") == job.get("generation")
                and doc["status"] in {"pending", "retry", "running"}
                and doc.get("lease_until", 0) <= time.time()):
            doc.update(status="blocked", last_error="queue_poison", lease_until=0)
            service.store.save_user(doc, etag)
        emit("poison_message", user_id=user_id)
    except (ValueError, KeyError, TypeError):
        emit("poison_message", code="invalid_message")


@app.queue_trigger(arg_name="message", queue_name="webrtc-scans-poison",
                   connection="ProvisioningStorage")
def poison_scan(message: func.QueueMessage):
    emit("poison_message", code="scan_queue_poison")


@app.route(route="health", methods=["GET"])
def health(req: func.HttpRequest):
    return admin_request(req, lambda service, actor: response({
        "service": "genesys-webrtc-provisioning", "dryRun": service.settings.dry_run,
        "paused": service.store.paused(), "policyVersion": service.settings.fingerprint,
    }))


@app.route(route="scan", methods=["POST"])
def request_scan(req: func.HttpRequest):
    def handle(service, actor):
        request_id = service.store.enqueue_scan()
        emit("admin_scan_requested", actor_id=actor, request_id=request_id)
        return response({"requestId": request_id, "status": "queued"}, 202)
    return admin_request(req, handle)


@app.route(route="provision", methods=["POST"])
def request_provision(req: func.HttpRequest):
    def handle(service, actor):
        body = req.get_json()
        if not isinstance(body, dict) or set(body) - {"userId", "resetCreateIntent"}:
            raise ValueError()
        reset = body.get("resetCreateIntent", False)
        if not isinstance(reset, bool):
            raise ValueError()
        user_id = str(UUID(body["userId"]))
        scheduled = service.request_user(user_id, reset)
        emit("admin_user_requested", actor_id=actor, user_id=user_id,
             reset_create_intent=reset)
        return response({"userId": user_id,
                         "status": "queued" if scheduled else "already_running"}, 202)
    return admin_request(req, handle)


@app.route(route="status", methods=["GET"])
def statuses(req: func.HttpRequest):
    def handle(service, actor):
        limit = int(req.params.get("limit", "100"))
        if not 1 <= limit <= 200:
            raise ValueError()
        status = req.params.get("status")
        if status and status not in {
            "pending", "running", "retry", "blocked", "succeeded",
            "succeeded_with_warnings", "dry_run", "cancelled", "excluded",
        }:
            raise ValueError()
        rows, truncated = service.store.list_status(status, limit)
        return response({"items": rows, "truncated": truncated,
                         "note": "For full inventory query WebRtcState."})
    return admin_request(req, handle)


@app.route(route="status/{user_id}", methods=["GET"])
def user_status(req: func.HttpRequest):
    def handle(service, actor):
        user_id = str(UUID(req.route_params["user_id"]))
        doc, _ = service.store.get_user(user_id)
        return response(doc or {"error": "not_found"}, 200 if doc else 404)
    return admin_request(req, handle)


@app.route(route="audit/{user_id}", methods=["GET"])
def user_audit(req: func.HttpRequest):
    return admin_request(req, lambda service, actor: response({
        "items": service.store.user_audit(str(UUID(req.route_params["user_id"]))),
        "note": "Up to 100 retained records, storage key order; not a chronological page.",
    }))


@app.route(route="control", methods=["PATCH"])
def set_control(req: func.HttpRequest):
    def handle(service, actor):
        body = req.get_json()
        if not isinstance(body, dict) or set(body) != {"paused"} or not isinstance(body["paused"], bool):
            raise ValueError()
        service.store.set_pause(body["paused"])
        emit("admin_pause_changed", actor_id=actor, paused=body["paused"])
        return response({"paused": service.store.paused()})
    return admin_request(req, handle)
