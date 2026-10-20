import base64
import json

from .errors import ProvisioningError


def require_admin(headers, settings):
    """Trust only the principal injected by Azure App Service Authentication.

    The Bicep template requires Easy Auth on every HTTP request. There is no
    local header-bypass flag; do not disable Easy Auth on the deployed app.
    """
    encoded = headers.get("x-ms-client-principal", "")
    try:
        principal = json.loads(base64.b64decode(encoded, validate=True))
        if principal.get("auth_typ") != "aad":
            raise ValueError()
        claims = {}
        for item in principal.get("claims", []):
            claims.setdefault(item["typ"], []).append(item["val"])
        tenant = (
            claims.get("http://schemas.microsoft.com/identity/claims/tenantid")
            or claims.get("tid") or [""]
        )[0]
        object_id = (
            claims.get("http://schemas.microsoft.com/identity/claims/objectidentifier")
            or claims.get("oid") or [""]
        )[0]
        roles = claims.get("roles", []) + claims.get(
            "http://schemas.microsoft.com/ws/2008/06/identity/claims/role", []
        )
    except (ValueError, TypeError, KeyError, UnicodeDecodeError):
        raise ProvisioningError("authentication_required", status=401) from None
    if tenant != settings.tenant_id:
        raise ProvisioningError("tenant_not_allowed", status=403)
    if object_id not in settings.admin_ids and "Provisioning.Admin" not in roles:
        raise ProvisioningError("administrator_required", status=403)
    return object_id
