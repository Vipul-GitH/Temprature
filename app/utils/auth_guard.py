from fastapi import Request
from fastapi.responses import RedirectResponse


def require_login(request: Request):
    """
    Ensure user is logged in. If not, redirect to /login.
    """
    username = request.session.get("username")
    if not username:
        return RedirectResponse(url="/login", status_code=302)
    return None


def require_it_role(request: Request):
    """
    Ensure user is IT/Admin. Others are redirected to /my-tickets.
    """
    role = request.session.get("role")
    if role not in ("Admin", "IT"):
        return RedirectResponse(url="/my-tickets", status_code=302)
    return None
