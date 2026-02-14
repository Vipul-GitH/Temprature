import logging
import os
import sys
import threading
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo

import requests
from sqlalchemy import Column, DateTime, Integer, String, Text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import declarative_base

from app.database import SecondarySessionLocal

WHATSAPP_API_URL = os.getenv("WHATSAPP_API_URL", "http://192.168.0.71:3004/api/messages/send")
WHATSAPP_ACCOUNT_ID = os.getenv("WHATSAPP_ACCOUNT_ID", "1")
WHATSAPP_TARGET = os.getenv("WHATSAPP_TARGET", "")

logger = logging.getLogger(__name__)

if not logger.handlers:
    log_format = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    logging.basicConfig(level=logging.INFO, format=log_format)
    logs_dir = Path("logs")
    logs_dir.mkdir(exist_ok=True)
    file_handler = logging.FileHandler(logs_dir / "infra_alerts.log", encoding="utf-8")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter(log_format))
    logger.addHandler(file_handler)

INDIA_ZONE = ZoneInfo("Asia/Kolkata")

router_name = "infra"  # future router placeholder

BLOCKED_TICKET_STATUSES = {"new", "in progress", "reject", "rejected"}

InfraBase = declarative_base()


class InfraTicket(InfraBase):
    __tablename__ = "infra_tickets"

    ticket_id = Column(Integer, primary_key=True, autoincrement=True)
    created_by = Column(String(120), nullable=False)
    department = Column(String(120), nullable=True)
    category = Column(String(80), nullable=False)
    subcategory = Column(String(80), nullable=False)
    workstation = Column(String(120), nullable=True, index=True)
    description = Column(Text, nullable=False)
    status = Column(String(40), nullable=False, default="New")
    image_path = Column(String(1024), nullable=True)
    # store timestamps in Indian time zone so infra team can read them directly
    created_at = Column(
        DateTime,
        nullable=False,
        default=lambda: datetime.now(INDIA_ZONE).replace(tzinfo=None),
    )
    # DB schema now requires is_delayed_pick flag, default to 0 (False)
    is_delayed_pick = Column(Integer, nullable=False, default=0, server_default="0")
    # Some installs also expect an is_invalid flag for workflow routing
    is_invalid = Column(Integer, nullable=False, default=0, server_default="0")


DEFAULT_INFRA_USER = os.getenv("INFRA_DEFAULT_USER", "IT Team")
DEFAULT_INFRA_DEPARTMENT = os.getenv("INFRA_DEFAULT_DEPARTMENT", "Operations")
STALE_CATEGORY = "software"
STALE_SUBCATEGORY = "sensor"
STALE_STATUS = "New"
INFRA_TICKETS_ENABLED = os.getenv("INFRA_TICKETS_ENABLED", "false").lower() not in {"0", "false", "no"}


def send_whatsapp_to_number(target: str | None, message: str):
    """Send WhatsApp message via internal API."""
    req_id = str(uuid.uuid4())[:8]
    target = target or WHATSAPP_TARGET
    if not target:
        logger.error("WA[%s] missing target; skipping send", req_id)
        return 400, "missing target"
    try:
        payload = {
            "accountId": int(WHATSAPP_ACCOUNT_ID) if str(WHATSAPP_ACCOUNT_ID).isdigit() else WHATSAPP_ACCOUNT_ID,
            "target": target,
            "message": message or "",
        }
        logger.info(
            "WA[%s] -> POST %s | target=%s | len=%s | accountId=%s",
            req_id,
            WHATSAPP_API_URL,
            target,
            len(message or ""),
            payload["accountId"],
        )
        r = requests.post(WHATSAPP_API_URL, json=payload, timeout=10)
        logger.info(
            "WA[%s] <- status=%s | body=%s",
            req_id,
            r.status_code,
            (r.text[:500] if r.text else ""),
        )
        return r.status_code, r.text
    except Exception as exc:
        logger.exception("WA[%s] send_whatsapp_to_number exception: %s", req_id, exc)
        return 500, str(exc)


def _build_ticket_message(ticket: InfraTicket) -> str:
    created_at = getattr(ticket, "created_at", None)
    created_str = (
        created_at.strftime("%d-%b-%Y %I:%M %p")
        if created_at
        else datetime.now().strftime("%d-%b-%Y %I:%M %p")
    )
    lines = [
        "*New Infra Ticket Created*",
        f"*Ticket:* #{ticket.ticket_id}",
        f"*Created By:* {ticket.created_by or '-'}",
        f"*Department:* {ticket.department or '-'}",
        f"*Category:* {ticket.category or '-'} / {ticket.subcategory or '-'}",
        f"*Workstation:* {ticket.workstation or '-'}",
        f"*Status:* {ticket.status}",
        f"*Description:* {ticket.description or '-'}",
        f"_Created At:_ {created_str}",
    ]
    return "\n".join(lines)


def notify_new_ticket_async(ticket: InfraTicket):
    if not WHATSAPP_API_URL:
        logger.warning("WA notify skipped: WHATSAPP_API_URL missing")
        return
    if not (WHATSAPP_TARGET or WHATSAPP_TARGET == "") and not getattr(ticket, "workstation", None):
        logger.warning("WA notify skipped: WHATSAPP_TARGET missing")
        return

    def _worker():
        try:
            msg = _build_ticket_message(ticket)
            logger.info(
                "InfraAlert -> sending WA for ticket_id=%s | msg_len=%s | target=%s",
                ticket.ticket_id,
                len(msg),
                WHATSAPP_TARGET or ticket.workstation or "-",
            )
            status, resp = send_whatsapp_to_number(WHATSAPP_TARGET or ticket.workstation, msg)
            if status in (200, 201):
                logger.info("InfraAlert sent | status=%s | resp=%s", status, (resp[:300] if resp else ""))
            else:
                logger.error("InfraAlert failed | status=%s | resp=%s", status, (resp[:500] if resp else ""))
        except Exception as exc:
            logger.exception("InfraAlert exception: %s", exc)

    threading.Thread(target=_worker, daemon=True).start()


def create_stale_sensor_ticket(
    workstation: str,
    description: str,
    *,
    created_by: str = DEFAULT_INFRA_USER,
    department: str = DEFAULT_INFRA_DEPARTMENT,
    category: str = STALE_CATEGORY,
    subcategory: str = STALE_SUBCATEGORY,
) -> Optional[InfraTicket]:
    """Insert a sensor ticket for a stale room but skip when an unresolved ticket already exists."""
    if not INFRA_TICKETS_ENABLED:
        logger.debug("[infra] tickets disabled; skipping insert for %s", workstation)
        return None
    if not workstation or not description:
        return None
    try:
        with SecondarySessionLocal() as session:
            existing = (
                session.query(InfraTicket)
                .filter(
                    InfraTicket.workstation == workstation,
                    InfraTicket.category == category,
                    InfraTicket.subcategory == subcategory,
                )
                .order_by(InfraTicket.created_at.desc())
                .first()
            )
            if existing:
                status = (existing.status or "").strip().lower()
                if status in BLOCKED_TICKET_STATUSES:
                    logger.debug(
                        "[infra] blocking ticket %s with status '%s'",
                        existing.ticket_id,
                        status,
                    )
                    return existing
            unresolved_match = (
                session.query(InfraTicket)
                .filter(
                    InfraTicket.workstation == workstation,
                    InfraTicket.description == description,
                )
                .order_by(InfraTicket.created_at.desc())
                .first()
            )
            if unresolved_match:
                existing_status = (unresolved_match.status or "").strip().lower()
                if existing_status in BLOCKED_TICKET_STATUSES:
                    logger.debug(
                        "[infra] found unresolved matching ticket %s, skipping",
                        unresolved_match.ticket_id,
                    )
                    return unresolved_match
            logger.debug("[infra] creating ticket for %s", workstation)
            ticket = InfraTicket(
                created_by=created_by,
                department=department,
                category=category,
                subcategory=subcategory,
                workstation=workstation,
                description=description,
                status=STALE_STATUS,
            )
            session.add(ticket)
            session.commit()
            session.refresh(ticket)
            created_at_local = ticket.created_at.replace(tzinfo=INDIA_ZONE)
            ct = created_at_local.strftime("%Y-%m-%d %H:%M:%S %Z")
            logger.info(
                "[infra] SUCCESS: created ticket %s for %s at %s (Asia/Kolkata)",
                ticket.ticket_id,
                workstation,
                ct,
            )
            notify_new_ticket_async(ticket)
            return ticket
    except SQLAlchemyError:
        err = sys.exc_info()[1]
        logger.exception(
            "[infra] ERROR: failed to create ticket for %s: %s",
            workstation,
            err,
        )
        return None
