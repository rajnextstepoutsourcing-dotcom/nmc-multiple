"""
db.py — Shared database connection for NMC tool
Connects to the same PostgreSQL as the main NextStep backend.
Records jobs, items, and usage to the central database.
"""

import os
import logging
from datetime import datetime
from typing import Optional
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session

log = logging.getLogger(__name__)

# ── Database connection ───────────────────────────────────────────────────────
_DATABASE_URL = os.environ.get("DATABASE_URL", "")

# Fix Render postgres:// → postgresql://
if _DATABASE_URL.startswith("postgres://"):
    _DATABASE_URL = _DATABASE_URL.replace("postgres://", "postgresql://", 1)

_engine = None
_SessionLocal = None

def get_engine():
    global _engine
    if _engine is None and _DATABASE_URL:
        try:
            _engine = create_engine(
                _DATABASE_URL,
                pool_pre_ping=True,
                pool_size=3,
                max_overflow=5,
            )
            log.info("[DB] Connected to central database")
        except Exception as e:
            log.error("[DB] Failed to connect: %s", e)
    return _engine

def get_session() -> Optional[Session]:
    global _SessionLocal
    engine = get_engine()
    if engine is None:
        return None
    if _SessionLocal is None:
        _SessionLocal = sessionmaker(bind=engine)
    return _SessionLocal()


# ── Tool ID cache ─────────────────────────────────────────────────────────────
_TOOL_ID: Optional[int] = None

def get_nmc_tool_id() -> Optional[int]:
    global _TOOL_ID
    if _TOOL_ID is not None:
        return _TOOL_ID
    session = get_session()
    if not session:
        return None
    try:
        result = session.execute(
            text("SELECT id FROM tools WHERE slug = 'nmc' LIMIT 1")
        ).fetchone()
        if result:
            _TOOL_ID = result[0]
        return _TOOL_ID
    except Exception as e:
        log.error("[DB] get_nmc_tool_id error: %s", e)
        return None
    finally:
        session.close()


# ── Tenant token check ────────────────────────────────────────────────────────
def get_tenant_tokens_remaining(tenant_id: int) -> int:
    """Returns how many tokens the tenant has remaining. -1 if DB unavailable."""
    session = get_session()
    if not session:
        return -1
    try:
        result = session.execute(
            text("SELECT tokens_total, tokens_used FROM tenants WHERE id = :tid"),
            {"tid": tenant_id}
        ).fetchone()
        if not result:
            return 0
        return max(0, (result[0] or 0) - (result[1] or 0))
    except Exception as e:
        log.error("[DB] get_tenant_tokens_remaining error: %s", e)
        return -1
    finally:
        session.close()


# ── Create job record ─────────────────────────────────────────────────────────
def create_job_record(
    *,
    tenant_id: int,
    user_id: int,
    total_items: int,
) -> Optional[int]:
    """Creates a job record in the central DB. Returns job_id or None."""
    session = get_session()
    if not session:
        return None
    try:
        tool_id = get_nmc_tool_id()
        result = session.execute(
            text("""
                INSERT INTO jobs (tenant_id, user_id, tool_id, status, total_items,
                                  successful_items, failed_items, created_at)
                VALUES (:tenant_id, :user_id, :tool_id, 'queued', :total_items, 0, 0, :now)
                RETURNING id
            """),
            {
                "tenant_id": tenant_id,
                "user_id": user_id,
                "tool_id": tool_id,
                "total_items": total_items,
                "now": datetime.utcnow(),
            }
        )
        session.commit()
        row = result.fetchone()
        return row[0] if row else None
    except Exception as e:
        log.error("[DB] create_job_record error: %s", e)
        session.rollback()
        return None
    finally:
        session.close()


# ── Update job status ─────────────────────────────────────────────────────────
def update_job_status(
    *,
    db_job_id: int,
    status: str,
    successful_items: int,
    failed_items: int,
) -> None:
    session = get_session()
    if not session:
        return
    try:
        session.execute(
            text("""
                UPDATE jobs
                SET status = :status,
                    successful_items = :successful,
                    failed_items = :failed,
                    completed_at = :now
                WHERE id = :job_id
            """),
            {
                "status": status,
                "successful": successful_items,
                "failed": failed_items,
                "now": datetime.utcnow() if status in ("completed", "failed") else None,
                "job_id": db_job_id,
            }
        )
        session.commit()
    except Exception as e:
        log.error("[DB] update_job_status error: %s", e)
        session.rollback()
    finally:
        session.close()


# ── Record usage (deduct tokens) ──────────────────────────────────────────────
def record_usage(
    *,
    tenant_id: int,
    user_id: int,
    db_job_id: Optional[int],
    successful_outputs: int,
) -> None:
    """Records token usage and deducts from tenant balance."""
    if successful_outputs <= 0:
        return
    session = get_session()
    if not session:
        return
    try:
        tool_id = get_nmc_tool_id()

        # Insert usage record
        session.execute(
            text("""
                INSERT INTO usage_records
                    (tenant_id, user_id, tool_id, job_id, billable_output_count, created_at)
                VALUES
                    (:tenant_id, :user_id, :tool_id, :job_id, :count, :now)
            """),
            {
                "tenant_id": tenant_id,
                "user_id": user_id,
                "tool_id": tool_id,
                "job_id": db_job_id,
                "count": successful_outputs,
                "now": datetime.utcnow(),
            }
        )

        # Deduct tokens from tenant
        session.execute(
            text("""
                UPDATE tenants
                SET tokens_used = tokens_used + :count
                WHERE id = :tenant_id
            """),
            {"count": successful_outputs, "tenant_id": tenant_id}
        )

        session.commit()
        log.info("[DB] Recorded %d token(s) for tenant %d", successful_outputs, tenant_id)
    except Exception as e:
        log.error("[DB] record_usage error: %s", e)
        session.rollback()
    finally:
        session.close()


# ── Validate internal token ───────────────────────────────────────────────────


def adjust_usage(
    *,
    tenant_id: int,
    user_id: int,
    db_job_id: Optional[int],
    delta_outputs: int,
) -> None:
    """Adjusts token usage up or down. Negative deltas reverse prior billable usage."""
    if delta_outputs == 0:
        return
    session = get_session()
    if not session:
        return
    try:
        tool_id = get_nmc_tool_id()
        session.execute(
            text("""
                INSERT INTO usage_records
                    (tenant_id, user_id, tool_id, job_id, billable_output_count, created_at)
                VALUES
                    (:tenant_id, :user_id, :tool_id, :job_id, :count, :now)
            """),
            {
                "tenant_id": tenant_id,
                "user_id": user_id,
                "tool_id": tool_id,
                "job_id": db_job_id,
                "count": delta_outputs,
                "now": datetime.utcnow(),
            }
        )
        session.execute(
            text("""
                UPDATE tenants
                SET tokens_used = GREATEST(tokens_used + :count, 0)
                WHERE id = :tenant_id
            """),
            {"count": delta_outputs, "tenant_id": tenant_id}
        )
        session.commit()
        log.info("[DB] Adjusted %d token(s) for tenant %d", delta_outputs, tenant_id)
    except Exception as e:
        log.error("[DB] adjust_usage error: %s", e)
        session.rollback()
    finally:
        session.close()


def validate_user_token(token: str) -> Optional[dict]:
    """
    Validates the NextStep session token passed from the main dashboard.
    Returns dict with tenant_id, user_id, role or None if invalid.
    """
    if not token:
        return None
    session = get_session()
    if not session:
        return None
    try:
        result = session.execute(
            text("""
                SELECT u.id, u.tenant_id, u.role, u.active, t.status
                FROM users u
                JOIN tenants t ON t.id = u.tenant_id
                WHERE u.id = (
                    SELECT user_id FROM user_sessions WHERE token = :token
                    AND expires_at > :now LIMIT 1
                )
            """),
            {"token": token, "now": datetime.utcnow()}
        ).fetchone()

        if not result:
            return None

        user_id, tenant_id, role, active, tenant_status = result

        if not active or tenant_status != "active":
            return None

        return {
            "user_id": user_id,
            "tenant_id": tenant_id,
            "role": role,
        }
    except Exception as e:
        log.error("[DB] validate_user_token error: %s", e)
        return None
    finally:
        session.close()
