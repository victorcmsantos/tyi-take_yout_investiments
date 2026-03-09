import os
import secrets
from datetime import datetime, timedelta, timezone
from pathlib import Path

from flask import current_app, g, has_request_context, session
from werkzeug.exceptions import Forbidden, Unauthorized
from werkzeug.security import check_password_hash, generate_password_hash

from .db import get_db
from .runtime_lock import exclusive_file_lock

USER_ROLE_ADMIN = "admin"
USER_ROLE_TRADER = "trader"
USER_ROLE_VIEWER = "viewer"
VALID_USER_ROLES = {USER_ROLE_ADMIN, USER_ROLE_TRADER, USER_ROLE_VIEWER}


def _now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _normalize_username(username: str):
    return (username or "").strip().lower()


def _secret_file_from_app():
    return Path(current_app.config["AUTH_SECRET_KEY_FILE"])


def _admin_bootstrap_file_from_app():
    return Path(current_app.config["ADMIN_BOOTSTRAP_FILE"])


def _initial_portfolio_name(username: str):
    return f"Carteira de {username}"


def _normalize_user_role(role, *, fallback: str = USER_ROLE_TRADER):
    value = str(role or "").strip().lower()
    if value in VALID_USER_ROLES:
        return value
    return fallback


def _resolve_user_role(role=None, *, is_admin: bool = False):
    if is_admin:
        return USER_ROLE_ADMIN
    return _normalize_user_role(role, fallback=USER_ROLE_TRADER)


def can_user_write(user: dict | None):
    if not user:
        return False
    role = _resolve_user_role(user.get("role"), is_admin=bool(user.get("is_admin")))
    return role != USER_ROLE_VIEWER


def _load_or_create_secret_key():
    configured = current_app.config.get("SECRET_KEY")
    if configured:
        return configured

    secret_file = _secret_file_from_app()
    if secret_file.exists():
        secret = secret_file.read_text(encoding="utf-8").strip()
        if secret:
            return secret

    secret_file.parent.mkdir(parents=True, exist_ok=True)
    secret = secrets.token_urlsafe(48)
    secret_file.write_text(secret, encoding="utf-8")
    return secret


def configure_auth(app):
    db_path = Path(app.config["DATABASE"])
    app.config.setdefault(
        "AUTH_SECRET_KEY_FILE",
        os.getenv("AUTH_SECRET_KEY_FILE", str(db_path.parent / ".flask-secret")),
    )
    app.config.setdefault(
        "ADMIN_BOOTSTRAP_FILE",
        os.getenv("ADMIN_BOOTSTRAP_FILE", str(db_path.parent / "admin-bootstrap.txt")),
    )
    app.config.setdefault("SESSION_COOKIE_HTTPONLY", True)
    app.config.setdefault("SESSION_COOKIE_SAMESITE", "Lax")
    app.config.setdefault("PERMANENT_SESSION_LIFETIME", timedelta(days=14))

    with exclusive_file_lock(app.config["DATABASE_STARTUP_LOCK_FILE"]):
        with app.app_context():
            app.secret_key = _load_or_create_secret_key()
            ensure_admin_user()


def ensure_admin_user():
    db = get_db()
    bootstrap_file = _admin_bootstrap_file_from_app()
    admin_row = db.execute(
        """
        SELECT id, username
        FROM users
        WHERE is_admin = 1
        ORDER BY id ASC
        LIMIT 1
        """
    ).fetchone()
    if admin_row:
        if bootstrap_file.exists():
            return {"created": False, "username": admin_row["username"]}

        password = secrets.token_urlsafe(12)
        now = _now_iso()
        db.execute(
            "UPDATE users SET password_hash = ?, role = ?, updated_at = ? WHERE id = ?",
            (generate_password_hash(password), USER_ROLE_ADMIN, now, int(admin_row["id"])),
        )
        db.commit()
        bootstrap_file.parent.mkdir(parents=True, exist_ok=True)
        bootstrap_file.write_text(
            f"username={admin_row['username']}\npassword={password}\ncreated_at={now}\n",
            encoding="utf-8",
        )
        current_app.logger.warning(
            "Senha do admin bootstrap foi regenerada. Consulte %s dentro do container.",
            bootstrap_file,
        )
        return {"created": False, "username": admin_row["username"], "password_reset": True}

    username = "admin"
    password = secrets.token_urlsafe(12)
    db.execute(
        """
        INSERT INTO users (
            username,
            password_hash,
            role,
            is_admin,
            is_active,
            created_at
        ) VALUES (?, ?, ?, 1, 1, ?)
        """,
        (username, generate_password_hash(password), USER_ROLE_ADMIN, _now_iso()),
    )
    db.commit()

    bootstrap_file.parent.mkdir(parents=True, exist_ok=True)
    bootstrap_file.write_text(
        f"username={username}\npassword={password}\ncreated_at={_now_iso()}\n",
        encoding="utf-8",
    )
    current_app.logger.warning(
        "Usuario admin bootstrap criado. Consulte %s dentro do container para obter a senha inicial.",
        bootstrap_file,
    )
    return {"created": True, "username": username, "bootstrap_file": str(bootstrap_file)}


def _user_row_to_dict(row):
    if not row:
        return None
    return {
        "id": int(row["id"]),
        "username": row["username"],
        "role": _resolve_user_role(row["role"], is_admin=bool(row["is_admin"])),
        "is_admin": bool(row["is_admin"]),
        "is_active": bool(row["is_active"]),
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
        "last_login_at": row["last_login_at"],
    }


def _create_initial_portfolio_for_user(user_id: int, portfolio_name: str = "Carteira Principal"):
    db = get_db()
    exists = db.execute(
        "SELECT id FROM portfolios WHERE user_id = ? AND LOWER(name) = LOWER(?)",
        (int(user_id), portfolio_name),
    ).fetchone()
    if exists:
        return
    db.execute(
        "INSERT INTO portfolios (user_id, name) VALUES (?, ?)",
        (int(user_id), portfolio_name),
    )
    db.commit()


def get_user_by_id(user_id):
    row = get_db().execute(
        """
        SELECT id, username, role, is_admin, is_active, created_at, updated_at, last_login_at
        FROM users
        WHERE id = ?
        """,
        (int(user_id),),
    ).fetchone()
    return _user_row_to_dict(row)


def get_current_user():
    if not has_request_context():
        return None
    cached = getattr(g, "current_user", None)
    if cached is not None:
        return cached

    user_id = session.get("user_id")
    if not user_id:
        g.current_user = None
        return None

    user = get_user_by_id(user_id)
    if not user or not user["is_active"]:
        session.clear()
        g.current_user = None
        return None

    g.current_user = user
    return user


def login_user(username: str, password: str):
    normalized = _normalize_username(username)
    if not normalized or not password:
        return None

    row = get_db().execute(
        """
        SELECT id, username, role, password_hash, is_admin, is_active, created_at, updated_at, last_login_at
        FROM users
        WHERE username = ?
        """,
        (normalized,),
    ).fetchone()
    if not row or not bool(row["is_active"]):
        return None
    if not check_password_hash(row["password_hash"], password):
        return None

    now = _now_iso()
    get_db().execute("UPDATE users SET last_login_at = ?, updated_at = ? WHERE id = ?", (now, now, int(row["id"])))
    get_db().commit()

    session.clear()
    session.permanent = True
    session["user_id"] = int(row["id"])

    user = _user_row_to_dict(row)
    user["last_login_at"] = now
    g.current_user = user
    return user


def logout_current_user():
    session.clear()
    g.current_user = None


def require_authenticated_user():
    user = get_current_user()
    if not user:
        raise Unauthorized("Autenticacao necessaria.")
    return user


def require_admin_user():
    user = require_authenticated_user()
    role = _resolve_user_role(user.get("role"), is_admin=bool(user.get("is_admin")))
    if role != USER_ROLE_ADMIN:
        raise Forbidden("Acesso restrito a administradores.")
    return user


def is_auth_exempt_path(path: str):
    return path in {
        "/api/health",
        "/api/auth/login",
        "/api/auth/me",
    }


def is_viewer_write_exempt_path(path: str):
    return path in {
        "/api/auth/logout",
    }


def list_users():
    rows = get_db().execute(
        """
        SELECT id, username, role, is_admin, is_active, created_at, updated_at, last_login_at
        FROM users
        ORDER BY username ASC
        """
    ).fetchall()
    return [_user_row_to_dict(row) for row in rows]


def create_user_account(username: str, password: str, is_admin: bool = False, role: str | None = None):
    normalized = _normalize_username(username)
    if not normalized:
        return False, "Nome de usuario obrigatorio.", None
    if len(normalized) < 3:
        return False, "Nome de usuario deve ter ao menos 3 caracteres.", None
    if not password or len(password) < 8:
        return False, "Senha deve ter ao menos 8 caracteres.", None

    resolved_role = _resolve_user_role(role, is_admin=is_admin)
    is_admin_flag = resolved_role == USER_ROLE_ADMIN

    db = get_db()
    exists = db.execute("SELECT id FROM users WHERE username = ?", (normalized,)).fetchone()
    if exists:
        return False, "Ja existe um usuario com esse nome.", None

    now = _now_iso()
    cursor = db.execute(
        """
        INSERT INTO users (
            username,
            password_hash,
            role,
            is_admin,
            is_active,
            created_at,
            updated_at
        ) VALUES (?, ?, ?, ?, 1, ?, ?)
        """,
        (
            normalized,
            generate_password_hash(password),
            resolved_role,
            1 if is_admin_flag else 0,
            now,
            now,
        ),
    )
    db.commit()
    created_user = get_user_by_id(cursor.lastrowid)
    if created_user and _resolve_user_role(created_user.get("role"), is_admin=bool(created_user.get("is_admin"))) != USER_ROLE_ADMIN:
        _create_initial_portfolio_for_user(
            created_user["id"],
            _initial_portfolio_name(created_user["username"]),
        )
    return True, "Usuario criado com sucesso.", get_user_by_id(cursor.lastrowid)


def set_user_active_state(user_id: int, is_active: bool, acting_user_id: int | None = None):
    db = get_db()
    row = db.execute(
        """
        SELECT id, username, role, is_admin, is_active, created_at, updated_at, last_login_at
        FROM users
        WHERE id = ?
        """,
        (int(user_id),),
    ).fetchone()
    if not row:
        return False, "Usuario nao encontrado.", None

    target = _user_row_to_dict(row)
    desired = bool(is_active)
    if target["is_active"] == desired:
        return True, "Usuario ja estava nesse estado.", target

    if not desired:
        if acting_user_id and int(target["id"]) == int(acting_user_id):
            return False, "Nao e permitido desabilitar o proprio usuario.", None
        if target["is_admin"]:
            active_admins = db.execute(
                "SELECT COUNT(*) AS total FROM users WHERE is_admin = 1 AND is_active = 1"
            ).fetchone()
            if active_admins and int(active_admins["total"]) <= 1:
                return False, "Nao e permitido desabilitar o ultimo administrador ativo.", None

    now = _now_iso()
    db.execute(
        "UPDATE users SET is_active = ?, updated_at = ? WHERE id = ?",
        (1 if desired else 0, now, int(user_id)),
    )
    db.commit()
    return True, "Usuario atualizado com sucesso.", get_user_by_id(user_id)


def set_user_role(user_id: int, role: str, acting_user_id: int | None = None):
    normalized_role = _normalize_user_role(role, fallback="")
    if normalized_role not in VALID_USER_ROLES:
        return False, "Perfil invalido. Use admin, trader ou viewer.", None

    db = get_db()
    row = db.execute(
        """
        SELECT id, username, role, is_admin, is_active, created_at, updated_at, last_login_at
        FROM users
        WHERE id = ?
        """,
        (int(user_id),),
    ).fetchone()
    if not row:
        return False, "Usuario nao encontrado.", None

    target = _user_row_to_dict(row)
    current_role = _resolve_user_role(target.get("role"), is_admin=bool(target.get("is_admin")))
    if current_role == normalized_role:
        return True, "Usuario ja possui esse perfil.", target

    # Evita remover o ultimo admin ativo do sistema.
    if current_role == USER_ROLE_ADMIN and normalized_role != USER_ROLE_ADMIN and bool(target.get("is_active")):
        active_admins = db.execute(
            "SELECT COUNT(*) AS total FROM users WHERE role = 'admin' AND is_active = 1"
        ).fetchone()
        if active_admins and int(active_admins["total"]) <= 1:
            return False, "Nao e permitido remover o perfil do ultimo administrador ativo.", None

    # Impede que o proprio usuario em sessao se remova de admin acidentalmente.
    if (
        acting_user_id
        and int(target["id"]) == int(acting_user_id)
        and current_role == USER_ROLE_ADMIN
        and normalized_role != USER_ROLE_ADMIN
    ):
        return False, "Nao e permitido remover o proprio perfil de administrador.", None

    now = _now_iso()
    db.execute(
        "UPDATE users SET role = ?, is_admin = ?, updated_at = ? WHERE id = ?",
        (
            normalized_role,
            1 if normalized_role == USER_ROLE_ADMIN else 0,
            now,
            int(user_id),
        ),
    )
    db.commit()
    return True, "Perfil de usuario atualizado com sucesso.", get_user_by_id(user_id)
