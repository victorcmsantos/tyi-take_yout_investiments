from flask import Flask, g, request

from .api_routes import api_bp
from .auth import can_user_write, configure_auth, get_current_user, is_auth_exempt_path, is_viewer_write_exempt_path
from .chart_sync import start_chart_sync
from .db import init_app as init_db_app
from .fixed_income_sync import start_fixed_income_sync
from .market_sync import start_market_sync
from .observability import configure_observability
from .upcoming_income_sync import start_upcoming_income_sync


def create_app() -> Flask:
    app = Flask(__name__)
    configure_observability(app)
    init_db_app(app)
    configure_auth(app)
    app.register_blueprint(api_bp, url_prefix="/api")
    start_market_sync(app)
    start_fixed_income_sync(app)
    start_chart_sync(app)
    start_upcoming_income_sync(app)

    @app.before_request
    def _ensure_options():
        if request.method == "OPTIONS":
            return "", 204
        g.current_user = None
        if request.path.startswith("/api/") and not is_auth_exempt_path(request.path):
            user = get_current_user()
            if not user:
                return {"ok": False, "error": "Autenticacao necessaria."}, 401
            if (
                request.method in {"POST", "PUT", "PATCH", "DELETE"}
                and not is_viewer_write_exempt_path(request.path)
                and not can_user_write(user)
            ):
                return {"ok": False, "error": "Perfil viewer possui acesso somente leitura."}, 403

    @app.after_request
    def _add_api_cors_headers(response):
        if request.path.startswith("/api/"):
            response.headers["Access-Control-Allow-Origin"] = "*"
            response.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
            response.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, PATCH, DELETE, OPTIONS"
        return response

    return app
