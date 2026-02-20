from datetime import datetime

from flask import Flask, request

from .api_routes import api_bp
from .db import init_app as init_db_app
from .market_sync import start_market_sync, trigger_market_sync_if_due
from .routes import main_bp


def create_app() -> Flask:
    app = Flask(__name__)
    init_db_app(app)
    app.register_blueprint(main_bp)
    app.register_blueprint(api_bp, url_prefix="/api")
    start_market_sync(app)

    @app.template_filter("date_br")
    def date_br(value):
        raw = (value or "").strip()
        if not raw:
            return ""
        for fmt in ("%Y-%m-%d", "%d/%m/%Y"):
            try:
                parsed = datetime.strptime(raw[:10], fmt)
                return parsed.strftime("%d/%m/%Y")
            except ValueError:
                continue
        return raw

    @app.before_request
    def _ensure_market_sync():
        if request.method == "OPTIONS":
            return "", 204
        endpoint = request.endpoint or ""
        if endpoint == "static":
            return
        trigger_market_sync_if_due(app, blocking=True)

    @app.after_request
    def _add_api_cors_headers(response):
        if request.path.startswith("/api/"):
            response.headers["Access-Control-Allow-Origin"] = "*"
            response.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
            response.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, PATCH, DELETE, OPTIONS"
        return response

    return app
