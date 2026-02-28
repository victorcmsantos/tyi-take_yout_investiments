from flask import Flask, request

from .api_routes import api_bp
from .chart_sync import start_chart_sync
from .db import init_app as init_db_app
from .fixed_income_sync import start_fixed_income_sync
from .market_sync import start_market_sync


def create_app() -> Flask:
    app = Flask(__name__)
    init_db_app(app)
    app.register_blueprint(api_bp, url_prefix="/api")
    start_market_sync(app)
    start_fixed_income_sync(app)
    start_chart_sync(app)

    @app.before_request
    def _ensure_options():
        if request.method == "OPTIONS":
            return "", 204

    @app.after_request
    def _add_api_cors_headers(response):
        if request.path.startswith("/api/"):
            response.headers["Access-Control-Allow-Origin"] = "*"
            response.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
            response.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, PATCH, DELETE, OPTIONS"
        return response

    return app
