from datetime import datetime

from flask import Flask

from .db import init_app as init_db_app
from .market_sync import start_market_sync, trigger_market_sync_if_due
from .routes import main_bp


def create_app() -> Flask:
    app = Flask(__name__)
    init_db_app(app)
    app.register_blueprint(main_bp)
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
        trigger_market_sync_if_due(app)

    return app
