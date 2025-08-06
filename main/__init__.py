"""Initialize Flask app."""
from flask import Flask, render_template
import secrets

from main.incident.incident import incident_bp
from main.utils.config import Config
from main.utils.extensions import db, migrate, bcrypt, login_manager, mail
from redis import Redis
from flask_session import Session

redis_instance = None


def page_not_found(e):
    return render_template('error/404.html'), 404


def page_internal_server(e):
    return render_template('error/500.html'), 500


def page_unauthorized(e):
    return render_template('error/403.html'), 403


def create_app(config_class=Config):
    """Create Flask application."""
    secret = secrets.token_urlsafe(32)
    global redis_instance
    app = Flask(__name__)
    app.register_error_handler(404, page_not_found)
    app.register_error_handler(500, page_internal_server)
    app.register_error_handler(403, page_unauthorized)
    app.config.from_object(config_class)
    app.secret_key = secret
    app.config['SESSION_TYPE'] = 'filesystem'
    Session(app)
    app.debug = True

    # Initialize Redis
    redis_instance = Redis(host='localhost', port=6379, db=0)

    # Initialize Flask extensions here
    db.init_app(app)
    migrate.init_app(app, db)
    bcrypt.init_app(app)
    login_manager.init_app(app)
    login_manager.login_view = "user_bp.login"
    login_manager.login_message_category = "info"
    mail.init_app(app)

    with app.app_context():
        # Import parts of our application
        from .user_managment.controllers.users import users_routes

        # FH blueprints
        from .FH.controllers.fh_home import fh_home_routes
        from .FH.controllers.fh_mlo import fh_mlo_routes
        from .FH.controllers.fh_link_capacity import fh_link_capacity_routes
        from .FH.controllers.fh_pmon import fh_pmon_routes
        from .FH.controllers.api import fh_api_routes
        from .incident import incident

        # radio blueprints
        from .Radio.controllers.radio_home import radio_home_routes
        from .Radio.controllers.alarm_group import alarm_group_routes
        from .Radio.controllers.api import radio_api_routes
        from .Radio.controllers.file_upload import file_upload_routes
        from .Radio.controllers.map import map_routes
        from .Radio.controllers.congestion import congestion_routes
        from .Radio.controllers.battery import baterry_routes

        # Register Blueprints
        app.register_blueprint(users_routes.user_bp)

        # radio blueprints registration
        app.register_blueprint(fh_home_routes.fh_home_bp, url_prefix='/fh')
        app.register_blueprint(fh_mlo_routes.fh_mlo_bp, url_prefix='/fh')
        app.register_blueprint(fh_link_capacity_routes.fh_link_capacity_bp, url_prefix='/fh')
        app.register_blueprint(fh_pmon_routes.fh_pmon_bp, url_prefix='/fh')
        app.register_blueprint(fh_api_routes.fh_api_bp, url_prefix='/fh/api')

        # radio blueprints registration
        app.register_blueprint(radio_home_routes.radio_home_bp, url_prefix='/radio')
        app.register_blueprint(alarm_group_routes.alarm_group_bp, url_prefix='/radio')
        app.register_blueprint(radio_api_routes.radio_api_bp, url_prefix='/radio/api')
        app.register_blueprint(file_upload_routes.file_upload_bp, url_prefix='/radio')
        app.register_blueprint(map_routes.radio_map_bp, url_prefix='/radio')
        app.register_blueprint(congestion_routes.radio_congestion_bp, url_prefix='/radio')
        app.register_blueprint(baterry_routes.radio_battery_bp, url_prefix='/radio')
        app.register_blueprint(incident_bp, url_prefix='/incident')
        return app


def get_redis_instance():
    global redis_instance
    return redis_instance
