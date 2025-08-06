from datetime import datetime

from flask_login import UserMixin

from main.utils.extensions import db, bcrypt, login_manager


class AlarmRadio(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    save_time = db.Column(db.DateTime, nullable=False, index=True, unique=False)
    comments = db.Column(db.String(256), nullable=True)
    severity = db.Column(db.String(256), nullable=False)
    name = db.Column(db.String(256), nullable=False)
    last_occurred = db.Column(db.DateTime, nullable=False)
    cleared_on = db.Column(db.String(256))
    location_info = db.Column(db.Text, nullable=False)
    ne_type = db.Column(db.String(256), nullable=False)
    alarm_source = db.Column(db.String(256), nullable=False)
    mo_name = db.Column(db.String(256), nullable=False)
    occurrence_times = db.Column(db.Integer, nullable=False)
    first_occurred = db.Column(db.DateTime, nullable=False)
    alarm_id = db.Column(db.Integer, nullable=False)
    acknowledged_on = db.Column(db.String(256))
    cleared_by = db.Column(db.String(256))
    acknowledged_by = db.Column(db.String(256))
    clearance_status = db.Column(db.String(256), nullable=False, index=True, unique=False)
    acknowledgement_status = db.Column(db.String(256))
    home_subnet = db.Column(db.String(256), nullable=True)
    alarm_duration = db.Column(db.String(256), nullable=True)


class CongestionRadio(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    time = db.Column(db.DateTime, nullable=False, unique=False)
    e_node_b_name = db.Column(db.String(256), nullable=True)
    integrity = db.Column(db.Float, nullable=False)
    max_speed_mbs = db.Column(db.Float, nullable=False)
    end_date = db.Column(db.DateTime, nullable=False, index=True, unique=False)


class RSL_Level(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    ip = db.Column(db.String(256), nullable=False)
    slot = db.Column(db.Integer, nullable=False)
    min_rsl = db.Column(db.Float, nullable=False)
    avg_rsl = db.Column(db.Float, nullable=False)
    max_rsl = db.Column(db.Float, nullable=False)
    name = db.Column(db.String(256), nullable=False, index=True)
    file = db.Column(db.String(256), nullable=False)
    ref_rsl = db.Column(db.Float, nullable=False)
    enda_name = db.Column(db.String(256), nullable=False)
    enda_latitude = db.Column(db.Float, nullable=False)
    enda_longitude = db.Column(db.Float, nullable=False)
    endb_name = db.Column(db.String(256), nullable=False)
    endb_latitude = db.Column(db.Float, nullable=False)
    endb_longitude = db.Column(db.Float, nullable=False)
    link_status = db.Column(db.String(256), nullable=False)
    comment = db.Column(db.String(256), nullable=True)
    creation_date = db.Column(db.DateTime, nullable=False, index=True, unique=False)


class NW_LOAD_DISTRIBUTION(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    ip = db.Column(db.String(256), nullable=False)
    slot = db.Column(db.String(16), nullable=False)
    avg_daily_tx_load = db.Column(db.Float, nullable=False)
    max_daily_tx_load = db.Column(db.Float, nullable=False)
    avg_daily_rx_load = db.Column(db.Float, nullable=False)
    max_daily_rx_load = db.Column(db.Float, nullable=False)
    name = db.Column(db.String(256), nullable=False)
    creation_date = db.Column(db.DateTime, nullable=False, index=True, unique=False)


class PMON(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    ip = db.Column(db.String(256), nullable=False)
    slot = db.Column(db.Integer, nullable=False)
    sanity = db.Column(db.String(256), nullable=False)
    mod_ref = db.Column(db.String(256), nullable=False)
    mod_min = db.Column(db.String(256), nullable=False)
    uas = db.Column(db.Integer, nullable=False)
    sep = db.Column(db.Integer, nullable=False)
    ses = db.Column(db.Integer, nullable=False)
    es = db.Column(db.Integer, nullable=False)
    bbe = db.Column(db.Integer, nullable=False)
    ofs = db.Column(db.Integer, nullable=False)
    rsl_min = db.Column(db.Float, nullable=True)
    rsl_max = db.Column(db.Float, nullable=True)
    rsl_avg = db.Column(db.Float, nullable=True)
    link_status = db.Column(db.String(256), nullable=True)
    comment = db.Column(db.String(256), nullable=True)
    creation_date = db.Column(db.DateTime, nullable=False, index=True, unique=False)
    is_high_value = db.Column(db.String(256), nullable=True)


@login_manager.user_loader
def load_user(user_id):
    return User.query.get(int(user_id))


class User(db.Model, UserMixin):
    id = db.Column(db.Integer, primary_key=True)
    lastname = db.Column(db.String(60), nullable=False)
    username = db.Column(db.String(60), nullable=False)
    email = db.Column(db.String(60), nullable=False)
    password_hash = db.Column(db.String(256), nullable=False)
    role = db.Column(db.String(60), nullable=True)
    is_approved = db.Column(db.Boolean, default=False)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

    def __init__(self, lastname, username, email, role, is_approved):
        self.lastname = lastname
        self.username = username
        self.email = email
        self.role = role
        self.is_approved = is_approved

    @property
    def password(self):
        raise AttributeError("Password is not readable.")

    def set_password(self, plain_text_password):
        self.password_hash = bcrypt.generate_password_hash(plain_text_password).decode('utf-8')

    def check_password_correction(self, attempted_password):
        return bcrypt.check_password_hash(self.password_hash, attempted_password)


class Tokens(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(60), nullable=False)
    code = db.Column(db.Integer, nullable=False)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

    def __init__(self, email, code):
        self.email = email
        self.code = code


class Battery(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(256), nullable=True)
    remaining_capacity = db.Column(db.Float, nullable=True)
    remaining_time = db.Column(db.String(256), nullable=True)
    power_cut_times = db.Column(db.String(256), nullable=True)
    creation_date = db.Column(db.DateTime, nullable=False, index=True, unique=False)
