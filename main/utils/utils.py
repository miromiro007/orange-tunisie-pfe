from functools import wraps
from flask import abort
from flask_login import current_user
from flask_mail import Message

from main.utils.extensions import mail, bcrypt


def role_required(role):
    def decorator(view_func):
        @wraps(view_func)
        def wrapper(*args, **kwargs):
            if not current_user.is_authenticated or current_user.role not in role:
                abort(403)  # Or redirect to an unauthorized page
            return view_func(*args, **kwargs)

        return wrapper

    return decorator


def send_email(sender, recipients, subject, message):
    msg = Message(subject, sender=sender, recipients=recipients)
    msg.body = message
    mail.send(msg)


def hash_password(password):
    return bcrypt.generate_password_hash(password).decode('utf-8')
