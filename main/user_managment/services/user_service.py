import pandas as pd

from main import db
from main.Radio.models.models import User


class UserService:

    @staticmethod
    def add_new_user(user):
        try:
            db.session.add(user)
            db.session.commit()
            return True
        except (Exception,):
            raise Exception("Impossible d'ajouter l'utilisateur' !")

    @staticmethod
    def add_default_admin_user():
        email = "transmissionradiofh@gmail.com"
        user_obj = UserService.get_user_by_email(email)
        if not user_obj:
            new_user = User("admin", "admin_trans", email, "ADMIN", True)
            new_user.set_password("admin")
            db.session.add(new_user)
            db.session.commit()

    @staticmethod
    def get_user_by_id(user_id):
        return User.query.filter_by(id=user_id).first()

    @staticmethod
    def get_user_by_email(email):
        return User.query.filter_by(email=email).first()

    @staticmethod
    def delete_user(email):
        user_obj = UserService.get_user_by_email(email)
        if user_obj:
            user_record = User.__table__.delete().where(User.email == email)
            db.session.execute(user_record)
            db.session.commit()
            return True
        else:
            raise Exception("L'utilisateur n'existe pas")

    @staticmethod
    def get_users():
        data = db.session.query(
            User.id,
            User.lastname,
            User.username,
            User.role,
            User.email,
            User.is_approved,
            User.created_at,
        )

        df = pd.DataFrame(
            [
                (
                    d.id,
                    d.lastname,
                    d.username,
                    d.email,
                    d.role,
                    d.is_approved,
                    d.created_at,
                )
                for d in data
            ],
            columns=[
                "ID",
                "Nom",
                "Prenom",
                "Email",
                "Role",
                "Status",
                "Date Creation",
            ],
        )
        return df

    @staticmethod
    def update_user(email, dict_values=None):
        if dict_values is None:
            dict_values = {}
        query = User.query.filter_by(email=email)
        query.update(dict_values)
        db.session.commit()
