from datetime import datetime

from main.Radio.models.models import Tokens
from main.utils.extensions import db


class TokenService:

    @staticmethod
    def add_new_token(token):
        try:
            db.session.add(token)
            db.session.commit()
            return True
        except (Exception,):
            raise Exception("Impossible d'ajouter le token' !")

    @staticmethod
    def get_token_by_email(email):
        return Tokens.query.filter_by(email=email).first()

    @staticmethod
    def get_token_by_code(code):
        return Tokens.query.filter_by(code=code).first()

    @staticmethod
    def delete_token(email):
        token_obj = TokenService.get_token_by_email(email)
        if token_obj:
            tokens_record = Tokens.__table__.delete().where(Tokens.email == email)
            db.session.execute(tokens_record)
            db.session.commit()
            return True
        else:
            raise Exception("Le token n'existe pas")

    @staticmethod
    def update_token(email, code=None):
        query = Tokens.query.filter_by(email=email)
        query.update({'code': code, 'created_at': datetime.now()})
        db.session.commit()
