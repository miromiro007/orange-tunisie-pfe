import pytest

from main import create_app
from main.Radio.models.models import User
from main.user_managment.services.user_service import UserService


@pytest.fixture()
def app():
    app = create_app()
    return app


def test_get_user_by_email(app):
    with app.app_context():
        user_obj = UserService.get_user_by_email("username.lastname@gmail.com")
        assert user_obj is None


def test_add_new_user(app):
    with app.app_context():
        email = "username.lastname@gmail.com"
        new_user = User("username", "lastname", email, None, False)
        new_user.set_password("password")
        UserService.add_new_user(new_user)

        user_obj = UserService.get_user_by_email(email)
        assert user_obj is not None
        UserService.delete_user(email)


def test_update_user(app):
    with app.app_context():
        email = "username.lastname@gmail.com"
        new_user = User("username", "lastname", email, None, False)
        new_user.set_password("password")
        UserService.add_new_user(new_user)

        values = {'lastname': 'support_trans'}

        UserService.update_user(email, values)

        user_obj = UserService.get_user_by_email(email)
        assert user_obj.lastname == values['lastname']
        UserService.delete_user(email)
