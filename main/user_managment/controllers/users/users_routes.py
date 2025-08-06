import random

from flask import Blueprint, render_template, request, flash, redirect, url_for, current_app, session
from flask_login import login_user, logout_user, login_required, current_user
from datetime import datetime, timedelta
from main.Radio.models.models import User, Tokens
from main.user_managment.services.token_service import TokenService
from main.utils.utils import role_required, send_email, hash_password
from main.user_managment.services.user_service import UserService

user_bp = Blueprint(
    'user_bp', __name__,
    template_folder='templates',
    static_folder='static'
)


@user_bp.route('/')
@user_bp.route('/login', methods=["GET", "POST"])
def login():
    if request.method == "POST":
        email = request.form['email']
        password = request.form['password']
        attempted_user = UserService.get_user_by_email(email)
        if attempted_user and attempted_user.check_password_correction(attempted_password=password):
            if not attempted_user.is_approved:
                flash(f"votre compte est en attente d'activation", category='info')
            else:
                login_user(attempted_user)

                if attempted_user.role == "USER_FH":
                    return redirect(url_for('fh_home_bp.home'))
                elif attempted_user.role == "USER_RADIO":
                    return redirect(url_for('radio_home_bp.home'))
                elif attempted_user.role == "USER_FH_RADIO" or attempted_user.role == "ADMIN":
                    return redirect(url_for('user_bp.menu_fh_fo'))
        else:
            flash(f'login ou mot de passe incorrect: ', category='info')
    return render_template("users/login.html")


@user_bp.route('/menu')
@login_required
@role_required(['USER_FH_RADIO', 'ADMIN'])
def menu_fh_fo():
    return render_template("users/menu_intermediate.html")


@user_bp.route('/register', methods=["GET", "POST"])
def register():
    if request.method == "POST":
        user_obj = UserService.get_user_by_email(request.form['email'])
        if user_obj:
            flash(f'Le compte existe déjà: ', category='info')
        else:
            new_user = User(request.form['nom'], request.form['prenom'], request.form['email'], None, False)
            new_user.set_password(request.form['password'])
            UserService.add_new_user(new_user)

            try:
                subject = 'Creation compte'
                message = f"Bonjour {request.form['prenom']}, \n\nVotre compte a été créé avec succès. Cependant," \
                          f" veuillez noter que vous ne pouvez pas encore vous connecter tant que votre demande n'a " \
                          f"pas été approuvée par l'administrateur."
                send_email(current_app.config['MAIL_USERNAME'], [request.form['email']], subject, message)
            except (Exception,):
                pass

            flash(f'Le compte a été crée avec success ', category='success')

    return render_template("users/register.html")


@user_bp.route('/password', methods=["GET", "POST"])
def recover_password():
    if request.method == "POST":
        email = request.form['email']
        user_obj = UserService.get_user_by_email(email)
        if user_obj:
            code = random.randint(100000, 999999)
            token_obj = TokenService.get_token_by_email(email)
            if token_obj:
                TokenService.update_token(email, code)
            else:
                token = Tokens(email, code)
                TokenService.add_new_token(token)

            try:
                subject = 'Code de vérification'
                message = f"Bonjour {user_obj.username}, \n\nVotre code de verification est : <h4>{code}</h4>. \n" \
                          f"Ce code expire dans 12 heures "
                send_email(current_app.config['MAIL_USERNAME'], [email], subject, message)
            except (Exception,):
                pass

            return redirect(url_for('user_bp.password_code_verification'))
        else:
            flash(f"Compte non trouvé : ", category='info')
    return render_template("users/forget_password.html")


@user_bp.route('/password/code', methods=["GET", "POST"])
def password_code_verification():
    if request.method == "POST":
        code = request.form['code']
        token_obj = TokenService.get_token_by_code(code)
        if token_obj:
            current = datetime.now()
            delta = current - token_obj.created_at
            twenty_four_hours = timedelta(hours=12)
            if delta > twenty_four_hours:
                flash(f"Code expiré, veuillez renvoyer une nouvelle demande ", category='info')
            else:
                return redirect(url_for('user_bp.reset_password', code=code))
        else:
            flash(f"Code invalide : ", category='info')
    return render_template("users/password_code.html")


@user_bp.route('/reset_password', methods=["GET", "POST"])
def reset_password():
    code = request.args.get('code')
    if not code:
        return redirect(url_for('user_bp.password_code_verification'))

    if request.method == "POST":
        code = request.form['code']
        token_obj = TokenService.get_token_by_code(code)
        if not token_obj:
            flash(f"Code de verification non valide ", category='info')
        else:
            new_password = hash_password(request.form['password'])
            dict_values = {
                "password_hash": new_password,
            }
            email = token_obj.email
            UserService.update_user(email, dict_values)
            TokenService.delete_token(email)
            flash(f"Mot de passe mis à jour avec success", category='info')

            try:
                user_obj = UserService.get_user_by_email(email)
                subject = 'Mot de passe modifié'
                message = f"Bonjour {user_obj.username}, \n\nVotre mot de passe a été modifié."
                send_email(current_app.config['MAIL_USERNAME'], [email], subject, message)
            except (Exception,):
                pass

    return render_template("users/reset_password_form.html", code=code)


@user_bp.route('/logout')
def logout():
    # session.pop(current_user.email)
    logout_user()
    flash('Vous etes maintenant déconnecté!', category='info')
    return redirect(url_for('user_bp.login'))


@user_bp.route('/users', methods=["GET", "POST"])
@login_required
def users_page():
    if request.method == "POST":
        email = request.form['hiddenEmail']
        role = request.form['role']
        status = request.form['status']

        if status == "False":
            status = False
        else:
            status = True

        dict_values = {
            "is_approved": status,
            "role": role
        }

        UserService.update_user(email, dict_values)
        user_obj = UserService.get_user_by_email(email)

        if status:
            try:
                subject = 'Compte Activé'
                message = f"Bonjour {user_obj.username}, \n\nVotre compte a été activé. Vous pouvez désormais vous " \
                          f"connecter à l'application en utilisant votre adresse e-mail et votre mot de passe."
                send_email(current_app.config['MAIL_USERNAME'], [email], subject, message)
            except (Exception,):
                pass
        else:
            try:
                subject = 'Compte Désactivé'
                message = f"Bonjour {user_obj.username}, \n\nVotre compte a été désactivé. Pour plus d'informations," \
                          f" veuillez contacter l'administrateur."
                send_email(current_app.config['MAIL_USERNAME'], [email], subject, message)
            except (Exception,):
                pass

    df_users = UserService.get_users()
    df_users = df_users.loc[df_users["Email"] != current_user.email]
    return render_template("users/users_home.html",
                           df_users=df_users)
