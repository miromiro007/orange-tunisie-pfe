import os
import pandas as pd
from flask import Blueprint, render_template, jsonify, request, current_app
from flask_login import login_required, current_user

from main.FH.fh_utils import extract_spec_from_mlo_file
from main.utils.logging_config import configure_logging
from main.utils.utils import role_required

fh_mlo_bp = Blueprint(
    'fh_mlo_bp', __name__,
    static_folder='static',
    template_folder='templates'
)

ALLOWED_EXTENSIONS = {'xlsx'}


def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


@fh_mlo_bp.route('/mlos')
@login_required
@role_required(['USER_FH_RADIO', 'ADMIN', 'USER_FH'])
def mlo_page():
    return render_template("FH/fh_mlo/mlo.html")


@fh_mlo_bp.route('/mlos/add', methods=['POST'])
def add_mlo():
    file = request.files['mlo_file']
    if file.filename == '':
        return jsonify({'status': 'error',
                        'message': 'Aucun fichier selectionné, Veuillez choisir un fichier puis réessayer !'})

    if file and allowed_file(file.filename):
        try:
            mw_link = extract_spec_from_mlo_file(file)
            if mw_link is None:
                return jsonify({'status': 'error',
                                'message': "Il semblerait que le fichier n'est pas correct, Veuillez choisir un autre "
                                           "fichier!"})
            else:
                mlo_dir = current_app.config['MLO_FOLDER']
                if not os.path.exists(mlo_dir):
                    os.makedirs(mlo_dir)
                filepath = os.path.join(mlo_dir, file.filename)
                df = pd.read_excel(file, engine="openpyxl")
                df.to_excel(filepath, index=False)

                logger = configure_logging()
                logger.info(f"Nouveau Fichier MLO : {file.filename} ajoute par {current_user.username}")

                # file.save(filepath)
        except (Exception,):
            return jsonify({'status': 'error',
                            'message': "Il semblerait que le fichier n'est pas correct, Veuillez choisir un autre "
                                       "fichier!"})
    else:
        return jsonify({'status': 'error',
                        'message': 'Format du fichier non prise en compte, Veuillez choisir un autre fichier!'})

    return jsonify({'status': 'success',
                    'message': 'Fichier supprimé avec success !'})


@fh_mlo_bp.route("/mlos/delete", methods=['GET', 'POST'])
def delete_mlo_file():
    if request.method == 'POST':
        mlo_name = request.form['filename']
        mlo_dir = current_app.config['MLO_FOLDER']
        filepath = os.path.join(mlo_dir, mlo_name)
        os.remove(filepath)

        logger = configure_logging()
        logger.info(f"Fichier MLO : {mlo_name} supprime par {current_user.username}")

    return jsonify({'status': 'success',
                    'message': 'Fichier supprimé avec success !'})
