import datetime

import pandas as pd
from dateutil.parser import *
from flask import Blueprint, render_template, jsonify, request, current_app
from flask_login import login_required, current_user

from main.FH.fh_utils import add_link_ref_rsl_to_df, preprocessing, compute_rsl_diff
from main.FH.services.rsl_service import RSLLevelService
from main.utils.logging_config import configure_logging
from main.utils.utils import role_required

fh_home_bp = Blueprint(
    'fh_home_bp', __name__,
    template_folder='templates',
    static_folder='static'
)

ALLOWED_EXTENSIONS = {'xlsx'}


def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


@fh_home_bp.route('/home')
@login_required
@role_required(['USER_FH_RADIO', 'ADMIN', 'USER_FH'])
def home():
    df = RSLLevelService.get_files_list()

    return render_template("FH/fh_home/index.html",
                           df_rsl=df)


@fh_home_bp.route('/rsl/upload', methods=['GET', 'POST'])
def rsl_upload():
    file = request.files['rsl_file']
    if file.filename == '':
        return jsonify({'status': 'error',
                        'message': 'Aucun fichier selectionné, Veuillez choisir un fichier puis réessayer !'})

    if file and allowed_file(file.filename):
        try:
            df = pd.read_excel(file, engine="openpyxl", skiprows=1)
            df=df[['IP', 'Slot', 'Min RSL', 'Avg RSL', 'Max RSL', 'Name']]
            columns = list(df.columns)
            expected_columns = ['IP', 'Slot', 'Min RSL', 'Avg RSL', 'Max RSL', 'Name']
            if columns.sort() == expected_columns.sort():
                vr_indx = df.loc[(df['Name'].str.contains('VR')) | (df['Name'] == ".")].index
                df = df.drop(vr_indx, inplace=False)
                df.reset_index(drop=True, inplace=True)

                df = add_link_ref_rsl_to_df(df, current_app.config['MLO_FOLDER'])

                df = preprocessing(df)

                df = compute_rsl_diff(df)

                df = df.drop('RSL DIFF', axis=1)

                df['Comments'] = ''

                current_datetime = datetime.datetime.now()
                formatted_datetime = current_datetime.strftime("%Y-%m-%d %H:%M:%S")

                df["Creation Date"] = formatted_datetime
                df["Creation Date"] = pd.to_datetime(df["Creation Date"])

                RSLLevelService.add_new_file(df)

                logger = configure_logging()
                logger.info(f"Nouveau Fichier RSL  ajoute par {current_user.username}")

            else:
                return jsonify({'status': 'error',
                                'message': "Il semblerait que le fichier n'est pas correct, Veuillez choisir un autre "
                                           "fichier!"})
        except (Exception,):
            return jsonify({'status': 'error',
                            'message': "Il semblerait que le fichier n'est pas correct, Veuillez choisir un autre "
                                       "fichier!"})
    else:
        return jsonify({'status': 'error',
                        'message': 'Format du fichier non prise en compte, Veuillez choisir un autre fichier!'})

    return jsonify({'status': 'success',
                    'message': 'Fichier ajouté avec success !'})


@fh_home_bp.route("/rsl/delete", methods=['GET', 'POST'])
def delete_rsl_file():
    if request.method == 'POST':
        creation_date = pd.to_datetime(parse(request.form['creation_date'], fuzzy=True).strftime("%d-%m-%Y %H:%M:%S"))
        RSLLevelService.remove_file(creation_date)

        logger = configure_logging()
        logger.info(f"Fichier RSL du {creation_date} supprime par {current_user.username}")

    return jsonify({'status': 'success',
                    'message': 'Fichier supprimé avec success !'})


@fh_home_bp.route("/rsl/edit", methods=['GET', 'POST'])
def edit_rsl_row():
    if request.method == 'POST':
        name = request.form['name']
        ip = request.form['ip']
        status = request.form['status']
        comment = request.form['comment']
        uploadDate = request.form['uploadDate']

        if uploadDate == "None":
            uploadDate = None

        RSLLevelService.update_rsl(name, ip, status, comment, uploadDate)
    return jsonify({'status': 'success',
                    'message': 'Fichier supprimé avec success !'})
