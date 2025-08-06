import datetime
from dateutil.parser import *
import pandas as pd
from flask import Blueprint, render_template, request, jsonify
from flask_login import login_required, current_user

from main.FH.services.pmon_service import PMONService
from main.utils.logging_config import configure_logging
from main.utils.utils import role_required

fh_pmon_bp = Blueprint(
    'fh_pmon_bp', __name__,
    static_folder='static',
    template_folder='templates'
)

ALLOWED_EXTENSIONS = {'xlsx'}


def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


@fh_pmon_bp.route('/pmon')
@login_required
@role_required(['USER_FH_RADIO', 'ADMIN', 'USER_FH'])
def pmon_page():
    df = PMONService.get_files_list()
    return render_template("FH/fh_pmon/pmon.html",
                           df_pmon_files=df)


@fh_pmon_bp.route('/pmon/upload', methods=['GET', 'POST'])
def pmon_upload():
    file = request.files['pmon_file']
    if file.filename == '':
        return jsonify({'status': 'error',
                        'message': 'Aucun fichier selectionné, Veuillez choisir un fichier puis réessayer !'})

    if file and allowed_file(file.filename):
        try:
            df = pd.read_excel(file, engine="openpyxl", skiprows=1)
            columns = list(df.columns)
            expected_columns = ['IP',
                                'Slot',
                                'Sanity',
                                'Mod (Ref)',
                                'Mod (Min)',
                                'UAS',
                                'SEP',
                                'SES',
                                'ES',
                                'BBE',
                                'OFS',
                                'RSL (Min)',
                                'RSL (Max)',
                                'RSL (Avg)'
                                ]

            if columns == expected_columns:
                df["RSL (Min)"] = df["RSL (Min)"].str.lower().str.replace("dBm".lower(), "").astype(float)
                df["RSL (Max)"] = df["RSL (Max)"].str.lower().str.replace("dBm".lower(), "").astype(float)
                df["RSL (Avg)"] = df["RSL (Avg)"].str.lower().str.replace("dBm".lower(), "").astype(float)

                df['Status'] = ''
                df['Comments'] = ''

                current_datetime = datetime.datetime.now()
                formatted_datetime = current_datetime.strftime("%Y-%m-%d %H:%M:%S")

                df["Creation Date"] = formatted_datetime
                df["Creation Date"] = pd.to_datetime(df["Creation Date"])

                df['High Value'] = ''

                df = df.fillna(value=0)

                PMONService.add_new_file(df)

                logger = configure_logging()
                logger.info(f"Nouveau Fichier PMON ajoute par {current_user.username}")

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


@fh_pmon_bp.route("/pmon/delete", methods=['GET', 'POST'])
def delete_pmon_file():
    if request.method == 'POST':
        creation_date = pd.to_datetime(parse(request.form['creation_date'], fuzzy=True).strftime("%d-%m-%Y %H:%M:%S"))
        PMONService.remove_file(creation_date)

        logger = configure_logging()
        logger.info(f"Fichier PMON du {creation_date} supprime par {current_user.username}")

    return jsonify({'status': 'success',
                    'message': 'Fichier supprimé avec success !'})


@fh_pmon_bp.route("/pmon/edit", methods=['GET', 'POST'])
def edit_pmon_row():
    if request.method == 'POST':
        ip = request.form['ip']
        comment = request.form['comment']
        high_value = request.form['highValue']
        uploadDate = request.form['uploadDate']

        if uploadDate == "None":
            uploadDate = None

        PMONService.update_pmon(ip, comment, uploadDate, high_value)
    return jsonify({'status': 'success',
                    'message': 'Fichier supprimé avec success !'})
