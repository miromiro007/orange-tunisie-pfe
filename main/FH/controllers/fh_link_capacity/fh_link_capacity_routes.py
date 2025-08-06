import datetime
from dateutil.parser import *
import pandas as pd
from flask import Blueprint, render_template, request, jsonify
from flask_login import login_required, current_user

from main.FH.services.load_distribution_service import LoadDistributionService
from main.utils.logging_config import configure_logging
from main.utils.utils import role_required

fh_link_capacity_bp = Blueprint(
    'fh_link_capacity_bp', __name__,
    static_folder='static',
    template_folder='templates'
)

ALLOWED_EXTENSIONS = {'xlsx'}


def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


@fh_link_capacity_bp.route('/link/capacity')
@login_required
@role_required(['USER_FH_RADIO', 'ADMIN', 'USER_FH'])
def capacity_page():
    df = LoadDistributionService.get_files_list()
    return render_template("FH/fh_link_capacity/link_capacity.html",
                           df_load=df)


@fh_link_capacity_bp.route('/load/distribution/upload', methods=['GET', 'POST'])
def load_distribution_upload():
    file = request.files['nw_load_file']
    if file.filename == '':
        return jsonify({'status': 'error',
                        'message': 'Aucun fichier selectionné, Veuillez choisir un fichier puis réessayer !'})

    if file and allowed_file(file.filename):
        try:
            df = pd.read_excel(file, engine="openpyxl", skiprows=1)
            columns = list(df.columns)
            expected_columns = ['IP',
                                'Slot',
                                'Average Daily TX Load',
                                'Max Daily TX Load',
                                'Average Daily RX Load',
                                'Max Daily RX Load',
                                'Name']
            if columns == expected_columns:

                df["Average Daily TX Load"] = df["Average Daily TX Load"].str.replace('%', '').astype(float)
                df["Max Daily TX Load"] = df["Max Daily TX Load"].str.replace('%', '').astype(float)
                df["Average Daily RX Load"] = df["Average Daily RX Load"].str.replace('%', '').astype(float)
                df["Max Daily RX Load"] = df["Max Daily RX Load"].str.replace('%', '').astype(float)

                current_datetime = datetime.datetime.now()
                formatted_datetime = current_datetime.strftime("%Y-%m-%d %H:%M:%S")

                df["Creation Date"] = formatted_datetime
                df["Creation Date"] = pd.to_datetime(df["Creation Date"])

                LoadDistributionService.add_new_file(df)

                logger = configure_logging()
                logger.info(f"Nouveau Fichier capaciite liens ajoute par {current_user.username}")

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


@fh_link_capacity_bp.route("/load/distribution/delete", methods=['GET', 'POST'])
def delete_load_distribution_file():
    if request.method == 'POST':
        creation_date = pd.to_datetime(parse(request.form['creation_date'], fuzzy=True).strftime("%d-%m-%Y %H:%M:%S"))
        LoadDistributionService.remove_file(creation_date)

        logger = configure_logging()
        logger.info(f"Fichier capaciite liens du {creation_date} supprime par {current_user.username}")

    return jsonify({'status': 'success',
                    'message': 'Fichier supprimé avec success !'})
