import os
import warnings
import zipfile

import pandas as pd
from dateutil.parser import *
from flask import Blueprint, render_template, request, jsonify
from flask import current_app
from flask_login import login_required, current_user
from werkzeug.utils import secure_filename

# from ..models.ne_report import NeReportFile
from main.Radio.services.alarm_radio_service import AlarmRadioService
from main.utils.logging_config import configure_logging
from main.utils.utils import role_required

warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')

file_upload_bp = Blueprint(
    'file_upload_bp', __name__,
    template_folder="templates",
    static_folder="static"
)

ALLOWED_EXTENSIONS = {'xlsx', 'zip'}


def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


def load_dataset(path, read_data, ne_report_file=None):
    df = None
    filename, extension = os.path.splitext(path)
    if extension == '.zip':
        directory = current_app.config['UPLOAD_FOLDER']
        is_first_file = True
        df_list = []
        for filename in os.listdir(directory):
            if filename.endswith('.xlsx'):
                filepath = os.path.join(directory, filename)
                if is_first_file:
                    # data = pd.read_excel(filepath, engine="openpyxl", skiprows=5)
                    df_list.append(read_data)
                    is_first_file = False
                else:
                    data = pd.read_excel(filepath, engine="openpyxl")
                    df_list.append(data)
        df = pd.concat(df_list, axis=0)
        df.reset_index(drop=True, inplace=True)
    else:
        df = read_data.copy()

    df.rename(columns={" ": "Comments"}, inplace=True)
    df["Last Occurred (NT)"] = pd.to_datetime(df["Last Occurred (NT)"])
    df["First Occurred (NT)"] = pd.to_datetime(df["First Occurred (NT)"])
    # df = df.drop_duplicates(['Severity', 'Name', 'Last Occurred (NT)', 'NE Type', 'Alarm Source', 'Alarm ID'],
    #                      keep='last')

    if ne_report_file:
        try:
            df_ne = pd.read_excel(ne_report_file, engine="openpyxl", usecols=["NE Name", "Home Subnet"])
            df_ne.set_index('NE Name', inplace=True)
            df['Home Subnet'] = ''
            for index in df_ne.index.values:
                df.loc[df['Alarm Source'].str.contains(index), 'Home Subnet'] = df_ne.loc[index]['Home Subnet']
        except (Exception,):
            return None

    return df


def empty_upload_dir(directory):
    for filename in os.listdir(directory):
        filepath = os.path.join(directory, filename)
        os.remove(filepath)


def get_file_save_time(file_path):
    saved_time = None
    filename, extension = os.path.splitext(file_path)
    df = None
    if extension == '.zip':
        # Open the zip file in read mode
        with zipfile.ZipFile(file_path, 'r') as zip_ref:
            # Extract all the contents of the zip file
            directory = current_app.config['UPLOAD_FOLDER']
            zip_ref.extractall(directory)

            # Get the list of extracted file names
            file_names = zip_ref.namelist()

            # Read the first CSV file in the extracted folder using pandas
            for file_name in file_names:
                if file_name.endswith('.xlsx'):
                    # df = pd.read_csv(f"{zip_ref.extract(file_name)}")
                    file_path = os.path.join(directory, file_name)
                    break
    try:
        df = pd.read_excel(file_path)
        workbook_dataframe = df.iloc[:1]
        words = workbook_dataframe.iloc[0][0].split()
        saved_time = pd.to_datetime(words[2] + ' ' + words[3])

        # set the 5th row as the header
        df = df.rename(columns=df.iloc[4]).drop(df.index[4])
        df = df.iloc[4:]
        df = df.reset_index(drop=True)
    except (Exception,):
        pass
    return saved_time, df


@file_upload_bp.route('/process_upload_form', methods=['POST'])
def process_upload_form():
    if request.method == 'POST':
        # check if the post request has the file part
        if 'file' not in request.files:
            return jsonify({'status': 'error', 'message': 'No file part'})
        file = request.files['file']
        # If the user does not select a file, the browser submits an
        # empty file without a filename.
        if file.filename == '':
            # print(file.filename + "-no file selected")
            return jsonify({'status': 'error',
                            'message': 'Aucun fichier selectionné, Veuillez choisir un fichier puis réessayer !'})
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            # print(filename)
            directory_path = current_app.config['UPLOAD_FOLDER']
            # Check if the directory already exists
            if not os.path.exists(directory_path):
                # Create the directory if it doesn't exist
                os.makedirs(directory_path)
            filepath = os.path.join(current_app.config['UPLOAD_FOLDER'], filename)
            file.save(filepath)

            save_time, data = get_file_save_time(filepath)
            if save_time:
                alarm_obj = AlarmRadioService.get_alarm_by_save_time(save_time)
                if alarm_obj:
                    empty_upload_dir(directory_path)
                    del data
                    return jsonify({'status': 'error',
                                    'message': 'Le fichier existe déjà, Veuillez choisir un autre fichier !'})
                else:
                    df = load_dataset(filepath, data)

                    if df is not None:
                        if 'Subnet' in df.columns:
                            df = df.rename(columns={'Subnet': 'Home Subnet'})
                        else:
                            df['Home Subnet'] = ''

                        if 'Alarm Duration' not in df.columns:
                            df['Alarm Duration'] = ''

                        alarms_columns = [
                            'Comments',
                            'Severity',
                            'Name',
                            'Last Occurred (NT)',
                            'Cleared On (NT)',
                            'Location Information',
                            'NE Type',
                            'Alarm Source',
                            'MO Name',
                            'Occurrence Times',
                            'First Occurred (NT)',
                            'Alarm ID',
                            'Acknowledged On (ST)',
                            'Cleared By',
                            'Acknowledged By',
                            'Clearance Status',
                            'Acknowledgement Status',
                            'Home Subnet',
                            'Alarm Duration'
                        ]
                        df = df[alarms_columns]

                        alarmService = AlarmRadioService()
                        alarmService.add_new_file(df, save_time)

                        logger = configure_logging()
                        logger.info(f"Nouveau fichier alarme_radio_{save_time} ajoute par {current_user.username}")

                    else:
                        empty_upload_dir(directory_path)
                        return jsonify({'status': 'error',
                                        'message': "Un problème est survenu lors de l'ajout du fichier "})
            else:
                empty_upload_dir(directory_path)
                return jsonify({'status': 'error',
                                'message': 'Format du fichier non prise en compte, Veuillez choisir un autre fichier!'})

            empty_upload_dir(directory_path)
            # print("file deleted")
            return jsonify({'status': 'success',
                            'message': 'Fichier ajouté avec success !'})

        else:
            return jsonify({'status': 'error',
                            'message': 'Format du fichier non prise en compte, Veuillez choisir un autre fichier!'})


@file_upload_bp.route("/upload", methods=['GET', 'POST'])
@login_required
@role_required(['USER_FH_RADIO', 'ADMIN', 'USER_RADIO'])
def upload():
    df = AlarmRadioService.get_file_list()
    file_list = [row["Save Time"] for index, row in df.iterrows()]

    # NE Report
    ne_dir = current_app.config['NE_REPORT_FOLDER']
    ne_file_list = os.listdir(ne_dir)

    # Site Radio
    sites = {}
    patterns = ["GSM", "UMTS", "LTE"]
    for pattern in patterns:
        site_dir = current_app.config[pattern + '_FOLDER']
        filename = os.listdir(site_dir)[0]
        sites.update({pattern: filename})
    return render_template("Radio/file_upload/file_upload.html",
                           file_list=file_list,
                           ne_file_list=ne_file_list,
                           sites=sites)


@file_upload_bp.route("/delete", methods=['GET', 'POST'])
def delete_file():
    if request.method == 'POST':
        save_time = pd.to_datetime(parse(request.form['save_time'], fuzzy=True).strftime("%Y-%m-%d %H:%M:%S"))
        alarmService = AlarmRadioService()
        alarmService.remove_file(save_time)

        logger = configure_logging()
        logger.info(f"Fichier alarme_radio_{save_time} supprime par {current_user.username}")

    return jsonify({'status': 'success',
                    'message': 'Fichier supprimé avec success !'})


@file_upload_bp.route('/upload_ne_report', methods=['POST'])
def upload_ne_report():
    # Get the uploaded CSV file
    ne_file = request.files['ne_report']

    if ne_file.filename == '':
        return jsonify({'status': 'error',
                        'message': 'Aucun fichier selectionné, Veuillez choisir un fichier puis réessayer !'})

    if ne_file and allowed_file(ne_file.filename):
        df = pd.read_excel(ne_file)
        column_names = list(df.columns)
        if ('NE Name' in column_names) & ('Home Subnet' in column_names):
            filename = secure_filename(ne_file.filename)
            directory_path = current_app.config['NE_REPORT_FOLDER']
            if not os.path.exists(directory_path):
                os.makedirs(directory_path)
            filepath = os.path.join(current_app.config['NE_REPORT_FOLDER'], filename)
            ne_file.save(filepath)
        else:
            return jsonify({'status': 'error',
                            'message': "Il semblerait que le fichier n'est pas correct, Veuillez choisir un autre "
                                       "fichier!"})
    else:
        return jsonify({'status': 'error',
                        'message': 'Format du fichier non prise en compte, Veuillez choisir un autre fichier!'})

    # Return a success response
    return jsonify({'status': 'success',
                    'message': 'Fichier ajouté avec success !'})


@file_upload_bp.route("/delete_ne_report", methods=['GET', 'POST'])
def delete_ne_report_file():
    if request.method == 'POST':
        filename = request.form['ne_file_name']
        ne_dir = current_app.config['NE_REPORT_FOLDER']
        filepath = os.path.join(ne_dir, filename)
        os.remove(filepath)
    return jsonify({'status': 'success',
                    'message': 'Fichier NE supprimé avec success !'})


@file_upload_bp.route('/upload_site_radio', methods=['POST'])
def upload_site_radio():
    # Get the uploaded CSV file
    site_file = request.files['site_radio']
    technology = request.form['technology']

    if site_file.filename == '':
        # print(file.filename + "-no file selected")
        return jsonify({'status': 'error',
                        'message': 'Aucun fichier selectionné, Veuillez choisir un fichier puis réessayer !'})

    if site_file and site_file.filename.endswith('.csv'):
        # Read the first line of the file
        headers = site_file.readline().decode('utf-8').split(';')
        if ('Latitude_Sector' in headers) & ('Longitude_Sector' in headers):
            site_dir = current_app.config[technology + '_FOLDER']
            filename = secure_filename(site_file.filename)
            if not os.path.exists(site_dir):
                os.makedirs(site_dir)
            filepath = os.path.join(site_dir, filename)
            empty_upload_dir(site_dir)

            # save file
            with open(filepath, 'wb') as f:
                f.write(';'.join(headers).encode('utf-8') + b'\n')
                f.write(site_file.read())

                logger = configure_logging()
                logger.info(f"Fichier site radio {technology} mise a jour par {current_user.username}")
        else:
            return jsonify({'status': 'error',
                            'message': "Il semblerait que le fichier n'est pas correct, Veuillez choisir un autre "
                                       "fichier!"})
    else:
        return jsonify({'status': 'error',
                        'message': 'Format du fichier non prise en compte, Veuillez choisir un autre fichier!'})

    # Return a success response
    return jsonify({'status': 'success',
                    'message': 'Fichier ajouté avec success !'})
