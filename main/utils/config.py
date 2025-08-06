import os


class Config:
    # database config
    SQLALCHEMY_DATABASE_URI = "mysql+pymysql://root:12345@localhost:3306/projet_pfe"
    SQLALCHEMY_TRACK_MODIFICATIONS = False

    # mailing config
    MAIL_SERVER = 'smtp.gmail.com'
    MAIL_PORT = 465
    MAIL_USE_TLS = False
    MAIL_USE_SSL = True
    MAIL_USERNAME = 'transmissionradiofh@gmail.com'
    MAIL_PASSWORD = 'srggomtvowxclssw'

    # folders path
    UPLOAD_FOLDER = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..', 'main/Radio/data/uploaded_file'))
    EXPORT_FOLDER = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..', 'main/Radio/data/export'))
    NE_REPORT_FOLDER = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..', 'main/Radio/data/ne_report'))
    GSM_FOLDER = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..', 'main/Radio/data/site/gsm'))
    UMTS_FOLDER = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..', 'main/Radio/data/site/umts'))
    LTE_FOLDER = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..', 'main/Radio/data/site/lte'))
    CREDENTIALS_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..', 'main/Radio/data/creds'
                                                                                     '/credentials.json'))
    MLO_FOLDER = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..', 'main/FH/data/MLO'))
    TEMP_FOLDER = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..', 'main/FH/data/temp'))
    DRO = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..', 'main/Radio/data/DRO'))