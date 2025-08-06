from flask import Blueprint, render_template
from flask_login import login_required

from main.Radio.services.alarm_radio_service import AlarmRadioService
from main.utils.utils import role_required

radio_home_bp = Blueprint(
    'radio_home_bp', __name__,
    static_folder='static',
    template_folder='templates'
)


@radio_home_bp.route('home')
@login_required
@role_required(['USER_FH_RADIO', 'ADMIN', 'USER_RADIO'])
def home():
    df = AlarmRadioService.get_file_list()
    file_list = [row["Save Time"] for index, row in df.iterrows()]

    # alarm_severity = df.Severity.value_counts().to_dict()

    return render_template("Radio/radio_home/index.html", file_list=file_list)
