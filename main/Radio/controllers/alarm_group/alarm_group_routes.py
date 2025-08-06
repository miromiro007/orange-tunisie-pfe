from flask import Blueprint, render_template
from flask_login import login_required

from main.Radio.services.alarm_radio_service import AlarmRadioService
from main.utils.utils import role_required

alarm_group_bp = Blueprint(
    'alarm_group_bp', __name__,
    template_folder="templates",
    static_url_path="static",
    static_folder="static"

)


@alarm_group_bp.route("/alarm_gp/<group_name>", methods=["GET", "POST"])
@login_required
@role_required(['USER_FH_RADIO', 'ADMIN', 'USER_RADIO'])
def alarm_group(group_name):
    current_alarm_group = group_name
    df = AlarmRadioService.get_file_list()
    file_list = [row["Save Time"] for index, row in df.iterrows()]

    return render_template("Radio/alarm_group/alarm_group.html",
                           current_alarm_group=current_alarm_group,
                           file_list=file_list)
