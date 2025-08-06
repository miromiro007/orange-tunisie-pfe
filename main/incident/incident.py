from flask import Flask, render_template, Blueprint
from flask_login import login_required

from main.utils.utils import role_required

incident_bp = Blueprint(
    'incident_bp', __name__,
    static_folder="static",
    template_folder="templates"
)

@incident_bp.route('/incident',methods=['GET'])
@login_required
@role_required(['USER_FH_RADIO', 'ADMIN', 'USER_FH','USER_RADIO'])
def render_report():
    report_embed_url = "https://app.powerbi.com/reportEmbed?reportId=a5378e12-69a0-43d9-85f4-0765650ea774&autoAuth=true&ctid=76965b8b-46f0-455b-9d56-37a500464222"
    return render_template('report.html', report_embed_url=report_embed_url)


