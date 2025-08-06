import os
import pandas as pd
from flask import Blueprint, render_template, request
import folium
from branca.element import Figure, IFrame
from flask_login import login_required

from main.Radio.controllers.api.radio_api_utils import get_sub_dataset, get_bagots
from flask import current_app
from main.Radio.services.alarm_radio_service import AlarmRadioService
from main.utils.utils import role_required

radio_map_bp = Blueprint(
    'radio_map_bp', __name__,
    static_folder='static',
    template_folder='templates'
)

COLORS_MAP = {'Critical': 'red',
              'Major': 'orange',
              'Minor': 'blue',
              'Warning': 'green'}


def get_first_column_name(filepath):
    file = open(filepath)
    first_col = file.readline().split(';')[0]
    file.close()
    return first_col


def get_site_coord(cell_name):
    lte_dir = current_app.config['LTE_FOLDER']
    filename = os.listdir(lte_dir)[0]
    filepath = os.path.join(lte_dir, filename)
    first_column = get_first_column_name(filepath)
    df = pd.read_csv(filepath,
                     usecols=[first_column, 'Longitude_Sector', 'Latitude_Sector'],
                     sep=";")
    df = df[df[first_column].str.contains(cell_name)]
    if df.empty:
        umts_dir = current_app.config['UMTS_FOLDER']
        filename = os.listdir(umts_dir)[0]
        filepath = os.path.join(umts_dir, filename)
        first_column = get_first_column_name(filepath)
        df = pd.read_csv(filepath,
                         usecols=[first_column, 'Longitude_Sector', 'Latitude_Sector'],
                         sep=";")
        df = df[df[first_column].str.contains(cell_name)]
        if df.empty:
            gsm_dir = current_app.config['GSM_FOLDER']
            filename = os.listdir(gsm_dir)[0]
            filepath = os.path.join(gsm_dir, filename)
            first_column = get_first_column_name(filepath)
            df = pd.read_csv(filepath,
                             usecols=[first_column, 'Longitude_Sector', 'Latitude_Sector'],
                             sep=";")
            df = df[df[first_column].str.contains(cell_name)]
            if df.empty:
                return None

    # print(df.head())
    first_row = df.iloc[0]
    coord = {
        "lat": first_row["Latitude_Sector"],
        "lon": first_row["Longitude_Sector"]
    }
    return coord


def extract_site_name(alarm_source):
    cell_name = None
    if "/" in alarm_source:
        cell_name = alarm_source.split("/")[1][:8]
    else:
        try:
            cell_name = alarm_source[:8]
        except (Exception,):
            return None

    return get_site_coord(cell_name)


@radio_map_bp.route("/map", methods=["POST", "GET"])
@login_required
@role_required(['USER_FH_RADIO', 'ADMIN', 'USER_RADIO'])
def view_map():
    response_data = {}
    alarm_group = request.form['alarmGroup']
    alarm_name = request.form['alarm_name']

    df = AlarmRadioService.get_active_alarms()
    df = get_sub_dataset(df, alarm_group)
    df = get_bagots(df)
    df = df.drop_duplicates(['Severity', 'Name', 'Last Occurred (NT)', 'NE Type', 'Alarm Source'], keep='last')

    alarm_name_list = list(df['Name'].value_counts(sort=True).index)
    if alarm_name:
        df = df[(df["Name"] == alarm_name)]
    else:
        df = df[(df["Name"] == alarm_name_list[0])]
        alarm_name = alarm_name_list[0]

    # df = query_active_alarms()
    # df = get_sub_dataset(df, "TDD Alarms")
    # df = query_active_alarms_by_name("Certificate Invalid")
    # df = df.drop_duplicates(['Severity', 'Name', 'Last Occurred (NT)', 'NE Type', 'Alarm Source'], keep='last')
    # df = df[(df["Name"] == "Certificate Invalid")]

    # first_src = df.iloc[0]["Alarm Source"]
    # coord = extract_site_name(first_src)

    fig = Figure(width=1200, height=650)
    # m = folium.Map(location=[28.644800, 77.216721])
    m = folium.Map(width=1200, height=650, location=[33.8869, 9.5375], zoom_start=7,
                   control_scale=True, tiles="stamenterrain")
    fig.add_child(m)
    folium.TileLayer('openstreetmap').add_to(m)
    folium.LayerControl().add_to(m)

    # Adding markers to the map
    css_code = '''
    <style>
        /* width */
        ::-webkit-scrollbar {
          width: 10px;
        }
        
        
        /* Handle */
        ::-webkit-scrollbar-thumb {
          background: #ff7f00;
          border-radius: 3px;
        }
    </style>
    '''
    for index, row in df.iterrows():
        coord = extract_site_name(row['Alarm Source'])
        if coord:
            html = f"""
                    <!DOCTYPE html>
                    <html lang="en">
                    <head>
                      <title>Bootstrap Example</title>
                      <meta charset="utf-8">
                      <meta name="viewport" content="width=device-width, initial-scale=1">
                      <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap@4.6.2/dist/css/bootstrap.min.css">
                      <script src="https://cdn.jsdelivr.net/npm/jquery@3.6.3/dist/jquery.slim.min.js"></script>
                      <script src="https://cdn.jsdelivr.net/npm/popper.js@1.16.1/dist/umd/popper.min.js"></script>
                      <script src="https://cdn.jsdelivr.net/npm/bootstrap@4.6.2/dist/js/bootstrap.bundle.min.js"></script>
                      {css_code}
                    </head>
                    <body>
                        <div class="container">
                          <h4 style="color:#ff7f00;" >{row['Name']}</h4>
                          <div class="accordion" id="exempleAccordeon">
                            <div class="card">
                              <div class="card-header" id="header1">
                                <button class="btn btn-link" type="button" data-toggle="collapse" data-target="#collapse1" 
                                  aria-expanded="true" aria-controls="collapse1" style="color:#ff7f00;">Location Information</button>
                              </div>
                              <div id="collapse1" class="collapse show" aria-labelledby="header1" data-parent="#exempleAccordeon">
                                <div class="card-body">
                                  <p>{row['Alarm Source']}</p>
                                  <span class="small">{row['Location Info']}</span>
                                </div>
                              </div>
                            </div>
                              
                            <div class="card">
                              <div class="card-header" id="header2">
                                <button class="btn btn-link" type="button" data-toggle="collapse" data-target="#collapse2" 
                                aria-expanded="true" aria-controls="collapse2" style="color:#ff7f00;">Severity</button>
                              </div>
                              <div id="collapse2" class="collapse" aria-labelledby="header2" data-parent="#exempleAccordeon">
                                <div class="card-body">
                                  <p>{row['Severity']}</p>
                                  <span class="small">Last Occurred on : {row['Last Occurred (NT)']}</span>
                                </div>
                              </div>
                            </div>
                    
                            <div class="card">
                              <div class="card-header" id="header3">
                                <button class="btn btn-link" type="button" data-toggle="collapse" data-target="#collapse3"
                                 aria-expanded="true" aria-controls="collapse3" style="color:#ff7f00;" >NE Type</button>
                              </div>
                              <div id="collapse3" class="collapse" aria-labelledby="header3" data-parent="#exempleAccordeon">
                                <div class="card-body">
                                  <p>{row['NE Type']}</p>
                                  <span class="small"></span>
                                </div>
                              </div>
                            </div> 
                          </div>
                        </div>
                      </body>
                    </html>
    
                """
            iframe = IFrame(html=html, width=500, height=300)
            popup = folium.Popup(iframe, max_width=500)

            folium.Marker(location=[coord["lat"], coord["lon"]],
                          popup=popup,
                          icon=folium.Icon(color=COLORS_MAP[row['Severity']]),
                          tooltip=row['Alarm Source']).add_to(m)

    response_data["map"] = m._repr_html_()
    response_data["alarm_name_list"] = alarm_name_list
    response_data["current_alarm"] = alarm_name
    response_data["alarm_group"] = alarm_group
    response_data["total_site"] = f"{len(list(df['Alarm Source'].unique())):,}"

    return render_template("Radio/map/map.html",
                           current_alarm_group="RET Alarms",
                           response_data=response_data
                           )
