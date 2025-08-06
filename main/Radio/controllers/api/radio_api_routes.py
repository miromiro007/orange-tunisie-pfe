import time

import numpy as np
from flask import Blueprint, request, send_from_directory, jsonify, Response
from main import get_redis_instance
from .radio_api_utils import *
from main.utils.redis_utils import write_df_to_redis
from flask import current_app
from ...services.alarm_radio_service import AlarmRadioService
from ...services.battery_service import BatteryService
from ...services.congestion_radio_service import CongestionRadioService

radio_api_bp = Blueprint(
    'radio_api_bp', __name__,
    static_folder="static",
    template_folder="templates"
)


@radio_api_bp.route("/redis_delete_keys")
def redis_test():
    redis_instance = get_redis_instance()
    # Delete all keys
    redis_instance.flushall()
    return "all keys deleted successfully"


@radio_api_bp.route("/redis_get_keys")
def redis_get_test():
    redis_instance = get_redis_instance()
    # Get all keys
    for key in redis_instance.scan_iter():
        print(key)
    return "ok keys"


@radio_api_bp.route("/home_data")
def get_home_data():
    upload_date = request.args.get('uploadDate')
    if upload_date == "None":
        upload_date = None

    df = AlarmRadioService.get_data(upload_date)
    df = get_bagots(df)
    df['Save Time'] = pd.to_datetime(df['Save Time'], unit='ms')
    df['Last Occurred (NT)'] = pd.to_datetime(df['Last Occurred (NT)'], unit='ms')
    df['First Occurred (NT)'] = pd.to_datetime(df['First Occurred (NT)'], unit='ms')

    search_word = request.args.get('search[value]')
    severity = request.args.get('severity')
    status = request.args.get('status')
    last_occurred = request.args.get('last_occurred')
    card_alarm_group = request.args.get('card_alarm_group')
    chart_severity = request.args.get('chart_severity')
    chart_alarm_name = request.args.get('chart_alarm_name')
    chart_alarm_source = request.args.get('chart_alarm_source')
    export_excel = int(request.args.get('export_excel'))
    ssv_status = request.args.get('ssv_status')
    filter_search = request.args.get('filter_search')

    if severity != "None":
        if severity == "Critical & Major":
            df = df[df.Severity.isin(["Critical", "Major"])]
        else:
            df = df[df.Severity == severity]

    if ssv_status != "None":
        df = filter_ssv_status(df, ssv_status)

    if status != "None":
        df = df[df["Clearance Status"] == status]

    if last_occurred != "None":
        df = filter_date(df, last_occurred)

    if search_word:
        df = search(df, search_word)

    if filter_search:
        df = search(df, filter_search)

    df2 = pd.DataFrame()

    if card_alarm_group != "None":
        if "Alarms" in card_alarm_group:
            df = get_sub_dataset(df, card_alarm_group)
            df2 = df.copy()
            df = df[df["Clearance Status"] == "Uncleared"]
        else:
            df = df[df["Clearance Status"] == "Uncleared"]

    if chart_severity != "None":
        df = df[(df.Severity == chart_severity) & (df["Clearance Status"] == "Uncleared")]

    if chart_alarm_name != "None":
        df = df[(df.Name == chart_alarm_name) & (df["Clearance Status"] == "Uncleared")]

    if chart_alarm_source != "None":
        if df2.empty:
            df = df[df["Alarm Source"] == chart_alarm_source]
        else:
            df = df2[df2["Alarm Source"] == chart_alarm_source]

    if export_excel:
        return save_df_to_excel(df, 'data.xlsx', current_app.config['EXPORT_FOLDER'])

    columns = ["Severity",
               "Name",
               "Last Occurred (NT)",
               "NE Type",
               "Alarm Source",
               "Clearance Status",
               "Occurrence Times",
               "Home Subnet",
               "Alarm Duration"
               ]

    df = df[columns]

    column_to_sort_index = request.args.get('order[0][column]')
    sort_order = request.args.get('order[0][dir]')

    ordering = {
        'asc': True,
        'desc': False
    }

    order_by = columns[int(column_to_sort_index)]

    df = df.sort_values(by=[order_by], ascending=ordering[sort_order])

    total_filtered = df.shape[0]

    # pagination
    start = request.args.get('start', type=int)
    length = request.args.get('length', type=int)
    df = df.iloc[start:start + length]

    # print(df.shape[0])

    df_json = df.to_json(orient='records', date_format='iso')
    parsed = json.loads(df_json)
    return jsonify({'data': parsed,
                    'recordsFiltered': total_filtered,
                    'recordsTotal': df.shape[0],
                    'draw': request.args.get('draw', type=int)
                    })


@radio_api_bp.route("/export_data/<path:filepath>", methods=["GET", "POST"])
def export_radio_data(filepath):
    directory_path = os.path.dirname(filepath)
    filename = os.path.basename(filepath)
    return send_from_directory(directory_path,
                               filename,
                               as_attachment=True)


@radio_api_bp.route("/home_charts", methods=["POST", "GET"])
def get_home_charts():
    upload_date = request.args.get('uploadDate')
    if upload_date == "None":
        upload_date = None

    df = AlarmRadioService.get_data(upload_date)
    df = get_bagots(df)
    df = prepare_dataset(df)

    df_active = get_alarm_group_active(df)

    df_ev = AlarmRadioService.get_evolution_data()
    df_ev = get_bagots(df_ev)
    df_ev['Save Time'] = pd.to_datetime(df_ev['Save Time'], unit='ms')

    # df = filter_date(df, "last_7_days")
    return jsonify({'graphs': get_radio_home_charts(df, df_ev, df_active),
                    'cards': get_home_cards(df_active, df)
                    })


@radio_api_bp.route("/home_filter", methods=["POST", "GET"])
def home_filter():
    severity = request.form['severity']
    status = request.form['status']
    last_occurred = request.form['last_occurred']
    upload_date = request.form['uploadDate']
    ssv_status = request.form['ssv_status']
    filter_search = request.form['filter_search']

    if upload_date == "None":
        upload_date = None
    df = AlarmRadioService.get_data(upload_date)
    df = prepare_dataset(df)
    df = get_bagots(df)

    df_ev = AlarmRadioService.get_evolution_data()
    df_ev['Save Time'] = pd.to_datetime(df_ev['Save Time'], unit='ms')
    df_ev = get_bagots(df_ev)

    df_active = get_alarm_group_active(df)

    if severity != "None":
        if severity == "Critical & Major":
            df = df[df.Severity.isin(["Critical", "Major"])]
            df_ev = df_ev[df_ev.Severity.isin(["Critical", "Major"])]
            df_active = df_active[df_active.Severity.isin(["Critical", "Major"])]
        else:
            df = df[df.Severity == severity]
            df_ev = df_ev[df_ev.Severity == severity]
            df_active = df_active[df_active.Severity == severity]

    if ssv_status != "None":
        df = filter_ssv_status(df, ssv_status)
        df_ev = filter_ssv_status(df_ev, ssv_status)
        df_active = filter_ssv_status(df_active, ssv_status)

    if filter_search != '':
        df = search(df, filter_search)
        df_ev = search(df_ev, filter_search)
        df_active = search(df_active, filter_search)

    if status != "None":
        if status == "Cleared":
            df = df[df["Clearance Status"] == status]

    if last_occurred != "None":
        df = filter_date(df, last_occurred)
        df_active = filter_date(df_active, last_occurred)

    return jsonify({'graphs': get_radio_home_charts(df, df_ev, df_active),
                    'cards': get_home_cards(df_active, df)
                    })


@radio_api_bp.route("/alarm_group_data", methods=["GET", "POST"])
def get_alarm_group_data():
    upload_date = request.form['uploadDate']
    if upload_date == "None":
        upload_date = None

    df = AlarmRadioService.get_data(upload_date)
    df = get_bagots(df)
    current_alarm_group = request.form['alarm_grp']

    ssv_status = request.form['ssv_status']
    if ssv_status != "None":
        df = filter_ssv_status(df, ssv_status)

    sub_df = get_alarm_group_active(get_sub_dataset(df, current_alarm_group))
    # sub_df = get_bagots(sub_df)
    alarm_group = get_alarm_group(sub_df)

    df_json = alarm_group.to_json(orient='records', date_format='iso')
    parsed = json.loads(df_json)
    return jsonify({'data': parsed})


@radio_api_bp.route("/alarm_group_site_data", methods=["GET", "POST"])
def get_alarm_group_site_data():
    upload_date = request.form['uploadDate']
    if upload_date == "None":
        upload_date = None

    df = AlarmRadioService.get_data(upload_date)
    df = get_bagots(df)
    df['Last Occurred (NT)'] = pd.to_datetime(df['Last Occurred (NT)'], unit='ms')
    df['First Occurred (NT)'] = pd.to_datetime(df['First Occurred (NT)'], unit='ms')

    current_alarm_name = request.form['alarm_name']
    current_alarm_group = request.form['alarm_grp']
    ssv_status = request.form['ssv_status']
    if ssv_status != "None":
        df = filter_ssv_status(df, ssv_status)

    df = get_alarm_group_active(get_sub_dataset(df, current_alarm_group))

    df = df[(df.Name == current_alarm_name) & (df["Clearance Status"] == "Uncleared")][["Alarm ID",
                                                                                        "Severity",
                                                                                        "Alarm Source",
                                                                                        "NE Type",
                                                                                        "Last Occurred (NT)",
                                                                                        "Occurrence Times"]]
    df_json = df.to_json(orient='records', date_format='iso')
    parsed = json.loads(df_json)
    return jsonify({'data': parsed})


@radio_api_bp.route("/alm_grp_pie", methods=["POST", "GET"])
def get_alarm_grp_charts():
    upload_date = request.form['uploadDate']
    if upload_date == "None":
        upload_date = None

    df = AlarmRadioService.get_data(upload_date)
    df = prepare_dataset(df)
    df = get_bagots(df)

    df_ev = AlarmRadioService.get_evolution_data()
    df_ev['Save Time'] = pd.to_datetime(df_ev['Save Time'], unit='ms')
    df_ev = get_bagots(df_ev)

    current_alarm_group = request.form['alarm_grp']

    ssv_status = request.form['ssv_status']
    if ssv_status != "None":
        df = filter_ssv_status(df, ssv_status)
        df_ev = filter_ssv_status(df_ev, ssv_status)

    df = get_sub_dataset(df, current_alarm_group)
    df_active = get_alarm_group_active(df)
    df_bagots = get_bagots(df)
    df_active_bagots = get_bagots(df_active)
    df_ev = get_sub_dataset(df_ev, current_alarm_group)
    figs = create_alarm_grp_charts(df_active, df_bagots, df_ev, df_active_bagots)

    return jsonify({'graphs': figs})


# Congestion
@radio_api_bp.route("/congestion/prs_data")
def get_congestion_prs_data():
    upload_date = request.args.get('uploadDate')
    if upload_date == "None":
        upload_date = None

    df = CongestionRadioService.get_data(upload_date)

    if df is None:
        return Response(status=200)

    df['Time'] = pd.to_datetime(df['Time'], unit='ms')

    # multiply by 100%
    df['Integrity'] = df['Integrity'].apply(lambda x: x * 100)

    total_filtered = df.shape[0]

    # pagination
    start = request.args.get('start', type=int)
    length = request.args.get('length', type=int)
    df = df.iloc[start:start + length]

    df = df.rename(columns={"VS.FEGE.RxMaxSpeed_Mbs(Mbps)": "RxMaxSpeed_Mbs"})

    df_json = df.to_json(orient='records', date_format='iso')
    parsed = json.loads(df_json)

    return jsonify({'data': parsed,
                    'recordsFiltered': total_filtered,
                    'recordsTotal': df.shape[0],
                    'draw': request.args.get('draw', type=int)
                    })


@radio_api_bp.route("/congestion/max_daily", methods=['POST'])
def get_congestion_max_daily():
    upload_date = request.form['uploadDate']
    if upload_date == "None":
        upload_date = None

    df = CongestionRadioService.get_data(upload_date)

    if df is None:
        return Response(status=200)

    df['Time'] = pd.to_datetime(df['Time'], unit='ms')

    df_max = None

    # multiply by 100%
    # df['Integrity'] = df['Integrity'].apply(lambda x: x * 100)
    last_date = max(df.Time)

    redis_instance = get_redis_instance()
    redis_key = f'radio_congestion_max_daily_{last_date.strftime("%d-%m-%Y %H:%M")}'
    cached_data = redis_instance.get(redis_key)
    if cached_data:
        df_max = pd.read_json(cached_data.decode('utf-8'))
    else:

        df_max = CongestionRadioService.max_daly_traiter(df)
        write_df_to_redis(df_max, redis_key)

    # Rename columns by removing the dot at the end
    new_columns = [col[:-1] if col.endswith('.') else col for col in df_max.columns]
    df_max = df_max.rename(columns=dict(zip(df_max.columns, new_columns)))

    # get columns and data from the DataFrame
    columns = list()
    for col in df_max.columns:
        columns.append({
            "title": col,
            "data": col
        })

    data = df_max.to_dict(orient='records')

    # build JSON response
    response_data = {
        'columns': columns,
        'data': data
    }

    return jsonify(response_data)


@radio_api_bp.route("/congestion/max_traffic", methods=['POST'])
def get_congestion_max_traffic():
    upload_date = request.form['uploadDate']
    if upload_date == "None":
        upload_date = None

    df = CongestionRadioService.get_data(upload_date)

    if df is None:
        return Response(status=200)

    df['Time'] = pd.to_datetime(df['Time'], unit='ms')

    df_traffic = None

    # multiply by 100%
    # df['Integrity'] = df['Integrity'].apply(lambda x: x * 100)
    last_date = max(df.Time)

    redis_instance = get_redis_instance()
    redis_key = f'radio_congestion_max_traffic_{last_date.strftime("%d-%m-%Y %H:%M")}'
    cached_data = redis_instance.get(redis_key)
    if cached_data:
        df_traffic = pd.read_json(cached_data.decode('utf-8'))
    else:
        # write_df_to_sheet('CongestionRadio', 'export PRS', current_app.config['CREDENTIALS_PATH'], df)
        # time.sleep(2)
        df_traffic = CongestionRadioService.trafic_max(df)
        write_df_to_redis(df_traffic, redis_key)

    # Rename columns by removing the dot at the end
    new_columns = [col[:-1] if col.endswith('.') else col for col in df_traffic.columns]
    df_traffic = df_traffic.rename(columns=dict(zip(df_traffic.columns, new_columns)))

    # df_traffic['avg'] = df_traffic['moyenne'].str.rstrip("%").astype(float)
    df_traffic['max'] = df_traffic[' % max'].str.rstrip("%").astype(float)

    # Define the conditions and corresponding values for Status Column
    conditions = [
        (df_traffic['max'] >= 0) & (df_traffic['max'] < 75),
        (df_traffic['max'] >= 75) & (df_traffic['max'] < 100),
        (df_traffic['max'] >= 100)
    ]
    values = ['Non Suspect', 'Suspect', 'A verifier']

    # Create the new column based on the conditions
    df_traffic['Status'] = pd.Series(pd.Categorical(np.select(conditions, values)))

    # Congestion plots
    fig_status = plot_congestion_status_barchart(df_traffic)
    fig_status = json.dumps(fig_status, cls=plotly.utils.PlotlyJSONEncoder)

    status = request.form['status']
    if status != "None":
        df_traffic = df_traffic.loc[df_traffic['Status'] == status]

    fig_percent = plot_percentage_range_barchart(df_traffic, "max")
    fig_percent = json.dumps(fig_percent, cls=plotly.utils.PlotlyJSONEncoder)

    df_traffic = df_traffic.drop(['max'], axis=1)

    # get columns and data from the DataFrame
    columns = list()
    for col in df_traffic.columns:
        columns.append({
            "title": col,
            "data": col
        })

    data = df_traffic.to_dict(orient='records')

    # build JSON response
    response_data = {
        'columns': columns,
        'data': data,
        'charts': [fig_status, fig_percent]
    }

    return jsonify(response_data)


# Battery
@radio_api_bp.route("/battery/data")
def get_battery_data_table():
    upload_date = request.args.get('uploadDate')
    if upload_date == "None":
        upload_date = None

    df = BatteryService.get_data(upload_date)

    if df is None:
        return Response(status=200)

    export_excel = int(request.args.get('export_excel'))
    if export_excel:
        return save_df_to_excel(df, 'export.xlsx', current_app.config['TEMP_FOLDER'])

    columns = list(df.columns)
    # sorting
    column_to_sort_index = request.args.get('order[0][column]')
    sort_order = request.args.get('order[0][dir]')

    ordering = {
        'asc': True,
        'desc': False
    }

    order_by = columns[int(column_to_sort_index)]
    df = df.sort_values(by=[order_by], ascending=ordering[sort_order])

    total_filtered = df.shape[0]

    # pagination
    start = request.args.get('start', type=int)
    length = request.args.get('length', type=int)
    df = df.iloc[start:start + length]

    df_json = df.to_json(orient='records', date_format='iso')
    parsed = json.loads(df_json)

    return jsonify({'data': parsed,
                    'recordsFiltered': total_filtered,
                    'recordsTotal': df.shape[0],
                    'draw': request.args.get('draw', type=int)
                    })


@radio_api_bp.route("/battery/graphs", methods=["POST"])
def get_battery_charts():
    upload_date = request.form['uploadDate']
    if upload_date == "None":
        upload_date = None

    df = BatteryService.get_data(upload_date)

    if df is None:
        return Response(status=200)

    fig = plot_power_cut_times(df)
    fig = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)

    fig2 = plot_battery_remaining_time(df)
    fig2 = json.dumps(fig2, cls=plotly.utils.PlotlyJSONEncoder)

    # build JSON response
    response_data = {
        'power_cut_fig': fig,
        'rTime_fig': fig2,
    }

    return jsonify(response_data)
