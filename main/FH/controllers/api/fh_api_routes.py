import json
import os

import pandas as pd
import plotly
from flask import Blueprint, request, Response, jsonify, current_app

from main.FH.fh_utils import plot_rsl_pie_chart, get_fh_meteo_map, mlo_datatable_search, \
    extract_spec_from_mlo_file, generate_mlo_details_html_code, rsl_datatable_search, load_distribution_datatable_search
from main.FH.services.load_distribution_service import LoadDistributionService
from main.FH.services.pmon_service import PMONService
from main.FH.services.rsl_service import RSLLevelService
from main.Radio.controllers.api.radio_api_utils import save_df_to_excel
from main.utils.plot_utils import plot_percentage_range_barchart

fh_api_bp = Blueprint(
    'fh_api_bp', __name__,
    static_folder="static",
    template_folder="templates"
)


@fh_api_bp.route("/rsl/level")
def get_rsl_data():
    upload_date = request.args.get('uploadDate')
    if upload_date == "None":
        upload_date = None

    df = RSLLevelService.get_data(upload_date)

    if df is None:
        return Response(status=200)

    df['RSL DIFF'] = df['RSL REF'] - df['Max RSL']
    columns = ["Status",
               "IP",
               "Max RSL",
               "RSL REF",
               "RSL DIFF",
               "Name",
               "File",
               "Comment"
               ]
    df = df[columns]

    linkStatus = request.args.get('linkStatus')
    if linkStatus != "None":
        df = df.loc[df['Status'] == linkStatus]

    search_word = request.args.get('search[value]')
    if search_word:
        df = rsl_datatable_search(df, search_word)

    B2B = request.args.get('B2B')
    if B2B == "B2B":
        b2b_df = df.loc[df['Name'].str.contains('B2B') | df['File'].str.contains('B2B')]
        not_b2b_df = df.loc[~(df['Name'].str.contains('B2B') | df['File'].str.contains('B2B'))]
        b2b_df2 = not_b2b_df[(not_b2b_df['Name'].str.count(r'\d') < 6) | (not_b2b_df['File'].str.count(r'\d') < 6)]
        df = pd.concat([b2b_df, b2b_df2], axis=0)
    elif B2B == "ExcludeB2B":
        not_b2b_df = df.loc[~(df['Name'].str.contains('B2B') | df['File'].str.contains('B2B'))]
        df = not_b2b_df[~((not_b2b_df['Name'].str.count(r'\d') < 6) | (not_b2b_df['File'].str.count(r'\d') < 6))]

    OTN = request.args.get('OTN')
    if OTN == "Franchise":
        df = df.loc[df['Name'].str.lower().str.contains('franchise') | df['File'].str.lower().str.contains('franchise')]
    elif OTN == "Boutique":
        df = df.loc[df['Name'].str.lower().str.contains('boutique')
                    | df['Name'].str.lower().str.contains('btq')
                    | df['Name'].str.lower().str.contains('orange')]

    export_excel = int(request.args.get('export_excel'))
    if export_excel:
        df = df.drop_duplicates(['Name'], keep='last')
        return save_df_to_excel(df, 'export.xlsx', current_app.config['TEMP_FOLDER'])

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


@fh_api_bp.route("/rsl/charts", methods=["POST"])
def get_rsl_charts():
    upload_date = request.form['uploadDate']
    if upload_date == "None":
        upload_date = None

    df = RSLLevelService.get_data(upload_date)

    if df is None:
        return Response(status=200)

    # df = compute_rsl_diff(df.copy())

    B2B = request.form['B2B']
    if B2B == "B2B":
        b2b_df = df.loc[df['Name'].str.contains('B2B') | df['File'].str.contains('B2B')]
        not_b2b_df = df.loc[~(df['Name'].str.contains('B2B') | df['File'].str.contains('B2B'))]
        b2b_df2 = not_b2b_df[(not_b2b_df['Name'].str.count(r'\d') < 6) | (not_b2b_df['File'].str.count(r'\d') < 6)]
        df = pd.concat([b2b_df, b2b_df2], axis=0)
    elif B2B == "ExcludeB2B":
        not_b2b_df = df.loc[~(df['Name'].str.contains('B2B') | df['File'].str.contains('B2B'))]
        df = not_b2b_df[~((not_b2b_df['Name'].str.count(r'\d') < 6) | (not_b2b_df['File'].str.count(r'\d') < 6))]

    OTN = request.form['OTN']
    if OTN == "Franchise":
        df = df.loc[df['Name'].str.lower().str.contains('franchise') | df['File'].str.lower().str.contains('franchise')]
    elif OTN == "Boutique":
        df = df.loc[df['Name'].str.lower().str.contains('boutique')
                    | df['Name'].str.lower().str.contains('btq')
                    | df['Name'].str.lower().str.contains('orange')]

    rsl_fig = plot_rsl_pie_chart(df)
    rsl_fig = json.dumps(rsl_fig, cls=plotly.utils.PlotlyJSONEncoder)

    # data filter
    linkStatus = request.form['linkStatus']
    if linkStatus != "None":
        df = df.loc[df['Status'] == linkStatus]

    # map
    fh_map = get_fh_meteo_map(df)

    # build JSON response
    response_data = {
        'rsl_fig': rsl_fig,
        'map_fh': fh_map._repr_html_()
    }

    return jsonify(response_data)


@fh_api_bp.route("/mlo/all")
def get_list_mlo():
    mlo_list = [file for file in os.listdir(current_app.config['MLO_FOLDER'])
                if ('imprime' not in file) and file.lower().endswith('.xlsx')]
    mlo_df = pd.DataFrame({'MLO': mlo_list})

    if mlo_df is None:
        return Response(status=200)

    search_word = request.args.get('search[value]')
    if search_word:
        mlo_df = mlo_datatable_search(mlo_df, search_word)

    total_filtered = mlo_df.shape[0]

    # pagination
    start = request.args.get('start', type=int)
    length = request.args.get('length', type=int)
    mlo_df = mlo_df.iloc[start:start + length]

    df_json = mlo_df.to_json(orient='records', date_format='iso')
    parsed = json.loads(df_json)

    return jsonify({'data': parsed,
                    'recordsFiltered': total_filtered,
                    'recordsTotal': mlo_df.shape[0],
                    'draw': request.args.get('draw', type=int)
                    })


@fh_api_bp.route("/mlo/details", methods=["POST"])
def get_mlo_details():
    mlo_name = request.form['mlo_name']
    filepath = ''
    if mlo_name == "default":
        mlo_list = [file for file in os.listdir(current_app.config['MLO_FOLDER'])
                    if ('imprime' not in file) and file.lower().endswith('.xlsx')]
        mlo_name = mlo_list[0]
        filepath = os.path.join(current_app.config['MLO_FOLDER'], mlo_name)
    else:
        filepath = os.path.join(current_app.config['MLO_FOLDER'], mlo_name)

    mw_link = extract_spec_from_mlo_file(filepath)
    if mw_link is not None:
        mlo_html_code = generate_mlo_details_html_code(mw_link, mlo_name)
        # build JSON response
        response_data = {
            'mlo_html_code': mlo_html_code
        }

        return jsonify(response_data)
    else:
        return Response(status=200)


# Link Capacity
@fh_api_bp.route("/capacity/load_distribution")
def get_load_distribution_data_table():
    upload_date = request.args.get('uploadDate')
    if upload_date == "None":
        upload_date = None

    df = LoadDistributionService.get_data(upload_date)

    if df is None:
        return Response(status=200)

    search_word = request.args.get('search[value]')
    if search_word:
        df = load_distribution_datatable_search(df, search_word)

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


@fh_api_bp.route("/load_distribution/chart", methods=["POST"])
def get_load_distribution_charts():
    upload_date = request.form['uploadDate']
    if upload_date == "None":
        upload_date = None

    df = LoadDistributionService.get_data(upload_date)

    if df is None:
        return Response(status=200)

    fig = plot_percentage_range_barchart(df, "Max Daily RX Load")
    fig = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)

    # build JSON response
    response_data = {
        'load_fig': fig,
    }

    return jsonify(response_data)


# PMON
@fh_api_bp.route("/pmon/table")
def get_pmon_data_table():
    upload_date = request.args.get('uploadDate')
    if upload_date == "None":
        upload_date = None

    df = PMONService.get_data(upload_date)

    if df is None:
        return Response(status=200)

    df_links = RSLLevelService.get_fh_links()
    df_links = df_links.drop_duplicates(['IP'])
    df_links.reset_index(drop=True, inplace=True)

    df = pd.merge(df, df_links, on='IP', how='left')

    columns = [
        "Name",
        "IP",
        "UAS",
        "ES",
        "BBE",
        "SES",
        "RSL Max",
        "RSL Min",
        "Comments",
        "High Value"
    ]
    df = df[columns]

    pmonValueFilter = request.args.get('pmonValueFilter')
    if pmonValueFilter == "UAS":
        df = df.loc[df["UAS"] != 0]
    elif pmonValueFilter == "SES":
        df = df.loc[df["SES"] != 0]
    elif pmonValueFilter == "UASandSES":
        df = df.loc[(df["UAS"] != 0) & (df["SES"] != 0)]
    elif pmonValueFilter == "UASorSES":
        df = df.loc[(df["UAS"] != 0) | (df["SES"] != 0)]

    OTN = request.args.get('OTN')
    if OTN == "Franchise":
        df = df.loc[df['Name'].str.lower().fillna('').str.contains('franchise')]
    elif OTN == "Boutique":
        df = df.loc[df['Name'].str.lower().fillna('').str.contains('boutique')
                    | df['Name'].str.lower().fillna('').str.contains('btq')
                    | df['Name'].str.lower().fillna('').str.contains('orange')]

    is_high_value = request.args.get('is_high_value')
    if is_high_value == "OUI":
        df = df.loc[df['High Value'].fillna('').str.contains("OUI")]
    elif is_high_value == "NON":
        df = df.loc[~df['High Value'].fillna('').str.contains("OUI")]

    export_excel = int(request.args.get('export_excel'))
    if export_excel:
        return save_df_to_excel(df, 'export.xlsx', current_app.config['TEMP_FOLDER'])

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
