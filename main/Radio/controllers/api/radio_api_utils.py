import json
import os

import plotly
from main.utils.plot_utils import *
from datetime import timedelta
import gspread

from oauth2client.service_account import ServiceAccountCredentials

pd.options.mode.chained_assignment = None


def get_alarm_group_active(df):
    return df[df["Clearance Status"] == "Uncleared"]


def get_bagots(df):
    index_bagots = df[(df['Alarm Source'].str.contains("RNC|BSC|OSS")) &
                      (~df['Alarm Source'].str.contains("/"))].index
    df = df.drop(index_bagots, inplace=False)
    return df


def get_sub_dataset(df, alarm_group_name):
    sub_df = pd.DataFrame()

    if alarm_group_name == "RET Alarms":
        sub_df = df.loc[df["Name"].isin([
            "ALD Maintenance Link Failure",
            "RET Antenna Data Loss",
            "RET Antenna Motor Fault",
            "RET Antenna Not Calibrated",
            "RET Antenna Running Data and Configuration Mismatch",
            "RF Unit ALD Current Out of Range",
            "RF Unit ALD Switch Configuration Mismatch"
        ])]

    elif alarm_group_name == "CLOCK Alarms":
        sub_df = df.loc[df['Name'].str.contains("Clock")]

    elif alarm_group_name == "ENV Alarms":
        sub_df = df.loc[df["Name"].isin([
            "battery current out of range",
            "battery Not in Position",
            "AC surge protector fault",
            "Base Station DC Power Supply Abnormal",
            "battery Power Unavailable",
            "battery Temperature Unacceptable",
            "BBU DC Output Out of Range",
            "BBU Fan Stalled",
            "Energie Non Urgente",
            "Fan Stalled",
            "Load Fuse Broken",
            "Mains Input Out of Range",
            "Monitoring Device Maintenance Link Failure",
            "Sensor Failure",
            "TEC Cooler Fault"
        ])]

    elif alarm_group_name == "TDD Alarms":
        sub_df = df.loc[df['Alarm Source'].str.contains("_TC|_TD|_TF")]

    elif alarm_group_name == "BH Alarms":
        sub_df = df.loc[df['Alarm Source'].str.contains("_BH|_BO")]

    elif alarm_group_name == "VSWR Alarms":
        sub_df = df.loc[df['Name'].str.contains("VSWR")]

    elif alarm_group_name == "CPRI Alarms":
        sub_df = df.loc[df['Name'].str.contains("CPRI")]

    elif alarm_group_name == "RTWP Alarms":
        sub_df = df.loc[df['Name'].str.contains("RTWP")]

    elif alarm_group_name == "Interference Alarms":
        sub_df = df.loc[df.Name == 'Custom Interference alarm']

    elif alarm_group_name == "License Alarms":
        sub_df = df.loc[df['Name'].str.contains("License")]
    elif alarm_group_name == "5G Alarms":
        sub_df = df.loc[
            df['NE Type'].str.contains("5G|NR", case=False) |
            df['Name'].str.contains("5G|NR|Beam Failure|Signal Lost", case=False)]

    return sub_df


def filter_ssv_status(df, ssv_status):
    new_df = pd.DataFrame()
    df['Home Subnet'] = df['Home Subnet'].fillna('').astype(str)
    if ssv_status == "SSVOK/FN8OK":
        new_df = df.loc[(df['Home Subnet'].str.contains("SSV_OK|SSVOK|SSV OK") &
                         df['Home Subnet'].str.contains("FN8OK|FN8_OK|FN8 OK")) | (df['Home Subnet'] == 'ROOT')]

    elif ssv_status == "SSVOK/FN8NOK":
        new_df = df.loc[df['Home Subnet'].str.contains("SSV_OK|SSVOK|SSV OK") &
                        df['Home Subnet'].str.contains("FN8NOK|FN8_NOK|FN8 NOK")]

    elif ssv_status == "SSVNOK":
        new_df = df.loc[df['Home Subnet'].str.contains("SSV_NOK|SSVNOK|SSV NOK")]

    elif ssv_status == "SSVOK":
        new_df = df.loc[df['Home Subnet'].str.contains("SSV_OK|SSVOK|SSV OK")]

    if new_df.empty:
        return df

    return new_df


def get_alarm_group(df_group):
    df_group_count = df_group["Name"].value_counts()
    df_count = pd.DataFrame([
        (sum(get_bagots(df_group[df_group["Name"] == index])["Occurrence Times"]),
         len(get_bagots(df_group[df_group["Name"] == index])["Alarm Source"].unique()))
        for index in df_group_count.index
    ],
        columns=[
            'Occurrence Times',
            'Total site']
    )

    df_count.insert(loc=0, column='Count', value=df_group_count.values)
    df_count.insert(loc=0, column='Alarm Name', value=df_group_count.index)

    df_count = df_count.sort_values(by="Total site", ascending=False)

    last_row = pd.DataFrame({'Alarm Name': ['Total'],
                             'Count': [df_group_count.values.sum()],
                             'Occurrence Times': [sum(df_count['Occurrence Times'])],
                             'Total site': [sum(df_count['Total site'])]})
    df_alarm_group = pd.concat([df_count, last_row], axis=0, ignore_index=True)

    return df_alarm_group


def search(df, word):
    word = word.lower()
    columns = ["Severity",
               "Name",
               "NE Type",
               "Alarm Source",
               "Clearance Status",
               "Home Subnet"]
    dfs = [df[df[col].str.lower().fillna('').str.contains(word)] for col in columns]
    return pd.concat(dfs).drop_duplicates()


def get_radio_home_charts(df, df_ev, df_active):
    df_src = get_bagots(df)
    # df_src_active = get_bagots(df_active)

    fig1 = plot_pie_chart(df_active)
    fig2 = plot_alarm_trend(df_ev)
    fig3 = plot_top_n_horizontal(df_active, "Name", 10, "", "Total", "Alarmes")
    fig4 = plot_top_n_horizontal(df_src, "Alarm Source", 10, "", "Total", "Source")
    fig5 = plot_alarm_trend_by_date(df_ev)
    fig6 = plot_duration_intervals(df)

    figs = [fig1, fig2, fig3, fig4, fig5, fig6]

    figs = [json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder) for fig in figs]

    return figs


def create_alarm_grp_charts(df_active, df_bagots, df_ev, df_active_bagots):
    fig = plot_pie_chart(df_active)
    fig2 = plot_top_n_horizontal(df_bagots, "Alarm Source", 10, "", "Total", "Source")
    fig3 = plot_top_n_horizontal(df_active, "Name", 10, "", "Total", "Alarms")
    fig4 = plot_alarm_trend(df_ev)
    fig5 = plot_severity_barchart_with(df_active_bagots, "Alarm Source", 15)
    fig6 = plot_alarm_trend_by_date(df_ev)
    figs = [fig, fig2, fig3, fig4, fig5, fig6]
    figs = [json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder) for fig in figs]

    return figs


def subtract_days_from_date(date, days):
    subtracted_date = pd.to_datetime(date) - timedelta(days=days)
    subtracted_date = subtracted_date.strftime("%d-%m-%Y %H:%M")
    return subtracted_date


def subtract_hours_from_date(date, hours):
    subtracted_date = pd.to_datetime(date) - timedelta(hours=hours)
    subtracted_date = subtracted_date.strftime("%Y/%m/%d %H:%M:%S")
    return subtracted_date


def select_date_range(df, start, end):
    mask = (df['Last Occurred (NT)'] > start) & (df['Last Occurred (NT)'] <= end)
    df = df.loc[mask]
    return df


def filter_date(df, last_occurred):
    end_date = df.sort_values(by="Last Occurred (NT)", ascending=False)["Last Occurred (NT)"].values[0]
    if last_occurred == "less_than_1_hour":
        start_date = subtract_hours_from_date(end_date, 1)
        df = select_date_range(df, start_date, end_date)
    elif last_occurred == "less_than_12_hours":
        start_date = subtract_hours_from_date(end_date, 12)
        df = select_date_range(df, start_date, end_date)
    elif last_occurred == "less_than_24_hours":
        start_date = subtract_hours_from_date(end_date, 24)
        df = select_date_range(df, start_date, end_date)
    elif last_occurred == "last_3_days":
        start_date = subtract_days_from_date(end_date, 3)
        df = select_date_range(df, start_date, end_date)
    elif last_occurred == "last_7_days":
        start_date = subtract_days_from_date(end_date, 7)
        df = select_date_range(df, start_date, end_date)

    return df


def get_home_cards(df_active, df):
    df_tdd = get_sub_dataset(df_active, "TDD Alarms")
    df_bh = get_sub_dataset(df_active, "BH Alarms")
    df_env = get_sub_dataset(df_active, "ENV Alarms")
    df_ret = get_sub_dataset(df_active, "RET Alarms")
    df_clock = get_sub_dataset(df_active, "CLOCK Alarms")
    df_vswr = get_sub_dataset(df_active, "VSWR Alarms")
    df_cpri = get_sub_dataset(df_active, "CPRI Alarms")
    df_rtwp = get_sub_dataset(df_active, "RTWP Alarms")
    df_interf = get_sub_dataset(df_active, "Interference Alarms")
    df_lice = get_sub_dataset(df_active, "License Alarms")
    df_5g = get_sub_dataset(df_active, "5G Alarms")  # Nouveau

    cards = {
        "card_active_alarm": f'{df_active.shape[0]:,}',
        "card_top_bagots": f'{df.shape[0]:,}',
        "card_tdd_active": f'{df_tdd.shape[0]:,}',
        "card_bh_active": f'{df_bh.shape[0]:,}',
        "card_active_clock": f'{df_clock.shape[0]:,}',
        "card_active_ret": f'{df_ret.shape[0]:,}',
        "card_active_env": f'{df_env.shape[0]:,}',
        "card_active_vswr": f'{df_vswr.shape[0]:,}',
        "card_active_cpri": f'{df_cpri.shape[0]:,}',
        "card_active_rtwp": f'{df_rtwp.shape[0]:,}',
        "card_active_interference": f'{df_interf.shape[0]:,}',
        "card_active_license": f'{df_lice.shape[0]:,}',
        "card_active_5g": f'{df_5g.shape[0]:,}',  # Nouveau
    }
    return cards


def read_sheet_to_df(file_name, sheet_name, credentials_file_path, start_at_row=0):
    try:
        # Authorize access to the Google Sheets API
        scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_file_path, scope)
        client = gspread.authorize(creds)

        # Open the desired Google Sheet by name
        sheet = client.open(file_name).worksheet(sheet_name)

        # Get the data in the sheet and convert it to a Pandas DataFrame
        data = sheet.get_all_values()
        df = pd.DataFrame(data[start_at_row + 1:], columns=data[start_at_row])

        # Convert any date columns to datetime objects
        date_columns = [col for col in df.columns if "date" in col.lower()]
        for col in date_columns:
            df[col] = pd.to_datetime(df[col], infer_datetime_format=True)

        return df
    except(Exception,):
        return None


def write_df_to_sheet(filename, sheet_name, credentials_file_path, prs_df):
    try:
        # Authorize access to the Google Sheets API
        scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_file_path, scope)
        client = gspread.authorize(creds)

        # Open the desired Google Sheet by name
        sheet = client.open(filename).worksheet(sheet_name)

        # Clear all values in the sheet
        sheet.clear()

        # Convert Date Type Columns
        prs_df["Time"] = pd.to_datetime(prs_df["Time"])
        new_col = prs_df["Time"].dt.strftime("%d/%m/%Y")
        prs_df.insert(loc=0, column='Date', value=new_col)
        prs_df["Time"] = prs_df["Time"].dt.strftime("%d-%m-%Y %H:%M")

        # Write the DataFrame to the sheet
        sheet.update([prs_df.columns.values.tolist()] + prs_df.values.tolist(), value_input_option='USER_ENTERED')
    except(Exception,):
        pass


def save_df_to_excel(df, filename, save_dir):
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)

    filepath = os.path.join(save_dir, filename)

    file = pd.ExcelWriter(filepath, engine='xlsxwriter', datetime_format='YYYY-MM-DD HH:MM:SS', mode='wb')
    df.to_excel(file, index=False, sheet_name='Sheet1')
    file.close()

    return filepath

