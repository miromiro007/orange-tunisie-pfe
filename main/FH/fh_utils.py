import os
import platform
import re
import time

import folium
import numpy as np
import pandas as pd
import plotly_express as px
from fuzzywuzzy import fuzz

COLORS_MAP = {'Lien_dépointé_(>10)': '#e60000',
              'Lien_dépointé_(<10)': '#ff9900',
              'Lien_OK': '#3399ff'}


def calculate_similarity_score(search_word, filename):
    # Calculer le score de similarité  avec fuzz.token_set_ratio
    score = fuzz.token_set_ratio(search_word.lower(), filename.lower())
    return score


def find_most_matched_filename(filelist, search_word):
    matching_scores = []

    for index, filename in enumerate(filelist):
        score = calculate_similarity_score(search_word, filename)
        matching_scores.append((filename, score, index))

    # trier dans l'ordre descendant les fichiers selon le score de similarité
    sorted_files = sorted(matching_scores, key=lambda x: x[1], reverse=True)

    if sorted_files:
        most_matched_filename = sorted_files[0][0]
        matched_index = sorted_files[0][2]
        return most_matched_filename, matched_index
    else:
        return None, None


def extract_spec_from_mlo_file(filepath):
    col_names = ["col1", "col2", "col3", "col4"]
    try:
        df = pd.read_excel(filepath, names=col_names)
        mw_link = {
            "EndA": {},
            "EndB": {}
        }

        key_col = None
        common_columns = ["Radio Type", "Modulation", "Operating Mode"]
        for index, value in df.iterrows():
            if (value["col2"] is np.nan) and (value["col3"] is np.nan and (value["col4"] is np.nan)):
                pass
            else:
                if (value["col1"] == "Link ID") or (value["col1"] == "Capacity"):
                    mw_link["EndA"][value["col1"]] = value["col2"]
                    mw_link["EndA"][value["col3"]] = value["col4"]
                    mw_link["EndB"][value["col1"]] = value["col2"]
                    mw_link["EndB"][value["col3"]] = value["col4"]
                else:
                    if value["col1"] is not np.nan:
                        key_col = value["col1"].replace("\n", " ")
                        mw_link["EndA"][key_col] = {}
                        mw_link["EndB"][key_col] = {}
                        if value["col2"] in common_columns:
                            mw_link["EndA"][key_col][value["col2"]] = value["col3"]
                            mw_link["EndB"][key_col][value["col2"]] = value["col3"]
                        else:
                            mw_link["EndA"][key_col][value["col2"]] = value["col3"]
                            mw_link["EndB"][key_col][value["col2"]] = value["col4"]
                    elif (value["col1"] is np.nan) and (value["col2"] is not np.nan):
                        if key_col is not None:
                            if value["col2"] in common_columns:
                                mw_link["EndA"][key_col][value["col2"]] = value["col3"]
                                mw_link["EndB"][key_col][value["col2"]] = value["col3"]
                            else:
                                mw_link["EndA"][key_col][value["col2"]] = value["col3"]
                                mw_link["EndB"][key_col][value["col2"]] = value["col4"]

        return mw_link
    except (Exception,):
        return None


def add_link_ref_rsl_to_df(df, dir_mlo):
    df['File'] = ''
    df['RSL REF'] = ''
    df['EndA_Name'] = ''
    df['EndA_Latitude'] = ''
    df['EndA_Longitude'] = ''
    df['EndB_Name'] = ''
    df['EndB_Latitude'] = ''
    df['EndB_Longitude'] = ''
    mlo_list = [file for file in os.listdir(dir_mlo) if ('imprime' not in file) and file.lower().endswith('.xlsx')]

    for mlo in mlo_list:
        filepath = os.path.join(dir_mlo, mlo)
        try:
            mw_link = extract_spec_from_mlo_file(filepath)
            df.loc[df['Name'].str.contains(os.path.splitext(mlo)[0], regex=False), 'RSL REF'] = \
                mw_link['EndA']['Performance']['Main Rx Level @ TX max (dBm)']

            df.loc[df['Name'].str.contains(os.path.splitext(mlo)[0], regex=False), 'File'] = mlo
            # EndA info
            df.loc[df['Name'].str.contains(os.path.splitext(mlo)[0], regex=False), 'EndA_Name'] = \
                mw_link['EndA']['Link Ends']['Property name']
            df.loc[df['Name'].str.contains(os.path.splitext(mlo)[0], regex=False), 'EndA_Latitude'] = \
                mw_link['EndA']['Link Ends']['Property Latitude'].replace('N', '').replace(',', '.')
            df.loc[df['Name'].str.contains(os.path.splitext(mlo)[0], regex=False), 'EndA_Longitude'] = \
                mw_link['EndA']['Link Ends']['Property Longitude'].replace('E', '').replace(',', '.')
            # EndB info
            df.loc[df['Name'].str.contains(os.path.splitext(mlo)[0], regex=False), 'EndB_Name'] = \
                mw_link['EndB']['Link Ends']['Property name']
            df.loc[df['Name'].str.contains(os.path.splitext(mlo)[0], regex=False), 'EndB_Latitude'] = \
                mw_link['EndB']['Link Ends']['Property Latitude'].replace('N', '').replace(',', '.')
            df.loc[df['Name'].str.contains(os.path.splitext(mlo)[0], regex=False), 'EndB_Longitude'] = \
                mw_link['EndB']['Link Ends']['Property Longitude'].replace('E', '').replace(',', '.')

        except (Exception,):
            continue

    df1 = df[df['RSL REF'] != ''].copy()
    df2 = df[df['RSL REF'] == ''].copy()

    mlo_list2 = [' '.join(re.findall(r'\w+', os.path.splitext(mlo)[0].replace('_', ' '))) for mlo in mlo_list]
    links_name = list(np.unique(df2.Name.values))

    for link in links_name:
        result = find_most_matched_filename(mlo_list2, ' '.join(re.findall(r'\w+', link.replace('_', ' '))))
        filepath = os.path.join(dir_mlo, mlo_list[result[1]])
        try:
            mw_link = extract_spec_from_mlo_file(filepath)
            df2.loc[(df2['Name'] == link), 'RSL REF'] = mw_link['EndA']['Performance']['Main Rx Level @ TX max (dBm)']

            df2.loc[(df2['Name'] == link), 'File'] = mlo_list[result[1]]

            # EndA info
            df2.loc[(df2['Name'] == link), 'EndA_Name'] = mw_link['EndA']['Link Ends']['Property name']
            df2.loc[(df2['Name'] == link), 'EndA_Latitude'] = mw_link['EndA']['Link Ends']['Property Latitude'].replace(
                'N', '').replace(',', '.')
            df2.loc[(df2['Name'] == link), 'EndA_Longitude'] = mw_link['EndA']['Link Ends'][
                'Property Longitude'].replace('E', '').replace(',', '.')
            # EndB info
            df2.loc[(df2['Name'] == link), 'EndB_Name'] = mw_link['EndB']['Link Ends']['Property name']
            df2.loc[(df2['Name'] == link), 'EndB_Latitude'] = mw_link['EndB']['Link Ends']['Property Latitude'].replace(
                'N', '').replace(',', '.')
            df2.loc[(df2['Name'] == link), 'EndB_Longitude'] = mw_link['EndB']['Link Ends'][
                'Property Longitude'].replace('E', '').replace(',', '.')

        except (Exception,):
            continue

    df = pd.concat([df1, df2], axis=0)
    df.reset_index(drop=True, inplace=True)

    return df


def preprocessing(df):
    df["Min RSL"] = df["Min RSL"].str.lower().str.replace("dBm".lower(), "")
    df["Avg RSL"] = df["Avg RSL"].str.lower().str.replace("dBm".lower(), "")
    df["Max RSL"] = df["Max RSL"].str.lower().str.replace("dBm".lower(), "")
    df["RSL REF"] = df["RSL REF"].str.lower().str.replace("dBm".lower(), "")
    df['RSL REF'] = df['RSL REF'].str.lower().str.replace(',', '.')

    df["Min RSL"] = df["Min RSL"].astype(float)
    df["Avg RSL"] = df["Avg RSL"].astype(float)
    df["Max RSL"] = df["Max RSL"].astype(float)
    df["RSL REF"] = df["RSL REF"].astype(float)

    df["EndA_Latitude"] = df["EndA_Latitude"].str.replace(r'\([^)]*\)', '', regex=True)
    df["EndA_Longitude"] = df["EndA_Longitude"].str.replace(r'\([^)]*\)', '', regex=True)
    df["EndB_Latitude"] = df["EndB_Latitude"].str.replace(r'\([^)]*\)', '', regex=True)
    df["EndB_Longitude"] = df["EndB_Longitude"].str.replace(r'\([^)]*\)', '', regex=True)

    df["EndA_Latitude"] = df["EndA_Latitude"].astype(float)
    df["EndA_Longitude"] = df["EndA_Longitude"].astype(float)
    df["EndB_Latitude"] = df["EndB_Latitude"].astype(float)
    df["EndB_Longitude"] = df["EndB_Longitude"].astype(float)

    return df


def compute_rsl_diff(df):
    # new col for diff rsl
    df['RSL DIFF'] = df['RSL REF'] - df['Max RSL']
    df['RSL DIFF'] = df['RSL DIFF'].astype(str)
    # Remove the minus sign from the 'values' column
    df['RSL DIFF'] = df['RSL DIFF'].str.replace('-', '')
    # Convert the 'values' column back to float
    df['RSL DIFF'] = df['RSL DIFF'].astype(float)

    # separate dataframe
    df1 = df.loc[(df['Max RSL'] > df['RSL REF'])].copy()
    df1.reset_index(inplace=True, drop=True)

    df2 = df.loc[~(df['Max RSL'] > df['RSL REF'])].copy()
    df2.reset_index(inplace=True, drop=True)

    # label df1
    df1["Status"] = "Lien_OK"

    # labelisation du status du lien en fonction de RSL DIFF
    conditions = [
        (df2['RSL DIFF'] >= 0) & (df2['RSL DIFF'] < 5),
        (df2['RSL DIFF'] >= 5) & (df2['RSL DIFF'] < 10),
        (df2['RSL DIFF'] >= 10)
    ]
    values = ['Lien_OK', 'Lien_dépointé_(<10)', 'Lien_dépointé_(>10)']
    df2['Status'] = pd.Series(pd.Categorical(np.select(conditions, values)))

    df_merged = pd.concat([df1, df2], axis=0)
    df_merged.reset_index(inplace=True, drop=True)

    return df_merged


def get_fh_meteo_map(df):
    df = df.drop_duplicates(['Name'], keep='last')
    df.reset_index(drop=True, inplace=True)

    m = folium.Map(location=[33.8869, 9.5375], zoom_start=7, control_scale=True, tiles="stamenterrain")

    COLORS_MAP = {'Lien_dépointé_(>10)': 'red',
                  'Lien_dépointé_(<10)': 'orange',
                  'Lien_OK': 'blue'}

    for index, row in df.iterrows():
        start_point = [row['EndA_Latitude'], row['EndA_Longitude']]
        end_point = [row['EndB_Latitude'], row['EndB_Longitude']]

        line = folium.PolyLine(locations=[start_point, end_point], color=COLORS_MAP[row['Status']])
        tooltip = f"{row['EndA_Name']}_{row['EndB_Name']}: {row['Status']}"

        line.add_child(folium.Tooltip(tooltip))
        m.add_child(line)

    return m


def get_file_creation_date(path_to_file):
    """
    Try to get the date that a file was created, falling back to when it was
    last modified if that isn't possible.
    See http://stackoverflow.com/a/39501288/1709587 for explanation.
    """
    if platform.system() == 'Windows':
        return time.ctime(os.path.getctime(path_to_file))
    else:
        stat = os.stat(path_to_file)
        try:
            return stat.st_birthtime
        except AttributeError:
            # We're probably on Linux. No easy way to get creation dates here,
            # so we'll settle for when its content was last modified.
            return stat.st_mtime


def plot_rsl_pie_chart(df):
    df_svr = pd.DataFrame({"Total": df.Status.value_counts()})
    fig = px.pie(df_svr, values='Total', names=df_svr.index, color=df_svr.index, title='RSL Level',
                 color_discrete_map=COLORS_MAP)
    fig.update_traces(hole=.6, hoverinfo="label+percent+name", textinfo='value',
                      marker=dict(line=dict(color='#a0a0ab', width=0.5)))
    fig.update_layout(
        title_text="",
        template='simple_white',
        legend=dict(
            yanchor="top",
            y=1.02,
            xanchor="left",
            x=1
        ),
        hovermode='closest',
        hoverdistance=2,
        margin=dict(l=0, r=0, t=0, b=0),
        # Add annotations in the center of the donut pies.
        annotations=[dict(text="" + str(df.shape[0]) + "", x=0.50, y=0.5, font_size=20, showarrow=False)])

    return fig


def mlo_datatable_search(df, word):
    word = word.lower()
    df = df.loc[df['MLO'].str.lower().str.contains(word)]
    return df


def rsl_datatable_search(df, word):
    word = word.lower()
    columns = ["Status",
               "Name",
               "File",
               "IP",
               "Comment"
               ]
    dfs = [df[df[col].str.lower().str.contains(word)] for col in columns]
    return pd.concat(dfs).drop_duplicates()


def load_distribution_datatable_search(df, word):
    word = word.lower()
    columns = ['IP',
               'Slot',
               'Name']
    dfs = [df[df[col].str.lower().str.contains(word)] for col in columns]
    return pd.concat(dfs).drop_duplicates()


def generate_mlo_details_html_code(mw_link, filename=None):
    html_code = '''
    <table class="tg table table-striped table-hover">
        <thead>
            <tr>
                <th class="tg-hle0" colspan="3">LINK SPECIFICATION</th>
                <th class="tg-hle0">
                    <a href="#" onclick="delete_mlo_file('{}');">  <i class="fa fa-trash" aria-hidden="true"
                                                       style="color:#ff7f00; width:20px; height:20px"></i>
                    </a>
                </th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td class="tg-g7sd">Link ID</td>
                <td class="tg-lboi">{}</td>
                <td class="tg-g7sd">Length&nbsp;&nbsp;&nbsp;(m)</td>
                <td class="tg-lboi">{}</td>
            </tr>
            <tr>
                <td class="tg-g7sd">Capacity</td>
                <td class="tg-lboi">{}</td>
                <td class="tg-g7sd">Calculation&nbsp;&nbsp;&nbsp;Method</td>
                <td class="tg-lboi">{}</td>
            </tr>
            <tr>
                <td class="tg-0pky"></td>
                <td class="tg-0pky"></td>
                <td class="tg-uzvj">End A</td>
                <td class="tg-uzvj">End B</td>
            </tr>
    '''.format(filename,
               mw_link['EndA']['Link ID'],
               mw_link['EndA']['Length (m)'],
               mw_link['EndA']['Capacity'],
               mw_link['EndA']['Calculation Method'])

    for key in mw_link["EndA"].keys():
        if isinstance(mw_link["EndA"][key], dict):
            html_code += '''
                <tr>
                    <td class="tg-0pky"></td>
                    <td class="tg-7btt" colspan="3">{}</td>
                </tr>
            '''.format(key)
            for k in mw_link["EndA"][key].keys():
                html_code += '''
                    <tr>
                        <td class="tg-g7sd"></td>
                        <td class="tg-g7sd">{} &nbsp;&nbsp;&nbsp;</td>
                        <td class="tg-9wq8">{}</td>
                        <td class="tg-9wq8">{}</td>
                    </tr>
                '''.format(k,
                           mw_link["EndA"][key][k],
                           mw_link["EndB"][key][k])

    # Close the HTML code
    html_code += '''
        </tbody>
    </table>
    '''

    return html_code
