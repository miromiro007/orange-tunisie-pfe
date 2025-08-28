import pandas as pd
import plotly.graph_objects as go
import plotly_express as px

COLORS_MAP = {'Critical': '#e60000',
              'Major': '#ff9900',
              'Minor': '#ffcc00',
              'Warning': '#3399ff'}


def convert_duration_to_minutes(duration_str):
    parts = duration_str.split()
    hours, minutes, seconds = 0, 0, 0
    i = 0
    while i < len(parts):
        value = int(parts[i])
        unit = parts[i + 1].lower()
        if unit.startswith('hour'):
            hours = value
        elif unit.startswith('minute'):
            minutes = value
        elif unit.startswith('second'):
            seconds = value
        i += 2  # Skip to the next unit
    return hours * 60 + minutes + seconds / 60


def load_dataset_csv(file_path):
    df = pd.read_csv(file_path, skiprows=5)
    return df


def prepare_dataset(df):
    # df.rename(columns={" ": "Alarm Type"}, inplace=True)
    df["Last Occurred (NT)"] = pd.to_datetime(df["Last Occurred (NT)"])
    df["First Occurred (NT)"] = pd.to_datetime(df["First Occurred (NT)"])
    df["Last_Occurred_ymd"] = df["Last Occurred (NT)"].dt.strftime("%Y-%m-%d")
    df["First_Occurred_ymd"] = df["First Occurred (NT)"].dt.strftime("%Y-%m-%d")
    return df


def plot_top_n(df, column_name, n, title, xaxis_title, yaxis_title):
    df_top_10 = pd.DataFrame({"count": df[column_name].value_counts()}).sort_values(by="count", ascending=False).head(n)
    fig = px.bar(df_top_10, y="count", x=df_top_10.index, color=df_top_10.index, title=title)
    fig.update_layout(showlegend=False,
                      template='simple_white',
                      xaxis_title=xaxis_title,
                      yaxis_title=yaxis_title,
                      )
    return fig


def plot_top_n_horizontal(df, column_name, n, title, xaxis_title, yaxis_title):
    top_n = df[column_name].value_counts().nlargest(n).sort_values()

    fig = go.Figure(go.Bar(
        x=top_n.values,
        y=top_n.index,
        orientation='h',
        text=top_n.values,
        texttemplate="", #%{x}
        textposition='inside'
    ))

    fig.update_layout(
        title=title,
        template='simple_white',
        showlegend=False,
        xaxis_title=xaxis_title,
        yaxis_title=yaxis_title,
        margin=dict(l=0, r=0, t=0, b=0)
    )

    return fig


def plot_bar_chart(df, column_name):
    sub_df = pd.DataFrame({"Total": df[column_name].value_counts()}).sort_values(by="Total", ascending=True)
    fig = px.bar(sub_df, y="Total", x=sub_df.index, )
    fig.update_layout(showlegend=False,
                      template='simple_white',
                      xaxis_title=column_name,
                      yaxis_title="Total",
                      )
    return fig


def plot_line_chart(df, column_name):
    sub_df = pd.DataFrame({"Total": df[column_name].value_counts()}).sort_values(by="Total", ascending=True)
    fig = px.line(sub_df, y="Total", x=sub_df.index, )
    fig.update_layout(showlegend=False,
                      template='simple_white',
                      xaxis_title=column_name,
                      yaxis_title="Total",
                      )
    return fig


def plot_pie_chart(df):
    df_svr = pd.DataFrame({"Total": df.Severity.value_counts()})
    fig = px.pie(df_svr, values='Total', names=df_svr.index, color=df_svr.index, title='Alarm By Severity',
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


def plot_severity_barchart_with(df, column_name, n):
    df_grp = df.groupby(['Severity', column_name]).size().reset_index(name='Total')
    df_grp = df_grp.sort_values(by='Total', ascending=False).head(n)
    fig = px.bar(df_grp, x=column_name, y='Total', color='Severity', text='Total',
                 color_discrete_map=COLORS_MAP)
    fig.update_traces(textposition='auto')
    return fig


def plot_alarm_trend(df):
    df_ev = df.groupby(["Save Time", "Severity"]).size().reset_index(name="Total")
    # df_ev["date"] = pd.to_datetime(df_ev["Save Time"].dt.strftime("%d/%m/%Y"))

    fig = px.bar(
        df_ev,
        x="Save Time",
        y=df_ev["Total"],
        color="Severity",
        color_discrete_map=COLORS_MAP,
        text=df_ev["Total"],
    )

    fig.update_traces(textposition="auto")
    fig.update_layout(
        xaxis_tickangle=-45,
        xaxis=dict(
            type="category",
            categoryorder='category ascending'),
        barmode="group",
        legend=dict(orientation="h", yanchor="top", y=1.02, xanchor="right", x=1, font=dict(size=10)),
        margin=dict(l=0, r=0, t=50, b=0)
    )

    return fig


def plot_alarm_trend_by_date(df):
    df_ev = df.groupby(["Save Time"]).size().reset_index(name="Total")
    # df_ev["date"] = df_ev["Save Time"].dt.strftime("%Y-%m-%d %H:%M:%S")

    fig = go.Figure(
        data=[go.Bar(
            x=df_ev["Save Time"],
            y=df_ev["Total"],
            text=df_ev["Total"],
            texttemplate="", #%{y}
            textposition='inside',
            marker_color='#ff9900'
        )],
        layout=dict(
            xaxis_tickangle=-45,
            xaxis=dict(
                type="category",
                categoryorder='category ascending'),
            template='simple_white',
            barmode="group",
            margin=dict(l=0, r=0, t=50, b=0)
        )
    )

    return fig


# Congestion charts
def plot_congestion_status_barchart(df):
    colors = {'A verifier': '#e60000',
              'Suspect': '#ff9900',
              'Non Suspect': '#3399ff'}

    df_status = pd.DataFrame({"Total": df.Status.value_counts()}).sort_values(by="Total", ascending=True)
    fig = px.bar(df_status, y="Total", x=df_status.index, color=df_status.index, text='Total',
                 color_discrete_map=colors)
    fig.update_layout(
        template='simple_white',
        xaxis_title="Status",
        yaxis_title="Total",
    )

    return fig


def plot_percentage_range_barchart(df, col):
    # Define the ranges and their labels
    ranges = [(0, 10), (10, 20), (20, 30), (30, 40), (40, 50), (50, 60),
              (60, 70), (70, 80), (80, 90), (90, 100), (100, float('inf'))]
    labels = ['0%-10%', '10%-20%', '20%-30%', '30%-40%', '40%-50%', '50%-60%',
              '60%-70%', '70%-80%', '80%-90%', '90%-100%', 'More than 100%']

    # Compute the counts for each range
    counts = [((df[col] >= r[0]) & (df[col] < r[1])).sum() for r in ranges]

    # Create a bar chart using Plotly
    fig = go.Figure(data=[go.Bar(x=labels, y=counts, text=counts, textposition='auto')])

    # Customize the chart layout
    fig.update_layout(title='Percentage Ranges', xaxis_title='Range', yaxis_title='Count')

    return fig


def plot_duration_intervals(df):
    try:
        df = df[~(df["Alarm Duration"] == "\t--")]
        duration_df = df['Alarm Duration'].apply(convert_duration_to_minutes).copy()

        intervals = {
            '0-10 minutes': (0, 10),
            '10-30 minutes': (10, 30),
            '30 minutes to 1 hour': (30, 60),
            '1 hour to 1 day': (60, 1440),
            'More than 1 day': (1440, float('inf'))
        }

        categorized_durations = {
            interval: [duration for duration in list(duration_df.values) if start <= duration < end]
            for interval, (start, end) in intervals.items()
        }

        interval_counts = {interval: len(durations) for interval, durations in categorized_durations.items()}

        interval_df = pd.DataFrame.from_dict(interval_counts, orient='index', columns=['Count']).reset_index()
        interval_df = interval_df.rename(columns={'index': 'Interval'})

        interval_df = interval_df.sort_values(by='Count', ascending=True)

        fig = go.Figure(go.Bar(
            x=interval_df.Count,
            y=interval_df.Interval,
            orientation='h',
            text=interval_df.values,
            texttemplate="", #%{x}
            textposition='inside'
        ))

        fig.update_layout(
            title="title",
            template='simple_white',
            showlegend=False,
            xaxis_title="Total",
            yaxis_title="Alarm Duration",
            margin=dict(l=0, r=0, t=0, b=0)
        )

        return fig
    except (Exception,):
        return None


def plot_power_cut_times(df):
    sub_df = df.sort_values(by="Power Cut Times", ascending=False).head(15)
    # Create a bar chart using Plotly
    fig = go.Figure(
        data=[go.Bar(x=sub_df.NAME, y=sub_df["Power Cut Times"], text=sub_df["Power Cut Times"], textposition='auto')])

    # Customize the chart layout
    fig.update_layout(title='Top Power Cuts', xaxis_title='Site', yaxis_title='Total')

    return fig


def plot_battery_remaining_time(df):
    # Define the ranges and their labels
    ranges = [(0, 10), (10, 30), (30, 60), (60, 120), (120, float('inf'))]
    labels = ['0 - 10 min', '10 - 30 min', '30min - 1 h', '1h - 2h', 'More than 2h']

    # Compute the counts for each range
    counts = [((df["Remaining Time(min)"] >= r[0]) & (df["Remaining Time(min)"] < r[1])).sum() for r in ranges]

    # Create a bar chart using Plotly
    fig = go.Figure(data=[go.Bar(x=labels, y=counts, text=counts, textposition='auto')])

    # Customize the chart layout
    fig.update_layout(title='Remaining Time', xaxis_title='interval', yaxis_title='Total')

    return fig
