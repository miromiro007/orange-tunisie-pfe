import pandas as pd
import pytest

from main import create_app
from main.Radio.services.alarm_radio_service import AlarmRadioService


@pytest.fixture
def app():
    app = create_app()
    return app


def test_get_alarm_by_save_time(app):
    with app.app_context():
        alarm_obj = AlarmRadioService.get_alarm_by_save_time("1960-08-15 09:34:55")
        assert alarm_obj is None


def test_add_new_file_success(app):
    with app.app_context():
        df = pd.DataFrame({
            'comments': ["Root alarm"],
            'severity': ["Minor"],
            'name': ["SCTP Link IP Address Unreachable"],
            'last_occurred': [pd.to_datetime("2023-05-16 09:34:28")],
            'cleared_on': ["2023-05-16 09:34:49"],
            'location_info': [
                "Cabinet No.=0, Subrack No.=7, Slot No.=0, Board Type=PMU, Specific Problem=Door Status Alarm"],
            'ne_type': ["BTS3900"],
            'alarm_source': ["MED_0079_LM"],
            'mo_name': ["Cabinet No.=0, Subrack No.=60, Slot No.=0, RRU60"],
            'occurrence_times': [1],
            'first_occurred': [pd.to_datetime("2023-02-11 00:04:57")],
            'alarm_id': ["26531"],
            'acknowledged_on': ["-"],
            'cleared_by': ["-"],
            'acknowledged_by': ["-"],
            'clearance_status': ["Uncleared"],
            'acknowledgement_status': ["Unacknowledged"],
            'home_subnet': ["ROOT"]
        })
        save_time = pd.to_datetime("1960-11-28 09:34:28")
        alarmService = AlarmRadioService()
        alarmService.add_new_file(df, save_time)

        alarm_obj = AlarmRadioService.get_alarm_by_save_time("1960-11-28 09:34:28")
        assert alarm_obj is not None

        alarmService.remove_file("1960-11-28 09:34:28")


def test_add_new_file_exception_raised(app):
    with app.app_context():
        with pytest.raises(Exception):
            df = pd.DataFrame({
                'comments': ["Root alarm"],
                'name': ["SCTP Link IP Address Unreachable"],
                'cleared_on': ["2023-05-16 09:34:49"],
                'location_info': [
                    "Cabinet No.=0, Subrack No.=7, Slot No.=0, Board Type=PMU, Specific Problem=Door Status Alarm"],
                'ne_type': ["BTS3900"],
                'alarm_source': ["MED_0079_LM"],
                'mo_name': ["Cabinet No.=0, Subrack No.=60, Slot No.=0, RRU60"],
                'occurrence_times': [1],
                'first_occurred': [pd.to_datetime("2023-02-11 00:04:57")],
                'alarm_id': ["26531"],
                'acknowledged_on': ["-"],
                'cleared_by': ["-"],
                'acknowledged_by': ["-"],
                'clearance_status': ["Uncleared"],
                'acknowledgement_status': ["Unacknowledged"],
                'home_subnet': ["ROOT"]
            })
            save_time = pd.to_datetime("1960-11-28 09:34:28")
            alarmService = AlarmRadioService()
            alarmService.add_new_file(df, save_time)


def test_remove_file_exception_raised(app):
    with app.app_context():
        with pytest.raises(Exception):
            alarmService = AlarmRadioService()
            alarmService.remove_file("1960-11-28 09:34:28")
