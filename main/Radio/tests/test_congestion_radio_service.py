import pandas as pd
import pytest

from main import create_app
from main.Radio.services.congestion_radio_service import CongestionRadioService


@pytest.fixture
def app():
    app = create_app()
    return app


def test_get_alarm_by_save_date(app):
    with app.app_context():
        cong_obj = CongestionRadioService.get_alarm_by_save_date("1960-08-15 09:34:55")
        assert cong_obj is None


def test_add_new_file_success(app):
    with app.app_context():
        df = pd.DataFrame({
            'time': [pd.to_datetime("1960-11-28 09:34:28")],
            'e_node_b_name': ["ARI_0045"],
            'integrity': [1.0],
            'max_speed_mbs': [150],
            'end_date': [pd.to_datetime("1960-11-28 09:34:28")],
        })

        CongestionRadioService.insert_new_file(df)

        cong_obj = CongestionRadioService.get_alarm_by_save_date("1960-11-28 09:34:28")
        assert cong_obj is not None

        CongestionRadioService.remove_file(pd.to_datetime("1960-11-28 09:34:28"))


def test_add_new_file_exception_raised(app):
    with app.app_context():
        with pytest.raises(Exception):
            df = pd.DataFrame({
                'time': [pd.to_datetime("1960-11-28 09:34:28")],
                'e_node_b_name': ["ARI_0045"],
                'integrity': [1.0],
                'max_speed_mbs': [150],
            })

            CongestionRadioService.insert_new_file(df)


def test_remove_file_exception_raised(app):
    with app.app_context():
        with pytest.raises(Exception):
            CongestionRadioService.remove_file("1960-11-28 09:34:28")
