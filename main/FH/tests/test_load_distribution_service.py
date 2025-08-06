import pandas as pd
import pytest

from main import create_app
from main.FH.services.load_distribution_service import LoadDistributionService


@pytest.fixture()
def app():
    app = create_app()
    return app


def test_get_data_by_upload_date(app):
    with app.app_context():
        pmon_obj = LoadDistributionService.get_data_by_upload_date("1960-08-15 09:34:55")
        assert pmon_obj is None


def test_add_new_file_success(app):
    with app.app_context():
        df = pd.DataFrame({
            'ip': ["172.18.174.28"],
            'slot': ["1"],
            'avg_daily_tx_load': [1.1090],
            'max_daily_tx_load': [106.4655],
            'avg_daily_rx_load': [0.0280],
            'max_daily_rx_load': [2.6859],
            'name': ["B2B_BLASHER_FACTORY_SOU_0059"],
            'creation_date': [pd.to_datetime("1960-11-28 09:34:28")]
        })

        LoadDistributionService.add_new_file(df)

        pmon_obj = LoadDistributionService.get_data_by_upload_date("1960-11-28 09:34:28")
        assert pmon_obj is not None

        LoadDistributionService.remove_file(pd.to_datetime("1960-11-28 09:34:28"))


def test_add_new_file_exception_raised(app):
    with app.app_context():
        with pytest.raises(Exception):
            df = pd.DataFrame({
                'ip': ["172.18.174.28"],
                'slot': ["1"],
                'avg_daily_tx_load': [1.1090],
                'max_daily_tx_load': [106.4655],
                'avg_daily_rx_load': [0.0280],
                'max_daily_rx_load': [2.6859],
                'name': [10],
            })

            LoadDistributionService.add_new_file(df)


def test_remove_file_exception_raised(app):
    with app.app_context():
        with pytest.raises(Exception):
            LoadDistributionService.remove_file(pd.to_datetime("1960-11-28 09:34:28"))
