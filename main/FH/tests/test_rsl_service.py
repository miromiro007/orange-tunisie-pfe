import pytest
import pandas as pd

from main import create_app
from main.FH.services.rsl_service import RSLLevelService


@pytest.fixture
def app():
    app = create_app()
    return app


def test_get_data_by_upload_date(app):
    with app.app_context():
        rsl_obj = RSLLevelService.get_data_by_upload_date("1960-08-15 09:34:55")
        assert rsl_obj is None


def test_add_new_file_success(app):
    with app.app_context():
        df = pd.DataFrame({
            'ip': ["172.18.174.28"],
            'slot': [1],
            'min_rsl': [-99.9],
            'avg_rsl': [-69.65],
            'max_rsl': [-39.4],
            'name': ["B2B_SONEM_ZAG_0127"],
            'file': ["SONEM_ZAG_0127.xlsx"],
            'ref_rsl': [-31.83],
            'enda_name': ["SONEM"],
            'enda_latitude': [36.039433],
            'enda_longitude': [9.696964],
            'endb_name': ["ZAG_0127_C02"],
            'endb_latitude': [36.124011],
            'endb_longitude': [9.788628],
            'link_status': ["Lien_dépointé_(<10)"],
            'comment': ["Uncleared"],
            'creation_date': [pd.to_datetime("1960-11-28 09:34:28")]
        })

        RSLLevelService.add_new_file(df)

        rsl_obj = RSLLevelService.get_data_by_upload_date("1960-11-28 09:34:28")
        assert rsl_obj is not None

        RSLLevelService.remove_file(pd.to_datetime("1960-11-28 09:34:28"))


def test_add_new_file_exception_raised(app):
    with app.app_context():
        with pytest.raises(Exception):
            df = pd.DataFrame({
                'ip': ["172.18.174.28"],
                'slot': [1],
                'min_rsl': ["-99.9"],
                'avg_rsl': [-69.65],
                'max_rsl': [-39.4],
                'name': ["B2B_SONEM_ZAG_0127"],
                'file': ["SONEM_ZAG_0127.xlsx"],
                'ref_rsl': [-31.83],
                'enda_name': ["SONEM"],
                'enda_latitude': [36.039433],
                'enda_longitude': [9.696964],
                'endb_name': ["ZAG_0127_C02"],
                'endb_latitude': [36.124011],
                'endb_longitude': ["9.788628"],
                'comment': ["Uncleared"],
            })

            RSLLevelService.add_new_file(df)


def test_remove_file_exception_raised(app):
    with app.app_context():
        with pytest.raises(Exception):
            RSLLevelService.remove_file(pd.to_datetime("1960-11-28 09:34:28"))
