import pytest
import pandas as pd

from main import create_app
from main.FH.services.pmon_service import PMONService


@pytest.fixture()
def app():
    app = create_app()
    return app


def test_get_data_by_upload_date(app):
    with app.app_context():
        pmon_obj = PMONService.get_data_by_upload_date("1960-08-15 09:34:55")
        assert pmon_obj is None


def test_add_new_file_success(app):
    with app.app_context():
        df = pd.DataFrame({
            'ip': ["172.18.174.28"],
            'slot': ["1"],
            'sanity': ["CURATIVE"],
            'mod_ref': ["16 QAM	"],
            'mod_min': ["Unknown"],
            'uas': [75812],
            'sep': [0],
            'ses': [0],
            'es': [0],
            'bbe': [0],
            'ofs': [1],
            'rsl_min': [-99.90],
            'rsl_max': [0.00],
            'rsl_avg': [-49.95],
            'link_status': [""],
            'comment': [""],
            'creation_date': [pd.to_datetime("1960-11-28 09:34:28")]
        })

        PMONService.add_new_file(df)

        pmon_obj = PMONService.get_data_by_upload_date("1960-11-28 09:34:28")
        assert pmon_obj is not None

        PMONService.remove_file(pd.to_datetime("1960-11-28 09:34:28"))


def test_add_new_file_exception_raised(app):
    with app.app_context():
        with pytest.raises(Exception):
            df = pd.DataFrame({
                'ip': ["172.18.174.28"],
                'slot': ["1"],
                'sanity': [5],
                'mod_ref': ["16 QAM	"],
                'mod_min': ["Unknown"],
                'uas': ["75812"],
                'sep': [0],
                'ses': [0],
                'es': [0],
                'bbe': [0],
                'ofs': [1],
                'rsl_min': [-99.90],
                'rsl_max': [0.00],
                'rsl_avg': [-49.95],
                'link_status': [""],
                'comment': [""],
            })

            PMONService.add_new_file(df)


def test_remove_file_exception_raised(app):
    with app.app_context():
        with pytest.raises(Exception):
            PMONService.remove_file(pd.to_datetime("1960-11-28 09:34:28"))
