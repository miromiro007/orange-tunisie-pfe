import pandas as pd
from sqlalchemy import func, desc

from main import db, get_redis_instance
from main.utils.redis_utils import write_df_to_redis, redis_delete_df
from main.Radio.models.models import PMON

pd.options.mode.chained_assignment = None


class PMONService:

    @staticmethod
    def add_new_file(df):
        try:
            new_names = [
                'ip',
                'slot',
                'sanity',
                'mod_ref',
                'mod_min',
                'uas',
                'sep',
                'ses',
                'es',
                'bbe',
                'ofs',
                'rsl_min',
                'rsl_max',
                'rsl_avg',
                'link_status',
                'comment',
                'creation_date',
                'is_high_value',
            ]

            # use rename to replace column names
            df.rename(columns=dict(zip(df.columns, new_names)), inplace=True)

            # convert the dataframe to a list of dictionaries
            records = df.to_dict(orient='records')

            # insert the data into the database using bulk_insert_mappings
            db.session.bulk_insert_mappings(PMON, records)
            db.session.commit()
            return True
        except (Exception,):
            raise (Exception("Impossible d'ajouter le fichier; veuillez verifier les colonnes fournies !"))

    @staticmethod
    def get_data(creation_date=None):
        last_date = creation_date or db.session.query(func.max(PMON.creation_date)).scalar()
        # unix_timestamp = int(datetime.timestamp(last_date))

        if not last_date:
            return None

        redis_instance = get_redis_instance()
        redis_key = f'fh_pmon_{pd.to_datetime(last_date).strftime("%d-%m-%Y %H:%M:%S")}'

        # Check if data exists in Redis cache
        cached_data = redis_instance.get(redis_key)
        if cached_data:
            return pd.read_json(cached_data.decode('utf-8'))

        # Data not found in Redis cache, query from database
        data = db.session.query(
            PMON.ip,
            PMON.uas,
            PMON.es,
            PMON.bbe,
            PMON.ses,
            PMON.rsl_max,
            PMON.rsl_min,
            PMON.link_status,
            PMON.comment,
            PMON.is_high_value
        ).filter(PMON.creation_date == last_date)

        df = pd.DataFrame(
            [
                (
                    d.ip,
                    d.uas,
                    d.es,
                    d.bbe,
                    d.ses,
                    d.rsl_max,
                    d.rsl_min,
                    d.link_status,
                    d.comment,
                    d.is_high_value,
                )
                for d in data
            ],
            columns=[
                "IP",
                "UAS",
                "ES",
                "BBE",
                "SES",
                "RSL Max",
                "RSL Min",
                "Status",
                "Comments",
                "High Value"
            ],
        )

        # Cache data in Redis
        write_df_to_redis(df, redis_key)

        return df

    @staticmethod
    def get_files_list():
        data = db.session.query(PMON.creation_date.distinct()). \
            order_by(desc(PMON.creation_date)).all()
        df = pd.DataFrame([d for d in data], columns=['CreationDate'])
        return df

    @staticmethod
    def remove_file(creation_date):
        pmon_obj = PMONService.get_data_by_upload_date(creation_date)
        if pmon_obj:
            delete_records = PMON.__table__.delete().where(PMON.creation_date == creation_date)
            db.session.execute(delete_records)
            db.session.commit()
            redis_key = f'fh_pmon_{pd.to_datetime(creation_date).strftime("%d-%m-%Y %H:%M:%S")}'
            redis_delete_df(redis_key)
            return True
        else:
            raise (Exception("Le fichier n'existe pas"))

    @staticmethod
    def update_pmon(ip, comment, upload_date, high_value):
        last_date = upload_date or db.session.query(func.max(PMON.creation_date)).scalar()

        query = PMON.query.filter_by(creation_date=last_date, ip=ip)
        query.update({
            "comment": comment,
            "is_high_value": high_value
        })
        db.session.commit()

        redis_key = f'fh_pmon_{pd.to_datetime(last_date).strftime("%d-%m-%Y %H:%M:%S")}'
        redis_delete_df(redis_key)

    @staticmethod
    def get_data_by_upload_date(upload_date):
        pmon_obj = PMON.query.filter_by(
            creation_date=upload_date,
        ).first()
        return pmon_obj


