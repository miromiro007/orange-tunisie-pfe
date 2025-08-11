import pandas as pd
from sqlalchemy import desc, func

from main import db, get_redis_instance
from main.utils.redis_utils import redis_delete_df, write_df_to_redis
from main.Radio.models.models import NW_LOAD_DISTRIBUTION

pd.options.mode.chained_assignment = None


class LoadDistributionService:

    @staticmethod
    def add_new_file(df):
        try:
            new_names = [
                'ip',
                'slot',
                'avg_daily_tx_load',
                'max_daily_tx_load',
                'avg_daily_rx_load',
                'max_daily_rx_load',
                'name',
                'creation_date',
            ]

            # use rename to replace column names
            df.rename(columns=dict(zip(df.columns, new_names)), inplace=True)

            # convert the dataframe to a list of dictionaries
            records = df.to_dict(orient='records')

            # insert the data into the database using bulk_insert_mappings
            db.session.bulk_insert_mappings(NW_LOAD_DISTRIBUTION, records)
            db.session.commit()

            return True
        except (Exception,):
            raise Exception("Impossible d'ajouter le fichier; veuillez verifier les colonnes fournies !")

    @staticmethod
    def get_data_by_upload_date(upload_date):
        load_obj = NW_LOAD_DISTRIBUTION.query.filter_by(
            creation_date=upload_date,
        ).first()
        return load_obj

    @staticmethod
    def get_files_list():
        data = db.session.query(NW_LOAD_DISTRIBUTION.creation_date.distinct()). \
            order_by(desc(NW_LOAD_DISTRIBUTION.creation_date)).all()
        df = pd.DataFrame([d for d in data], columns=['CreationDate'])
        return df

    @staticmethod
    def remove_file(creation_date):
        load_obj = LoadDistributionService.get_data_by_upload_date(creation_date)
        if load_obj:
            delete_records = NW_LOAD_DISTRIBUTION.__table__.delete().where(
                NW_LOAD_DISTRIBUTION.creation_date == creation_date)
            db.session.execute(delete_records)
            db.session.commit()
            redis_key = f'fh_load_distribution_{pd.to_datetime(creation_date).strftime("%d-%m-%Y %H:%M:%S")}'
            redis_delete_df(redis_key)
            return True
        else:
            raise (Exception("Le fichier n'existe pas"))

    @staticmethod
    def get_data(creation_date=None):
        last_date = creation_date or db.session.query(func.max(NW_LOAD_DISTRIBUTION.creation_date)).scalar()
        # unix_timestamp = int(datetime.timestamp(last_date))

        if not last_date:
            return None

        redis_instance = get_redis_instance()
        redis_key = f'fh_load_distribution_{pd.to_datetime(last_date).strftime("%d-%m-%Y %H:%M:%S")}'

        # Check if data exists in Redis cache
        cached_data = redis_instance.get(redis_key)
        if cached_data:
            return pd.read_json(cached_data.decode('utf-8'))

        # Data not found in Redis cache, query from database
        data = db.session.query(
            NW_LOAD_DISTRIBUTION.ip,
            NW_LOAD_DISTRIBUTION.slot,
            NW_LOAD_DISTRIBUTION.avg_daily_tx_load,
            NW_LOAD_DISTRIBUTION.max_daily_tx_load,
            NW_LOAD_DISTRIBUTION.avg_daily_rx_load,
            NW_LOAD_DISTRIBUTION.max_daily_rx_load,
            NW_LOAD_DISTRIBUTION.name
        ).filter(NW_LOAD_DISTRIBUTION.creation_date == last_date)

        df = pd.DataFrame(
            [
                (
                    d.ip,
                    d.slot,
                    d.avg_daily_tx_load,
                    d.max_daily_tx_load,
                    d.avg_daily_rx_load,
                    d.max_daily_rx_load,
                    d.name,
                )
                for d in data
            ],
            columns=['IP',
                     'Slot',
                     'Average Daily TX Load',
                     'Max Daily TX Load',
                     'Average Daily RX Load',
                     'Max Daily RX Load',
                     'Name'], )

        # Cache data in Redis
        write_df_to_redis(df, redis_key)

        return df

