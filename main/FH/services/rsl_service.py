import pandas as pd
from sqlalchemy import func, desc

from main import db, get_redis_instance
from main.utils.redis_utils import write_df_to_redis, redis_delete_df
from main.Radio.models.models import RSL_Level

pd.options.mode.chained_assignment = None


class RSLLevelService:

    @staticmethod
    def add_new_file(df):
        try:
            new_names = [
                'ip',
                'slot',
                'min_rsl',
                'avg_rsl',
                'max_rsl',
                'name',
                'file',
                'ref_rsl',
                'enda_name',
                'enda_latitude',
                'enda_longitude',
                'endb_name',
                'endb_latitude',
                'endb_longitude',
                'link_status',
                'comment',
                'creation_date',
            ]

            # use rename to replace column names
            df.rename(columns=dict(zip(df.columns, new_names)), inplace=True)

            # convert the dataframe to a list of dictionaries
            records = df.to_dict(orient='records')

            # insert the data into the database using bulk_insert_mappings
            db.session.bulk_insert_mappings(RSL_Level, records)
            db.session.commit()
            return True
        except (Exception,):
            raise Exception("Impossible d'ajouter le fichier; veuillez verifier les colonnes fournies !")

    @staticmethod
    def get_data_by_upload_date(upload_date):
        rsl_obj = RSL_Level.query.filter_by(
            creation_date=upload_date,
        ).first()
        return rsl_obj

    @staticmethod
    def get_data(creation_date=None):
        last_date = creation_date or db.session.query(func.max(RSL_Level.creation_date)).scalar()
        # unix_timestamp = int(datetime.timestamp(last_date))

        if not last_date:
            return None

        redis_instance = get_redis_instance()
        redis_key = f'fh_rsl_level_{pd.to_datetime(last_date).strftime("%d-%m-%Y %H:%M:%S")}'

        # Check if data exists in Redis cache
        cached_data = redis_instance.get(redis_key)
        if cached_data:
            return pd.read_json(cached_data.decode('utf-8'))

        # Data not found in Redis cache, query from database
        data = db.session.query(
            RSL_Level.ip,
            RSL_Level.slot,
            RSL_Level.min_rsl,
            RSL_Level.avg_rsl,
            RSL_Level.max_rsl,
            RSL_Level.name,
            RSL_Level.file,
            RSL_Level.ref_rsl,
            RSL_Level.enda_name,
            RSL_Level.enda_latitude,
            RSL_Level.enda_longitude,
            RSL_Level.endb_name,
            RSL_Level.endb_latitude,
            RSL_Level.endb_longitude,
            RSL_Level.link_status,
            RSL_Level.comment
        ).filter(RSL_Level.creation_date == last_date)

        df = pd.DataFrame(
            [
                (
                    d.ip,
                    d.slot,
                    d.min_rsl,
                    d.avg_rsl,
                    d.max_rsl,
                    d.name,
                    d.file,
                    d.ref_rsl,
                    d.enda_name,
                    d.enda_latitude,
                    d.enda_longitude,
                    d.endb_name,
                    d.endb_latitude,
                    d.endb_longitude,
                    d.link_status,
                    d.comment
                )
                for d in data
            ],
            columns=[
                'IP',
                'Slot',
                'Min RSL',
                'Avg RSL',
                'Max RSL',
                'Name',
                'File',
                'RSL REF',
                'EndA_Name',
                'EndA_Latitude',
                'EndA_Longitude',
                'EndB_Name',
                'EndB_Latitude',
                'EndB_Longitude',
                'Status',
                'Comment'
            ],
        )

        # Cache data in Redis
        write_df_to_redis(df, redis_key)

        return df

    @staticmethod
    def get_files_list():
        data = db.session.query(RSL_Level.creation_date.distinct()).order_by(desc(RSL_Level.creation_date)).all()
        df = pd.DataFrame([d for d in data], columns=['CreationDate'])
        return df

    @staticmethod
    def get_fh_links():
        data = db.session.query(RSL_Level).distinct(RSL_Level.ip, RSL_Level.name)
        df = pd.DataFrame([(d.ip, d.name) for d in data], columns=['IP', 'Name'])
        return df

    @staticmethod
    def remove_file(creation_date):
        rsl_obj = RSLLevelService.get_data_by_upload_date(creation_date)
        if rsl_obj:
            delete_records = RSL_Level.__table__.delete().where(RSL_Level.creation_date == creation_date)
            db.session.execute(delete_records)
            db.session.commit()
            redis_key = f'fh_rsl_level_{pd.to_datetime(creation_date).strftime("%d-%m-%Y %H:%M:%S")}'
            redis_delete_df(redis_key)
            return True
        else:
            raise Exception("Le fichier n'existe pas")

    @staticmethod
    def update_rsl(name, ip, status, comment, upload_date):
        last_date = upload_date or db.session.query(func.max(RSL_Level.creation_date)).scalar()

        query = RSL_Level.query.filter_by(creation_date=last_date, name=name, ip=ip)
        query.update({
            "link_status": status,
            "comment": comment
        })
        db.session.commit()

        redis_key = f'fh_rsl_level_{pd.to_datetime(last_date).strftime("%d-%m-%Y %H:%M:%S")}'
        redis_delete_df(redis_key)
