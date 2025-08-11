import pandas as pd
from sqlalchemy import desc, func
from main import db, get_redis_instance
from main.utils.redis_utils import redis_drop_congestion_key, write_df_to_redis
from main.Radio.models.models import CongestionRadio

pd.options.mode.chained_assignment = None


class CongestionRadioService:

    @staticmethod
    def insert_new_file(df):
        try:
            new_names = [
                'time',
                'e_node_b_name',
                'integrity',
                'max_speed_mbs',
                'end_date'
            ]

            # use rename to replace column names
            df.rename(columns=dict(zip(df.columns, new_names)), inplace=True)

            # convert the dataframe to a list of dictionaries
            records = df.to_dict(orient='records')

            # insert the data into the database using bulk_insert_mappings
            db.session.bulk_insert_mappings(CongestionRadio, records)
            db.session.commit()
            return True

        except (Exception,):
            raise Exception("Impossible d'ajouter le fichier; veuillez verifier les colonnes fournies !")

    @staticmethod
    def get_file_list():
        data = db.session.query(CongestionRadio.end_date.distinct()).order_by(desc(CongestionRadio.end_date)).all()
        df = pd.DataFrame([d for d in data], columns=['EndDate'])
        return df

    @staticmethod
    def remove_file(end_date):
        congestion_obj = CongestionRadioService.get_alarm_by_save_date(end_date)
        if congestion_obj:
            delete_records = CongestionRadio.__table__.delete().where(CongestionRadio.end_date == end_date)
            db.session.execute(delete_records)
            db.session.commit()
            redis_drop_congestion_key(end_date.strftime("%d-%m-%Y %H:%M"))
            return True
        else:
            raise (Exception("Le fichier n'existe pas"))

    @staticmethod
    def get_data(end_date=None):
        last_date = end_date or db.session.query(func.max(CongestionRadio.end_date)).scalar()
        # unix_timestamp = int(datetime.timestamp(last_date))

        if not last_date:
            return None

        redis_instance = get_redis_instance()
        redis_key = f'radio_congestion_{pd.to_datetime(last_date).strftime("%d-%m-%Y %H:%M")}'

        # Check if data exists in Redis cache
        cached_data = redis_instance.get(redis_key)
        if cached_data:
            return pd.read_json(cached_data.decode('utf-8'))

        # Data not found in Redis cache, query from database
        data = db.session.query(
            CongestionRadio.time,
            CongestionRadio.e_node_b_name,
            CongestionRadio.integrity,
            CongestionRadio.max_speed_mbs
        ).filter(CongestionRadio.end_date == last_date)

        df = pd.DataFrame(
            [
                (
                    d.time,
                    d.e_node_b_name,
                    d.integrity,
                    d.max_speed_mbs
                )
                for d in data
            ],
            columns=[
                'Time',
                'eNodeB Name',
                'Integrity',
                'VS.FEGE.RxMaxSpeed_Mbs(Mbps)'
            ],
        )

        # Cache data in Redis
        write_df_to_redis(df, redis_key)

        return df

    @staticmethod
    def get_alarm_by_save_date(end_date):
        congestion_obj = CongestionRadio.query.filter_by(
            end_date=end_date,
        ).first()
        return congestion_obj

    @staticmethod
    def max_daly_traiter(df):
        df['Time'] = pd.to_datetime(df['Time'])

        df['Date'] = df['Time'].dt.strftime('%d-%B')

        pivoted_df = df.pivot_table(index='eNodeB Name', columns='Date', values='VS.FEGE.RxMaxSpeed_Mbs(Mbps)',
                                    aggfunc='max')

        pivoted_df = pivoted_df.fillna(0)

        pivoted_df['Max'] = pivoted_df.max(axis=1)

        pivoted_df.reset_index(inplace=True)
        return pivoted_df
    @staticmethod
    def trafic_max(df):
        df1 = pd.read_excel('main/Radio/data/DRO/Statut Sharing (DRO).xlsx')
        df = df[['Time', 'eNodeB Name', 'Integrity',
                 'VS.FEGE.RxMaxSpeed_Mbs(Mbps)']]

        df['Date'] = df['Time'].dt.strftime('%d-%B')
        df['Code OTN'] = df['eNodeB Name'].str[:-6]
        df = pd.merge(df, df1, on='Code OTN', how='left')

        # df.to_excel('biii.xlsx')
        df = df[
            ['Date', 'Time', 'eNodeB Name', 'Capacité', 'Opérateur', 'Type Backhaul', 'VS.FEGE.RxMaxSpeed_Mbs(Mbps)']]
        df['80% Capacité'] = df['Capacité'] * 0.8

        # Comparer VS.FEGE.RxMaxSpeed_Mbs(Mbps) avec 90% de Capacité et compter les occurrences
        df['> 80% Capacité'] = df['VS.FEGE.RxMaxSpeed_Mbs(Mbps)'] > df['80% Capacité']

        # Ajouter une nouvelle colonne avec le nombre d'occurrences
        df['occurrence > 80% du max'] = df.groupby('eNodeB Name')['> 80% Capacité'].transform('sum')

        # Supprimer les colonnes intermédiaires si nécessaire
        df.drop(columns=['80% Capacité', '> 80% Capacité'], inplace=True)
        df['site owner'] = df['Opérateur'] + '_' + df['Type Backhaul']
        df = df[['Date', 'Time', 'eNodeB Name', 'site owner', 'Capacité', 'VS.FEGE.RxMaxSpeed_Mbs(Mbps)',
                 'occurrence > 80% du max']]

        pivoted_df = df.pivot_table(index=['eNodeB Name', 'site owner', 'Capacité', 'occurrence > 80% du max'],
                                    columns='Date', values='VS.FEGE.RxMaxSpeed_Mbs(Mbps)', aggfunc='max')
        pivoted_df = pivoted_df.fillna(0)

        # Ajouter une colonne Max pour la valeur maximale de chaque ligne
        pivoted_df['Valeur max ( Mbps )'] = pivoted_df.max(axis=1)
        df_reset = pivoted_df.reset_index()
        df_reset = df_reset.rename(columns={'eNodeB Name': 'site'})
        df_reset[' % max'] = df_reset['Valeur max ( Mbps )']
        date_columns = ['10-May', '11-May', '12-May', '13-May', '14-May', '15-May', '16-May', ' % max']

        # Calculer le pourcentage par rapport à la colonne 'Capacité'
        for col in date_columns:
            df_reset[col] = (df_reset[col] / df_reset['Capacité']) * 100

        df_reset[date_columns] = df_reset[date_columns].applymap(lambda x: f"{x:.2f} %")

        return df_reset

    import numpy as np
    import pandas as pd
    from sqlalchemy import func, desc

    from main import db, get_redis_instance
    from main.Radio.models.models import Battery
    from main.utils.redis_utils import write_df_to_redis, redis_delete_df

    pd.options.mode.chained_assignment = None

    class BatteryService:

        @staticmethod
        def add_new_file(df):
            try:
                new_names = [
                    'name',
                    'remaining_capacity',
                    'remaining_time',
                    'power_cut_times',
                    'creation_date'
                ]

                # use rename to replace column names
                df.rename(columns=dict(zip(df.columns, new_names)), inplace=True)
                df = df.replace({np.nan: None})

                # convert the dataframe to a list of dictionaries
                records = df.to_dict(orient='records')

                # insert the data into the database using bulk_insert_mappings
                db.session.bulk_insert_mappings(Battery, records)
                db.session.commit()
                return True
            except (Exception,):
                raise Exception("Impossible d'ajouter le fichier; veuillez verifier les colonnes fournies !")

        @staticmethod
        def get_data(creation_date=None):
            last_date = creation_date or db.session.query(func.max(Battery.creation_date)).scalar()
            # unix_timestamp = int(datetime.timestamp(last_date))

            if not last_date:
                return None

            redis_instance = get_redis_instance()
            redis_key = f'radio_battery_{pd.to_datetime(last_date).strftime("%d-%m-%Y %H:%M:%S")}'

            # Check if data exists in Redis cache
            cached_data = redis_instance.get(redis_key)
            if cached_data:
                return pd.read_json(cached_data.decode('utf-8'))

            # Data not found in Redis cache, query from database
            data = db.session.query(
                Battery.name,
                Battery.remaining_capacity,
                Battery.remaining_time,
                Battery.power_cut_times
            ).filter(Battery.creation_date == last_date)

            df = pd.DataFrame(
                [
                    (
                        d.name,
                        d.remaining_capacity,
                        d.remaining_time,
                        d.power_cut_times,
                    )
                    for d in data
                ],
                columns=[
                    "NAME",
                    "Remaining Capacity(%)",
                    "Remaining Time(min)",
                    "Power Cut Times",
                ],
            )

            # Cache data in Redis
            write_df_to_redis(df, redis_key)

            return df

        @staticmethod
        def get_files_list():
            data = db.session.query(Battery.creation_date.distinct()). \
                order_by(desc(Battery.creation_date)).all()
            df = pd.DataFrame([d for d in data], columns=['CreationDate'])
            return df

        @staticmethod
        def remove_file(creation_date):
            battery_obj = BatteryService.get_data_by_upload_date(creation_date)
            if battery_obj:
                delete_records = Battery.__table__.delete().where(Battery.creation_date == creation_date)
                db.session.execute(delete_records)
                db.session.commit()
                redis_key = f'radio_battery_{pd.to_datetime(creation_date).strftime("%d-%m-%Y %H:%M:%S")}'
                redis_delete_df(redis_key)
                return True
            else:
                raise Exception("Le fichier n'existe pas")

        @staticmethod
        def get_data_by_upload_date(upload_date):
            battery_obj = Battery.query.filter_by(
                creation_date=upload_date,
            ).first()
            return battery_obj
