import pandas as pd
from sqlalchemy import func, desc, and_
from main import db, get_redis_instance
from main.utils.redis_utils import write_df_to_redis, redis_drop_key
from main.Radio.models.models import AlarmRadio

pd.options.mode.chained_assignment = None


class AlarmRadioService:

    @staticmethod
    def get_data(upload_date=None):
        last_date = upload_date or db.session.query(func.max(AlarmRadio.save_time)).scalar()
        # unix_timestamp = int(datetime.timestamp(last_date))

        redis_instance = get_redis_instance()
        redis_key = f'radio_data_{last_date}'

        # Check if data exists in Redis cache
        cached_data = redis_instance.get(redis_key)
        if cached_data:
            return pd.read_json(cached_data.decode('utf-8'))

        # Data not found in Redis cache, query from database
        data = db.session.query(
            AlarmRadio.severity,
            AlarmRadio.name,
            AlarmRadio.last_occurred,
            AlarmRadio.ne_type,
            AlarmRadio.alarm_source,
            AlarmRadio.alarm_id,
            AlarmRadio.clearance_status,
            AlarmRadio.first_occurred,
            AlarmRadio.save_time,
            AlarmRadio.home_subnet,
            AlarmRadio.occurrence_times,
            AlarmRadio.alarm_duration
        ).filter(AlarmRadio.save_time == last_date)

        df = pd.DataFrame(
            [
                (
                    d.severity,
                    d.name,
                    d.last_occurred,
                    d.ne_type,
                    d.alarm_source,
                    d.alarm_id,
                    d.clearance_status,
                    d.first_occurred,
                    d.save_time,
                    d.home_subnet,
                    d.occurrence_times,
                    d.alarm_duration
                )
                for d in data
            ],
            columns=[
                'Severity',
                'Name',
                'Last Occurred (NT)',
                'NE Type',
                'Alarm Source',
                'Alarm ID',
                'Clearance Status',
                'First Occurred (NT)',
                'Save Time',
                'Home Subnet',
                'Occurrence Times',
                'Alarm Duration'
            ],
        )

        # Cache data in Redis
        write_df_to_redis(df, redis_key)

        return df

    @staticmethod
    def get_alarm_by_save_time(save_time):
        alarm_obj = AlarmRadio.query.filter_by(
            save_time=save_time,
        ).first()
        return alarm_obj

    @staticmethod
    def get_evolution_data():
        latest_dates = db.session.query(AlarmRadio.save_time.distinct()) \
            .order_by(desc(AlarmRadio.save_time)).limit(14).all()
        df_dates = pd.DataFrame([date for date in latest_dates], columns=['Save Time'])
        last_date = max(df_dates['Save Time'])
        start_date = min(df_dates['Save Time'])

        # last_date = db.session.query(func.max(AlarmRadio.save_time)).scalar()
        # unix_timestamp = int(datetime.timestamp(last_date))

        redis_instance = get_redis_instance()
        redis_key = f'radio_evo_{last_date}'

        # Check if data exists in Redis cache
        cached_data = redis_instance.get(redis_key)
        if cached_data:
            return pd.read_json(cached_data.decode('utf-8'))

        data = db.session.query(
            AlarmRadio.severity,
            AlarmRadio.name,
            AlarmRadio.last_occurred,
            AlarmRadio.ne_type,
            AlarmRadio.alarm_source,
            AlarmRadio.alarm_id,
            AlarmRadio.clearance_status,
            AlarmRadio.first_occurred,
            AlarmRadio.save_time,
            AlarmRadio.home_subnet
        ).filter(and_(AlarmRadio.clearance_status == "Uncleared",
                      AlarmRadio.save_time >= start_date,
                      AlarmRadio.save_time <= last_date))

        df = pd.DataFrame(
            [
                (d.severity,
                 d.name,
                 d.last_occurred,
                 d.ne_type,
                 d.alarm_source,
                 d.alarm_id,
                 d.clearance_status,
                 d.first_occurred,
                 d.save_time,
                 d.home_subnet) for d in data],
            columns=[
                'Severity',
                'Name',
                'Last Occurred (NT)',
                'NE Type', 'Alarm Source',
                'Alarm ID',
                'Clearance Status',
                'First Occurred (NT)',
                'Save Time',
                'Home Subnet'])

        # Cache data in Redis
        write_df_to_redis(df, redis_key)

        return df

    @staticmethod
    def get_file_list():
        data = db.session.query(AlarmRadio.save_time.distinct()).order_by(desc(AlarmRadio.save_time)).all()
        df = pd.DataFrame([d for d in data], columns=['Save Time'])
        return df

    @staticmethod
    def get_active_alarms(upload_date=None):
        last_date = None
        if upload_date:
            last_date = upload_date
        else:
            last_date = db.session.query(func.max(AlarmRadio.save_time)).scalar()
        data = db.session.query(AlarmRadio.severity,
                                AlarmRadio.name,
                                AlarmRadio.last_occurred,
                                AlarmRadio.ne_type,
                                AlarmRadio.alarm_source,
                                AlarmRadio.location_info,
                                AlarmRadio.clearance_status
                                ).filter(AlarmRadio.save_time == last_date, AlarmRadio.clearance_status == "Uncleared")

        df = pd.DataFrame([(d.severity, d.name, d.last_occurred, d.ne_type, d.alarm_source,
                            d.location_info, d.clearance_status) for d in data],
                          columns=['Severity', 'Name', 'Last Occurred (NT)', 'NE Type', 'Alarm Source',
                                   'Location Info', 'Clearance Status'])

        return df

    @staticmethod
    def query_active_alarms_by_name(alarm_name, upload_date=None):
        last_date = None
        if upload_date:
            last_date = upload_date
        else:
            last_date = db.session.query(func.max(AlarmRadio.save_time)).scalar()
        data = db.session.query(AlarmRadio.severity,
                                AlarmRadio.name,
                                AlarmRadio.last_occurred,
                                AlarmRadio.ne_type,
                                AlarmRadio.alarm_source,
                                AlarmRadio.location_info,
                                AlarmRadio.clearance_status
                                ).filter(and_(AlarmRadio.save_time == last_date,
                                              AlarmRadio.name == alarm_name,
                                              AlarmRadio.clearance_status == "Uncleared"))

        df = pd.DataFrame([(d.severity, d.name, d.last_occurred, d.ne_type, d.alarm_source,
                            d.location_info, d.clearance_status) for d in data],
                          columns=['Severity', 'Name', 'Last Occurred (NT)', 'NE Type', 'Alarm Source',
                                   'Location Info', 'Clearance Status'])

        return df

    @staticmethod
    def update_evolution_dataset():
        latest_dates = db.session.query(AlarmRadio.save_time.distinct()) \
            .order_by(desc(AlarmRadio.save_time)).limit(14).all()
        df_dates = pd.DataFrame([date for date in latest_dates], columns=['Save Time'])
        last_date = max(df_dates['Save Time'])
        start_date = min(df_dates['Save Time'])

        # last_date = db.session.query(func.max(AlarmRadio.save_time)).scalar()

        redis_instance = get_redis_instance()
        redis_key = f'radio_evo_{last_date}'

        data = db.session.query(
            AlarmRadio.severity,
            AlarmRadio.name,
            AlarmRadio.last_occurred,
            AlarmRadio.ne_type,
            AlarmRadio.alarm_source,
            AlarmRadio.alarm_id,
            AlarmRadio.clearance_status,
            AlarmRadio.first_occurred,
            AlarmRadio.save_time,
            AlarmRadio.home_subnet
        ).filter(and_(AlarmRadio.clearance_status == "Uncleared",
                      AlarmRadio.save_time >= start_date,
                      AlarmRadio.save_time <= last_date))

        df = pd.DataFrame(
            [
                (d.severity,
                 d.name,
                 d.last_occurred,
                 d.ne_type,
                 d.alarm_source,
                 d.alarm_id,
                 d.clearance_status,
                 d.first_occurred,
                 d.save_time,
                 d.home_subnet) for d in data],
            columns=[
                'Severity',
                'Name',
                'Last Occurred (NT)',
                'NE Type', 'Alarm Source',
                'Alarm ID',
                'Clearance Status',
                'First Occurred (NT)',
                'Save Time',
                'Home Subnet'])

        # Cache data in Redis
        write_df_to_redis(df, redis_key)

        return

    def add_new_file(self, df, save_time):
        try:
            new_names = [
                'comments',
                'severity',
                'name',
                'last_occurred',
                'cleared_on',
                'location_info',
                'ne_type',
                'alarm_source',
                'mo_name',
                'occurrence_times',
                'first_occurred',
                'alarm_id',
                'acknowledged_on',
                'cleared_by',
                'acknowledged_by',
                'clearance_status',
                'acknowledgement_status',
                'home_subnet',
                'alarm_duration'
            ]

            # use rename to replace column names
            df.rename(columns=dict(zip(df.columns, new_names)), inplace=True)

            # add a new column called 'save_time' at the beginning of the DataFrame
            df.insert(loc=0, column='save_time', value=save_time)

            # convert the dataframe to a list of dictionaries
            records = df.to_dict(orient='records')

            # insert the data into the database using bulk_insert_mappings
            db.session.bulk_insert_mappings(AlarmRadio, records)
            db.session.commit()
            self.update_evolution_dataset()

            return True
        except (Exception,):
            raise Exception("Impossible d'ajouter le fichier; veuillez verifier les colonnes fournies !")

    def remove_file(self, save_time):
        alarm_obj = AlarmRadioService.get_alarm_by_save_time(save_time)
        if alarm_obj:
            delete_records = AlarmRadio.__table__.delete().where(AlarmRadio.save_time == save_time)
            db.session.execute(delete_records)
            db.session.commit()
            redis_drop_key(save_time)
            self.update_evolution_dataset()
            return True
        else:
            raise Exception("Le fichier n'existe pas")

