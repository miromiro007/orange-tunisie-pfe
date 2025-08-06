from main import get_redis_instance


def get_cached_dates(key_prefix):
    redis_instance = get_redis_instance()
    cached_dates = sorted(
        [key.decode().split('_')[-1] for key in redis_instance.scan_iter(key_prefix)])
    # cached_dates = [datetime.fromtimestamp(timestamp) for timestamp in cached_dates]
    return cached_dates


def redis_drop_key(save_time):
    redis_instance = get_redis_instance()
    redis_evo_key = f'radio_evo_{save_time}'
    redis_data_key = f'radio_data_{save_time}'
    redis_instance.delete(redis_evo_key)
    redis_instance.delete(redis_data_key)


def redis_drop_congestion_key(end_date):
    redis_instance = get_redis_instance()
    redis_cong_key = f'radio_congestion_{end_date}'
    redis_instance.delete(redis_cong_key)
    redis_instance.delete(f'radio_congestion_max_daily_{end_date}')
    redis_instance.delete(f'radio_congestion_max_traffic_{end_date}')


def redis_delete_df(key):
    redis_instance = get_redis_instance()
    redis_instance.delete(key)


def write_df_to_redis(df, redis_key):
    redis_instance = get_redis_instance()

    # Cache data in Redis
    redis_instance.set(redis_key, df.to_json())

    # Delete cached data for previous dates if the maximum number of cached dates is exceeded
    max_cached_dates = 10
    # cached_dates = sorted([int(key.decode().split('_')[-1]) for key in redis_instance.scan_iter('radio_evo_*')])
    key_prefix = redis_key.split('_')[:-1]
    key_prefix = '_'.join(key_prefix)
    cached_dates = get_cached_dates(key_prefix + '_*')
    if len(cached_dates) > max_cached_dates:
        for date in cached_dates[:len(cached_dates) - max_cached_dates]:
            redis_instance.delete(key_prefix + '_' + date)
