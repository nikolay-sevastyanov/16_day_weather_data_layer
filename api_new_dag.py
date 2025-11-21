# Импорт
import pendulum
import requests
import pandas as pd

# Импорт Airflow
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowFailException

# Переменные
ALMATY_LAT = 43.24
ALMATY_LON = 76.9
FORECAST_DAYS = 16
POSTGRES_CONN_ID = "postgres_default"


# 1. Определение DAG
@dag(
    dag_id="almaty_weather_etl_pipeline",
    start_date=pendulum.datetime(2025, 1, 19, tz="Asia/Almaty"),
    schedule="0 18 * * *",
    catchup=False,
    tags=["weather", "postgres", "etl"],
)
# Функция DAG - weather_data_pipeline
def weather_data_pipeline():
    @task
    def create_tables():

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        # Запрос создания таблицы real_weather в postgres
        real_weather_table = """
        CREATE TABLE IF NOT EXISTS real_weather (
            id SERIAL PRIMARY KEY,
            fetch_timestamp TIMESTAMP NOT NULL,
            weather_date DATE NOT NULL,
            weather_time TIME NOT NULL,
            temp_c FLOAT(1),
            wind_speed_kph FLOAT(1),
            precipitation_mm FLOAT(1),
            UNIQUE (weather_date, weather_time)
        );
        """

        # Запрос создания таблицы forecast_weather в postgres
        forecast_weather_table = """
        CREATE TABLE IF NOT EXISTS forecast_weather (
            id SERIAL PRIMARY KEY,
            fetch_timestamp TIMESTAMP NOT NULL,
            weather_date DATE NOT NULL,
            weather_time TIME NOT NULL,
            temp_c FLOAT(1),
            wind_speed_kph FLOAT(1),
            precipitation_mm FLOAT(1),
            UNIQUE (weather_date, weather_time)
        );
        """

        # Вызов запросов в БД
        hook.run(real_weather_table)
        hook.run(forecast_weather_table)

    # Таск - сбор данных с api
    @task
    def fetch_weather_data(lat: float, lon: float, days: int) -> dict:

        # Ссылка на api
        forecast_api_url = "https://api.open-meteo.com/v1/forecast"

        # Параметры данных для api
        params = {
            "latitude": lat,
            "longitude": lon,
            "hourly": "temperature_2m,precipitation,weathercode,windspeed_10m",
            "forecast_days": days,
            "timezone": "Asia/Almaty",
            "models": "best_match"
        }

        # Запрос к api
        try:
            response = requests.get(forecast_api_url, params=params)
            response.raise_for_status()
            data = response.json()
            return data.get("hourly", {})
        except requests.exceptions.RequestException as e:
            raise AirflowFailException(f"Ошибка при запросе данных с Open-Meteo API: {e}")


    # task - обработка данных (transform)
    @task
    def transform_and_split(raw_data: dict, execution_date_str: str) -> dict:

        # Конвертация строки с датой выполнения в объект datetime с учетом часового пояса
        execution_date = pendulum.parse(execution_date_str)

        if not raw_data or 'time' not in raw_data:
            raise AirflowFailException("Получены пустые или некорректные данные.")

        # 1. Создание DataFrame для вставки в БД как json
        df = pd.DataFrame({
            'timestamp': pd.to_datetime(raw_data['time']),
            'temp_c': raw_data['temperature_2m'],
            'precipitation_mm': raw_data['precipitation'],
            'wind_speed_kph': raw_data['windspeed_10m']
        })

        # Перевод времени в алматинское в данныз
        df['timestamp'] = df['timestamp'].dt.tz_localize('Asia/Almaty')

        # 2. Нормализация
        df['weather_date'] = df['timestamp'].dt.strftime("%Y-%m-%d")
        df['weather_time'] = df['timestamp'].dt.strftime("%H:%M:%S")


        # Установка времени сбора данных (fetch_timestamp)
        fetch_ts = execution_date.replace(tzinfo=None)
        df['fetch_timestamp'] = fetch_ts
        df['fetch_timestamp'] = df['fetch_timestamp'].dt.strftime('%d-%m-%Y %H:%M:%S')

        # 3. Разделение данных

        end_of_last_hour = execution_date.replace(minute=0, second=0, microsecond=0)

        real_df = df[df['timestamp'] < end_of_last_hour].sort_values('timestamp', ascending=False).head(1)
        forecast_df = df[df['timestamp'] >= end_of_last_hour]

        # 1. Форматирование fetch_timestamp (datetime)
        df['timestamp'] = df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')



        # 4. Также убедимся, что все численные данные являются стандартными float/int
        df['temp_c'] = df['temp_c'].astype(float)
        df['wind_speed_kph'] = df['wind_speed_kph'].astype(float)
        df['precipitation_mm'] = df['precipitation_mm'].astype(float)

        # 4. Преобразование в формат для загрузки в БД
        columns_to_insert = ['fetch_timestamp', 'weather_date', 'weather_time', 'temp_c', 'wind_speed_kph',
                             'precipitation_mm']

        real_data_tuples = [tuple(row) for row in real_df[columns_to_insert].itertuples(index=False)]
        forecast_data_tuples = [tuple(row) for row in forecast_df[columns_to_insert].itertuples(index=False)]

        return {
            "real_weather_data": real_data_tuples,
            "forecast_weather_data": forecast_data_tuples,
        }


    # task - подгрузка данных в БД
    @task
    def load_data_to_pg(data_tuples: list[tuple], table_name: str):

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        # Если data_tuples = None - print
        if not data_tuples:
            print(f"Таблица **{table_name}**: Нет данных для загрузки.")
            return

        # Список данных для вставки в postgres
        columns = "(fetch_timestamp, weather_date, weather_time, temp_c, wind_speed_kph, precipitation_mm)"

        # Для forecast_weather будет обновлять строки с каждым новым запуском, чтобы в случае если строки уже существуют,
        # DAG не выводил ошибку а просто актуализировал данные.
        if table_name == "forecast_weather":
            sql_conflict_clause = """
                    ON CONFLICT (weather_date, weather_time) 
                    DO UPDATE SET
                        fetch_timestamp = EXCLUDED.fetch_timestamp,
                        temp_c = EXCLUDED.temp_c,
                        wind_speed_kph = EXCLUDED.wind_speed_kph,
                        precipitation_mm = EXCLUDED.precipitation_mm;
                """
        else:
            # Для real_weather (исторические данные) оставляем DO NOTHING
            sql_conflict_clause = "ON CONFLICT (weather_date, weather_time) DO NOTHING;"

        # Обычный sql запрос, относящися к 2 таблицам
        sql = f"""
                INSERT INTO {table_name} {columns}
                VALUES (%s, %s, %s, %s, %s, %s)
                {sql_conflict_clause}
            """

        try:

            # 2. Выполнение запроса с hook.run()

            # Преобразование кортежей в список значений для вставки
            values_str = ', '.join(hook.get_sql_param(row) for row in data_tuples)

            # Полный SQL-запрос
            full_sql = f"""
                    INSERT INTO {table_name} (fetch_timestamp, weather_date, weather_time, temp_c, wind_speed_kph, precipitation_mm)
                    VALUES {values_str}
                    {sql_conflict_clause}
                """

            # Используем hook.run для выполнения сложных запросов с ON CONFLICT
            hook.run(full_sql)


            print(f"Успешно обработано **{len(data_tuples)}** записей в таблицу **{table_name}**.")

        except Exception as e:

            insert_queries = []
            for row in data_tuples:
                # Безопасное форматирование параметров для SQL
                formatted_row = ', '.join([f"'{item}'" if isinstance(item, str) else str(item) for item in row])

                insert_queries.append(f"""
                        INSERT INTO {table_name} (fetch_timestamp, weather_date, weather_time, temp_c, wind_speed_kph, precipitation_mm)
                        VALUES ({formatted_row})
                        {sql_conflict_clause}
                    """)

            # Выполняем все запросы в одной транзакции
            hook.run(insert_queries)
            print(f"Успешно обработано **{len(data_tuples)}** записей в таблицу **{table_name}**.")

        except Exception as e:
            raise AirflowFailException(f"Ошибка при загрузке данных в {table_name}: {e}")

    # 6. Определение зависимостей (последовательности задач)

    create_tables_task = create_tables()
    raw_weather_data = fetch_weather_data(lat=ALMATY_LAT, lon=ALMATY_LON, days=FORECAST_DAYS)

    transformed_data = transform_and_split(
        raw_weather_data,
        execution_date_str="{{ ti.start_date }}"
    )

    # Параллельная загрузка (используем override для уникальных Task ID)
    load_real = load_data_to_pg.override(task_id="load_real_weather")(
        data_tuples=transformed_data["real_weather_data"],
        table_name="real_weather"
    )

    load_forecast = load_data_to_pg.override(task_id="load_forecast_weather")(
        data_tuples=transformed_data["forecast_weather_data"],
        table_name="forecast_weather"
    )

    # Установка зависимостей
    create_tables_task >> raw_weather_data >> transformed_data
    transformed_data >> [load_real, load_forecast]


almaty_weather_dag = weather_data_pipeline()