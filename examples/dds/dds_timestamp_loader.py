from logging import Logger
import logging
from typing import List
from datetime import datetime
import json
from examples.stg import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel

# active_to = datetime(year=2099, month=12, day=31)

class TimestampObj(BaseModel):
    id: int
    object_id: str
    object_value: str 
    update_ts: datetime
    

class TimestampOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg
    
    def list_timestamp(self, timestamp_threshold: int, limit: int) -> List[TimestampObj]:
        with self._db.client().cursor(row_factory=class_row(TimestampObj)) as cur:
            cur.execute(
                """
                    SELECT id, object_id, object_value, update_ts
                    FROM stg.ordersystem_orders
                    WHERE id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.

                """, {
                    "threshold": timestamp_threshold,

                }
            )
            objs = cur.fetchall() #get all results from postgres
        return objs
    


class TimestampDestRepository:
    

    def insert_timestmp(self, conn: Connection, tmsp: TimestampObj) -> None:
        timestmp_dict = json.loads(tmsp.object_value)
        dt: str = timestmp_dict['date']
        result_date = datetime.strptime(dt, '%Y-%m-%d %H:%M:%S')

#################
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_timestamps(id, ts, year, month, day, date, time)
                    VALUES (%(id)s, %(ts)s, %(year)s, %(month)s, %(day)s, %(date)s, %(time)s)
                    ON CONFLICT (id) DO UPDATE
                    SET
                        ts = EXCLUDED.ts,
                        year = EXCLUDED.year,
                        month = EXCLUDED.month,
                        day = EXCLUDED.day,
                        date = EXCLUDED.date,
                        time = EXCLUDED.time;
                """,
                {
                    "id": tmsp.id,
                    "ts": result_date,
                    "year": result_date.year,
                    "month": result_date.month,
                    "day": result_date.day,
                    "date": result_date.date(),
                    "time": result_date.time(),
                 
                    
                },
            )


class TimestampLoader:
    WF_KEY = "example_dm_timestmps_origin_to_dds_workflow"
    LAST_LOADED_ID_KEY_TIMESTAMP = "last_loaded_id"
    BATCH_LIMIT = 50  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = TimestampOriginRepository(pg_dest)
        self.dds = TimestampDestRepository()
        self.settings_repository = StgEtlSettingsRepository(scheme = 'dds')
        self.log = logging.getLogger()

    def load_timestamp(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:
            
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY_TIMESTAMP: -1})
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY_TIMESTAMP]


            load_queue = self.origin.list_timestamp(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} users to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for tmsp in load_queue:
                self.dds.insert_timestmp(conn, tmsp)
            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY_TIMESTAMP] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY_TIMESTAMP]}")
