from logging import Logger
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

class RestaurantObj(BaseModel):
    id: int
    object_id: str
    object_value: str 
    update_ts: datetime
    

class RestaurantOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg
    
    def list_restaurants(self, restaurants_threshold: int, limit: int) -> List[RestaurantObj]:
        with self._db.client().cursor(row_factory=class_row(RestaurantObj)) as cur:
            cur.execute(
                """
                    SELECT id, object_id, object_value, update_ts
                    FROM stg.ordersystem_restaurants
                    WHERE id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.

                """, {
                    "threshold": restaurants_threshold,

                }
            )
            objs = cur.fetchall() #get all results from postgres
        return objs
    


class RestaurantDestRepository:
    

    def insert_restaurant(self, conn: Connection, restaurant: RestaurantObj) -> None:
        restaurant_dict = json.loads(restaurant.object_value)
       
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_restaurants(id, restaurant_id, restaurant_name, active_from, active_to)
                    VALUES (%(id)s, %(restaurant_id)s, %(restaurant_name)s, %(active_from)s, %(active_to)s)
                    ON CONFLICT (id) DO UPDATE
                    SET
                        restaurant_id = EXCLUDED.restaurant_id,
                        restaurant_name = EXCLUDED.restaurant_name,
                        active_from = EXCLUDED.active_from;
                """,
                {
                    "id": restaurant.id,
                    "restaurant_id": restaurant_dict["_id"],
                    "restaurant_name": restaurant_dict["name"],
                    "active_from": restaurant_dict["update_ts"],
                    "active_to": datetime(year=2099, month=12, day=31)
                    
                },
            )


class RestaurantLoader:
    WF_KEY = "example_dm_restaurants_origin_to_dds_workflow"
    LAST_LOADED_ID_KEY_RESTAURANT = "last_loaded_id"
    BATCH_LIMIT = 50  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = RestaurantOriginRepository(pg_dest)
        self.dds = RestaurantDestRepository()
        self.settings_repository = StgEtlSettingsRepository(scheme = 'dds')
        self.log = log

    def load_restaurant(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:
            
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY_RESTAURANT: -1})
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY_RESTAURANT]


            load_queue = self.origin.list_restaurants(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} users to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for restaurant in load_queue:
                self.dds.insert_restaurant(conn, restaurant)
            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY_RESTAURANT] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY_RESTAURANT]}")
