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
    


class ProductDestRepository:
    

    def insert_product(self, conn: Connection, restaurant: RestaurantObj) -> None:
        restaurant_dict = json.loads(restaurant.object_value)
        menu = restaurant_dict["menu"]
        for product in menu:

            _id = product["_id"]
            
            name = product["name"]
            price = product["price"]

            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.dm_products(restaurant_id, product_id, product_name, product_price, active_from, active_to)
                        VALUES (%(restaurant_id)s, %(product_id)s, %(product_name)s, %(product_price)s, %(active_from)s, %(active_to)s)
                        ON CONFLICT (id) DO UPDATE
                        SET
                            restaurant_id = EXCLUDED.restaurant_id,
                            product_id = EXCLUDED.product_id,
                            product_name = EXCLUDED.product_name,
                            product_price = EXCLUDED.product_price,
                            active_from = EXCLUDED.active_from,
                            active_to = EXCLUDED.active_to;
                    """,
                    {
                        "restaurant_id": restaurant.id,
                        "product_id": _id,
                        "product_name": name,
                        "product_price": price,
                        "active_from": restaurant_dict["update_ts"],
                        "active_to": datetime(year=2099, month=12, day=31)
                        
                    },
                )


class ProductLoader:
    WF_KEY = "example_dm_product_origin_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 50  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = RestaurantOriginRepository(pg_dest)
        self.dds = ProductDestRepository()
        self.settings_repository = StgEtlSettingsRepository(scheme = 'dds')
        self.log = log

    def load_restaurant(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:
            
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]


            load_queue = self.origin.list_restaurants(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} users to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for restaurant in load_queue:
                self.dds.insert_product(conn, restaurant)
            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
