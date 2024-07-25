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


class OrdersJsonObj(BaseModel):
    _id: str
    final_status: str
    restaurant: dict
    update_ts: str
    user: dict
    
class OrdersObj(BaseModel):
    id: int
    object_id: str
    object_value: str
    update_ts: datetime
        
class RestaurantObj(BaseModel):
    id: int
    restaurant_id: str

class UserObj(BaseModel):
    id: int
    user_id: str


class TmspObj(BaseModel):
    id: int
    ts: datetime


class OrdersOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg
    
    def list_orders(self, orders_threshold: int, limit: int) -> List[OrdersObj]:
        with self._db.client().cursor(row_factory=class_row(OrdersObj)) as cur:
            cur.execute(
                """
                    SELECT id, object_id, object_value, update_ts
                    FROM stg.ordersystem_orders
                    WHERE id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.

                """, {
                    "threshold": orders_threshold,

                }
            )
            objs = cur.fetchall() #get all results from postgres
        return objs
    


class OrdersDestRepository:
    
    def get_rest_id(self, conn: Connection, restaurant_id: str):
        
        with conn.cursor(row_factory=class_row(RestaurantObj)) as cur:
            cur.execute(
                """
                    SELECT id, restaurant_id
                    FROM dds.dm_restaurants
                    WHERE restaurant_id = %(restaurant_id)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.

                """, {
                    "restaurant_id": restaurant_id,

                }
            )
            obj = cur.fetchall()[0] #get all results from postgres
        return obj.id
    
    def get_timestamp_id(self, conn: Connection, update_ts: str):
        with conn.cursor(row_factory=class_row(TmspObj)) as cur:
            cur.execute(
                """
                    SELECT id, ts
                    FROM dds.dm_timestamps
                    WHERE ts = %(update_ts)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.

                """, {
                    "update_ts": update_ts,

                }
            )
            obj = cur.fetchall()[0] #get all results from postgres
        return obj.id

    def get_user_id(self, conn: Connection, user_hash_id: str):
        with conn.cursor(row_factory=class_row(UserObj)) as cur:
            cur.execute(
                """
                    SELECT id, user_id
                    FROM dds.dm_users
                    WHERE user_id = %(user)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.

                """, {
                    "user": user_hash_id,

                }
            )
            obj = cur.fetchall()[0] #get all results from postgres
        return obj.id
    
    

    def insert_product(self, conn: Connection, order: OrdersObj) -> None:
        order_dict = json.loads(order.object_value)
        _id = order_dict["_id"]
        final_status = order_dict["final_status"]
        restaurant = order_dict["restaurant"]["id"]
        update_ts = order_dict["update_ts"]
        user = order_dict["user"]["id"]

        rest_id = self.get_rest_id(conn, restaurant)
        timestamp_id = self.get_timestamp_id(conn, update_ts)
        user_id = self.get_user_id(conn, user)
        
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_orders(id, order_key, order_status, restaurant_id, timestamp_id, user_id)
                    VALUES (%(id)s, %(order_key)s, %(order_status)s, %(restaurant_id)s, %(timestamp_id)s, %(user_id)s)
                    ON CONFLICT (id) DO UPDATE
                    SET

                        order_key = EXCLUDED.order_key,
                        order_status = EXCLUDED.order_status;
                """,
                {
                    "id": order.id,
                    "order_key": _id,
                    "order_status": final_status,

                    "restaurant_id": rest_id,
                    "timestamp_id": timestamp_id,
                    "user_id": user_id
                    
                    
                },
            )

        


class OrdersLoader:
    WF_KEY = "example_dm_order_origin_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 50  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = OrdersOriginRepository(pg_dest)
        self.dds = OrdersDestRepository()
        self.settings_repository = StgEtlSettingsRepository(scheme = 'dds')
        self.log = logging.getLogger()

    def load_order(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:
            
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]


            load_queue = self.origin.list_orders(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} users to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for order in load_queue:
                self.dds.insert_product(conn, order)
            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
