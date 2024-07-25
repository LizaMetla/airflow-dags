import logging


from examples.dds.dds_fct_sales_loader import FctProductsLoader
import pendulum
from airflow.decorators import dag, task

from examples.dds.dds_users_loader import UserLoader
from examples.dds.dds_restaurants_loader import RestaurantLoader 
from examples.dds.dds_timestamp_loader import TimestampLoader 
from examples.dds.dds_product_loader import ProductLoader 
from examples.dds.dds_orders_loader import OrdersLoader

from lib import ConnectionBuilder


log = logging.getLogger(__name__)


@dag(
    "dds_load_dag",
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'stg', 'origin', 'example'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def dds_load_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")




    # Объявляем таск, который загружает данные.
    @task(task_id="dds_users_load")
    def load_users():
        # создаем экземпляр класса, в котором реализована логика.
        users_loader = UserLoader(dwh_pg_connect, log)
        users_loader.load_users()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные таски.
    users_loader = load_users()





    @task(task_id="dds_rest_load")
    def load_rest():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = RestaurantLoader(dwh_pg_connect, log)
        rest_loader.load_restaurant()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные таски.
    rest_loader = load_rest()



    @task(task_id="dds_timestamp_load")
    def load_tmsp():
        # создаем экземпляр класса, в котором реализована логика.
        tmsp_loader = TimestampLoader(dwh_pg_connect, log)
        tmsp_loader.load_timestamp()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные таски.
    timestamp_loader = load_tmsp()


    @task(task_id="dds_product_load")
    def load_product():
         # создаем экземпляр класса, в котором реализована логика.
        product_loader = ProductLoader(dwh_pg_connect, log)
        product_loader.load_restaurant()  # Вызываем функцию, которая перельет данные.

        # Инициализируем объявленные таски.
    product_loader = load_product()


    @task(task_id="dds_order_load")
    def load_order():
         # создаем экземпляр класса, в котором реализована логика.
        order_loader = OrdersLoader(dwh_pg_connect, log)
        order_loader.load_order()  # Вызываем функцию, которая перельет данные.

        # Инициализируем объявленные таски.
    order_loader_task = load_order()

    @task(task_id="dds_events_load")
    def load_events_task():
         # создаем экземпляр класса, в котором реализована логика.
        order_loader = FctProductsLoader(dwh_pg_connect)
        order_loader.load_product_facts()  # Вызываем функцию, которая перельет данные.

        # Инициализируем объявленные таски.
    events_loader = load_events_task()

    # Далее задаем последовательность выполнения тасков.
    users_loader >> rest_loader >> timestamp_loader >> product_loader >> order_loader_task >> events_loader  # type: ignore
    


dds_load_dag = dds_load_dag()
