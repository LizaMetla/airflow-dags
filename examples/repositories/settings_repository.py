import json
from typing import Dict, Optional

from psycopg.rows import class_row
from pydantic import BaseModel

from pg_connect import PgConnect


class SettingRecord(BaseModel):
    id: int
    elt_workflow_key: str
    elt_workflow_settings: str


class EtlSetting:
    def __init__(self, wf_key: str, setting: Dict) -> None:
        self.elt_workflow_key = wf_key
        self.elt_workflow_settings = setting


class StgEtlSettingsRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def get_setting(self, etl_key: str) -> Optional[EtlSetting]:
        with self._db.client() as conn:
            with conn.cursor(row_factory=class_row(SettingRecord)) as cur:
                cur.execute(
                    """
                    SELECT
                        id,
                        elt_workflow_key,
                        elt_workflow_settings
                    FROM stg.srv_etl_settings
                    WHERE elt_workflow_key = %(etl_key)s;
                """,
                    {"etl_key": etl_key},
                )
                obj = cur.fetchone()

        if not obj:
            return None

        return EtlSetting(obj.elt_workflow_key, json.loads(obj.elt_workflow_settings))

    def save_setting(self, sett: EtlSetting) -> None:
        with self._db.client() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO stg.srv_etl_settings(elt_workflow_key, elt_workflow_settings)
                    VALUES (%(etl_key)s, %(etl_setting)s)
                    ON CONFLICT (elt_workflow_key) DO UPDATE
                    SET elt_workflow_settings = EXCLUDED.elt_workflow_settings;
                """,
                    {
                        "etl_key": sett.elt_workflow_key,
                        "etl_setting": json.dumps(sett.elt_workflow_settings)
                    },
                )
                conn.commit()
