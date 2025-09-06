import os
import time
from typing import Optional, List, Dict, Any

from pyee.executor import ExecutorEventEmitter
from sqlcipher3 import dbapi2 as sqlite

from wxutil.logger import logger
from wxutil.utils import get_db_key, get_wx_info

import zstandard


def decompress(data):
    try:
        dctx = zstandard.ZstdDecompressor()
        x = dctx.decompress(data).strip(b"\x00").strip()
        return x.decode("utf-8").strip()
    except:
        return data


class WeChatDB:

    def __init__(self, pid: Optional[int] = None) -> None:
        self.info = get_wx_info("v4", pid)
        self.pid = self.info["pid"]
        self.key = self.info["key"]
        self.data_dir = self.info["data_dir"]
        self.msg_db = self.get_msg_db()
        self.conn = self.create_connection(rf"db_storage\message\{self.msg_db}")
        self.wxid = self.data_dir.split("\\")[-1]
        self.event_emitter = ExecutorEventEmitter()

    def get_db_path(self, db_name: str) -> str:
        return os.path.join(self.data_dir, db_name)

    def get_msg_db(self) -> str:
        msg0_file = os.path.join(self.data_dir, r"db_storage\message\message_0.db")
        msg1_file = os.path.join(self.data_dir, r"db_storage\message\message_1.db")
        if not os.path.exists(msg1_file):
            return "message_0.db"
        if os.path.getmtime(msg0_file) > os.path.getmtime(msg1_file):
            return "message_0.db"
        else:
            return "message_1.db"

    def create_connection(self, db_name: str) -> sqlite.Connection:
        conn = sqlite.connect(self.get_db_path(db_name))
        db_key = get_db_key(self.key, self.get_db_path(db_name), "4")
        print(db_key)
        conn.execute(f"PRAGMA key = \"x'{db_key}'\";")
        conn.execute(f"PRAGMA cipher_page_size = 4096;")
        conn.execute(f"PRAGMA kdf_iter = 256000;")
        conn.execute(f"PRAGMA cipher_hmac_algorithm = HMAC_SHA512;")
        conn.execute(f"PRAGMA cipher_kdf_algorithm = PBKDF2_HMAC_SHA512;")
        return conn

    def get_message(self, row):
        return {
            "local_id": row[0],
            "server_id": row[1],
            "local_type": row[2],
            "sort_seq": row[3],
            "real_sender_id": row[4],
            "create_time": row[5],
            "status": row[6],
            "upload_status": row[7],
            "download_status": row[8],
            "server_seq": row[9],
            "origin_source": row[10],
            "source": row[11],
            "message_content": row[12],
            "compress_content": row[13],
            "packed_info_data": row[14],
            "WCDB_CT_message_content": row[15],
            "WCDB_CT_source": row[16],
            "sender": row[17]
        }

    def get_event(self, table, row):
        if not row:
            return None

        message = self.get_message(row)
        data = {
            "table": table,
            "id": message["local_id"],
            "msg_id": message["server_id"],
            "sequence": message["sort_seq"],
            "type": message["local_type"],
            "sub_type": None,
            "is_sender": message["origin_source"],
            "create_time": message["create_time"],
            "msg": decompress(message["message_content"]),
            "raw_msg": None,
            "at_user_list": [],
            "room_wxid": self.id_to_wxid(message["packed_info_data"][1]),
            "from_wxid": message["sender"],
            "to_wxid": None,
            "extra": message["packed_info_data"]
        }

        if message["source"] is not None:
            data["raw_msg"] = decompress(message["source"])

        if data["is_sender"] == 1:
            data["room_wxid"] = self.id_to_wxid(message["packed_info_data"][-1])
        else:
            data["room_wxid"] = self.id_to_wxid(message["packed_info_data"][1])

        return data

    def get_recently_messages(self, table_name: str, count: int = 10, order: str = "DESC") -> List[
        Optional[Dict[str, Any]]]:
        with self.conn:
            rows = self.conn.execute(
                """
                SELECT 
                    m.*,
                    n.user_name AS sender
                FROM {} AS m
                LEFT JOIN Name2Id AS n ON m.real_sender_id = n.rowid
                ORDER BY m.local_id {}
                LIMIT ?;""".format(table_name, order),
                (count,)).fetchall()
            return [self.get_event(table_name, row) for row in rows]

    def get_msg_tables(self):
        with self.conn:
            rows = self.conn.execute("""
            SELECT 
                name
            FROM sqlite_master
            WHERE type='table'
            AND name LIKE 'Msg%';""").fetchall()
            return [row[0] for row in rows]

    def id_to_wxid(self, id):
        with self.conn:
            row = self.conn.execute("""
            SELECT user_name FROM Name2Id WHERE rowid = ?;
            """, (id,)).fetchone()
            if not row:
                return
            return row[0]

    def run(self, period=0.1):
        msg_table_max_local_id = {}
        self.msg_tables = self.get_msg_tables()
        for msg_table in self.msg_tables:
            msg_table_max_local_id[msg_table] = 0

        for table_name in msg_table_max_local_id:
            recently_messages = self.get_recently_messages(table_name, 1)
            current_local_id = recently_messages[0]["id"] if recently_messages and recently_messages[0] else 0
            msg_table_max_local_id[table_name] = current_local_id

        print(msg_table_max_local_id)

        logger.info("Start listening...")
        while True:
            current_msg_tables = self.get_msg_tables()
            new_msg_tables = list(set(current_msg_tables) - set(self.msg_tables))
            for msg_table in new_msg_tables:
                msg_table_max_local_id[msg_table] = 0
            self.msg_tables = current_msg_tables

            for table_name, current_local_id in msg_table_max_local_id.items():
                with self.conn:
                    rows = self.conn.execute("""
                    SELECT 
                        m.*,
                        n.user_name AS sender
                    FROM {} AS m
                    LEFT JOIN Name2Id AS n ON m.real_sender_id = n.rowid
                    WHERE local_id > ?;""".format(table_name), (current_local_id,)).fetchall()
                    for row in rows:
                        event = self.get_event(table_name, row)
                        logger.debug(event)
                        if event:
                            msg_table_max_local_id[table_name] = event["id"]
                            self.event_emitter.emit(f"0", self, event)
                            self.event_emitter.emit(f"{event['type']}", self, event)

            time.sleep(period)
