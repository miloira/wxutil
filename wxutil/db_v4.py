import glob
import hashlib
import os
import re
import time
from typing import Any, Callable, Dict, List, Optional, Tuple, Union, NoReturn

from pyee.executor import ExecutorEventEmitter
from sqlcipher3 import dbapi2 as sqlite

from wxutil.logger import logger
from wxutil.utils import decompress, get_db_key, get_wx_info, parse_xml

ALL_MESSAGE = 0
TEXT_MESSAGE = 1
TEXT2_MESSAGE = 2
IMAGE_MESSAGE = 3
VOICE_MESSAGE = 34
CARD_MESSAGE = 42
VIDEO_MESSAGE = 43
EMOTION_MESSAGE = 47
LOCATION_MESSAGE = 48
VOIP_MESSAGE = 50
OPEN_IM_CARD_MESSAGE = 66
SYSTEM_MESSAGE = 10000
FILE_MESSAGE = 25769803825
FILE_WAIT_MESSAGE = 317827579953
LINK_MESSAGE = 21474836529
LINK2_MESSAGE = 292057776177
SONG_MESSAGE = 12884901937
LINK4_MESSAGE = 4294967345
LINK5_MESSAGE = 326417514545
LINK6_MESSAGE = 17179869233
RED_ENVELOPE_MESSAGE = 8594229559345
TRANSFER_MESSAGE = 8589934592049
QUOTE_MESSAGE = 244813135921
MERGED_FORWARD_MESSAGE = 81604378673
APP_MESSAGE = 141733920817
APP2_MESSAGE = 154618822705
WECHAT_VIDEO_MESSAGE = 219043332145
COLLECTION_MESSAGE = 103079215153
PAT_MESSAGE = 266287972401
GROUP_ANNOUNCEMENT_MESSAGE = 373662154801


class WeChatDB:
    def __init__(self, pid: Optional[int] = None) -> None:
        self.info = get_wx_info("v4", pid)
        self.pid = self.info["pid"]
        self.key = self.info["key"]
        self.data_dir = self.info["data_dir"]
        self.msg_db = self.get_msg_db()
        self.msg_db_wal = self.get_db_path(rf"db_storage\message\{self.msg_db}-wal")
        self.conn = self.create_connection(rf"db_storage\message\{self.msg_db}")
        self.wxid = self.data_dir.rstrip("\\").split("\\")[-1][:-5]
        self.event_emitter = ExecutorEventEmitter()

    def get_db_path(self, db_name: str) -> str:
        return os.path.join(self.data_dir, db_name)

    def get_msg_db(self) -> str:
        db_files = glob.glob(
            os.path.join(
                os.path.join(self.data_dir, "db_storage", "message"), "message_*.db"
            )
        )
        db_files = [
            db_file for db_file in db_files if re.match(r".*message_\d+\.db$", db_file)
        ]

        if not db_files:
            raise Exception("No message database found.")

        latest_file = max(db_files, key=os.path.getmtime)
        return os.path.basename(latest_file)

    def create_connection(self, db_name: str) -> sqlite.Connection:
        conn = sqlite.connect(self.get_db_path(db_name), check_same_thread=False)
        db_key = get_db_key(self.key, self.get_db_path(db_name), "4")
        conn.execute(f"PRAGMA key = \"x'{db_key}'\";")
        conn.execute(f"PRAGMA cipher_page_size = 4096;")
        conn.execute(f"PRAGMA kdf_iter = 256000;")
        conn.execute(f"PRAGMA cipher_hmac_algorithm = HMAC_SHA512;")
        conn.execute(f"PRAGMA cipher_kdf_algorithm = PBKDF2_HMAC_SHA512;")
        return conn

    def get_message(self, row: Tuple) -> Dict:
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
            "sender": row[17],
        }

    def get_event(self, table: str, row: Optional[Tuple]) -> Optional[Dict]:
        if not row:
            return None

        message = self.get_message(row)
        data = {
            "table": table,
            "id": message["local_id"],
            "msg_id": message["server_id"],
            "sequence": message["sort_seq"],
            "type": message["local_type"],
            "is_sender": 1 if message["sender"] == self.wxid else 0,
            "msg": decompress(message["message_content"]),
            "source": None,
            "at_user_list": [],
            "room_wxid": None,
            "from_wxid": message["sender"],
            "to_wxid": None,
            "extra": message["packed_info_data"],
            "status": message["status"],
            "create_time": message["create_time"],
        }

        if message["source"]:
            data["source"] = parse_xml(decompress(message["source"]))
            if (
                data["source"]
                and data["source"].get("msgsource")
                and data["source"]["msgsource"].get("atuserlist")
            ):
                data["at_user_list"] = data["source"]["msgsource"]["atuserlist"].split(
                    ","
                )

        if data["type"] != 1:
            try:
                data["msg"] = parse_xml(data["msg"])
            except Exception:
                pass

        if data["is_sender"] == 1:
            wxid = self.id_to_wxid(message["packed_info_data"][:4][-1])
            if wxid.endswith("@chatroom"):
                data["room_wxid"] = wxid
            else:
                data["to_wxid"] = wxid
        else:
            wxid = self.id_to_wxid(message["packed_info_data"][:4][1])
            if wxid.endswith("@chatroom"):
                data["room_wxid"] = wxid
            else:
                data["to_wxid"] = self.id_to_wxid(message["packed_info_data"][:4][-1])

        return data

    def get_msg_table(self, wxid: str) -> str:
        return f"Msg_{hashlib.md5(wxid.encode()).hexdigest()}"

    def get_text_msg(
        self,
        self_wxid: str,
        to_wxid: str,
        content: str,
        seconds: int = 30,
        limit: int = 1,
    ) -> List[Optional[Dict]]:
        create_time = int(time.time()) - seconds
        table = self.get_msg_table(to_wxid)
        with self.conn:
            data = self.conn.execute(
                """
                SELECT 
                    m.*,
                    n.user_name AS sender
                FROM {} AS m
                LEFT JOIN Name2Id AS n ON m.real_sender_id = n.rowid
                WHERE m.local_type = 1 
                AND n.user_name = ? 
                AND m.message_content like ?
                AND m.create_time > ?
                ORDER BY m.local_id DESC
                LIMIT ?;
                """.format(table),
                (self_wxid, f"%{content}%", create_time, limit),
            ).fetchall()
            return [self.get_event(table, item) for item in data]

    def get_image_msg(
        self, self_wxid: str, to_wxid: str, md5: str, seconds: int = 30, limit: int = 1
    ) -> List[Optional[Dict]]:
        data = []
        create_time = int(time.time()) - seconds
        table = self.get_msg_table(to_wxid)
        with self.conn:
            rows = self.conn.execute(
                """
                SELECT 
                    m.*,
                    n.user_name AS sender
                FROM {} AS m
                LEFT JOIN Name2Id AS n ON m.real_sender_id = n.rowid
                WHERE m.local_type = 3 
                AND n.user_name = ? 
                AND m.create_time > ?
                ORDER BY m.local_id DESC
                LIMIT ?;
                """.format(table),
                (self_wxid, create_time, limit),
            ).fetchall()
            for row in rows:
                message_content = parse_xml(decompress(row[12]))
                if message_content["msg"]["img"]["@md5"] == md5:
                    data.append(row)
        return [self.get_event(table, item) for item in data]

    def get_file_msg(
        self, self_wxid: str, to_wxid: str, md5: str, seconds: int = 30, limit: int = 1
    ) -> List[Optional[Dict]]:
        data = []
        create_time = int(time.time()) - seconds
        table = self.get_msg_table(to_wxid)
        with self.conn:
            rows = self.conn.execute(
                """
                SELECT 
                    m.*,
                    n.user_name AS sender
                FROM {} AS m
                LEFT JOIN Name2Id AS n ON m.real_sender_id = n.rowid
                WHERE m.local_type = 25769803825
                AND n.user_name = ? 
                AND m.create_time > ?
                ORDER BY m.local_id DESC
                LIMIT ?;
                """.format(table),
                (self_wxid, create_time, limit),
            ).fetchall()
            for row in rows:
                message_content = parse_xml(decompress(row[12]))
                if message_content["msg"]["appmsg"]["md5"] == md5:
                    data.append(row)
        return [self.get_event(table, item) for item in data]

    def get_recently_messages(
        self, table: str, count: int = 10, order: str = "DESC"
    ) -> List[Optional[Dict]]:
        with self.conn:
            rows = self.conn.execute(
                """
                SELECT 
                    m.*,
                    n.user_name AS sender
                FROM {} AS m
                LEFT JOIN Name2Id AS n ON m.real_sender_id = n.rowid
                ORDER BY m.local_id {}
                LIMIT ?;
                """.format(table, order),
                (count,),
            ).fetchall()
            return [self.get_event(table, row) for row in rows]

    def get_recently_messages2(
        self, table: str, self_wxid: str, count: int = 10, order: str = "DESC"
    ) -> List[Optional[Dict]]:
        with self.conn:
            rows = self.conn.execute(
                """
                SELECT 
                    m.*,
                    n.user_name AS sender
                FROM {} AS m
                LEFT JOIN Name2Id AS n ON m.real_sender_id = n.rowid
                WHERE n.user_name = ?
                ORDER BY m.local_id {}
                LIMIT ?;
                """.format(table, order),
                (self_wxid, count),
            ).fetchall()
            return [self.get_event(table, row) for row in rows]

    def get_msg_tables(self) -> List[str]:
        with self.conn:
            rows = self.conn.execute("""
            SELECT 
                name
            FROM sqlite_master
            WHERE type='table'
            AND name LIKE 'Msg%';
            """).fetchall()
            return [row[0] for row in rows]

    def id_to_wxid(self, id: int) -> Optional[str]:
        with self.conn:
            row = self.conn.execute(
                """
            SELECT
                user_name 
            FROM Name2Id 
            WHERE rowid = ?;
            """,
                (id,),
            ).fetchone()
            if not row:
                return
            return row[0]

    def handle(
        self, events: Union[int, list] = 0, once: bool = False
    ) -> Callable[[Callable[..., Any]], None]:
        def wrapper(func: Callable[..., Any]) -> None:
            listen = self.event_emitter.on if not once else self.event_emitter.once
            if isinstance(events, int):
                listen(str(events), func)
            elif isinstance(events, list):
                for event in events:
                    listen(str(event), func)
            else:
                raise TypeError("events must be int or list.")

        return wrapper

    def run(self, period: float = 0.1) -> NoReturn:
        msg_table_max_local_id = {}
        self.msg_tables = self.get_msg_tables()
        for msg_table in self.msg_tables:
            recently_messages = self.get_recently_messages(msg_table, 1)
            current_max_local_id = (
                recently_messages[0]["id"]
                if recently_messages and recently_messages[0]
                else 0
            )
            msg_table_max_local_id[msg_table] = current_max_local_id

        logger.info(self.info)
        logger.info("Message listening...")
        last_mtime = os.path.getmtime(self.msg_db_wal)
        while True:
            mtime = os.path.getmtime(self.msg_db_wal)
            if mtime != last_mtime:
                current_msg_tables = self.get_msg_tables()
                new_msg_tables = list(set(current_msg_tables) - set(self.msg_tables))
                self.msg_tables = current_msg_tables
                for new_msg_table in new_msg_tables:
                    msg_table_max_local_id[new_msg_table] = 0

                for table, max_local_id in msg_table_max_local_id.items():
                    with self.conn:
                        rows = self.conn.execute(
                            """
                        SELECT 
                            m.*,
                            n.user_name AS sender
                        FROM {} AS m
                        LEFT JOIN Name2Id AS n ON m.real_sender_id = n.rowid
                        WHERE local_id > ?;
                        """.format(table),
                            (max_local_id,),
                        ).fetchall()
                        for row in rows:
                            event = self.get_event(table, row)
                            logger.debug(event)
                            if event:
                                msg_table_max_local_id[table] = event["id"]
                                self.event_emitter.emit(f"0", self, event)
                                self.event_emitter.emit(f"{event['type']}", self, event)

                last_mtime = mtime

            time.sleep(period)

    def __str__(self) -> str:
        return f"<WeChatDB pid={repr(self.pid)} wxid={repr(self.wxid)} msg_db={repr(self.msg_db)}>"


if __name__ == "__main__":
    wechat_db = WeChatDB()

    @wechat_db.handle(ALL_MESSAGE)
    def _(wechat_db, event):
        print(event)

    wechat_db.run()
