from functools import lru_cache
import os
import time
from typing import Optional, List, Dict, NoReturn, Tuple, Union, Callable, Any

from pyee.executor import ExecutorEventEmitter
from sqlcipher3 import dbapi2 as sqlite

from wxutil.logger import logger
from wxutil.utils import get_db_key, get_wx_info, parse_xml, decompress

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
        self.wxid_table_mapping = {}

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

    @lru_cache
    def get_msg_table_by_wxid(self, wxid: str) -> str:
        msg_tables = self.get_msg_tables()
        for msg_table in msg_tables:
            messages = self.get_recently_messages2(msg_table, wxid, 1)
            if messages:
                message = messages[0]
                if message["room_wxid"] is None:
                    self.wxid_table_mapping[message["from_wxid"]] = msg_table
                else:
                    self.wxid_table_mapping[message["room_wxid"]] = msg_table
        return self.wxid_table_mapping.get(wxid)

    def get_text_msg(self, self_wxid: str, to_wxid: str, content: str, seconds: int = 30, limit: int = 1) -> List[
        Optional[Dict]]:
        create_time = int(time.time()) - seconds
        table = self.get_msg_table_by_wxid(to_wxid)
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

    def get_image_msg(self, self_wxid: str, to_wxid: str, md5: str, seconds: int = 30, limit: int = 1) -> List[
        Optional[Dict]]:
        data = []
        create_time = int(time.time()) - seconds
        table = self.get_msg_table_by_wxid(to_wxid)
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

    def get_file_msg(self, self_wxid: str, to_wxid: str, md5: str, seconds: int = 30, limit: int = 1) -> List[
        Optional[Dict]]:
        data = []
        create_time = int(time.time()) - seconds
        table = self.get_msg_table_by_wxid(to_wxid)
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


wechat_db = WeChatDB()


@wechat_db.handle(ALL_MESSAGE)
def _(wechat_db, event):
    print(event)


wechat_db.run()

# db_name = r"C:\Users\69012\Documents\xwechat_files\wxid_g7leryvu7kqm22_a246\db_storage\message\message_0.db"
# wx_info = get_wx_info("v4")
# with open(wechat_db.msg_db, "wb") as f:
#     data = decrypt_db_file_v4(
#         path=db_name,
#         pkey=wx_info["key"]
#     )
#     f.write(data)

# data = binascii.unhexlify("28B52FFD600004851400C6A87F26F0CCEA01DBD2BD136B8CC056A4DE557C4456A0DB7782CF5B8C2DB7842CD5C397DB44D6CDA634700067007D0093CA289D5E93D6AB52F9D4F9DC2BC20C4D4A7C8F8E6DCED44A2C3D42D2FF92D2DDA1477FA6BF23691CE6DA98CCDBF05D38D091B89CD97077CB0BA7CA09317C28DB4CAF09DD78F9AC168AF4821795B43AD2265BBA6C17D8D65142D1C0BA1D291A018E034047DA2EBAE37266A1ED8880280E9CF79233DB1CBD42B6BE5BF83CF5D64BF143815074A489F7C2A1104251625B63BDB3CCB1DEF125D61A29A3237919A67356E13B52E6615BD6DCE76379A5FBF4E8D621089D3EFC499FFF877ED2FAFFCFE3487FD2F8F5E93F2F1FC7E7218EF1238D1452FA94D60F1B4A92903554A8EF3A2AC56745AA62E0986C181E9388024935239FF5452494BE54A64BE5604D0607130D8400413B288DC002E5B732D189C07E060FAB816C7243D2DCD9810B659BB2CE1EDCDC458984A2E158783DF47A2850C2EB5E9969A6F5A12EE3F554ECB6E6CB612EDDA631CD5DBE4A47CA9FEE1EB0DE0F3793B3458A06D66D46C488120A88CDC2A707A284C63128607BCA3A9B59CFE2CC613CBB61BA7BD8DC85E2590D626B46C4E876DEDACB98DD5AD3B4D7E10C6738C33A6B9AC2584CBC199137B68BF62C87C1512267B4C688141044A18A5385FAE9AB2ADA674DEA916069905416C8A3FAAC8A15A98FB2540E6A1359B03C22B164644F023B20200282A8A3720CDF011A147209B0397E6A53A3F0AC41940B5023088561411281225E50232D5F7176B9DA1D8356D7B24B4C1B514A44CD004A7010296EB472311B520B232D6809E05C8A0DDA2180B6143DE37C17995D0652B4C643C2DA88A04A54BEA1F37498390745AE82C67DA365076E7B06194FF44E1D23BE8D228BE688FFBC73CC4C2C1BCB5D454058809819")
# print(data)
# print(decompress(data))

wechat_db.get_text_msg("wxid_g7leryvu7kqm22", "wxid_vqj81fdula0x22", "测试")
