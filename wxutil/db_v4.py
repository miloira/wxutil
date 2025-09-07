import os
import time
from typing import Optional, List, Dict, Any

from pyee.executor import ExecutorEventEmitter
from sqlcipher3 import dbapi2 as sqlite

from wxutil.logger import logger
from wxutil.utils import get_db_key, get_wx_info, parse_xml

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
        # self.info = get_wx_info("v4", pid)
        self.info = {'pid': 11736, 'version': '4.0.3.22', 'account': 'm690126048/wxid_g7leryvu7kqm22',
                     'data_dir': 'C:\\Users\\69012\\Documents\\xwechat_files\\wxid_g7leryvu7kqm22_a246\\',
                     'key': 'b2a1c68323c14ebbb18ce6be0b91b12ccef961d15e504e3eb1d1b58bf9b058f0'}
        self.pid = self.info["pid"]
        self.key = self.info["key"]
        self.data_dir = self.info["data_dir"]
        self.msg_db = self.get_msg_db()
        self.conn = self.create_connection(rf"db_storage\message\{self.msg_db}")
        self.wxid = self.data_dir.rstrip("\\").split("\\")[-1][:-5]
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
            "is_sender": 1 if message["sender"] == self.wxid else 0,
            "create_time": message["create_time"],
            "msg": decompress(message["message_content"]),
            "raw_msg": None,
            "at_user_list": [],
            "room_wxid": None,
            "from_wxid": message["sender"],
            "to_wxid": None,
            "extra": message["packed_info_data"]
        }

        if message["source"]:
            data["raw_msg"] = parse_xml(decompress(message["source"]))
            if data["raw_msg"].get("msgsource", {}).get("atuserlist"):
                data["at_user_list"] = data["raw_msg"]["msgsource"]["atuserlist"].split(",")

        if data["is_sender"] == 1:
            wxid = self.id_to_wxid(message["packed_info_data"][-1])
            if wxid.endswith("@chatroom"):
                data["room_wxid"] = wxid
            else:
                data["to_wxid"] = wxid
        else:
            wxid = self.id_to_wxid(message["packed_info_data"][1])
            if wxid.endswith("@chatroom"):
                data["room_wxid"] = wxid
            else:
                data["to_wxid"] = self.id_to_wxid(message["packed_info_data"][-1])

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


wechat_db = WeChatDB()
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
