import os
import json
import time
import asyncio
import websockets
from kafka import KafkaProducer
from web3 import Web3
from web3.providers.websocket import WebsocketProvider
from web3.exceptions import BlockNotFound
from datetime import datetime, timedelta, timezone
from fastapi import FastAPI
from fastapi.responses import FileResponse
from pydantic import BaseModel
import smtplib
from email.mime.text import MIMEText
from goplus.address import Address
from dotenv import load_dotenv
import uuid

load_dotenv()

app = FastAPI()

# Kafka配置
ETH_WS = os.getenv("ETH_WS")
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS").split(",")
KAFKA_USERNAME = os.getenv("KAFKA_USERNAME")
KAFKA_PASSWORD = os.getenv("KAFKA_PASSWORD")
TOPIC_BLOCK = os.getenv("TOPIC_BLOCK", "raw-blocks")
TOPIC_ALERT = os.getenv("TOPIC_ALERT", "eth_alerts")
MAIL_USER = os.getenv('MAIL_USER')
MAIL_PASS = os.getenv('MAIL_PASS')

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKERS,
    security_protocol="SASL_SSL",
    sasl_mechanism="PLAIN",
    sasl_plain_username=KAFKA_USERNAME,
    sasl_plain_password=KAFKA_PASSWORD,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    retries=5
)

ETH_WS = os.getenv("ETH_WS", "wss://eth-mainnet.g.alchemy.com/v2/X4bmm7I5BQeOpOFIKhVDPQzfWcgOoJ18")
w3 = Web3(WebsocketProvider(ETH_WS))

ALERT_VALUE = 100 * 1e18  # 100 ETH

def fetch_block_with_retry(number, retries=3, delay=1):
    for _ in range(retries):
        try:
            return w3.eth.get_block(number, full_transactions=True)
        except BlockNotFound:
            time.sleep(delay)
    return None

def to_beijing_time(utc_ts):
    utc_dt = datetime.fromtimestamp(utc_ts, tz=timezone.utc)
    beijing_dt = utc_dt + timedelta(hours=8)
    return beijing_dt.strftime('%Y-%m-%d %H:%M:%S')

def send_qq_mail(to_email, subject, content):
    mail_host = "smtp.qq.com"
    mail_port = 465  # SSL端口
    mail_user = MAIL_USER
    mail_pass = MAIL_PASS

    message = MIMEText(content, 'plain', 'utf-8')
    message['From'] = mail_user
    message['To'] = to_email
    message['Subject'] = subject

    try:
        smtpObj = smtplib.SMTP_SSL(mail_host, mail_port)
        smtpObj.login(mail_user, mail_pass)
        smtpObj.sendmail(mail_user, [to_email], message.as_string())
        smtpObj.quit()
        print("✅ 邮件发送成功！")
    except Exception as e:
        print("❌ 邮件发送失败:", e)

def send_alert(msg, to_email):
    producer.send(TOPIC_ALERT, msg)
    producer.flush()
    subject = f"【ETH风控警报】{msg.get('type', '未知类型')}"
    content = json.dumps(msg, ensure_ascii=False, indent=2)
    send_qq_mail(to_email, subject, content)

def compact(s):
    return f"{s[:5]}⋯{s[-7:]}" if s and len(s) > 12 else s

class MonitorInput(BaseModel):
    mode: str                # "tx" 或 "block"
    target_count: int
    to_email: str

@app.post("/monitor")
async def monitor_api(params: MonitorInput):
    """
    FastAPI接口，输入参数采集区块/交易，返回前10条及json下载链接
    """
    mode = params.mode
    target_count = params.target_count
    to_email = params.to_email

    tx_collected = 0
    block_collected = 0
    all_txs = []
    all_blocks = []

    async with websockets.connect(ETH_WS, ping_interval=20, ping_timeout=10) as ws:
        await ws.send(json.dumps({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_subscribe",
            "params": ["newHeads"]
        }))
        resp = json.loads(await ws.recv())
        sub_block_id = resp.get("result")

        async for msg in ws:
            data = json.loads(msg)
            if data.get("method") != "eth_subscription":
                continue
            sub_id = data["params"]["subscription"]
            result = data["params"]["result"]
            if sub_id == sub_block_id and isinstance(result, dict):
                num = int(result["number"], 16)
                blk = fetch_block_with_retry(num)
                if blk:
                    block_data = {
                        "number": blk.number,
                        "timestamp": to_beijing_time(blk.timestamp),
                        "tx_count": len(blk.transactions),
                        "txs": []
                    }

                    for tx in blk.transactions:
                        tx_hash = tx.hash.hex()
                        from_addr = tx['from'].lower()
                        to_addr = tx.to.lower() if tx.to else ""
                        input_data = tx.input.hex() if hasattr(tx.input, 'hex') else str(tx.input)
                        value_eth = tx.value / 1e18

                        # 风控1: 大额转账报警
                        if tx.value > ALERT_VALUE:
                            send_alert({
                                "type": "large_transfer",
                                "hash": tx_hash,
                                "from": from_addr,
                                "to": to_addr,
                                "value": value_eth,
                                "block": blk.number
                            }, to_email)

                        # 风控2: GoPlus黑名单
                        try:
                            data = Address(access_token=None).address_security(address=from_addr)
                            result = data.to_dict()
                            if result.get("blacklist_doubt", "0") == "1":
                                send_alert({
                                    "type": "goplus_blacklist",
                                    "hash": tx_hash,
                                    "from": from_addr,
                                    "to": to_addr,
                                    "block": blk.number,
                                    "goplus_result": result
                                }, to_email)
                            time.sleep(0.1)
                        except Exception as e:
                            print("GoPlus API 检查异常:", e)

                        tx_simple = {
                            "hash": compact(tx_hash),
                            "from": compact(from_addr),
                            "to": compact(to_addr) if to_addr else None,
                            "value": tx.value,
                            "gas": tx.gas,
                            "gasPrice": tx.gasPrice,
                            "nonce": tx.nonce,
                            "input": compact(input_data)
                        }
                        block_data["txs"].append(tx_simple)
                        all_txs.append(tx_simple)

                        if mode == "tx":
                            tx_collected += 1
                            if tx_collected >= target_count:
                                # ---- 写入全部数据到 /tmp
                                filename = f"monitor_{int(time.time())}_{uuid.uuid4().hex}.json"
                                filepath = f"/tmp/{filename}"
                                with open(filepath, "w", encoding="utf-8") as f:
                                    json.dump(all_txs, f, ensure_ascii=False, indent=2)

                                # ---- 返回前10条 + 文件下载链接
                                download_url = f"/download/{filename}"
                                return {
                                    "mode": mode,
                                    "total_collected": tx_collected,
                                    "preview": all_txs[:10],
                                    "download_url": download_url
                                }

                    if mode == "block":
                        all_blocks.append(block_data)
                        block_collected += 1
                        if block_collected >= target_count:
                            filename = f"monitor_{int(time.time())}_{uuid.uuid4().hex}.json"
                            filepath = f"/tmp/{filename}"
                            with open(filepath, "w", encoding="utf-8") as f:
                                json.dump(all_blocks, f, ensure_ascii=False, indent=2)

                            download_url = f"/download/{filename}"
                            # 只预览最后一个区块的前10条交易
                            preview = block_data["txs"][:10]
                            return {
                                "mode": mode,
                                "total_collected": block_collected,
                                "preview": preview,
                                "download_url": download_url
                            }
    return {"error": "采集未成功"}

# ---- 新增一个静态文件路由用于下载 ----
from fastapi.responses import FileResponse

@app.get("/download/{filename}")
def download_file(filename: str):
    file_path = f"/tmp/{filename}"
    if os.path.exists(file_path):
        return FileResponse(path=file_path, filename=filename, media_type='application/json')
    else:
        return {"error": "文件不存在"}
