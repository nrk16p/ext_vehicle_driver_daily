import requests
import pandas as pd
from datetime import datetime
from io import BytesIO
from pymongo import MongoClient
import sys
import os
import urllib3

# ================================
# ⚙️ CONFIG
# ================================
MONGO_URI = os.getenv("MONGO_URI")
PHPSESSID = os.getenv("PHPSESSID")

DB_NAME = "atms"
COLLECTION_NAME = "vehicle_daily_asia"

URL = "https://www.mena-atms.com/report/print.out/print.excel/type/vehicle.daily.transaction"

# Disable SSL warning because verify=False is used
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# ================================
# 🧠 FUNCTION: GET MONTH START TO TODAY
# ================================
def get_month_start_to_today():
    """
    Example:
    If today is 09/05/2026
    return:
      t_date = 01/05/2026
      num_of_day = 9
    """
    today = datetime.now()
    start_date = today.replace(day=1)

    t_date = start_date.strftime("%d/%m/%Y")
    num_of_day = (today.date() - start_date.date()).days + 1

    return t_date, str(num_of_day)


# ================================
# 📥 FUNCTION: DOWNLOAD + PROCESS
# ================================
def fetch_data(session, t_date, num_of_day, fleet_group_id):
    print(f"🚚 Fetch fleet_group_id: {fleet_group_id}")
    print(f"📅 t_date: {t_date}, num_of_day: {num_of_day}")

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": (
            "https://www.mena-atms.com/report/excel/index.excel/"
            f"type/vehicle.daily.transaction?t_date={t_date}"
        )
    }

    data = {
        "fleet_group_id": str(fleet_group_id),
        "fleet_id": "",
        "t_date": t_date,
        "num_of_day": num_of_day,
        "submit": "พิมพ์",
        "display_type": "multiple-day",
        "report_type": "vehicle.daily.transaction"
    }

    try:
        response = session.post(
            URL,
            headers=headers,
            data=data,
            verify=False,
            stream=True,
            timeout=120
        )

        print(f"🌐 Status Code fleet {fleet_group_id}: {response.status_code}")

        content = b"".join(response.iter_content(8192))

        if not content.startswith(b"PK"):
            print(f"❌ Not Excel file for fleet_group_id {fleet_group_id}")
            print("🔎 Response preview:")
            print(content[:500])
            return pd.DataFrame()

        df = pd.read_excel(BytesIO(content), skiprows=2, engine="openpyxl")

        df.columns = df.columns.astype(str).str.strip()
        df = df.dropna(how="all")

        required_columns = [
            "Unnamed: 0",
            "Unnamed: 1",
            "Unnamed: 3",
            "รหัส",
            "ชื่อ",
            "เบอร์รถ",
            "ทะเบียน",
            "สถานะ",
            "คนขับ",
            "รหัส.1",
            "ชื่อ.1"
        ]

        missing_columns = [col for col in required_columns if col not in df.columns]

        if missing_columns:
            print(f"❌ Missing columns for fleet_group_id {fleet_group_id}: {missing_columns}")
            print("📌 Available columns:")
            print(df.columns.tolist())
            return pd.DataFrame()

        df = df[required_columns]

        df = df.rename(columns={
            "Unnamed: 0": "วันที่",
            "Unnamed: 1": "ฟลีท",
            "Unnamed: 3": "ลูกค้า",
            "ชื่อ": "แพล้นท์",
            "รหัส.1": "รหัสคนขับ",
            "ชื่อ.1": "ชื่อคนขับ"
        })

        df["fleet_group_id"] = fleet_group_id
        df["t_date"] = t_date
        df["num_of_day"] = int(num_of_day)
        df["etl_run_at"] = datetime.now()

        print(f"✅ Fleet {fleet_group_id} rows: {len(df)}")

        return df

    except Exception as e:
        print(f"❌ Error fetching fleet_group_id {fleet_group_id}: {e}")
        return pd.DataFrame()


# ================================
# 🚀 MAIN ETL
# ================================
def run():
    print("🚀 Start ETL...")

    if not MONGO_URI:
        print("❌ MONGO_URI is missing. Please set environment variable MONGO_URI.")
        sys.exit(1)

    if not PHPSESSID:
        print("❌ PHPSESSID is missing. Please set environment variable PHPSESSID.")
        sys.exit(1)

    t_date, num_of_day = get_month_start_to_today()

    print(f"📅 Start date: {t_date}")
    print(f"📆 Number of days: {num_of_day}")

    session = requests.Session()
    session.cookies.set("PHPSESSID", PHPSESSID)

    fleet_ids = [1, 2]
    all_df = []

    for fleet_id in fleet_ids:
        df = fetch_data(
            session=session,
            t_date=t_date,
            num_of_day=num_of_day,
            fleet_group_id=fleet_id
        )

        if not df.empty:
            all_df.append(df)

    if len(all_df) == 0:
        print("❌ No data from all fleets")
        sys.exit(1)

    df = pd.concat(all_df, ignore_index=True)

    print(f"📊 Final shape: {df.shape}")
    print("📌 Final columns:")
    print(df.columns.tolist())

    # ================================
    # 🔗 MONGO CONNECT
    # ================================
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]

    # ================================
    # 🧹 DELETE EXISTING SAME PERIOD DATA
    # ================================
    print(f"🧹 Deleting old records for t_date = {t_date} ...")
    delete_result = collection.delete_many({"t_date": t_date})
    print(f"🗑️ Deleted records: {delete_result.deleted_count}")

    # ================================
    # ⚡ INDEX
    # ================================
    collection.create_index([("t_date", 1)])
    collection.create_index([("ทะเบียน", 1)])
    collection.create_index([("fleet_group_id", 1)])
    collection.create_index([("etl_run_at", 1)])

    # ================================
    # 🚀 INSERT
    # ================================
    records = df.to_dict("records")

    if records:
        collection.insert_many(records)
        print(f"✅ Inserted records: {len(records)}")
    else:
        print("⚠️ No records to insert")

    client.close()

    print("🎉 ETL SUCCESS")


# ================================
# ▶️ RUN
# ================================
if __name__ == "__main__":
    run()