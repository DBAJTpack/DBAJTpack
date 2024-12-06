import yfinance as yf
import pandas as pd
import pyodbc  # สำหรับเชื่อมต่อกับ MSSQL

# กำหนด symbol สำหรับ WTI Crude Oil Futures
symbol = 'CL=F'

end_date = pd.to_datetime('today') - pd.DateOffset(days=30)

start_date = end_date - pd.DateOffset(days=1)

data = yf.download(symbol, start=start_date, end=end_date, interval="1d")

# แสดงข้อมูล
print(data)

# ตั้งค่าการเชื่อมต่อกับ MSSQL
server = '203.151.136.66'
database = 'Exchange'
username = 'thamonwan.e'
password = 'Thamonwan170124'

# การเชื่อมต่อฐานข้อมูล MSSQL
conn = pyodbc.connect(f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}')
cursor = conn.cursor()

# เรียกใช้ Stored Procedure เพื่อ insert ข้อมูล
insert_sp = "{CALL InsertWTIData (?, ?, ?, ?, ?)}"

# วนลูปผ่านแต่ละแถวใน DataFrame ที่ได้จาก yfinance
for index, row in data.iterrows():
    # ตรวจสอบชนิดของ row.name ว่าเป็น datetime
    if isinstance(row.name, pd.Timestamp):
        date_oil = row.name
    else:
        date_oil = pd.to_datetime(row.name)  # แปลงเป็น datetime ถ้าไม่ใช่

    # ใช้ .iloc[] เพื่อเข้าถึงค่าตามตำแหน่ง
    last_price = row.iloc[1]  # 'Close'
    price_open = row.iloc[4]  # 'Open'
    price_high = row.iloc[2]  # 'High'
    price_low = row.iloc[3]   # 'Low'

    # เรียกใช้ Stored Procedure เพื่อ insert ข้อมูล
    try:
        cursor.execute(insert_sp, 
                       date_oil,  # date_oil
                       last_price,  # last (ราคาปิด)
                       price_open,  # price_open (ราคาเปิด)
                       price_high,  # price_high (ราคาสูงสุด)
                       price_low)  # price_low (ราคาต่ำสุด)
    except pyodbc.Error as e:
        print(f"เกิดข้อผิดพลาด: {e}")
        print(f"ข้อมูลที่พยายาม insert: {row}")

# บันทึกการเปลี่ยนแปลงในฐานข้อมูล
conn.commit()

# แสดงข้อความว่าการ insert สำเร็จ
print("ข้อมูลถูกบันทึกลงในฐานข้อมูลเรียบร้อยแล้ว")

# ปิดการเชื่อมต่อฐานข้อมูล
cursor.close()
conn.close()

