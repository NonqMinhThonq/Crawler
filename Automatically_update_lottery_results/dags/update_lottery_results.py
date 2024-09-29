from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
import pendulum
import logging

# Cấu hình logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Múi giờ địa phương (Vietnam)
local_tz = pendulum.timezone('Asia/Ho_Chi_Minh')

# Hàm cập nhật kết quả xổ số
def update_lottery_results():
    # Kết nối đến MongoDB
    client = MongoClient('mongodb+srv://Thonq:minhthon9@cluster0.4kssajh.mongodb.net/?retryWrites=true&w=majority')  # Thay đổi URL nếu cần
    db = client['ket_qua_xs']  # Tên cơ sở dữ liệu
    collection = db['ket_qua']  # Tên bộ sưu tập

    # Gửi yêu cầu đến trang web
    url = 'https://xskt.com.vn/xsmb'
    response = requests.get(url)

    # Phân tích cú pháp HTML
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        table = soup.find('table', class_='result')
        ket_qua = {}
        
        if table:
            # Tìm tất cả các hàng (trong bảng có thể có nhiều thẻ <tr>)
            rows = table.find_all('tr')
            for row in rows:
                cells = row.find_all('td')
                # Nếu có ít nhất hai ô trong hàng
                if len(cells) > 1:
                    # Lấy giải đặc biệt
                    if cells[0].text.strip() == 'ĐB':
                        g_db = cells[1].find('em').text.strip()
                        ket_qua['giải đặc biệt'] = g_db
                    else:
                        # Lấy các giải từ Nhất đến Bảy
                        if cells[0].text.strip() == 'G1':
                            ket_qua['giải nhất'] = cells[1].get_text(separator=' ').strip()
                        elif cells[0].text.strip() == 'G2':
                            ket_qua['giải nhì'] = cells[1].get_text(separator=' ').strip()
                        elif cells[0].text.strip() == 'G3':
                            ket_qua['giải ba'] = cells[1].get_text(separator=' ').strip()
                        elif cells[0].text.strip() == 'G4':
                            ket_qua['giải tư'] = cells[1].get_text(separator=' ').strip()
                        elif cells[0].text.strip() == 'G5':
                            ket_qua['giải năm'] = cells[1].get_text(separator=' ').strip()
                        elif cells[0].text.strip() == 'G6':
                            ket_qua['giải sáu'] = cells[1].get_text(separator=' ').strip()
                        elif cells[0].text.strip() == 'G7':
                            ket_qua['giải bảy'] = cells[1].get_text(separator=' ').strip()

            # Lấy ngày và thời gian của kết quả xổ số với múi giờ Việt Nam
            current_time = datetime.now(local_tz).strftime('%H:%M %d-%m-%Y')  # Định dạng ngày và giờ
            ket_qua['_id'] = current_time  # Sử dụng ngày và giờ làm ID
            
            logger.info(f"Kết quả xổ số được cập nhật vào lúc: {current_time}")
            
            # Kiểm tra nếu document đã tồn tại
            result = collection.update_one({'_id': current_time}, {'$set': ket_qua}, upsert=True)
            
            if result.upserted_id is not None:
                logger.info(f"Kết quả đã được lưu mới vào MongoDB với ID: {current_time}")
            else:
                logger.info(f"Kết quả đã được cập nhật trong MongoDB với ID: {current_time}")

    else:
        logger.error('Lỗi khi gửi yêu cầu đến trang web')

# Định nghĩa DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Tạo DAG với lịch trình 5 phút/lần
with DAG(
    dag_id='update_lottery_results',
    default_args=default_args,
    description='DAG cập nhật kết quả xổ số mỗi 5 phút',
    schedule_interval='*/5 * * * *',  # Cứ mỗi 5 phút
    start_date=datetime(2024, 9, 29, tzinfo=local_tz),  # Múi giờ đúng
    catchup=False
) as dag:
    
    # Định nghĩa tác vụ Python
    update_task = PythonOperator(
        task_id='update_lottery_results_task',
        python_callable=update_lottery_results,
    )

    # Chạy tác vụ
    update_task
