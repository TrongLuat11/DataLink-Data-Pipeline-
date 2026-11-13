"""
DataLink Control Plane — APScheduler Service
Quản lý lịch chạy tự động định kỳ (chạy ngầm siêu nhẹ, tiêu thụ 0.0% CPU khi chờ)
"""
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from runner import execute_full_pipeline

scheduler = BackgroundScheduler()
is_active = True

def scheduled_job():
    """Hàm chạy tự động khi đến lịch hẹn"""
    print("[SCHEDULER] Kích hoạt chạy pipeline tự động định kỳ...")
    execute_full_pipeline(triggered_by="scheduler")

def start_scheduler():
    """Khởi động bộ lập lịch (mặc định 02:00 sáng mỗi ngày)"""
    global is_active
    if not scheduler.running:
        trigger = CronTrigger(hour=2, minute=0)
        scheduler.add_job(
            scheduled_job,
            trigger=trigger,
            id="daily_datalink_refresh",
            name="Chạy pipeline DataLink hàng ngày lúc 02:00",
            replace_existing=True
        )
        scheduler.start()
        is_active = True
        print("[SCHEDULER] Đã khởi động bộ lập lịch ngầm thành công!")

def toggle_scheduler() -> bool:
    """Bật / Tắt bộ lập lịch ngầm"""
    global is_active
    job = scheduler.get_job("daily_datalink_refresh")
    if not scheduler.running:
        start_scheduler()
        return True

    if is_active:
        if job:
            job.pause()
        is_active = False
        print("[SCHEDULER] Đã tạm dừng lịch chạy ngầm.")
    else:
        if job:
            job.resume()
        is_active = True
        print("[SCHEDULER] Đã kích hoạt lại lịch chạy ngầm.")

    return is_active

def is_scheduler_active() -> bool:
    """Kiểm tra trạng thái bộ lập lịch"""
    return is_active

def get_next_run_time() -> str:
    """Lấy thời điểm chạy tiếp theo để hiển thị lên web"""
    if not is_active:
        return "Đã tạm dừng"
    job = scheduler.get_job("daily_datalink_refresh")
    if job and job.next_run_time:
        return job.next_run_time.strftime("%d/%m/%Y %H:%M:%S")
    return "Chưa kích hoạt"