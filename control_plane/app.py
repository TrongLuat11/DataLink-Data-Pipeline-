"""
DataLink Control Plane — FastAPI Application
"""
import os
from fastapi import FastAPI, Request, Form, Depends, HTTPException, status
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware

from database import authenticate_user, get_recent_runs
from runner import (
    execute_ingest_pipeline,
    execute_full_pipeline,
    get_dbt_results_details,
    execute_dbt_command,
    TRANSFORM_DIR
)
from scheduler import start_scheduler, get_next_run_time, toggle_scheduler, is_scheduler_active
from fastapi.staticfiles import StaticFiles

app = FastAPI(title="DataLink Control Plane")
app.add_middleware(SessionMiddleware, secret_key=os.getenv("CONTROL_PLANE_SECRET_KEY", "secret_jwt_key_2024"))

templates = Jinja2Templates(directory="templates")

# Mount dbt docs static nếu thư mục target tồn tại
dbt_target_dir = TRANSFORM_DIR / "target"
if dbt_target_dir.exists():
    app.mount("/dbt-docs-static", StaticFiles(directory=str(dbt_target_dir), html=True), name="dbt_docs_static")

@app.on_event("startup")
def startup_event():
    start_scheduler()

# ════════════ ROUTERS ════════════

@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    user = request.session.get("user")
    if not user:
        return RedirectResponse(url="/login")
    return RedirectResponse(url="/dashboard")

@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request):
    return templates.TemplateResponse(request=request, name="login.html", context={"error": None})

@app.post("/login", response_class=HTMLResponse)
def login_action(request: Request, username: str = Form(...), password: str = Form(...)):
    user = authenticate_user(username, password)
    if not user:
        return templates.TemplateResponse(request=request, name="login.html", context={"error": "Sai tên đăng nhập hoặc mật khẩu!"})
  
    # Lưu vào session
    request.session["user"] = {
        "username": user["username"],
        "full_name": user["full_name"],
        "role": user["role"]
    }
    return RedirectResponse(url="/dashboard", status_code=status.HTTP_303_SEE_OTHER)

@app.get("/logout")
def logout_action(request: Request):
    request.session.clear()
    return RedirectResponse(url="/login", status_code=status.HTTP_303_SEE_OTHER)

@app.get("/dashboard", response_class=HTMLResponse)
def dashboard_page(request: Request):
    user = request.session.get("user")
    if not user:
        return RedirectResponse(url="/login")
  
    recent_runs = get_recent_runs(limit=200)
    next_run = get_next_run_time()
    scheduler_active = is_scheduler_active()
    dbt_details = get_dbt_results_details()
  
    return templates.TemplateResponse(request=request, name="dashboard.html", context={
        "user": user,
        "runs": recent_runs,
        "next_run": next_run,
        "scheduler_active": scheduler_active,
        "dbt": dbt_details
    })

@app.get("/dbt", response_class=HTMLResponse)
def dbt_page(request: Request):
    """Trang chuyên sâu: Xem chi tiết kết quả dbt và thực hiện các thao tác dbt độc lập"""
    user = request.session.get("user")
    if not user:
        return RedirectResponse(url="/login")
    
    dbt_details = get_dbt_results_details()
    return templates.TemplateResponse(request=request, name="dbt_view.html", context={
        "user": user,
        "dbt": dbt_details
    })

@app.get("/dbt/docs")
def dbt_docs_page(request: Request):
    """Mở giao diện tương tác dbt Lineage & Docs Catalog"""
    user = request.session.get("user")
    if not user:
        return RedirectResponse(url="/login")
    return RedirectResponse(url="/dbt-docs-static/index.html")

@app.get("/api/dbt/results")
def api_dbt_results(request: Request):
    """Lấy dữ liệu JSON chi tiết các models, tests và số liệu dbt mới nhất"""
    user = request.session.get("user")
    if not user:
        raise HTTPException(status_code=401, detail="Chưa đăng nhập!")
    return get_dbt_results_details()

@app.post("/api/dbt/action")
async def api_dbt_action(request: Request):
    """
    Kích hoạt thao tác dbt riêng biệt:
    action: run | test | build | staging | marts | docs | seed
    select: tùy chọn lọc model/test cụ thể
    full_refresh: boolean
    """
    user = request.session.get("user")
    if not user or user.get("role") not in ["admin", "operator"]:
        return {"status": "ERROR", "message": "Bạn không có quyền thực hiện thao tác dbt!"}

    payload = {}
    try:
        payload = await request.json()
    except Exception:
        form = await request.form()
        payload = dict(form)

    action = payload.get("action", "run")
    select = payload.get("select")
    full_refresh = bool(payload.get("full_refresh", False))

    result = execute_dbt_command(
        action=action,
        select=select,
        full_refresh=full_refresh,
        triggered_by=user.get("username", "admin")
    )
    return result

@app.post("/api/pipeline/run")
def trigger_pipeline(request: Request):
    """
    Kích hoạt Pipeline nạp dữ liệu:
    - Nếu đã có dữ liệu: Tự động cập nhật thêm (UPSERT/Merge)
    - Nếu chưa có: Tự tạo bảng và nạp mới
    """
    user = request.session.get("user")
    if not user or user.get("role") not in ["admin", "operator"]:
        return {"status": "ERROR", "message": "Bạn không có quyền kích hoạt pipeline!"}
  
    # Tự động chạy toàn bộ luồng E2E: Ingest (dlt) -> Transform Staging & Marts (dbt) -> Run Tests
    result = execute_full_pipeline(triggered_by=user.get("username", "admin"))
    return result

@app.post("/api/scheduler/toggle")
def toggle_scheduler_action(request: Request):
    """Bật / Tắt bộ lập lịch ngầm tự động"""
    user = request.session.get("user")
    if not user or user.get("role") not in ["admin", "operator"]:
        return {"status": "ERROR", "message": "Bạn không có quyền thay đổi lịch hẹn!"}

    new_state = toggle_scheduler()
    return {
        "status": "SUCCESS",
        "active": new_state,
        "next_run": get_next_run_time()
    }

@app.get("/reports", response_class=HTMLResponse)
def reports_page(request: Request):
    user = request.session.get("user")
    if not user:
        return RedirectResponse(url="/login")
  
    from database import get_business_kpis
    kpi = get_business_kpis()

    dashboards = [
        {
            "id": 5,
            "title": "TỔNG QUAN ĐIỀU HÀNH",
            "code": "DASHBOARD 01",
            "icon": "bi-graph-up-arrow",
            "desc": "Báo cáo doanh số bán lẻ, số lượng xuất kho, tăng trưởng doanh thu và cơ cấu phân khúc giá sàn Tiki.",
            "dataset": "mart_data.fact_sales, dim_products",
            "charts_count": 8,
            "url": "http://localhost:8088/superset/dashboard/5/?standalone=true"
        },
        {
            "id": 6,
            "title": "HIỆU SUẤT BÁN LẺ & NGÀNH HÀNG",
            "code": "DASHBOARD 02",
            "icon": "bi-shop",
            "desc": "Phân tích doanh thu theo từng danh mục ngành hàng, thị phần thương hiệu lớn và bảng xếp hạng gian hàng xuất sắc.",
            "dataset": "mart_data.fact_sales, dim_sellers",
            "charts_count": 9,
            "url": "http://localhost:8088/superset/dashboard/6/?standalone=true"
        },
        {
            "id": 7,
            "title": "CHIẾN DỊCH KHUYẾN MÃI & FLASH SALE",
            "code": "DASHBOARD 03",
            "icon": "bi-lightning-charge",
            "desc": "Đo lường hiệu quả sự kiện giờ vàng Deal Flash Sale, tỷ lệ cháy hàng (Sell-out), mức chiết khấu và tổng mức tiết kiệm cho khách.",
            "dataset": "mart_data.fact_flash_sales",
            "charts_count": 7,
            "url": "http://localhost:8088/superset/dashboard/7/?standalone=true"
        },
        {
            "id": 8,
            "title": "TRẢI NGHIỆM KHÁCH HÀNG & PHẢN HỒI",
            "code": "DASHBOARD 04",
            "icon": "bi-chat-heart",
            "desc": "Phân tích chỉ số hài lòng (CSAT), phân bố xếp hạng 1-5 sao, phân loại cảm xúc (Sentiment) và phản hồi thực tế của khách hàng.",
            "dataset": "mart_data.fact_reviews",
            "charts_count": 6,
            "url": "http://localhost:8088/superset/dashboard/8/?standalone=true"
        },
        {
            "id": 9,
            "title": "VẬN HÀNH DỮ LIỆU & KIỂM SOÁT HỆ THỐNG",
            "code": "DASHBOARD 05",
            "icon": "bi-shield-check",
            "desc": "Giám sát tính toàn vẹn Data Warehouse Medallion, kết quả 41+ dbt tests chất lượng và lịch sử nạp dữ liệu dlt Ingestion.",
            "dataset": "control_plane.pipeline_runs",
            "charts_count": 6,
            "url": "http://localhost:8088/superset/dashboard/9/?standalone=true"
        }
    ]

    return templates.TemplateResponse(request=request, name="embedded_report.html", context={
        "user": user,
        "kpi": kpi,
        "dashboards": dashboards,
        "superset_url": "http://localhost:8088"
    })