"""
DataLink Control Plane — Pipeline Execution Runner
"""
import subprocess
import time
import sys
import json
from datetime import datetime
from pathlib import Path
from database import log_pipeline_start, log_pipeline_finish

BASE_DIR = Path(__file__).resolve().parent.parent
INGEST_SCRIPT = BASE_DIR / "ingestion" / "ingest_openfoodfact.py"
TRANSFORM_DIR = BASE_DIR / "transform"

def run_ingest_step() -> tuple[bool, str]:
    """Chạy script nạp dữ liệu Tiki API (dlt Ingestion) - Hỗ trợ tự động UPSERT"""
    try:
        venv_py = BASE_DIR / "ingestion" / "venv" / "bin" / "python"
        py_bin = str(venv_py) if venv_py.exists() else sys.executable
        cmd = [py_bin, str(INGEST_SCRIPT)]
        result = subprocess.run(
            cmd, cwd=str(BASE_DIR / "ingestion"),
            capture_output=True, text=True, timeout=600
        )
        output = f"=== TIKI INGESTION LOG ===\n{result.stdout}\n"
        if result.stderr:
            output += f"=== STDERR ===\n{result.stderr}\n"
        return (result.returncode == 0, output)
    except Exception as e:
        return (False, f"Lỗi thực thi Ingestion: {str(e)}")

def run_dbt_step() -> tuple[bool, str]:
    """Chạy lệnh dbt build (chạy models + tests)"""
    try:
        venv_dbt = BASE_DIR / "ingestion" / "venv" / "bin" / "dbt"
        dbt_bin = str(venv_dbt) if venv_dbt.exists() else "dbt"
        cmd = [dbt_bin, "build"]
        result = subprocess.run(
            cmd, cwd=str(TRANSFORM_DIR),
            capture_output=True, text=True, timeout=600
        )
        output = f"=== DBT BUILD LOG ===\n{result.stdout}\n"
        if result.stderr:
            output += f"=== STDERR ===\n{result.stderr}\n"
        return (result.returncode == 0, output)
    except Exception as e:
        return (False, f"Lỗi thực thi dbt: {str(e)}")

def get_dbt_results_details() -> dict:
    """
    Đọc và phân tích chi tiết kết quả chạy gần nhất từ dbt target/run_results.json & manifest.json.
    Trả về cấu trúc chi tiết: models, tests, seeds, số liệu thống kê.
    """
    target_dir = TRANSFORM_DIR / "target"
    run_results_path = target_dir / "run_results.json"
    manifest_path = target_dir / "manifest.json"

    manifest_nodes = {}
    if manifest_path.exists():
        try:
            with open(manifest_path, "r", encoding="utf-8") as f:
                manifest_nodes = json.load(f).get("nodes", {})
        except Exception:
            manifest_nodes = {}

    if not run_results_path.exists():
        return {
            "has_results": False,
            "generated_at": None,
            "elapsed_time": 0,
            "stats": {
                "total_items": 0, "total_models": 0, "passed_models": 0, "failed_models": 0,
                "total_tests": 0, "passed_tests": 0, "failed_tests": 0, "warn_tests": 0,
                "total_seeds": 0, "pass_rate": 100
            },
            "models": [],
            "tests": [],
            "seeds": []
        }

    try:
        with open(run_results_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        return {
            "has_results": False,
            "error": str(e),
            "stats": {
                "total_items": 0, "total_models": 0, "passed_models": 0, "failed_models": 0,
                "total_tests": 0, "passed_tests": 0, "failed_tests": 0, "warn_tests": 0,
                "total_seeds": 0, "pass_rate": 100
            },
            "models": [],
            "tests": [],
            "seeds": []
        }

    results = data.get("results", [])
    elapsed_time = round(data.get("elapsed_time", 0), 2)
    metadata = data.get("metadata", {})
    gen_at_raw = metadata.get("generated_at", "")

    gen_at = gen_at_raw
    if gen_at_raw:
        try:
            dt = datetime.fromisoformat(gen_at_raw.replace("Z", "+00:00"))
            gen_at = dt.strftime("%d/%m/%Y %H:%M:%S")
        except Exception:
            gen_at = gen_at_raw[:19].replace("T", " ")

    models = []
    tests = []
    seeds = []

    for r in results:
        uid = r.get("unique_id", "")
        parts = uid.split(".")
        res_type = parts[0] if parts else "unknown"
        item_name = parts[-1] if len(parts) <= 2 else parts[2]
        status = r.get("status", "unknown").lower()
        exec_time = round(r.get("execution_time", 0), 3)
        message = r.get("message") or ""
        failures = r.get("failures", 0)

        node_meta = manifest_nodes.get(uid, {})
        description = node_meta.get("description", "")
        config = node_meta.get("config", {})
        mat = config.get("materialized", "view")

        if res_type == "model":
            layer = "staging" if ("staging" in uid or "stg_" in item_name) else ("marts" if ("marts" in uid or item_name.startswith(("fact_", "dim_"))) else "other")
            models.append({
                "name": item_name,
                "unique_id": uid,
                "layer": layer,
                "materialization": mat,
                "description": description or f"Bảng dữ liệu mô hình {item_name}",
                "status": status,
                "execution_time": exec_time,
                "relation": r.get("relation_name", ""),
                "message": message
            })
        elif res_type == "test":
            test_type = "custom"
            if "not_null" in item_name:
                test_type = "not_null"
            elif "unique" in item_name:
                test_type = "unique"
            elif "accepted_values" in item_name:
                test_type = "accepted_values"
            elif "relationships" in item_name:
                test_type = "relationships"
            elif "assert_" in item_name:
                test_type = "assertion"

            tests.append({
                "name": item_name,
                "unique_id": uid,
                "test_type": test_type,
                "status": status,
                "failures": failures,
                "execution_time": exec_time,
                "message": message
            })
        elif res_type == "seed":
            seeds.append({
                "name": item_name,
                "unique_id": uid,
                "status": status,
                "execution_time": exec_time,
                "message": message
            })

    total_models = len(models)
    passed_models = sum(1 for m in models if m["status"] in ["success", "pass"])
    failed_models = total_models - passed_models

    total_tests = len(tests)
    passed_tests = sum(1 for t in tests if t["status"] == "pass")
    failed_tests = sum(1 for t in tests if t["status"] in ["fail", "error"])
    warn_tests = sum(1 for t in tests if t["status"] == "warn")

    total_items = total_models + total_tests + len(seeds)
    total_passed = passed_models + passed_tests + sum(1 for s in seeds if s["status"] in ["success", "pass"])
    pass_rate = round((total_passed / total_items * 100), 1) if total_items > 0 else 100

    return {
        "has_results": True,
        "generated_at": gen_at,
        "elapsed_time": elapsed_time,
        "stats": {
            "total_items": total_items,
            "total_models": total_models,
            "passed_models": passed_models,
            "failed_models": failed_models,
            "total_tests": total_tests,
            "passed_tests": passed_tests,
            "failed_tests": failed_tests,
            "warn_tests": warn_tests,
            "total_seeds": len(seeds),
            "pass_rate": pass_rate
        },
        "models": models,
        "tests": tests,
        "seeds": seeds
    }

def execute_dbt_command(action: str = "run", select: str = None, full_refresh: bool = False, triggered_by: str = "manual") -> dict:
    """
    Thực thi các lệnh dbt chuyên biệt:
    - run: dbt run (compile & build models)
    - test: dbt test (data quality assertion)
    - build: dbt build (models + tests theo DAG)
    - staging: dbt run --select staging
    - marts: dbt run --select marts
    - docs: dbt docs generate
    - seed: dbt seed
    - custom model/test: select cụ thể
    """
    start_time = time.time()
    venv_dbt = BASE_DIR / "ingestion" / "venv" / "bin" / "dbt"
    dbt_bin = str(venv_dbt) if venv_dbt.exists() else "dbt"

    cmd = [dbt_bin]
    pipeline_label = f"dbt {action}"

    if action == "run":
        cmd.append("run")
        pipeline_label = "dbt run (All Models)"
    elif action == "test":
        cmd.append("test")
        pipeline_label = "dbt test (Data Quality)"
    elif action == "build":
        cmd.append("build")
        pipeline_label = "dbt build (DAG All)"
    elif action == "staging":
        cmd.extend(["run", "--select", "staging"])
        pipeline_label = "dbt run --select staging"
    elif action == "marts":
        cmd.extend(["run", "--select", "marts"])
        pipeline_label = "dbt run --select marts"
    elif action == "docs":
        cmd.extend(["docs", "generate"])
        pipeline_label = "dbt docs generate"
    elif action == "seed":
        cmd.append("seed")
        pipeline_label = "dbt seed"
    else:
        cmd.append(action)

    if select:
        cmd.extend(["--select", select])
        pipeline_label += f" [{select}]"

    if full_refresh:
        cmd.append("--full-refresh")
        pipeline_label += " (Full Refresh)"

    run_id = log_pipeline_start(pipeline_label, triggered_by)

    try:
        result = subprocess.run(
            cmd, cwd=str(TRANSFORM_DIR),
            capture_output=True, text=True, timeout=600
        )
        output = f"=== DBT COMMAND: {' '.join(cmd)} ===\n{result.stdout}\n"
        if result.stderr:
            output += f"=== STDERR ===\n{result.stderr}\n"

        duration = round(time.time() - start_time, 2)
        final_status = "SUCCESS" if result.returncode == 0 else "FAILED"
        log_pipeline_finish(run_id, final_status, output, duration)

        summary = get_dbt_results_details()

        return {
            "status": final_status,
            "duration": duration,
            "logs": output,
            "action": action,
            "summary": summary
        }
    except Exception as e:
        duration = round(time.time() - start_time, 2)
        err_msg = f"Lỗi thực thi dbt {action}: {str(e)}"
        log_pipeline_finish(run_id, "FAILED", err_msg, duration)
        return {
            "status": "FAILED",
            "duration": duration,
            "logs": err_msg,
            "action": action
        }

def execute_ingest_pipeline(triggered_by: str = "manual") -> dict:
    """
    Thực thi Ingestion Pipeline (dlt):
    - Nếu đã có dữ liệu: Tự động cập nhật thêm/mới (UPSERT/Merge).
    - Nếu chưa có: Nạp mới từ đầu.
    """
    start_time = time.time()
    run_id = log_pipeline_start("Tiki Retail Ingestion (dlt)", triggered_by)
    full_logs = [">>> Bắt đầu nạp dữ liệu từ Tiki Retail API (dlt)..."]

    ingest_ok, ingest_log = run_ingest_step()
    full_logs.append(ingest_log)

    duration = round(time.time() - start_time, 2)
    final_status = "SUCCESS" if ingest_ok else "FAILED"
    log_pipeline_finish(run_id, final_status, "\n".join(full_logs), duration)

    return {"status": final_status, "duration": duration, "logs": "\n".join(full_logs)}

def execute_full_pipeline(triggered_by: str = "manual") -> dict:
    """Thực thi toàn bộ luồng E2E: Ingest -> dbt Transform -> dbt Test"""
    start_time = time.time()
    run_id = log_pipeline_start("Full E2E Pipeline (dlt + dbt)", triggered_by)
    full_logs = []

    # Bước 1: Ingest dlt
    full_logs.append(">>> BƯỚC 1: Bắt đầu nạp dữ liệu từ Tiki Retail API (dlt)...")
    ingest_ok, ingest_log = run_ingest_step()
    full_logs.append(ingest_log)

    if not ingest_ok:
        duration = round(time.time() - start_time, 2)
        log_pipeline_finish(run_id, "FAILED", "\n".join(full_logs), duration)
        return {"status": "FAILED", "step": "ingest", "logs": "\n".join(full_logs)}

    # Bước 2: dbt build
    full_logs.append(">>> BƯỚC 2: Bắt đầu Transform dữ liệu & Chạy Tests (dbt)...")
    dbt_ok, dbt_log = run_dbt_step()
    full_logs.append(dbt_log)

    duration = round(time.time() - start_time, 2)
    final_status = "SUCCESS" if dbt_ok else "FAILED"
    log_pipeline_finish(run_id, final_status, "\n".join(full_logs), duration)

    return {"status": final_status, "duration": duration, "logs": "\n".join(full_logs)}