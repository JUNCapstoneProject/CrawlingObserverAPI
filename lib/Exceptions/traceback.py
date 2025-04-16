import os
import datetime
import traceback


def save_traceback_to_file(tag: str, idx: int, e: Exception):
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    log_dir = os.path.join(base_dir, "Logs")
    os.makedirs(log_dir, exist_ok=True)

    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"traceback_{tag}_{idx}_{timestamp}.log"
    filepath = os.path.join(log_dir, filename)

    with open(filepath, "w", encoding="utf-8") as f:
        f.write(f"[{timestamp}] 예외 발생 (tag: {tag}, index: {idx})\n")
        f.write(f"Error: {str(e)}\n\n")
        f.write(traceback.format_exc())  # ⬅ 핵심

    print(f"예외 트레이스백 로그 저장됨: {filepath}")
