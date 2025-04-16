import pandas as pd
from typing import List

REQUIRED_FIELDS = {
    "income_statement": [
        "Total Revenue",
        "Operating Income",
        "Net Income",
        "EBITDA",
        "Diluted EPS",
    ],
    "balance_sheet": [
        "Total Assets",
        "Total Liabilities Net Minority Interest",
        "Stockholders Equity",
    ],
    "cash_flow": [
        "Operating Cash Flow",
        "Investing Cash Flow",
        "Financing Cash Flow",
        "Free Cash Flow",
    ],
}


def normalize_key(key: str) -> str:
    """공백 제거 + 소문자로 정규화된 키 리턴"""
    return key.replace(" ", "").lower()


def check_required_fields(row: dict, statement_type: str) -> List[str]:
    """
    row에서 누락된 필수 필드를 반환 (None, NaN 포함)
    - 키 이름은 대소문자/공백 무시하여 매칭
    """
    required = REQUIRED_FIELDS.get(statement_type, [])
    normalized_row = {normalize_key(k): v for k, v in row.items()}

    missing = []
    for field in required:
        norm_field = normalize_key(field)
        if norm_field not in normalized_row or pd.isna(normalized_row[norm_field]):
            missing.append(field)

    return missing


def is_valid_financial_row(row: dict, statement_type: str) -> bool:
    """필수 필드가 모두 채워졌는지 여부 반환"""
    return not check_required_fields(row, statement_type)


def merge_missing_fields(
    base_row: dict, backup_rows: List[dict], statement_type: str
) -> dict:
    """
    base_row의 누락된 필드를 backup_rows에서 가능한 한 채워 넣음
    - 디버깅 로그 포함
    """
    filled_row = base_row.copy()
    missing = check_required_fields(filled_row, statement_type)

    # print(f"\n[DEBUG] 초기 누락 필드 ({statement_type}): {missing}")

    for i, backup in enumerate(backup_rows):
        # print(f"[DEBUG] → 백업 분기 {i+1} 확인 중")

        normalized_backup = {normalize_key(k): (k, v) for k, v in backup.items()}

        for field in missing:
            norm_field = normalize_key(field)
            if norm_field in normalized_backup:
                original_key, value = normalized_backup[norm_field]
                if not pd.isna(value):
                    filled_row[field] = value

        missing = check_required_fields(filled_row, statement_type)
        # print(f"[DEBUG] 현재 남은 누락 필드: {missing}")

        if not missing:
            # print(f"[DEBUG] ✅ 모든 필드 보완 완료, 루프 종료")
            break

    return filled_row
