REQUIRED_FIELDS = {
    "income_statement": [
        "Total Revenue",
        "Operating Income",
        "Net Income",
        "EBITDA"
        ],

    "balance_sheet": [
        "Total Assets",
        "Total Liabilities Net Minority Interest",
        "Stockholders Equity"
    ],

    "cash_flow": [
        "Operating Cash Flow",
        "Investing Cash Flow",
        "Financing Cash Flow",
        "Free Cash Flow"
    ]
}

def check_required_fields(row: dict, statement_type: str) -> list[str]:
    """
    누락된 필수 필드를 리스트로 반환
    """
    required = REQUIRED_FIELDS.get(statement_type, [])
    return [field for field in required if row.get(field) is None]

def is_valid_financial_row(row: dict, statement_type: str) -> bool:
    return len(check_required_fields(row, statement_type)) == 0