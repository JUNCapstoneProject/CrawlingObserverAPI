host = "msiwol.iptime.org"
requests_message = {
    "header": {
        "Host": host,
        "Origin": f"https://{host}",
        "Referer": f"https://{host}/api/analysis/crawling/distributor/notifier",
        "Request URL": f"https://{host}/api/analysis/news/sentiment_classifier",
        "Request Method": "GET",
        "Authorization": "Bearer",
    },
    "body": {"client_id": None, "client_secret": None, "item": {}},  # 아래 item중 한개
}

news_item = {
    "event_type": "news",
    "data": {
        "news_data": None,
        "stock_history": {
            "Date": [],
            "Open": [],
            "Close": [],
            "Adj Close": [],
            "High": [],
            "Low": [],
            "Volume": [],
            "Market Cap": [],
        },
        "market_history": {
            "Date": [],
            "Open": [],
            "Close": [],
            "Adj Close": [],
            "High": [],
            "Low": [],
            "Volume": [],
            # 'Market Cap': [],
        },
        "income_statement": {
            "Total Revenue": [],
            "Normalized Income": [],
        },
        "info": {
            "priceToBook": [],
        },
    },
}

finance_item = {
    "event_type": "finance",
    "data": {
        "balance_sheet": {
            "Current Assets": [],
            "Current Liabilities": [],
            "Cash And Cash Equivalents": [],
            "Accounts Receivable": [],
            "Cash Cash Equivalents And Short Term Investments": [],
            "Cash Equivalents": [],
            "Cash Financial": [],
            "Other Short Term Investments": [],
            "Stockholders Equity": [],
            "Total Assets": [],
            "Retained Earnings": [],
            "Inventory": [],
        },
        "income_statement": {
            "Total Revenue": [],
            "Cost Of Revenue": [],  # 이거 추가됨
            "Gross Profit": [],
            "Selling General And Administration": [],
            "Operating Income": [],
            "Other Non Operating Income Expenses": [],
            "Reconciled Depreciation": [],
            "EBITDA": [],
        },
        "cash_flow": {
            "Operating Cash Flow": [],
            "Investing Cash Flow": [],
            "Capital Expenditure": [],
        },
        "chart": {
            "timestamp": [],  # yyyy-mm-dd HH:MM:SS 형식
            # "v": [],  # Volume
            # "vw": [],  # Volume Weighted Average Price
            "o": [],  # Open
            "c": [],  # Close
            # "h": [],  # High
            # "l": [],  # Low
            # "t": [],  # Time
            # "n": []   # Number of trades
        },
    },
}
