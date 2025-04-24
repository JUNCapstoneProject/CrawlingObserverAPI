# handlers.py
EXTRACT_HANDLERS = {}


def register_handler(field_name):
    """핸들러 등록 데코레이터"""

    def decorator(func):
        EXTRACT_HANDLERS[field_name] = func
        return func

    return decorator


@register_handler("href")
def extract_href(soup, selectors):
    for selector in selectors:
        href_element = soup.select_one(selector)
        if href_element:
            return href_element.get("href")
    return None


@register_handler("organization")
def extract_organization(soup, selectors):
    for selector in selectors:
        el = soup.select_one(selector)
        if el:
            return el.get_text(strip=True)
    return None


@register_handler("author")
def extract_author(soup, selectors):
    for selector in selectors:
        el = soup.select_one(selector)
        if el:
            return el.get_text(strip=True)
    return "Unknown"


@register_handler("title")
def extract_title(soup, selectors):
    for selector in selectors:
        el = soup.select_one(selector)
        if el:
            return el.get_text(strip=True)
    return None


@register_handler("posted_at")
def extract_posted_at(soup, selectors):
    for selector in selectors:
        el = soup.select_one(selector)
        if el and el.has_attr("datetime"):
            return el["datetime"].split(" ")[0]
    return None


@register_handler("content")
def extract_content(soup, selectors):
    texts = []
    for selector in selectors:
        texts.extend([e.get_text(strip=True) for e in soup.select(selector)])
    return " ".join(texts).strip() if texts else None


@register_handler("tag")
def extract_tag(soup, selectors):
    tags = []
    for selector in selectors:
        tags.extend([e.get_text(strip=True) for e in soup.select(selector)])
    return ",".join(tags) if tags else None
