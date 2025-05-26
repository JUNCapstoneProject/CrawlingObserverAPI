from deep_translator import GoogleTranslator


def translate_title(title: str, target_lang: str = "ko") -> str:
    try:
        return GoogleTranslator(source="en", target=target_lang).translate(title)
    except Exception as e:
        return title
