import deepl
from lib.Config.config import Config


def translate_title(title: str, target_lang: str = "KO") -> str:
    api_key = Config.get("API_KEYS.deepl")
    if not api_key:
        raise RuntimeError("DeepL API key is missing in config.")

    try:
        translator = deepl.Translator(api_key)
        result = translator.translate_text(
            title, source_lang="EN", target_lang=target_lang
        )
        return result.text
    except Exception as e:
        return title
