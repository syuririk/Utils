from .config import Config

class API:
    @staticmethod
    def set_api_key(key: str):
        Config.api_key = key

    @staticmethod
    def get_api_key():
        if Config.api_key is None:
            raise ValueError("API key not set")
        return Config.api_key