from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # Подключение к базе:
    POSTGRES_HOST: str = "localhost"
    POSTGRES_USER: str
    POSTGRES_PASSWORD: str
    POSTGRES_DB: str

    # Получение данных к погоде:
    WEATHER_URL: str = "https://api.open-meteo.com/v1/forecast"
    LATITUDE: float = 55.69853856903821
    LONGITUDE: float = 37.35957649999993
    PERIOD: int = 180

    # Настройки выгрузки:
    FILE_NAME: str = "weather_data.xlsx"
    ROW_NUMBER: int = 10


    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")


settings = Settings()
