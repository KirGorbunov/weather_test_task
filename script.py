import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import NoReturn

import aiohttp
import pandas as pd
from sqlalchemy import (Column, DateTime, Float, Integer, String,
                        create_engine, select)
from sqlalchemy.exc import OperationalError, SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import declarative_base, sessionmaker

from settings import settings

# Настройки логирования
logging.basicConfig(filename="weather_script.log", level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Создание базы данных с помощью SQLAlchemy
Base = declarative_base()


class Weather(Base):
    """
    Модель данных для хранения информации о погоде.
    """
    __tablename__ = "weather"

    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime(timezone=True), nullable=False)
    latitude = Column(Float, nullable=False)
    longitude = Column(Float, nullable=False)
    temperature = Column(Float, nullable=True)
    wind_speed = Column(Float, nullable=True)
    wind_direction = Column(String, nullable=True)
    pressure = Column(Float, nullable=True)
    precipitation = Column(Float, nullable=True)
    rain = Column(Float, nullable=True)
    showers = Column(Float, nullable=True)
    snowfall = Column(Float, nullable=True)


# Настройки подключения к БД
db_user = settings.POSTGRES_USER
db_pwd = settings.POSTGRES_PASSWORD
db_host = settings.POSTGRES_HOST
db_name = settings.POSTGRES_DB

ASYNC_DATABASE_URL = f"postgresql+asyncpg://{db_user}:{db_pwd}@{db_host}/{db_name}"
SYNC_DATABASE_URL = f"postgresql://{db_user}:{db_pwd}@{db_host}/{db_name}"

# Движки для подключения к базе
async_engine = create_async_engine(ASYNC_DATABASE_URL)
sync_engine = create_engine(SYNC_DATABASE_URL)

# Сессия для асинхронного подключения
async_session = sessionmaker(
    bind=async_engine,
    class_=AsyncSession,
    expire_on_commit=False
)

# Сессия для синхронного подключения
sync_session = sessionmaker(
    bind=sync_engine,
    expire_on_commit=False
)


def init_db() -> NoReturn:
    """
    Попытка создания таблиц через синхронное подключение к базе данных в бесконечном цикле.
    """
    while True:
        try:
            with sync_session() as session:
                with session.begin():
                    Base.metadata.create_all(bind=session.connection())
            logger.info("Таблицы успешно созданы или уже существуют.")
            break
        except OperationalError as e:
            logger.error(f"Ошибка доступа к базе данных: {e}. Повторная попытка через 5 секунд...")
            time.sleep(5)
        except SQLAlchemyError as e:
            logger.error(f"Ошибка работы с базой данных: {e}.")
            break


async def get_weather(latitude: float, longitude: float) -> dict | None:
    """
    Получение данных о погоде по API.
    """
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "current": ["temperature_2m", "precipitation", "rain", "showers", "snowfall", "surface_pressure",
                    "wind_speed_10m", "wind_direction_10m"],
        "wind_speed_unit": "ms"
    }

    async with aiohttp.ClientSession() as session:
        async with session.get(settings.WEATHER_URL, params=params) as response:
            if response.status == 200:
                weather_data = await response.json()
                return weather_data
            else:
                logger.error(f"Ошибка получения данных: {response.status}")
                return None


def wind_direction_to_text(degrees: float) -> str | None:
    """
    Преобразование направления ветра в текстовый формат.
    """
    directions = ["C", "СВ", "В", "ЮВ", "Ю", "ЮЗ", "З", "СЗ"]
    if degrees < 0 or degrees > 360:
        return None
    elif degrees >= 337.5:
        return "C"
    index = int((degrees + 22.5) // 45)
    return directions[index]


async def save_weather_to_db(session: AsyncSession,
                             latitude: float,
                             longitude: float,
                             weather_data: dict | None) -> NoReturn:
    """
    Сохранение полученных данных о погоде в базу данных.
    """
    if "current" in weather_data:
        current_weather = weather_data["current"]
        temperature = current_weather.get("temperature_2m", None)  # Температура в градусах Цельсия
        wind_speed = current_weather.get("wind_speed_10m", None)  # Скорость ветра в м/с
        wind_direction = current_weather.get("wind_direction_10m", None)  # Направление ветра в градусах
        pressure = current_weather.get("surface_pressure", None)  # Давление в гектопаскалях
        precipitation = current_weather.get("precipitation", None)  # Осадки (мм)
        rain = current_weather.get("rain", None)  # Дождь (мм)
        showers = current_weather.get("showers", None)  # Ливень (мм)
        snowfall = current_weather.get("snowfall", None)  # Снег (мм)
        timestamp = current_weather.get("time", None)
        # Параметры требующие преобразования:
        if wind_direction is not None:
            wind_direction = wind_direction_to_text(float(wind_direction))  # Преобразование в текстовый формат
        if pressure is not None:
            pressure = pressure * 0.75006  # Перевод в мм рт. ст.
        if snowfall is not None:
            snowfall = snowfall * 10  # Перевод в мм
        if timestamp is not None:
            timestamp = datetime.fromisoformat(timestamp)
            timestamp = timestamp.replace(tzinfo=timezone.utc)  # время в UTC

        new_weather = Weather(
            timestamp=timestamp,
            latitude=latitude,
            longitude=longitude,
            temperature=temperature,
            wind_speed=wind_speed,
            wind_direction=wind_direction,
            pressure=pressure,
            precipitation=precipitation,
            rain=rain,
            showers=showers,
            snowfall=snowfall
        )
        session.add(new_weather)
        await session.commit()


async def get_last_weather(session: AsyncSession) -> list[Weather]:
    """
    Получение последних записей о погоде из базы данных.
    """
    stmt = select(Weather).order_by(Weather.timestamp.desc()).limit(settings.ROW_NUMBER)
    result = await session.execute(stmt)
    return result.scalars().all()


def export_to_excel(data: list[Weather]) -> NoReturn:
    """
    Экспорт данных о погоде в Excel файл.
    """
    df = pd.DataFrame([{
        "Дата и время": row.timestamp.replace(tzinfo=None),
        "Широта": row.latitude,
        "Долгота": row.longitude,
        "Температура, град.": row.temperature,
        "Скорость ветра, м/с": row.wind_speed,
        "Направление ветра": row.wind_direction,
        "Давление, мм рт. ст.": row.pressure,
        "Осадки, мм": row.precipitation,
        "Дождь, мм": row.rain,
        "Ливень, мм": row.showers,
        "Снег, мм": row.snowfall
    } for row in data])

    current_datetime = datetime.now().strftime('%Y%m%d_%H%M%S')
    # Сохранение DataFrame в Excel-файл
    df.to_excel(f"{settings.FILE_NAME}_{current_datetime}.xlsx", index=False)

    print(f"Данные успешно экспортированы в файл {settings.FILE_NAME}_{current_datetime}.xlsx'.")


async def fetch_weather() -> NoReturn:
    """
    Периодическое получение данных о погоде и их сохранение в базу данных.
    """
    async with async_session() as session:
        while True:
            # Получаем текущие данные о погоде
            weather_data = await get_weather(settings.LATITUDE, settings.LATITUDE)

            # Сохраняем данных в БД
            await save_weather_to_db(session, settings.LATITUDE, settings.LATITUDE, weather_data)

            # Ожидание перед следующим запросом
            await asyncio.sleep(settings.PERIOD)


async def export_weather_to_excel() -> NoReturn:
    """
    Экспорт данных в Excel.
    """
    async with async_session() as session:
        last_data = await get_last_weather(session)
        export_to_excel(last_data)


async def handle_user_input() -> NoReturn:
    """
    Получение и обработка команд от пользователя.
    """
    while True:
        command = await asyncio.to_thread(input, "Введите команду ('export' для экспорта или 'exit' для выхода): ")
        if command == "export":
            print(f"Экспортируем последние {settings.ROW_NUMBER} записей в Excel...")
            await export_weather_to_excel()
        elif command == "exit":
            print("Завершение программы.")
            break
        else:
            print("Неизвестная команда. Попробуйте снова.")


async def main_loop() -> NoReturn:
    """
    Основная функция для запуска асинхронных задач.
    """
    weather_task = asyncio.create_task(fetch_weather())
    user_input_task = asyncio.create_task(handle_user_input())

    # Отслеживание завершения любой из асинхронных тасков
    await asyncio.wait([weather_task, user_input_task], return_when=asyncio.FIRST_COMPLETED)


if __name__ == "__main__":
    init_db()
    asyncio.run(main_loop())
