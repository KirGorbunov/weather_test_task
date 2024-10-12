from datetime import datetime, timezone

import aiohttp
import pandas as pd
import sqlalchemy.exc
from sqlalchemy import Column, Float, Integer, String, DateTime, select
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker, declarative_base

# Настройки базы данных PostgreSQL
DATABASE_URL = "postgresql+asyncpg://weather_user:weather_password@localhost/weather_db"
SYNC_DATABASE_URL = "postgresql://weather_user:weather_password@localhost/weather_db"

# Создание базы данных с помощью SQLAlchemy
Base = declarative_base()


# Модель данных для хранения погоды
class Weather(Base):
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


# Движки для подключения к базе
async_engine = create_async_engine(DATABASE_URL)
sync_engine = create_engine(SYNC_DATABASE_URL, echo=True)

async_session = sessionmaker(
    async_engine, class_=AsyncSession, expire_on_commit=False
)


# Синхронный движок для создания таблиц
def init_db():
    try:
        with sync_engine.begin() as conn:
            Base.metadata.create_all(conn)
        print("Таблицы успешно созданы или уже существуют.")
    except sqlalchemy.exc.OperationalError as e:
        print(f"Ошибка доступа к базе данных: {e}")
    except sqlalchemy.exc.SQLAlchemyError as e:
        print(f"Ошибка работы с базой данных: {e}")


# Асинхронная функция для получения погоды
async def get_weather(latitude, longitude):
    weather_url = "https://api.open-meteo.com/v1/forecast"

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "current": ["temperature_2m", "precipitation", "rain", "showers", "snowfall", "surface_pressure",
                    "wind_speed_10m", "wind_direction_10m"],
        "wind_speed_unit": "ms"
    }

    async with aiohttp.ClientSession() as session:
        async with session.get(weather_url, params=params) as response:
            if response.status == 200:
                weather_data = await response.json()
                return weather_data
            else:
                print(f"Ошибка получения данных: {response.status}")
                return None


# Преобразование направления ветра в текстовый формат
def wind_direction_to_text(degrees):
    directions = [
        "C", "СВ", "В", "ЮВ",
        "Ю", "ЮЗ", "З", "СЗ"
    ]
    if degrees >= 337.5:
        return "C"
    index = int((degrees + 22.5) // 45)
    return directions[index]


# Функция для сохранения данных в базу данных
async def save_weather_to_db(session, latitude, longitude, weather_data):
    if "current" in weather_data:
        current_weather = weather_data["current"]
        temperature = current_weather.get("temperature_2m", None)  # Температура в градусах Цельсия
        wind_speed = current_weather.get("wind_speed_10m", None)  # Скорость ветра в м/с
        wind_direction = wind_direction_to_text(current_weather.get("wind_direction_10m", None))  # Направление ветра
        pressure = current_weather.get("surface_pressure", None)  # Давление
        precipitation = current_weather.get("precipitation", None)  # Осадки (мм)
        rain = current_weather.get("rain", None)  # Дождь (мм)
        showers = current_weather.get("showers", None)  # Ливень (мм)
        snowfall = current_weather.get("snowfall", None)  # Снег (мм)
        if pressure is not None:
            pressure = pressure * 0.75006  # Перевод в мм рт. ст.
        if snowfall is not None:
            snowfall = snowfall * 10  # Перевод в мм

        # Время из API или текущее время в UTC
        time_str = current_weather.get("time", None)
        timestamp = datetime.fromisoformat(time_str)
        timestamp = timestamp.replace(tzinfo=timezone.utc)

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


# Функция для получения последних 10 записей из базы данных
async def get_last_10_weather(session):
    # Создание запроса на выборку последних 10 записей
    stmt = select(Weather).order_by(Weather.timestamp.desc()).limit(10)
    result = await session.execute(stmt)
    return result.scalars().all()


# Функция для экспорта данных в файл Excel
def export_to_excel(data):
    # Преобразуем данные в DataFrame
    df = pd.DataFrame([{
        "Timestamp": row.timestamp.replace(tzinfo=None),
        "Latitude": row.latitude,
        "Longitude": row.longitude,
        "Temperature": row.temperature,
        "Wind Speed": row.wind_speed,
        "Wind Direction": row.wind_direction,
        "Pressure": row.pressure,
        "Precipitation": row.precipitation,
        "Rain": row.rain,
        "Showers": row.showers,
        "Snowfall": row.snowfall
    } for row in data])

    # Сохраняем DataFrame в Excel-файл
    df.to_excel("weather_data.xlsx", index=False)

    print("Данные успешно экспортированы в файл 'weather_data.xlsx'.")
