import aiohttp
import sqlalchemy.exc
from sqlalchemy import Column, Float, Integer, String, DateTime
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
                print(weather_data)
                return weather_data
            else:
                print(f"Ошибка получения данных: {response.status}")
                return None
