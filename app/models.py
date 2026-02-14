from sqlalchemy import (
    Column,
    Integer,
    String,
   
    DateTime,
   
)
from .database import Base

from sqlalchemy import (
    Column,
    Integer,
    String,
    DateTime, 
    Float,
   
)


class TemperatureReading(Base):
    __tablename__ = "temperature_data"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    temperature = Column(Float, nullable=False)
    timestamp = Column(DateTime, nullable=False)
    mfredge = Column(String(255), nullable=True, index=True)  # device/room identifier
