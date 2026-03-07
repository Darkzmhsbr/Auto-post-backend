import os
from sqlalchemy import create_engine, Column, Integer, String, Boolean, DateTime, ForeignKey, Text, Time, BigInteger
from sqlalchemy.dialects.postgresql import JSONB, BYTEA
from sqlalchemy.orm import declarative_base, sessionmaker, relationship
from datetime import datetime
from pytz import timezone

BRAZIL_TZ = timezone('America/Sao_Paulo')
def now_brazil():
    return datetime.now(BRAZIL_TZ)

DATABASE_URL = os.getenv("DATABASE_URL")
if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

if DATABASE_URL:
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)
else:
    engine = create_engine("sqlite:///./autopost_local.db")

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# ==========================================
# TABELAS DO SISTEMA AUTOPOST (V2)
# ==========================================

class AutopostSession(Base):
    __tablename__ = "autopost_sessions_v2" 
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(String, nullable=False, index=True) 
    phone_number = Column(String(20))
    api_id = Column(String)
    api_hash = Column(String)
    session_data = Column(BYTEA, nullable=True) 
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=now_brazil)
    channels = relationship("AutopostChannel", back_populates="session", cascade="all, delete-orphan")

# 👇 NOVA TABELA: BOTS OFICIAIS (A ESTRUTURA PONTE) 👇
class AutopostBot(Base):
    __tablename__ = "autopost_bots"
    
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(String, nullable=False, index=True)
    bot_token = Column(String, nullable=False, unique=True)
    bot_name = Column(String)      # Nome puxado da API do Telegram
    bot_username = Column(String)  # @dobot puxado da API
    
    origin_channel_id = Column(String) # De onde ele puxa (O Canal Oculto Premium)
    dest_channel_id = Column(String)   # Pra onde ele manda (VIP / Free)
    
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=now_brazil)
    
    # Relacionamento: um bot pode ter vários canais vinculados
    channels = relationship("AutopostChannel", back_populates="bot", foreign_keys="AutopostChannel.bot_id")

class AutopostChannel(Base):
    __tablename__ = "autopost_channels_v2" 
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(String, nullable=False) 
    session_id = Column(Integer, ForeignKey("autopost_sessions_v2.id"))
    bot_id = Column(Integer, ForeignKey("autopost_bots.id"), nullable=True)
    bot_token = Column(String, nullable=True)
    origin_channel_id = Column(BigInteger)
    origin_channel_name = Column(String)
    dest_channel_id = Column(BigInteger)        # Destino principal (legado, mantido por compatibilidade)
    dest_channel_name = Column(String)
    channel_type = Column(String) 
    interval_minutes = Column(Integer, default=30)
    schedule_start = Column(Time, nullable=True)
    schedule_end = Column(Time, nullable=True)
    cta_find = Column(Text, nullable=True)
    cta_replace = Column(Text, nullable=True)
    post_order = Column(String, default="fifo") 
    # 👇 NOVOS: Legenda personalizada com formatação HTML/Telegram
    custom_caption = Column(Text, nullable=True)       # Legenda HTML personalizada
    use_custom_caption = Column(Boolean, default=False) # Toggle ativar/desativar
    caption_mode = Column(String, default="replace")    # "replace" ou "append"
    is_active = Column(Boolean, default=True)
    last_post_id = Column(Integer, default=0)
    total_forwarded = Column(Integer, default=0)
    created_at = Column(DateTime, default=now_brazil)
    session = relationship("AutopostSession", back_populates="channels")
    bot = relationship("AutopostBot", back_populates="channels", foreign_keys=[bot_id])
    queue = relationship("AutopostQueue", back_populates="channel_pair", cascade="all, delete-orphan")
    # 👇 NOVO: Múltiplos destinos
    destinations = relationship("AutopostDestination", back_populates="channel", cascade="all, delete-orphan")

# 👇 NOVA TABELA: MÚLTIPLOS DESTINOS POR CANAL 👇
class AutopostDestination(Base):
    __tablename__ = "autopost_destinations"
    
    id = Column(Integer, primary_key=True, index=True)
    channel_id = Column(Integer, ForeignKey("autopost_channels_v2.id"), nullable=False)
    dest_channel_id = Column(BigInteger, nullable=False)
    dest_channel_name = Column(String)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=now_brazil)
    
    channel = relationship("AutopostChannel", back_populates="destinations")

class AutopostQueue(Base):
    __tablename__ = "autopost_queue_v2" 
    id = Column(Integer, primary_key=True, index=True)
    channel_pair_id = Column(Integer, ForeignKey("autopost_channels_v2.id"))
    origin_msg_id = Column(Integer)
    media_type = Column(String) 
    content_json = Column(JSONB) 
    status = Column(String, default="pending") 
    scheduled_for = Column(DateTime, nullable=True)
    sent_at = Column(DateTime, nullable=True)
    error_msg = Column(Text, nullable=True)
    channel_pair = relationship("AutopostChannel", back_populates="queue")

class AutopostLog(Base):
    __tablename__ = "autopost_logs_v2" 
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(String, nullable=False) 
    action = Column(String) 
    details = Column(JSONB, nullable=True)
    created_at = Column(DateTime, default=now_brazil)

def init_db():
    Base.metadata.create_all(bind=engine)