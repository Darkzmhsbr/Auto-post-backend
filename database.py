import os
from sqlalchemy import create_engine, Column, Integer, String, Boolean, DateTime, ForeignKey, Text, Time, BigInteger
from sqlalchemy.dialects.postgresql import JSONB, BYTEA
from sqlalchemy.orm import declarative_base, sessionmaker, relationship
from datetime import datetime
from pytz import timezone

# Configuração de Fuso Horário - Brasília
BRAZIL_TZ = timezone('America/Sao_Paulo')
def now_brazil():
    return datetime.now(BRAZIL_TZ)

# Conexão com o Banco de Dados (Puxa a URL do Railway)
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
# TABELAS DO SISTEMA AUTOPOST (Conforme PDF)
# ==========================================

class AutopostSession(Base):
    __tablename__ = "autopost_sessions"
    
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, nullable=False, index=True) # ID do usuário da plataforma principal
    phone_number = Column(String(20))
    api_id = Column(String)
    api_hash = Column(String)
    session_data = Column(BYTEA, nullable=True) # Sessão do Telethon criptografada
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=now_brazil)

    channels = relationship("AutopostChannel", back_populates="session", cascade="all, delete-orphan")

class AutopostChannel(Base):
    __tablename__ = "autopost_channels"
    
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, nullable=False)
    session_id = Column(Integer, ForeignKey("autopost_sessions.id"))
    
    bot_token = Column(String, nullable=True)
    origin_channel_id = Column(BigInteger)
    origin_channel_name = Column(String)
    dest_channel_id = Column(BigInteger)
    dest_channel_name = Column(String)
    channel_type = Column(String) # 'previas' ou 'vip'
    
    interval_minutes = Column(Integer, default=30)
    schedule_start = Column(Time, nullable=True)
    schedule_end = Column(Time, nullable=True)
    
    cta_find = Column(Text, nullable=True)
    cta_replace = Column(Text, nullable=True)
    post_order = Column(String, default="fifo") # 'fifo' ou 'random'
    
    is_active = Column(Boolean, default=True)
    last_post_id = Column(Integer, default=0)
    total_forwarded = Column(Integer, default=0)
    created_at = Column(DateTime, default=now_brazil)

    session = relationship("AutopostSession", back_populates="channels")
    queue = relationship("AutopostQueue", back_populates="channel_pair", cascade="all, delete-orphan")

class AutopostQueue(Base):
    __tablename__ = "autopost_queue"
    
    id = Column(Integer, primary_key=True, index=True)
    channel_pair_id = Column(Integer, ForeignKey("autopost_channels.id"))
    
    origin_msg_id = Column(Integer)
    media_type = Column(String) # 'text', 'photo', 'video', 'album'
    content_json = Column(JSONB) # Conteúdo extraído da mensagem
    
    status = Column(String, default="pending") # 'pending', 'sent', 'failed', 'skipped'
    scheduled_for = Column(DateTime, nullable=True)
    sent_at = Column(DateTime, nullable=True)
    error_msg = Column(Text, nullable=True)

    channel_pair = relationship("AutopostChannel", back_populates="queue")

class AutopostLog(Base):
    __tablename__ = "autopost_logs"
    
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, nullable=False)
    action = Column(String) # 'forward', 'skip', 'error', 'config_change'
    details = Column(JSONB, nullable=True)
    created_at = Column(DateTime, default=now_brazil)

def init_db():
    Base.metadata.create_all(bind=engine)