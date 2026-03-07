import os
import httpx
from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.orm import Session
from pydantic import BaseModel
from typing import List, Optional
from jose import jwt, JWTError

# Importações do Telethon (Automação do Telegram)
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import SessionPasswordNeededError

from database import init_db, SessionLocal, AutopostChannel, AutopostSession, AutopostBot, AutopostQueue, AutopostLog, engine, Base
from engine import start_engine, stop_engine, get_engine_status

init_db()
app = FastAPI(title="Zenyx AutoPost API", version="1.0")

# Inicia e para o Engine junto com o FastAPI
@app.on_event("startup")
async def on_startup():
    start_engine()  # Captura o event loop do uvicorn aqui

@app.on_event("shutdown")
async def on_shutdown():
    stop_engine()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "https://autopost.zenyxvips.com"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

security = HTTPBearer()
SECRET_KEY = os.getenv("SECRET_KEY", "chave-secreta-padrao") 
ALGORITHM = "HS256"

pending_logins = {}

# ==========================================
# CÉREBRO DA AUTENTICAÇÃO (SSO)
# ==========================================
def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    token = credentials.credentials
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM], options={"verify_aud": False})
        user_id = payload.get("sub") or payload.get("id") or payload.get("user_id")
        if not user_id:
            raise HTTPException(status_code=401, detail="Token inválido: Usuário não encontrado")
        return str(user_id)
    except JWTError:
        raise HTTPException(status_code=401, detail="Token expirado ou inválido.")

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# ==========================================
# MODELOS DE DADOS (Pydantic)
# ==========================================
class TelegramRequestCode(BaseModel):
    phone: str
    api_id: str
    api_hash: str

class TelegramVerifyCode(BaseModel):
    code: str
    password: Optional[str] = None

class BotCreate(BaseModel):
    bot_token: str
    origin_channel_id: str
    dest_channel_id: str

class BotResponse(BaseModel):
    id: int
    bot_name: str
    bot_username: str
    origin_channel_id: str
    dest_channel_id: str
    is_active: bool

    class Config:
        from_attributes = True

# 👇 Modelos de Canais (atualizados com bot_id e campos de agendamento) 👇
class ChannelCreate(BaseModel):
    bot_id: Optional[int] = None
    origin_channel_id: int
    origin_channel_name: str
    dest_channel_id: int
    dest_channel_name: str
    channel_type: str
    interval_minutes: int
    schedule_start: Optional[str] = None  # Formato "HH:MM"
    schedule_end: Optional[str] = None    # Formato "HH:MM"
    post_order: Optional[str] = "fifo"
    cta_find: Optional[str] = None
    cta_replace: Optional[str] = None

class ChannelResponse(BaseModel):
    id: int
    bot_id: Optional[int] = None
    bot_name: Optional[str] = None      # 👈 Nome do bot vinculado (preenchido na rota)
    bot_username: Optional[str] = None  # 👈 @username do bot vinculado
    origin_channel_id: int
    origin_channel_name: str
    dest_channel_id: int
    dest_channel_name: str
    channel_type: str
    interval_minutes: int
    schedule_start: Optional[str] = None
    schedule_end: Optional[str] = None
    post_order: Optional[str] = "fifo"
    cta_find: Optional[str] = None
    cta_replace: Optional[str] = None
    is_active: bool
    total_forwarded: int

    class Config:
        from_attributes = True

# ==========================================
# 1. ROTAS DE CONEXÃO DO TELEGRAM (USERBOT)
# ==========================================
@app.get("/api/telegram/status")
def get_telegram_status(user_id: str = Depends(get_current_user), db: Session = Depends(get_db)):
    session = db.query(AutopostSession).filter(AutopostSession.user_id == user_id).first()
    if session and session.is_active and session.session_data:
        return {"status": "ativa", "phone": session.phone_number}
    return {"status": "desconectada"}

@app.post("/api/telegram/request-code")
async def request_telegram_code(req: TelegramRequestCode, user_id: str = Depends(get_current_user)):
    try:
        client = TelegramClient(StringSession(), int(req.api_id), req.api_hash)
        await client.connect()
        sent_code = await client.send_code_request(req.phone)
        pending_logins[user_id] = {
            "client": client,
            "phone": req.phone,
            "api_id": req.api_id,
            "api_hash": req.api_hash,
            "phone_code_hash": sent_code.phone_code_hash
        }
        return {"message": "Código enviado para o seu aplicativo do Telegram!"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Erro ao pedir código: {str(e)}")

@app.post("/api/telegram/verify-code")
async def verify_telegram_code(req: TelegramVerifyCode, user_id: str = Depends(get_current_user), db: Session = Depends(get_db)):
    if user_id not in pending_logins:
        raise HTTPException(status_code=400, detail="Sessão expirada. Peça o código novamente.")
    data = pending_logins[user_id]
    client = data["client"]
    try:
        if req.password:
            await client.sign_in(password=req.password)
        else:
            await client.sign_in(phone=data["phone"], code=req.code, phone_code_hash=data["phone_code_hash"])
        session_str = client.session.save()
        await client.disconnect()
        del pending_logins[user_id]
        
        db_session = db.query(AutopostSession).filter(AutopostSession.user_id == user_id).first()
        if not db_session:
            db_session = AutopostSession(user_id=user_id)
            db.add(db_session)
        db_session.phone_number = data["phone"]
        db_session.api_id = data["api_id"]
        db_session.api_hash = data["api_hash"]
        db_session.session_data = session_str.encode('utf-8')
        db_session.is_active = True
        db.commit()
        return {"message": "Telegram conectado com sucesso!"}
    except SessionPasswordNeededError:
        return {"message": "Atenção: Senha de 2 Etapas necessária.", "needs_password": True}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Erro ao verificar: {str(e)}")

@app.post("/api/telegram/logout")
def telegram_logout(user_id: str = Depends(get_current_user), db: Session = Depends(get_db)):
    db_session = db.query(AutopostSession).filter(AutopostSession.user_id == user_id).first()
    if db_session:
        db_session.is_active = False
        db_session.session_data = None
        db.commit()
    return {"message": "Sessão desconectada!"}

# ==========================================
# 2. ROTAS DE GERENCIAMENTO DE BOTS (PONTE)
# ==========================================
@app.post("/api/bots", response_model=BotResponse)
async def create_bot(bot_data: BotCreate, user_id: str = Depends(get_current_user), db: Session = Depends(get_db)):
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(f"https://api.telegram.org/bot{bot_data.bot_token}/getMe")
            tg_data = resp.json()
            if not tg_data.get("ok"):
                raise HTTPException(status_code=400, detail="Token do Bot inválido.")
            bot_name = tg_data["result"]["first_name"]
            bot_username = tg_data["result"]["username"]
    except Exception as e:
        raise HTTPException(status_code=400, detail="Erro de conexão com o Telegram. Verifique o Token.")

    novo_bot = AutopostBot(
        user_id=user_id,
        bot_token=bot_data.bot_token,
        bot_name=bot_name,
        bot_username=bot_username,
        origin_channel_id=bot_data.origin_channel_id,
        dest_channel_id=bot_data.dest_channel_id
    )
    try:
        db.add(novo_bot)
        db.commit()
        db.refresh(novo_bot)
        return novo_bot
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=400, detail="Este bot já está cadastrado no sistema.")

@app.get("/api/bots", response_model=List[BotResponse])
def list_bots(user_id: str = Depends(get_current_user), db: Session = Depends(get_db)):
    return db.query(AutopostBot).filter(AutopostBot.user_id == user_id).all()

@app.delete("/api/bots/{bot_id}")
def delete_bot(bot_id: int, user_id: str = Depends(get_current_user), db: Session = Depends(get_db)):
    bot = db.query(AutopostBot).filter(AutopostBot.id == bot_id, AutopostBot.user_id == user_id).first()
    if not bot:
        raise HTTPException(status_code=404, detail="Bot não encontrado.")
    db.delete(bot)
    db.commit()
    return {"message": "Bot deletado com sucesso!"}

# ==========================================
# 3. ROTAS DE CANAIS (CLONAGEM/AUTOPOST)
# ==========================================

# Helper para serializar canal com dados do bot
def _serialize_channel(canal, db):
    """Converte AutopostChannel em dict com bot_name e bot_username"""
    data = {
        "id": canal.id,
        "bot_id": canal.bot_id,
        "bot_name": None,
        "bot_username": None,
        "origin_channel_id": canal.origin_channel_id,
        "origin_channel_name": canal.origin_channel_name,
        "dest_channel_id": canal.dest_channel_id,
        "dest_channel_name": canal.dest_channel_name,
        "channel_type": canal.channel_type,
        "interval_minutes": canal.interval_minutes,
        "schedule_start": canal.schedule_start.strftime("%H:%M") if canal.schedule_start else None,
        "schedule_end": canal.schedule_end.strftime("%H:%M") if canal.schedule_end else None,
        "post_order": canal.post_order or "fifo",
        "cta_find": canal.cta_find,
        "cta_replace": canal.cta_replace,
        "is_active": canal.is_active,
        "total_forwarded": canal.total_forwarded or 0,
    }
    # Puxa nome do bot se vinculado
    if canal.bot_id:
        bot = db.query(AutopostBot).filter(AutopostBot.id == canal.bot_id).first()
        if bot:
            data["bot_name"] = bot.bot_name
            data["bot_username"] = bot.bot_username
    return data

@app.get("/api/autopost/channels")
def list_channels(user_id: str = Depends(get_current_user), db: Session = Depends(get_db)):
    canais = db.query(AutopostChannel).filter(AutopostChannel.user_id == user_id).all()
    return [_serialize_channel(c, db) for c in canais]

@app.post("/api/autopost/channels")
def create_channel(canal: ChannelCreate, user_id: str = Depends(get_current_user), db: Session = Depends(get_db)):
    from datetime import time as dt_time
    
    # Converte strings "HH:MM" para objetos Time
    start_time = None
    end_time = None
    if canal.schedule_start:
        parts = canal.schedule_start.split(":")
        start_time = dt_time(int(parts[0]), int(parts[1]))
    if canal.schedule_end:
        parts = canal.schedule_end.split(":")
        end_time = dt_time(int(parts[0]), int(parts[1]))
    
    # Se bot_id fornecido, valida se pertence ao usuário
    if canal.bot_id:
        bot = db.query(AutopostBot).filter(AutopostBot.id == canal.bot_id, AutopostBot.user_id == user_id).first()
        if not bot:
            raise HTTPException(status_code=400, detail="Bot não encontrado ou não pertence a você.")
    
    novo_canal = AutopostChannel(
        user_id=user_id,
        bot_id=canal.bot_id,
        origin_channel_id=canal.origin_channel_id,
        origin_channel_name=canal.origin_channel_name,
        dest_channel_id=canal.dest_channel_id,
        dest_channel_name=canal.dest_channel_name,
        channel_type=canal.channel_type,
        interval_minutes=canal.interval_minutes,
        schedule_start=start_time,
        schedule_end=end_time,
        post_order=canal.post_order or "fifo",
        cta_find=canal.cta_find,
        cta_replace=canal.cta_replace
    )
    db.add(novo_canal)
    db.commit()
    db.refresh(novo_canal)
    return _serialize_channel(novo_canal, db)

@app.delete("/api/autopost/channels/{channel_id}")
def delete_channel(channel_id: int, user_id: str = Depends(get_current_user), db: Session = Depends(get_db)):
    canal = db.query(AutopostChannel).filter(AutopostChannel.id == channel_id, AutopostChannel.user_id == user_id).first()
    if not canal:
        raise HTTPException(status_code=404, detail="Configuração não encontrada.")
    db.delete(canal)
    db.commit()
    return {"message": "Canal removido com sucesso!"}

@app.post("/api/autopost/channels/{channel_id}/toggle")
def toggle_channel(channel_id: int, user_id: str = Depends(get_current_user), db: Session = Depends(get_db)):
    canal = db.query(AutopostChannel).filter(AutopostChannel.id == channel_id, AutopostChannel.user_id == user_id).first()
    if not canal:
        raise HTTPException(status_code=404, detail="Configuração não encontrada.")
    canal.is_active = not canal.is_active
    db.commit()
    return {"message": f"Canal {'ativado' if canal.is_active else 'pausado'} com sucesso!", "is_active": canal.is_active}

# ==========================================
# 4. STATUS GERAL E ROOT
# ==========================================
@app.get("/")
def read_root():
    return {"status": "online", "message": "Zenyx AutoPost Backend operando com sucesso!"}

@app.get("/api/auth/me")
def verify_auth(user_id: str = Depends(get_current_user)):
    return {"status": "success", "user_id": user_id, "message": "Autenticação confirmada!"}

@app.get("/api/autopost/stats")
def get_stats(user_id: str = Depends(get_current_user), db: Session = Depends(get_db)):
    total_canais = db.query(AutopostChannel).filter(AutopostChannel.user_id == user_id).count()
    canais_ativos = db.query(AutopostChannel).filter(AutopostChannel.user_id == user_id, AutopostChannel.is_active == True).count()
    total_bots = db.query(AutopostBot).filter(AutopostBot.user_id == user_id).count()
    
    # Conta posts enviados hoje
    from datetime import datetime, timedelta
    from pytz import timezone as tz
    hoje_inicio = datetime.now(tz('America/Sao_Paulo')).replace(hour=0, minute=0, second=0, microsecond=0)
    
    user_channel_ids = [
        ch.id for ch in
        db.query(AutopostChannel).filter(AutopostChannel.user_id == user_id).all()
    ]
    posts_hoje = 0
    if user_channel_ids:
        posts_hoje = db.query(AutopostQueue).filter(
            AutopostQueue.channel_pair_id.in_(user_channel_ids),
            AutopostQueue.status == "sent",
            AutopostQueue.sent_at >= hoje_inicio
        ).count()
    
    session = db.query(AutopostSession).filter(AutopostSession.user_id == user_id).first()
    status_sessao = "ativa" if (session and session.is_active and session.session_data) else "desconectada"
    
    engine_info = get_engine_status()
    
    return {
        "total_canais_configurados": total_canais,
        "canais_ativos": canais_ativos,
        "total_bots": total_bots,
        "posts_enviados_hoje": posts_hoje,
        "status_sessao": status_sessao,
        "engine": engine_info
    }

# ==========================================
# 5. ROTAS DO ENGINE (STATUS + CONTROLE)
# ==========================================
@app.get("/api/engine/status")
def engine_status_route(user_id: str = Depends(get_current_user)):
    """Retorna o status atual do motor AutoPost"""
    return get_engine_status()

# ==========================================
# 6. ROTAS DA FILA DE ENVIOS
# ==========================================
@app.get("/api/autopost/queue")
def list_queue(
    user_id: str = Depends(get_current_user),
    db: Session = Depends(get_db),
    status_filter: Optional[str] = None,
    limit: int = 50
):
    """Lista itens da fila de envios do usuário"""
    # Busca IDs dos canais do usuário
    user_channel_ids = [
        ch.id for ch in 
        db.query(AutopostChannel).filter(AutopostChannel.user_id == user_id).all()
    ]
    
    if not user_channel_ids:
        return []
    
    query = db.query(AutopostQueue).filter(
        AutopostQueue.channel_pair_id.in_(user_channel_ids)
    )
    
    if status_filter:
        query = query.filter(AutopostQueue.status == status_filter)
    
    items = query.order_by(AutopostQueue.id.desc()).limit(limit).all()
    
    result = []
    for item in items:
        # Puxa nome do canal vinculado
        canal = db.query(AutopostChannel).filter(AutopostChannel.id == item.channel_pair_id).first()
        result.append({
            "id": item.id,
            "channel_pair_id": item.channel_pair_id,
            "origin_channel_name": canal.origin_channel_name if canal else "—",
            "dest_channel_name": canal.dest_channel_name if canal else "—",
            "origin_msg_id": item.origin_msg_id,
            "media_type": item.media_type,
            "content_json": item.content_json,
            "status": item.status,
            "scheduled_for": item.scheduled_for.isoformat() if item.scheduled_for else None,
            "sent_at": item.sent_at.isoformat() if item.sent_at else None,
            "error_msg": item.error_msg,
        })
    
    return result

@app.delete("/api/autopost/queue/{item_id}")
def delete_queue_item(item_id: int, user_id: str = Depends(get_current_user), db: Session = Depends(get_db)):
    """Remove um item da fila"""
    item = db.query(AutopostQueue).filter(AutopostQueue.id == item_id).first()
    if not item:
        raise HTTPException(status_code=404, detail="Item não encontrado.")
    
    # Verifica se o canal pertence ao usuário
    canal = db.query(AutopostChannel).filter(
        AutopostChannel.id == item.channel_pair_id,
        AutopostChannel.user_id == user_id
    ).first()
    if not canal:
        raise HTTPException(status_code=403, detail="Sem permissão.")
    
    db.delete(item)
    db.commit()
    return {"message": "Item removido da fila!"}

# ==========================================
# 7. ROTAS DE LOGS / HISTÓRICO
# ==========================================
@app.get("/api/autopost/logs")
def list_logs(
    user_id: str = Depends(get_current_user),
    db: Session = Depends(get_db),
    action_filter: Optional[str] = None,
    limit: int = 100
):
    """Lista logs de atividade do usuário"""
    query = db.query(AutopostLog).filter(AutopostLog.user_id == user_id)
    
    if action_filter:
        query = query.filter(AutopostLog.action == action_filter)
    
    logs = query.order_by(AutopostLog.id.desc()).limit(limit).all()
    
    return [
        {
            "id": log.id,
            "action": log.action,
            "details": log.details,
            "created_at": log.created_at.isoformat() if log.created_at else None,
        }
        for log in logs
    ]

@app.delete("/api/autopost/logs")
def clear_logs(user_id: str = Depends(get_current_user), db: Session = Depends(get_db)):
    """Limpa todos os logs do usuário"""
    db.query(AutopostLog).filter(AutopostLog.user_id == user_id).delete()
    db.commit()
    return {"message": "Histórico limpo com sucesso!"}

# ==========================================
# 8. ROTA DE MIGRAÇÃO (Acessar via URL para aplicar novas colunas)
# ==========================================
@app.get("/api/migrate")
def run_migration(db: Session = Depends(get_db)):
    """
    Rota de migração manual. Acesse:
    https://api-autopost.zenyxvips.com/api/migrate
    
    Adiciona a coluna bot_id na tabela autopost_channels_v2
    (e cria a tabela autopost_bots se não existir).
    """
    from sqlalchemy import text, inspect
    
    results = []
    
    try:
        inspector = inspect(engine)
        existing_tables = inspector.get_table_names()
        
        # 1. Cria tabela autopost_bots se não existir
        if "autopost_bots" not in existing_tables:
            Base.metadata.tables["autopost_bots"].create(bind=engine)
            results.append("✅ Tabela 'autopost_bots' criada com sucesso!")
        else:
            results.append("ℹ️ Tabela 'autopost_bots' já existe.")
        
        # 2. Adiciona coluna bot_id em autopost_channels_v2 se não existir
        columns = [col["name"] for col in inspector.get_columns("autopost_channels_v2")]
        
        if "bot_id" not in columns:
            db.execute(text("ALTER TABLE autopost_channels_v2 ADD COLUMN bot_id INTEGER REFERENCES autopost_bots(id)"))
            db.commit()
            results.append("✅ Coluna 'bot_id' adicionada em 'autopost_channels_v2'!")
        else:
            results.append("ℹ️ Coluna 'bot_id' já existe em 'autopost_channels_v2'.")
        
        return {"status": "success", "migrations": results}
    
    except Exception as e:
        db.rollback()
        return {"status": "error", "detail": str(e), "migrations": results}