"""
Zenyx AutoPost Engine - Motor de Processamento
================================================
Roda em background via APScheduler dentro do FastAPI.
Processa canais ativos: clona, encaminha ou espiona (ponte) mensagens.
"""

import asyncio
import logging
import json
from datetime import datetime, time as dt_time
from pytz import timezone
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.types import (
    MessageMediaPhoto, MessageMediaDocument, 
    MessageMediaWebPage, MessageMediaContact
)

from database import SessionLocal, AutopostChannel, AutopostSession, AutopostBot, AutopostQueue, AutopostLog

# ==========================================
# CONFIGURAÇÃO
# ==========================================
BRAZIL_TZ = timezone('America/Sao_Paulo')
logger = logging.getLogger("autopost_engine")
logger.setLevel(logging.INFO)

if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('[%(asctime)s] ENGINE | %(levelname)s | %(message)s'))
    logger.addHandler(handler)

# Cache de clientes Telethon ativos (user_id -> TelegramClient)
_userbot_clients = {}

# Cache de bots oficiais ativos (bot_token -> TelegramClient)  
_bot_clients = {}


# ==========================================
# HELPERS
# ==========================================
def now_brazil():
    return datetime.now(BRAZIL_TZ)


def is_within_schedule(channel):
    """Verifica se o horário atual está dentro do agendamento do canal"""
    if not channel.schedule_start or not channel.schedule_end:
        return True  # Sem agendamento = 24h
    
    now = now_brazil().time()
    start = channel.schedule_start
    end = channel.schedule_end
    
    # Suporte para horários que cruzam meia-noite (ex: 22:00 - 06:00)
    if start <= end:
        return start <= now <= end
    else:
        return now >= start or now <= end


def apply_cta_replacement(text, cta_find, cta_replace):
    """Substitui o CTA/link no texto da mensagem"""
    if not text or not cta_find:
        return text
    return text.replace(cta_find, cta_replace or '')


def create_log(db, user_id, action, details=None):
    """Registra ação no log"""
    log = AutopostLog(
        user_id=user_id,
        action=action,
        details=details
    )
    db.add(log)
    db.commit()


def create_queue_entry(db, channel_pair_id, origin_msg_id, media_type, content_json, status="pending"):
    """Cria entrada na fila"""
    entry = AutopostQueue(
        channel_pair_id=channel_pair_id,
        origin_msg_id=origin_msg_id,
        media_type=media_type,
        content_json=content_json,
        status=status,
        scheduled_for=now_brazil()
    )
    db.add(entry)
    db.commit()
    return entry


# ==========================================
# GERENCIAMENTO DE CLIENTES TELETHON
# ==========================================
async def get_userbot_client(session_record):
    """Obtém ou cria um cliente Telethon para o userbot do usuário"""
    user_id = session_record.user_id
    
    if user_id in _userbot_clients:
        client = _userbot_clients[user_id]
        if client.is_connected():
            return client
        # Reconecta se desconectou
        try:
            await client.connect()
            return client
        except Exception:
            del _userbot_clients[user_id]
    
    # Cria novo cliente
    try:
        session_str = session_record.session_data.decode('utf-8') if isinstance(session_record.session_data, bytes) else session_record.session_data
        client = TelegramClient(
            StringSession(session_str),
            int(session_record.api_id),
            session_record.api_hash
        )
        await client.connect()
        
        if not await client.is_user_authorized():
            logger.warning(f"Userbot de {user_id} não está autorizado. Sessão pode ter expirado.")
            return None
        
        _userbot_clients[user_id] = client
        logger.info(f"Userbot conectado para user_id={user_id}")
        return client
    except Exception as e:
        logger.error(f"Erro ao conectar userbot de {user_id}: {e}")
        return None


async def get_bot_client(bot_token):
    """Obtém ou cria um cliente TelegramClient para o bot oficial"""
    if bot_token in _bot_clients:
        client = _bot_clients[bot_token]
        if client.is_connected():
            return client
        try:
            await client.connect()
            return client
        except Exception:
            del _bot_clients[bot_token]
    
    try:
        # Bot clients usam api_id/api_hash genéricos (Telethon aceita qualquer um para bots)
        client = TelegramClient(
            StringSession(),
            api_id=6,  # api_id padrão do Telethon para bots
            api_hash="eb06d4abfb49dc3eeb1aeb98ae0f581e"  # api_hash padrão
        )
        await client.start(bot_token=bot_token)
        _bot_clients[bot_token] = client
        logger.info(f"Bot conectado: {bot_token[:15]}...")
        return client
    except Exception as e:
        logger.error(f"Erro ao conectar bot {bot_token[:15]}...: {e}")
        return None


# ==========================================
# PROCESSADORES POR MODO
# ==========================================
async def process_clone(userbot, channel, message, db):
    """
    MODO CLONE: Copia texto/mídia e reposta como novo no destino.
    Usa o Userbot direto ou Bot oficial se configurado.
    """
    dest_client = userbot
    bot_record = None
    
    # Se tem bot vinculado, usa o bot pra postar no destino
    if channel.bot_id:
        bot_record = db.query(AutopostBot).filter(AutopostBot.id == channel.bot_id).first()
        if bot_record:
            bot_client = await get_bot_client(bot_record.bot_token)
            if bot_client:
                dest_client = bot_client
            else:
                logger.warning(f"Bot {bot_record.bot_name} offline. Usando userbot como fallback.")
    
    try:
        text = message.text or message.message or ''
        text = apply_cta_replacement(text, channel.cta_find, channel.cta_replace)
        
        media_type = "text"
        
        if message.media:
            if isinstance(message.media, MessageMediaPhoto):
                media_type = "photo"
            elif isinstance(message.media, MessageMediaDocument):
                media_type = "document"
            else:
                media_type = "other_media"
            
            # Envia com mídia
            await dest_client.send_message(
                int(channel.dest_channel_id),
                message=text if text else None,
                file=message.media
            )
        else:
            # Só texto
            if text:
                await dest_client.send_message(
                    int(channel.dest_channel_id),
                    message=text
                )
            else:
                return None  # Mensagem vazia, ignora
        
        # Registra na fila como enviado
        queue_entry = create_queue_entry(
            db, channel.id, message.id, media_type,
            {"text_preview": text[:200] if text else "", "mode": "clone"},
            status="sent"
        )
        queue_entry.sent_at = now_brazil()
        db.commit()
        
        logger.info(f"CLONE | Canal {channel.id} | msg {message.id} → destino {channel.dest_channel_id}")
        return queue_entry
        
    except Exception as e:
        error_msg = str(e)
        create_queue_entry(
            db, channel.id, message.id, "error",
            {"error": error_msg, "mode": "clone"},
            status="error"
        )
        logger.error(f"CLONE ERRO | Canal {channel.id} | msg {message.id}: {error_msg}")
        return None


async def process_forward(userbot, channel, message, db):
    """
    MODO FORWARD: Encaminha nativamente (forward) a mensagem.
    Usa o Bot oficial se configurado na ponte.
    """
    dest_client = userbot
    
    if channel.bot_id:
        bot_record = db.query(AutopostBot).filter(AutopostBot.id == channel.bot_id).first()
        if bot_record:
            bot_client = await get_bot_client(bot_record.bot_token)
            if bot_client:
                dest_client = bot_client
    
    try:
        await dest_client.forward_messages(
            int(channel.dest_channel_id),
            message.id,
            int(channel.origin_channel_id)
        )
        
        queue_entry = create_queue_entry(
            db, channel.id, message.id, "forward",
            {"text_preview": (message.text or "")[:200], "mode": "forward"},
            status="sent"
        )
        queue_entry.sent_at = now_brazil()
        db.commit()
        
        logger.info(f"FORWARD | Canal {channel.id} | msg {message.id} → destino {channel.dest_channel_id}")
        return queue_entry
        
    except Exception as e:
        error_msg = str(e)
        create_queue_entry(
            db, channel.id, message.id, "error",
            {"error": error_msg, "mode": "forward"},
            status="error"
        )
        logger.error(f"FORWARD ERRO | Canal {channel.id} | msg {message.id}: {error_msg}")
        return None


async def process_spy(userbot, channel, message, db):
    """
    MODO ESPIONAR (PONTE PREMIUM):
    1. Userbot lê do canal concorrente (origem)
    2. Userbot reposta no Canal Oculto do bot (canal intermediário = origin do bot)
    3. Bot oficial encaminha do Canal Oculto → Destino final
    
    Isso preserva emojis premium e evita punição por spam.
    """
    if not channel.bot_id:
        logger.warning(f"SPY | Canal {channel.id} não tem bot vinculado. Fazendo clone direto.")
        return await process_clone(userbot, channel, message, db)
    
    bot_record = db.query(AutopostBot).filter(AutopostBot.id == channel.bot_id).first()
    if not bot_record:
        logger.warning(f"SPY | Bot {channel.bot_id} não encontrado. Fazendo clone direto.")
        return await process_clone(userbot, channel, message, db)
    
    bot_client = await get_bot_client(bot_record.bot_token)
    if not bot_client:
        logger.warning(f"SPY | Bot {bot_record.bot_name} offline. Fazendo clone direto.")
        return await process_clone(userbot, channel, message, db)
    
    try:
        # PASSO 1: Userbot clona pro Canal Oculto (origin do bot)
        text = message.text or message.message or ''
        text = apply_cta_replacement(text, channel.cta_find, channel.cta_replace)
        
        bridge_channel_id = int(bot_record.origin_channel_id)
        
        if message.media:
            bridge_msg = await userbot.send_message(
                bridge_channel_id,
                message=text if text else None,
                file=message.media
            )
        else:
            if not text:
                return None
            bridge_msg = await userbot.send_message(
                bridge_channel_id,
                message=text
            )
        
        # Pequena pausa para o Telegram processar
        await asyncio.sleep(1)
        
        # PASSO 2: Bot oficial encaminha do Canal Oculto → Destino final
        dest_channel_id = int(bot_record.dest_channel_id)
        
        await bot_client.forward_messages(
            dest_channel_id,
            bridge_msg.id,
            bridge_channel_id
        )
        
        media_type = "spy_bridge"
        if message.media:
            if isinstance(message.media, MessageMediaPhoto):
                media_type = "spy_photo"
            elif isinstance(message.media, MessageMediaDocument):
                media_type = "spy_document"
        
        queue_entry = create_queue_entry(
            db, channel.id, message.id, media_type,
            {
                "text_preview": text[:200] if text else "",
                "mode": "spy",
                "bridge_channel": str(bridge_channel_id),
                "bridge_msg_id": bridge_msg.id,
                "bot_name": bot_record.bot_name
            },
            status="sent"
        )
        queue_entry.sent_at = now_brazil()
        db.commit()
        
        logger.info(f"SPY PONTE | Canal {channel.id} | msg {message.id} → ponte {bridge_channel_id} → destino {dest_channel_id}")
        return queue_entry
        
    except Exception as e:
        error_msg = str(e)
        create_queue_entry(
            db, channel.id, message.id, "error",
            {"error": error_msg, "mode": "spy", "bot_name": bot_record.bot_name},
            status="error"
        )
        logger.error(f"SPY ERRO | Canal {channel.id} | msg {message.id}: {error_msg}")
        return None


# ==========================================
# PROCESSADOR PRINCIPAL DE UM CANAL
# ==========================================
async def process_channel(channel, session_record, db):
    """Processa um canal ativo: lê mensagens novas e despacha pelo modo correto"""
    
    # Verifica agendamento
    if not is_within_schedule(channel):
        return 0
    
    # Conecta userbot
    userbot = await get_userbot_client(session_record)
    if not userbot:
        create_log(db, channel.user_id, "engine_error", {
            "channel_id": channel.id,
            "error": "Userbot não disponível"
        })
        return 0
    
    try:
        # Lê mensagens novas (depois do last_post_id)
        origin_id = int(channel.origin_channel_id)
        min_id = channel.last_post_id or 0
        
        messages = await userbot.get_messages(
            origin_id,
            min_id=min_id,
            limit=10  # Processa até 10 por ciclo para evitar flood
        )
        
        if not messages:
            return 0
        
        # Filtra mensagens realmente novas (min_id é exclusivo no Telethon)
        new_messages = [m for m in messages if m.id > min_id and m.message is not None or m.media is not None]
        
        if not new_messages:
            return 0
        
        # Ordena por ID (mais antigo primeiro = FIFO por padrão)
        new_messages.sort(key=lambda m: m.id)
        
        # Aplica ordem configurada
        if channel.post_order == 'lifo':
            new_messages.reverse()
        elif channel.post_order == 'random':
            import random
            random.shuffle(new_messages)
        
        processed = 0
        
        for msg in new_messages:
            # Seleciona processador pelo modo
            if channel.channel_type == 'forward':
                result = await process_forward(userbot, channel, msg, db)
            elif channel.channel_type == 'spy':
                result = await process_spy(userbot, channel, msg, db)
            else:  # clone (padrão)
                result = await process_clone(userbot, channel, msg, db)
            
            if result:
                processed += 1
                # Atualiza last_post_id e contador
                if msg.id > (channel.last_post_id or 0):
                    channel.last_post_id = msg.id
                channel.total_forwarded = (channel.total_forwarded or 0) + 1
                db.commit()
            
            # Delay entre posts para evitar flood (2 segundos)
            await asyncio.sleep(2)
        
        if processed > 0:
            create_log(db, channel.user_id, "posts_sent", {
                "channel_id": channel.id,
                "origin": str(channel.origin_channel_id),
                "dest": str(channel.dest_channel_id),
                "mode": channel.channel_type,
                "count": processed
            })
        
        return processed
        
    except Exception as e:
        logger.error(f"Erro ao processar canal {channel.id}: {e}")
        create_log(db, channel.user_id, "engine_error", {
            "channel_id": channel.id,
            "error": str(e)
        })
        return 0


# ==========================================
# LOOP PRINCIPAL DO ENGINE
# ==========================================
async def engine_tick():
    """
    Executado a cada ciclo pelo APScheduler.
    Percorre todos os canais ativos e processa cada um.
    """
    db = SessionLocal()
    
    try:
        # Busca todos os canais ativos
        active_channels = db.query(AutopostChannel).filter(
            AutopostChannel.is_active == True
        ).all()
        
        if not active_channels:
            return
        
        logger.info(f"Engine tick: {len(active_channels)} canais ativos encontrados")
        
        # Agrupa por user_id para reutilizar conexão do userbot
        channels_by_user = {}
        for ch in active_channels:
            if ch.user_id not in channels_by_user:
                channels_by_user[ch.user_id] = []
            channels_by_user[ch.user_id].append(ch)
        
        total_processed = 0
        
        for user_id, user_channels in channels_by_user.items():
            # Busca sessão do userbot
            session_record = db.query(AutopostSession).filter(
                AutopostSession.user_id == user_id,
                AutopostSession.is_active == True
            ).first()
            
            if not session_record or not session_record.session_data:
                logger.warning(f"Sem sessão ativa para user_id={user_id}. Pulando {len(user_channels)} canais.")
                continue
            
            for channel in user_channels:
                try:
                    count = await process_channel(channel, session_record, db)
                    total_processed += count
                except Exception as e:
                    logger.error(f"Erro no canal {channel.id}: {e}")
                    continue
        
        if total_processed > 0:
            logger.info(f"Engine tick finalizado: {total_processed} posts processados")
    
    except Exception as e:
        logger.error(f"Erro geral no engine tick: {e}")
    
    finally:
        db.close()


def run_engine_tick():
    """Wrapper síncrono para o APScheduler chamar a função async"""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            # Se já estiver rodando (dentro do FastAPI/uvicorn), cria task
            asyncio.ensure_future(engine_tick())
        else:
            loop.run_until_complete(engine_tick())
    except RuntimeError:
        # Se não houver loop, cria um novo
        asyncio.run(engine_tick())


# ==========================================
# STARTUP / SHUTDOWN DO ENGINE
# ==========================================
_scheduler = None

def start_engine():
    """Inicia o APScheduler com o engine_tick rodando a cada 60 segundos"""
    global _scheduler
    
    from apscheduler.schedulers.background import BackgroundScheduler
    
    _scheduler = BackgroundScheduler(timezone="America/Sao_Paulo")
    _scheduler.add_job(
        run_engine_tick,
        'interval',
        seconds=60,  # Verifica a cada 60 segundos
        id='autopost_engine_tick',
        name='AutoPost Engine Tick',
        replace_existing=True,
        max_instances=1,  # Nunca executa em paralelo
        coalesce=True     # Se perdeu execuções, só roda 1 vez
    )
    _scheduler.start()
    logger.info("🚀 AutoPost Engine iniciado! Verificando canais a cada 60 segundos.")


def stop_engine():
    """Para o APScheduler graciosamente"""
    global _scheduler
    
    if _scheduler:
        _scheduler.shutdown(wait=False)
        logger.info("🛑 AutoPost Engine parado.")
    
    # Desconecta clientes
    async def _disconnect_all():
        for uid, client in _userbot_clients.items():
            try:
                await client.disconnect()
            except Exception:
                pass
        for token, client in _bot_clients.items():
            try:
                await client.disconnect()
            except Exception:
                pass
    
    try:
        asyncio.run(_disconnect_all())
    except Exception:
        pass
    
    _userbot_clients.clear()
    _bot_clients.clear()


# ==========================================
# STATUS DO ENGINE (para API expor)
# ==========================================
def get_engine_status():
    """Retorna status do engine para exibir no frontend"""
    return {
        "running": _scheduler is not None and _scheduler.running if _scheduler else False,
        "userbots_connected": len(_userbot_clients),
        "bots_connected": len(_bot_clients),
        "next_tick": str(_scheduler.get_jobs()[0].next_run_time) if _scheduler and _scheduler.get_jobs() else None
    }