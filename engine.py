"""
Zenyx AutoPost Engine - Motor de Processamento (v2 - Album Support)
====================================================================
Roda em background via APScheduler dentro do FastAPI.
Processa canais ativos: clona, encaminha ou espiona (ponte) mensagens.
Suporta posts individuais E álbuns (grouped_id) preservando o agrupamento.
"""

import asyncio
import logging
import json
from collections import OrderedDict
from datetime import datetime, time as dt_time
from pytz import timezone
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.types import (
    MessageMediaPhoto, MessageMediaDocument,
    MessageMediaWebPage, MessageMediaContact
)
from telethon.extensions import html  # 👇 NOVO: Importando a extensão HTML para preservar emojis e formatação

from database import SessionLocal, AutopostChannel, AutopostSession, AutopostBot, AutopostQueue, AutopostLog, AutopostDestination

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

# Cache de clientes Telethon ativos
_userbot_clients = {}   # user_id -> TelegramClient
_bot_clients = {}       # bot_token -> TelegramClient


# ==========================================
# HELPERS
# ==========================================
def now_brazil():
    return datetime.now(BRAZIL_TZ)


def is_within_schedule(channel):
    """Verifica se o horário atual está dentro do agendamento do canal"""
    if not channel.schedule_start or not channel.schedule_end:
        return True
    now = now_brazil().time()
    start = channel.schedule_start
    end = channel.schedule_end
    if start <= end:
        return start <= now <= end
    else:
        return now >= start or now <= end


def apply_cta_replacement(text, cta_find, cta_replace):
    """Substitui o CTA/link no texto da mensagem"""
    if not text or not cta_find:
        return text
    return text.replace(cta_find, cta_replace or '')


def apply_custom_caption(original_text, channel):
    """Aplica legenda personalizada se configurada"""
    if not channel.use_custom_caption or not channel.custom_caption:
        return original_text
    
    if channel.caption_mode == 'append':
        # Adiciona a legenda personalizada abaixo da original
        separator = "\n\n" if original_text else ""
        return (original_text or '') + separator + channel.custom_caption
    else:
        # Substitui completamente a legenda original
        return channel.custom_caption


def get_all_dest_channel_ids(channel, db):
    """
    Retorna lista de todos os IDs de destino (principal + extras).
    Cada item é dict: {id, dest_channel_id, dest_channel_name}
    """
    destinations = []
    
    # Destino principal (legado)
    if channel.dest_channel_id:
        destinations.append({
            "dest_channel_id": int(channel.dest_channel_id),
            "dest_channel_name": channel.dest_channel_name or "Principal",
        })
    
    # Destinos adicionais
    extras = db.query(AutopostDestination).filter(
        AutopostDestination.channel_id == channel.id,
        AutopostDestination.is_active == True
    ).all()
    
    for d in extras:
        destinations.append({
            "dest_channel_id": int(d.dest_channel_id),
            "dest_channel_name": d.dest_channel_name or f"Destino #{d.id}",
        })
    
    return destinations


def create_log(db, user_id, action, details=None):
    """Registra ação no log"""
    log = AutopostLog(user_id=user_id, action=action, details=details)
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


def group_messages_by_album(messages):
    """
    Agrupa mensagens por grouped_id (álbuns).
    Mensagens sem grouped_id ficam como grupo individual.
    Retorna lista de listas: [[msg], [msg1, msg2, msg3], [msg], ...]
    Mantém a ordem original (baseada no primeiro msg de cada grupo).
    """
    groups = OrderedDict()  # chave -> [messages]

    for msg in messages:
        gid = getattr(msg, 'grouped_id', None)
        if gid:
            if gid not in groups:
                groups[gid] = []
            groups[gid].append(msg)
        else:
            # Mensagem individual: usa chave única
            groups[f"single_{msg.id}"] = [msg]

    return list(groups.values())


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
        try:
            await client.connect()
            return client
        except Exception:
            del _userbot_clients[user_id]

    try:
        session_str = session_record.session_data.decode('utf-8') if isinstance(session_record.session_data, bytes) else session_record.session_data
        client = TelegramClient(
            StringSession(session_str),
            int(session_record.api_id),
            session_record.api_hash
        )
        await client.connect()

        if not await client.is_user_authorized():
            logger.warning(f"Userbot de {user_id} não autorizado. Sessão pode ter expirado.")
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
        client = TelegramClient(
            StringSession(),
            api_id=6,
            api_hash="eb06d4abfb49dc3eeb1aeb98ae0f581e"
        )
        await client.start(bot_token=bot_token)
        _bot_clients[bot_token] = client
        logger.info(f"Bot conectado: {bot_token[:15]}...")
        return client
    except Exception as e:
        logger.error(f"Erro ao conectar bot {bot_token[:15]}...: {e}")
        return None


def _get_bot_record(channel, db):
    """Helper: retorna bot_record se houver bot vinculado"""
    if channel.bot_id:
        return db.query(AutopostBot).filter(AutopostBot.id == channel.bot_id).first()
    return None


# ==========================================
# PROCESSADORES POR MODO (COM SUPORTE A ÁLBUM)
# ==========================================

async def process_clone(userbot, channel, msg_group, db):
    """
    MODO CLONE: Copia texto/mídia e reposta como novo.
    Suporta álbuns, múltiplos destinos, e legenda personalizada.
    """
    dest_client = userbot
    bot_record = _get_bot_record(channel, db)

    if bot_record:
        bot_client = await get_bot_client(bot_record.bot_token)
        if bot_client:
            dest_client = bot_client
        else:
            logger.warning(f"Bot {bot_record.bot_name} offline. Usando userbot como fallback.")

    is_album = len(msg_group) > 1
    first_msg = msg_group[0]
    msg_ids = [m.id for m in msg_group]
    all_destinations = get_all_dest_channel_ids(channel, db)
    
    if not all_destinations:
        logger.warning(f"CLONE | Canal {channel.id} sem destinos configurados.")
        return None

    try:
        # Prepara conteúdo uma vez
        if is_album:
            media_files = []
            album_caption = None
            for msg in msg_group:
                if msg.media:
                    media_files.append(msg.media)
                
                # 👇 ATUALIZAÇÃO: Extraindo texto como HTML para manter emojis Premium e formatações
                raw_text = msg.message or ''
                if raw_text and getattr(msg, 'entities', None):
                    msg_text = html.unparse(raw_text, msg.entities)
                else:
                    msg_text = raw_text

                if msg_text and album_caption is None:
                    album_caption = apply_cta_replacement(msg_text, channel.cta_find, channel.cta_replace)
            
            # Aplica legenda personalizada
            album_caption = apply_custom_caption(album_caption, channel)
            
            if not media_files:
                return None
            
            # Envia para CADA destino
            for dest in all_destinations:
                try:
                    await dest_client.send_file(
                        dest["dest_channel_id"],
                        file=media_files,
                        caption=album_caption if album_caption else None,
                        parse_mode='html' # 👇 ATUALIZAÇÃO: Avisando o Telegram que tem formatação
                    )
                    logger.info(f"CLONE ÁLBUM | Canal {channel.id} | {len(media_files)} mídias → {dest['dest_channel_name']} ({dest['dest_channel_id']})")
                except Exception as e:
                    logger.error(f"CLONE ÁLBUM ERRO | destino {dest['dest_channel_id']}: {e}")
                await asyncio.sleep(0.5)  # Delay entre destinos
            
            queue_entry = create_queue_entry(
                db, channel.id, first_msg.id, f"album_{len(media_files)}",
                {"text_preview": (album_caption or "")[:200], "mode": "clone", "album_size": len(media_files), "msg_ids": msg_ids, "destinations": len(all_destinations)},
                status="sent"
            )
            queue_entry.sent_at = now_brazil()
            db.commit()
            return queue_entry

        else:
            msg = first_msg
            # 👇 ATUALIZAÇÃO: Extraindo texto como HTML para manter emojis Premium
            raw_text = msg.message or ''
            if raw_text and getattr(msg, 'entities', None):
                text = html.unparse(raw_text, msg.entities)
            else:
                text = raw_text

            text = apply_cta_replacement(text, channel.cta_find, channel.cta_replace)
            text = apply_custom_caption(text, channel)
            media_type = "text"

            if msg.media:
                if isinstance(msg.media, MessageMediaPhoto):
                    media_type = "photo"
                elif isinstance(msg.media, MessageMediaDocument):
                    media_type = "document"
                else:
                    media_type = "other_media"

            for dest in all_destinations:
                try:
                    if msg.media:
                        await dest_client.send_message(dest["dest_channel_id"], message=text if text else None, file=msg.media, parse_mode='html') # 👇 ATUALIZAÇÃO: parse_mode='html'
                    elif text:
                        await dest_client.send_message(dest["dest_channel_id"], message=text, parse_mode='html') # 👇 ATUALIZAÇÃO: parse_mode='html'
                    else:
                        continue
                    logger.info(f"CLONE | Canal {channel.id} | msg {msg.id} → {dest['dest_channel_name']} ({dest['dest_channel_id']})")
                except Exception as e:
                    logger.error(f"CLONE ERRO | destino {dest['dest_channel_id']}: {e}")
                await asyncio.sleep(0.5)

            queue_entry = create_queue_entry(
                db, channel.id, msg.id, media_type,
                {"text_preview": text[:200] if text else "", "mode": "clone", "destinations": len(all_destinations)},
                status="sent"
            )
            queue_entry.sent_at = now_brazil()
            db.commit()
            return queue_entry

    except Exception as e:
        error_msg = str(e)
        create_queue_entry(db, channel.id, first_msg.id, "error", {"error": error_msg, "mode": "clone", "msg_ids": msg_ids}, status="error")
        logger.error(f"CLONE ERRO | Canal {channel.id} | msgs {msg_ids}: {error_msg}")
        return None


async def process_forward(userbot, channel, msg_group, db):
    """
    MODO FORWARD: Encaminha nativamente para múltiplos destinos.
    forward_messages com lista de IDs preserva álbuns.
    """
    dest_client = userbot
    bot_record = _get_bot_record(channel, db)

    if bot_record:
        bot_client = await get_bot_client(bot_record.bot_token)
        if bot_client:
            dest_client = bot_client

    is_album = len(msg_group) > 1
    first_msg = msg_group[0]
    msg_ids = [m.id for m in msg_group]
    all_destinations = get_all_dest_channel_ids(channel, db)

    if not all_destinations:
        logger.warning(f"FORWARD | Canal {channel.id} sem destinos configurados.")
        return None

    try:
        for dest in all_destinations:
            try:
                await dest_client.forward_messages(
                    dest["dest_channel_id"],
                    msg_ids,
                    int(channel.origin_channel_id)
                )
                if is_album:
                    logger.info(f"FORWARD ÁLBUM | Canal {channel.id} | {len(msg_ids)} msgs → {dest['dest_channel_name']} ({dest['dest_channel_id']})")
                else:
                    logger.info(f"FORWARD | Canal {channel.id} | msg {first_msg.id} → {dest['dest_channel_name']} ({dest['dest_channel_id']})")
            except Exception as e:
                logger.error(f"FORWARD ERRO | destino {dest['dest_channel_id']}: {e}")
            await asyncio.sleep(0.5)

        media_type = f"forward_album_{len(msg_ids)}" if is_album else "forward"
        text_preview = first_msg.text or first_msg.message or ""

        queue_entry = create_queue_entry(
            db, channel.id, first_msg.id, media_type,
            {"text_preview": text_preview[:200], "mode": "forward", "album_size": len(msg_ids) if is_album else 1, "msg_ids": msg_ids, "destinations": len(all_destinations)},
            status="sent"
        )
        queue_entry.sent_at = now_brazil()
        db.commit()
        return queue_entry

    except Exception as e:
        error_msg = str(e)
        create_queue_entry(db, channel.id, first_msg.id, "error", {"error": error_msg, "mode": "forward", "msg_ids": msg_ids}, status="error")
        logger.error(f"FORWARD ERRO | Canal {channel.id} | msgs {msg_ids}: {error_msg}")
        return None


async def process_spy(userbot, channel, msg_group, db):
    """
    MODO ESPIONAR (PONTE PREMIUM) com múltiplos destinos:
    1. Userbot lê do concorrente
    2. Userbot clona pro Canal Oculto (com legenda personalizada se configurada)
    3. Bot oficial encaminha do Canal Oculto → TODOS os destinos configurados
    """
    if not channel.bot_id:
        logger.warning(f"SPY | Canal {channel.id} sem bot vinculado. Fazendo clone direto.")
        return await process_clone(userbot, channel, msg_group, db)

    bot_record = db.query(AutopostBot).filter(AutopostBot.id == channel.bot_id).first()
    if not bot_record:
        return await process_clone(userbot, channel, msg_group, db)

    bot_client = await get_bot_client(bot_record.bot_token)
    if not bot_client:
        return await process_clone(userbot, channel, msg_group, db)

    is_album = len(msg_group) > 1
    first_msg = msg_group[0]
    msg_ids = [m.id for m in msg_group]
    bridge_channel_id = int(bot_record.origin_channel_id)
    
    # Coleta todos os destinos (principal + extras)
    all_destinations = get_all_dest_channel_ids(channel, db)
    if not all_destinations:
        all_destinations = [{"dest_channel_id": int(bot_record.dest_channel_id), "dest_channel_name": "Bot Destino"}]

    try:
        if is_album:
            media_files = []
            album_caption = None
            for msg in msg_group:
                if msg.media:
                    media_files.append(msg.media)
                
                # 👇 ATUALIZAÇÃO: Extraindo texto como HTML para ponte
                raw_text = msg.message or ''
                if raw_text and getattr(msg, 'entities', None):
                    msg_text = html.unparse(raw_text, msg.entities)
                else:
                    msg_text = raw_text

                if msg_text and album_caption is None:
                    album_caption = apply_cta_replacement(msg_text, channel.cta_find, channel.cta_replace)
            
            album_caption = apply_custom_caption(album_caption, channel)
            if not media_files:
                return None

            # PASSO 1: Userbot envia álbum pro Canal Oculto
            bridge_msgs = await userbot.send_file(bridge_channel_id, file=media_files, caption=album_caption if album_caption else None, parse_mode='html') # 👇 ATUALIZAÇÃO
            await asyncio.sleep(1.5)

            if isinstance(bridge_msgs, list):
                bridge_msg_ids = [m.id for m in bridge_msgs]
            else:
                bridge_msg_ids = [bridge_msgs.id]

            # PASSO 2: Bot encaminha para TODOS os destinos
            for dest in all_destinations:
                try:
                    await bot_client.forward_messages(dest["dest_channel_id"], bridge_msg_ids, bridge_channel_id)
                    logger.info(f"SPY ÁLBUM PONTE | Canal {channel.id} → {dest['dest_channel_name']} ({dest['dest_channel_id']})")
                except Exception as e:
                    logger.error(f"SPY ÁLBUM ERRO | destino {dest['dest_channel_id']}: {e}")
                await asyncio.sleep(0.5)

            media_type = f"spy_album_{len(media_files)}"

        else:
            msg = first_msg
            # 👇 ATUALIZAÇÃO: Extraindo HTML para ponte única
            raw_text = msg.message or ''
            if raw_text and getattr(msg, 'entities', None):
                text = html.unparse(raw_text, msg.entities)
            else:
                text = raw_text

            text = apply_cta_replacement(text, channel.cta_find, channel.cta_replace)
            text = apply_custom_caption(text, channel)

            if msg.media:
                bridge_msg = await userbot.send_message(bridge_channel_id, message=text if text else None, file=msg.media, parse_mode='html') # 👇 ATUALIZAÇÃO
            elif text:
                bridge_msg = await userbot.send_message(bridge_channel_id, message=text, parse_mode='html') # 👇 ATUALIZAÇÃO
            else:
                return None

            await asyncio.sleep(1)
            bridge_msg_ids = [bridge_msg.id]

            for dest in all_destinations:
                try:
                    await bot_client.forward_messages(dest["dest_channel_id"], bridge_msg_ids, bridge_channel_id)
                    logger.info(f"SPY PONTE | Canal {channel.id} | msg {first_msg.id} → {dest['dest_channel_name']} ({dest['dest_channel_id']})")
                except Exception as e:
                    logger.error(f"SPY ERRO | destino {dest['dest_channel_id']}: {e}")
                await asyncio.sleep(0.5)

            media_type = "spy_bridge"
            if msg.media:
                if isinstance(msg.media, MessageMediaPhoto): media_type = "spy_photo"
                elif isinstance(msg.media, MessageMediaDocument): media_type = "spy_document"

        text_preview = ""
        for m in msg_group:
            t = m.text or m.message or ''
            if t:
                text_preview = apply_cta_replacement(t, channel.cta_find, channel.cta_replace)[:200]
                break

        queue_entry = create_queue_entry(
            db, channel.id, first_msg.id, media_type,
            {"text_preview": text_preview, "mode": "spy", "bridge_channel": str(bridge_channel_id), "album_size": len(msg_group), "msg_ids": msg_ids, "bot_name": bot_record.bot_name, "destinations": len(all_destinations)},
            status="sent"
        )
        queue_entry.sent_at = now_brazil()
        db.commit()
        return queue_entry

    except Exception as e:
        error_msg = str(e)
        create_queue_entry(db, channel.id, first_msg.id, "error", {"error": error_msg, "mode": "spy", "bot_name": bot_record.bot_name, "msg_ids": msg_ids}, status="error")
        logger.error(f"SPY ERRO | Canal {channel.id} | msgs {msg_ids}: {error_msg}")
        return None


# ==========================================
# PROCESSADOR PRINCIPAL DE UM CANAL
# ==========================================
async def process_channel(channel, session_record, db):
    """Processa um canal ativo: lê mensagens novas, agrupa álbuns, e despacha"""

    if not is_within_schedule(channel):
        return 0

    userbot = await get_userbot_client(session_record)
    if not userbot:
        create_log(db, channel.user_id, "engine_error", {
            "channel_id": channel.id,
            "error": "Userbot não disponível"
        })
        return 0

    try:
        origin_id = int(channel.origin_channel_id)
        min_id = channel.last_post_id or 0

        # Busca mais mensagens para capturar álbuns completos
        messages = await userbot.get_messages(
            origin_id,
            min_id=min_id,
            limit=20
        )

        if not messages:
            return 0

        # Filtra mensagens realmente novas
        new_messages = [m for m in messages if m.id > min_id and (m.message is not None or m.media is not None)]

        if not new_messages:
            return 0

        # Ordena por ID (mais antigo primeiro)
        new_messages.sort(key=lambda m: m.id)

        # ===== AGRUPAMENTO DE ÁLBUNS =====
        msg_groups = group_messages_by_album(new_messages)

        # Aplica ordem configurada (nos grupos, não msgs individuais)
        if channel.post_order == 'lifo':
            msg_groups.reverse()
        elif channel.post_order == 'random':
            import random
            random.shuffle(msg_groups)

        processed = 0
        max_group_id = min_id

        for group in msg_groups:
            if channel.channel_type == 'forward':
                result = await process_forward(userbot, channel, group, db)
            elif channel.channel_type == 'spy':
                result = await process_spy(userbot, channel, group, db)
            else:
                result = await process_clone(userbot, channel, group, db)

            if result:
                processed += 1
                group_max_id = max(m.id for m in group)
                if group_max_id > max_group_id:
                    max_group_id = group_max_id
                channel.total_forwarded = (channel.total_forwarded or 0) + 1
                db.commit()

            # Delay: 2.5s para álbuns, 2s para posts individuais
            delay = 2.5 if len(group) > 1 else 2
            await asyncio.sleep(delay)

        # Atualiza last_post_id no final
        if max_group_id > (channel.last_post_id or 0):
            channel.last_post_id = max_group_id
            db.commit()

        if processed > 0:
            albums_count = sum(1 for g in msg_groups if len(g) > 1)
            create_log(db, channel.user_id, "posts_sent", {
                "channel_id": channel.id,
                "origin": str(channel.origin_channel_id),
                "dest": str(channel.dest_channel_id),
                "mode": channel.channel_type,
                "count": processed,
                "albums_detected": albums_count
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
    """Executado a cada ciclo pelo APScheduler."""
    db = SessionLocal()

    try:
        active_channels = db.query(AutopostChannel).filter(
            AutopostChannel.is_active == True
        ).all()

        if not active_channels:
            return

        logger.info(f"Engine tick: {len(active_channels)} canais ativos encontrados")

        channels_by_user = {}
        for ch in active_channels:
            if ch.user_id not in channels_by_user:
                channels_by_user[ch.user_id] = []
            channels_by_user[ch.user_id].append(ch)

        total_processed = 0

        for user_id, user_channels in channels_by_user.items():
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
    """Wrapper síncrono para o APScheduler chamar a função async no loop correto"""
    global _main_loop
    
    if _main_loop is None or _main_loop.is_closed():
        logger.warning("Event loop principal não disponível. Pulando tick.")
        return
    
    # Executa no loop do uvicorn (mesmo loop onde Telethon conectou)
    future = asyncio.run_coroutine_threadsafe(engine_tick(), _main_loop)
    try:
        # Aguarda até 25 segundos (antes do próximo tick de 30s)
        future.result(timeout=25)
    except Exception as e:
        logger.error(f"Erro no engine tick: {e}")


# ==========================================
# STARTUP / SHUTDOWN
# ==========================================
_scheduler = None
_main_loop = None  # Referência ao event loop do uvicorn


def start_engine():
    """Inicia o APScheduler com o engine_tick rodando a cada 60 segundos"""
    global _scheduler, _main_loop

    # Captura o event loop do uvicorn/FastAPI
    try:
        _main_loop = asyncio.get_running_loop()
    except RuntimeError:
        _main_loop = asyncio.get_event_loop()
    
    logger.info(f"Event loop capturado: {_main_loop}")

    from apscheduler.schedulers.background import BackgroundScheduler

    _scheduler = BackgroundScheduler(timezone="America/Sao_Paulo")
    _scheduler.add_job(
        run_engine_tick,
        'interval',
        seconds=30,
        id='autopost_engine_tick',
        name='AutoPost Engine Tick',
        replace_existing=True,
        max_instances=1,
        coalesce=True
    )
    _scheduler.start()
    logger.info("🚀 AutoPost Engine v3 (Multi-Dest + Custom Caption) iniciado! Tick a cada 30s.")


def stop_engine():
    """Para o APScheduler graciosamente"""
    global _scheduler, _main_loop

    if _scheduler:
        _scheduler.shutdown(wait=False)
        logger.info("🛑 AutoPost Engine parado.")

    async def _disconnect_all():
        for uid, client in list(_userbot_clients.items()):
            try:
                await client.disconnect()
            except Exception:
                pass
        for token, client in list(_bot_clients.items()):
            try:
                await client.disconnect()
            except Exception:
                pass

    # Tenta desconectar no loop principal
    if _main_loop and not _main_loop.is_closed():
        try:
            asyncio.run_coroutine_threadsafe(_disconnect_all(), _main_loop)
        except Exception:
            pass
    
    _userbot_clients.clear()
    _bot_clients.clear()
    _main_loop = None


# ==========================================
# STATUS DO ENGINE
# ==========================================
def get_engine_status():
    """Retorna status do engine para exibir no frontend"""
    return {
        "running": _scheduler is not None and _scheduler.running if _scheduler else False,
        "userbots_connected": len(_userbot_clients),
        "bots_connected": len(_bot_clients),
        "next_tick": str(_scheduler.get_jobs()[0].next_run_time) if _scheduler and _scheduler.get_jobs() else None
    }