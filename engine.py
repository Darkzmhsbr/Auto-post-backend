"""
Zenyx AutoPost Engine - Motor de Processamento (v2 - Album Support)
====================================================================
Roda em background via APScheduler dentro do FastAPI.
Processa canais ativos: clona, encaminha ou espiona (ponte) mensagens.
Suporta posts individuais E álbuns (grouped_id) preservando o agrupamento.
"""

import os
import asyncio
import logging
import json
import re
import copy
from collections import OrderedDict
from datetime import datetime, time as dt_time
from pytz import timezone
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.types import (
    MessageMediaPhoto, MessageMediaDocument,
    MessageMediaWebPage, MessageMediaContact,
    MessageEntityCustomEmoji
)
from telethon.extensions import html  

# 👇 ESCUDO DE PROTEÇÃO PARA VERSÕES ANTIGAS DO TELETHON 👇
try:
    from telethon.tl.functions.channels import GetForumTopicsRequest, CreateForumTopicRequest
    TOPICS_SUPPORTED = True
except ImportError:
    TOPICS_SUPPORTED = False

from database import SessionLocal, AutopostChannel, AutopostSession, AutopostBot, AutopostQueue, AutopostLog, AutopostDestination, AutopostTopicMap

# ==========================================
# CONFIGURAÇÃO
# ==========================================
BRAZIL_TZ = timezone('America/Sao_Paulo')
MAX_MEDIA_SIZE_BYTES = 50 * 1024 * 1024  # 50MB — Limite máximo de mídia para clonagem (Clonex)
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


def apply_cta_replacement(text, cta_find, cta_replace, cta_mode="exact"):
    """
    Substitui o CTA/link no texto da mensagem.
    """
    if not text or not cta_replace:
        return text
    
    if cta_mode == "smart":
        def replace_href(match):
            return f'<a href="{cta_replace}">'
        text = re.sub(r'<a\s+href="https?://t\.me/[^"]*">', replace_href, text)
        text = re.sub(r'<a\s+href="https?://telegram\.me/[^"]*">', replace_href, text)
        
        text = re.sub(r'(?<!href=")(?<!href=\')https?://t\.me/\S+', cta_replace, text)
        text = re.sub(r'(?<!href=")(?<!href=\')https?://telegram\.me/\S+', cta_replace, text)
        text = re.sub(r'(?<!["/])(?<!\w)t\.me/\S+', cta_replace, text)
        
        return text
    else:
        if not cta_find:
            return text
        return text.replace(cta_find, cta_replace)


def apply_custom_caption(original_text, channel):
    """Aplica legenda personalizada se configurada"""
    if not channel.use_custom_caption or not channel.custom_caption:
        return original_text
    
    if channel.caption_mode == 'append':
        separator = "\n\n" if original_text else ""
        return (original_text or '') + separator + channel.custom_caption
    else:
        return channel.custom_caption


def has_custom_emoji(entities):
    """Verifica se a lista de entities contém emojis premium (custom emoji)."""
    if not entities:
        return False
    return any(isinstance(e, MessageEntityCustomEmoji) for e in entities)


def extract_text_and_entities(msg, channel):
    """
    Extrai texto e entities de uma mensagem de forma inteligente.
    
    Se a mensagem contém custom emojis (Premium), preserva as entities originais 
    e aplica CTA/caption no texto puro E diretamente nas entities (ajustando as posições).
    
    Se NÃO contém custom emojis, usa html.unparse normalmente (comportamento legado).
    
    Retorna: (text, entities_list_or_None, use_html_parse_mode)
      - Se entities_list_or_None é None → usar parse_mode='html'
      - Se entities_list_or_None é lista → usar formatting_entities=... (sem parse_mode)
    """
    raw_text = msg.message or ''
    entities = getattr(msg, 'entities', None) or []
    
    if not raw_text:
        return '', None, True
    
    if has_custom_emoji(entities):
        # 🌟 MODO PREMIUM EMOJI: Preservar entities originais e editar links diretamente!
        ents = [copy.copy(e) for e in entities] # Copia rasa segura para modificar offset/length/url
        
        cta_mode = getattr(channel, 'cta_mode', 'exact')
        cta_find = channel.cta_find
        cta_replace = channel.cta_replace
        
        if cta_replace:
            # 1. Atualizar links EMBUTIDOS (Ex: Palavra com link camuflado nas Entities)
            for e in ents:
                if hasattr(e, 'url') and e.url:
                    if cta_mode == 'smart':
                        if "t.me/" in e.url or "telegram.me/" in e.url:
                            e.url = cta_replace
                    elif cta_mode == 'exact' and cta_find and cta_find in e.url:
                        e.url = e.url.replace(cta_find, cta_replace)
            
            # 2. Atualizar links ESCRITOS NO TEXTO e recalcular posições das entities (Offsets)
            new_text = raw_text
            if cta_mode == 'exact' and cta_find and cta_find in new_text:
                diff = len(cta_replace) - len(cta_find)
                starts = [m.start() for m in re.finditer(re.escape(cta_find), new_text)]
                new_text = new_text.replace(cta_find, cta_replace)
                
                if diff != 0:
                    for e in ents:
                        rb = sum(1 for s in starts if s < e.offset)
                        ri = sum(1 for s in starts if e.offset <= s < e.offset + e.length)
                        e.offset += (rb * diff)
                        e.length += (ri * diff)
                        
            elif cta_mode == 'smart':
                pattern = re.compile(r'https?://(?:t\.me|telegram\.me)/\S+|(?<!["/])(?<!\w)t\.me/\S+')
                matches = list(pattern.finditer(new_text))
                if matches:
                    shifts = []
                    last_end = 0
                    temp_text = ""
                    for m in matches:
                        start, end = m.span()
                        diff = len(cta_replace) - (end - start)
                        temp_text += new_text[last_end:start] + cta_replace
                        shifts.append((start, diff))
                        last_end = end
                    temp_text += new_text[last_end:]
                    new_text = temp_text
                    
                    for e in ents:
                        sb = sum(d for pos, d in shifts if pos < e.offset)
                        si = sum(d for pos, d in shifts if e.offset <= pos < e.offset + e.length)
                        e.offset += sb
                        e.length += si
                        
            raw_text = new_text
        
        # 3. Aplicar a Legenda Personalizada (Caption)
        if channel.use_custom_caption and channel.custom_caption:
            if channel.caption_mode == 'append':
                separator = "\n\n" if raw_text else ""
                raw_text = raw_text + separator + channel.custom_caption
                return raw_text, ents, False
            else:
                # Se for Replace total, perdemos a necessidade dos emojis antigos (pois o texto sumiu)
                # Convertendo pro HTML pra pegar a formatação básica
                return channel.custom_caption, None, True
                
        return raw_text, ents, False
        
    else:
        # Modo padrão: HTML (sem custom emoji)
        if entities:
            text = html.unparse(raw_text, entities)
        else:
            text = raw_text
        text = apply_cta_replacement(text, channel.cta_find, channel.cta_replace, getattr(channel, 'cta_mode', 'exact'))
        text = apply_custom_caption(text, channel)
        return text, None, True


def get_all_dest_channel_ids(channel, db):
    """Retorna lista de todos os IDs de destino (principal + extras)."""
    destinations = []
    
    if channel.dest_channel_id:
        destinations.append({
            "dest_channel_id": int(channel.dest_channel_id),
            "dest_channel_name": channel.dest_channel_name or "Principal",
        })
    
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


def check_media_size(msg):
    """
    Verifica se a mídia de uma mensagem excede o limite de 50MB (Clonex).
    Retorna True se a mídia for ACEITÁVEL (abaixo do limite ou sem mídia).
    Retorna False se exceder o limite.
    """
    if not msg.media:
        return True  # Sem mídia = OK
    
    # MessageMediaDocument tem o atributo document.size
    if isinstance(msg.media, MessageMediaDocument):
        doc = getattr(msg.media, 'document', None)
        if doc:
            file_size = getattr(doc, 'size', 0) or 0
            if file_size > MAX_MEDIA_SIZE_BYTES:
                size_mb = file_size / (1024 * 1024)
                logger.warning(f"⛔ MÍDIA EXCEDE 50MB ({size_mb:.1f}MB) | msg_id={msg.id} — Pulando.")
                return False
    
    # MessageMediaPhoto geralmente é pequena, mas checamos por segurança
    if isinstance(msg.media, MessageMediaPhoto):
        photo = getattr(msg.media, 'photo', None)
        if photo:
            # Fotos usam photo.sizes — pegamos o maior tamanho disponível
            sizes = getattr(photo, 'sizes', [])
            for s in sizes:
                file_size = getattr(s, 'size', 0) or 0
                if file_size > MAX_MEDIA_SIZE_BYTES:
                    size_mb = file_size / (1024 * 1024)
                    logger.warning(f"⛔ FOTO EXCEDE 50MB ({size_mb:.1f}MB) | msg_id={msg.id} — Pulando.")
                    return False
    
    return True  # Dentro do limite


def group_messages_by_album(messages):
    """Agrupa mensagens por grouped_id (álbuns)."""
    groups = OrderedDict() 

    for msg in messages:
        gid = getattr(msg, 'grouped_id', None)
        if gid:
            if gid not in groups:
                groups[gid] = []
            groups[gid].append(msg)
        else:
            groups[f"single_{msg.id}"] = [msg]

    return list(groups.values())


# ==========================================
# ESPELHAMENTO INTELIGENTE DE TÓPICOS
# ==========================================

async def get_or_create_topic(client, chat_id, topic_name):
    """Busca um tópico pelo nome no destino. Se não existir, cria automaticamente."""
    if not TOPICS_SUPPORTED:
        logger.warning(f"Espelhamento ignorado: Telethon antigo. Atualize para >=1.36.0 para criar '{topic_name}'")
        return None

    try:
        topics = await client(GetForumTopicsRequest(
            channel=chat_id,
            q=topic_name,
            offset_date=0,
            offset_id=0,
            offset_topic=0,
            limit=100
        ))
        for t in topics.topics:
            if getattr(t, 'title', '') == topic_name:
                return t.id
    except Exception as e:
        pass

    try:
        result = await client(CreateForumTopicRequest(
            channel=chat_id,
            title=topic_name
        ))
        for update in result.updates:
            if hasattr(update, 'message') and hasattr(update.message, 'id'):
                logger.info(f"✨ Tópico Inteligente criado: '{topic_name}' no chat {chat_id}")
                return update.message.id
    except Exception as e:
        logger.error(f"Erro ao criar tópico '{topic_name}' em {chat_id}: {e}")
    
    return None

async def resolve_dest_topics(userbot, dest_client, channel, first_msg, all_destinations, db):
    """Descobre se a postagem veio de um tópico na origem e resolve o destino."""
    dest_topic_ids = {}
    origin_topic_id = None
    
    if first_msg.reply_to and getattr(first_msg.reply_to, 'forum_topic', False):
        origin_topic_id = getattr(first_msg.reply_to, 'reply_to_top_id', None) or getattr(first_msg.reply_to, 'reply_to_msg_id', None)
    
    if origin_topic_id:
        topic_name = None
        
        for dest in all_destinations:
            dest_id = dest["dest_channel_id"]
            
            mapped = db.query(AutopostTopicMap).filter(
                AutopostTopicMap.channel_id == channel.id,
                AutopostTopicMap.origin_topic_id == origin_topic_id
            ).first()
            
            if mapped and mapped.dest_topic_id:
                dest_topic_ids[dest_id] = int(mapped.dest_topic_id)
                
            elif getattr(channel, 'auto_topic_clone', False):
                if topic_name is None:
                    topic_name = f"Tópico {origin_topic_id}"
                    try:
                        topic_msg = await userbot.get_messages(int(channel.origin_channel_id), ids=origin_topic_id)
                        if topic_msg and hasattr(topic_msg, 'action') and hasattr(topic_msg.action, 'title'):
                            topic_name = topic_msg.action.title
                    except Exception as e:
                        logger.error(f"Erro ao ler nome do tópico na origem: {e}")
                        
                d_topic_id = await get_or_create_topic(dest_client, dest_id, topic_name)
                
                if d_topic_id:
                    dest_topic_ids[dest_id] = d_topic_id
                    if not mapped:
                        try:
                            new_map = AutopostTopicMap(
                                channel_id=channel.id,
                                origin_topic_id=origin_topic_id,
                                origin_topic_name=topic_name,
                                dest_topic_id=d_topic_id,
                                dest_topic_name=topic_name
                            )
                            db.add(new_map)
                            db.commit()
                        except Exception:
                            db.rollback()
                            
    return dest_topic_ids


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
    if channel.bot_id:
        return db.query(AutopostBot).filter(AutopostBot.id == channel.bot_id).first()
    return None


# ==========================================
# PROCESSADORES POR MODO (COM SUPORTE A ÁLBUM E TÓPICOS)
# ==========================================

async def process_clone(userbot, channel, msg_group, db):
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

    dest_topic_ids = await resolve_dest_topics(userbot, dest_client, channel, first_msg, all_destinations, db)
    downloaded_paths = []

    try:
        if is_album:
            album_caption = None
            album_entities = None
            album_use_html = True
            for msg in msg_group:
                if album_caption is None:
                    text, ents, use_html = extract_text_and_entities(msg, channel)
                    if text:
                        album_caption = text
                        album_entities = ents
                        album_use_html = use_html
            
            must_download = (dest_client != userbot)
            
            for dest in all_destinations:
                reply_to_id = dest_topic_ids.get(dest["dest_channel_id"])
                success = False
                
                # Tenta enviar nativamente (instantâneo e preserva o álbum perfeito)
                if not must_download:
                    try:
                        native_media = [m.media for m in msg_group if m.media]
                        if native_media:
                            send_kwargs = {
                                "caption": album_caption if album_caption else None,
                                "reply_to": reply_to_id
                            }
                            if album_entities and not album_use_html:
                                send_kwargs["formatting_entities"] = album_entities
                            else:
                                send_kwargs["parse_mode"] = 'html'
                            await dest_client.send_file(
                                dest["dest_channel_id"],
                                file=native_media,
                                **send_kwargs
                            )
                            logger.info(f"CLONE ÁLBUM NATIVO | Canal {channel.id} → {dest['dest_channel_name']}")
                            success = True
                    except Exception as e:
                        logger.warning(f"Falha no clone nativo (canal protegido?), ativando fallback de download: {e}")
                        must_download = True
                
                # Se falhou ou precisa baixar, executa o download lento
                if must_download and not success:
                    if not downloaded_paths:
                        for m in msg_group:
                            if m.media:
                                try:
                                    p = await userbot.download_media(m.media)
                                    if p: downloaded_paths.append(p)
                                except Exception as dl_err:
                                    logger.error(f"Erro no download: {dl_err}")
                    
                    if downloaded_paths:
                        try:
                            dl_kwargs = {
                                "caption": album_caption if album_caption else None,
                                "reply_to": reply_to_id
                            }
                            if album_entities and not album_use_html:
                                dl_kwargs["formatting_entities"] = album_entities
                            else:
                                dl_kwargs["parse_mode"] = 'html'
                            await dest_client.send_file(
                                dest["dest_channel_id"],
                                file=downloaded_paths,
                                **dl_kwargs
                            )
                            logger.info(f"CLONE ÁLBUM DOWNLOAD | Canal {channel.id} → {dest['dest_channel_name']}")
                        except Exception as e:
                            logger.error(f"CLONE ÁLBUM ERRO | {dest['dest_channel_name']}: {e}")
                
                await asyncio.sleep(0.5)
            
            queue_entry = create_queue_entry(
                db, channel.id, first_msg.id, f"album_{len(msg_group)}",
                {"text_preview": (album_caption or "")[:200], "mode": "clone", "album_size": len(msg_group), "msg_ids": msg_ids, "destinations": len(all_destinations)},
                status="sent"
            )
            queue_entry.sent_at = now_brazil()
            db.commit()
            return queue_entry

        else:
            msg = first_msg
            text, msg_entities, use_html = extract_text_and_entities(msg, channel)
            media_type = "text"
            
            if msg.media:
                if isinstance(msg.media, MessageMediaPhoto): media_type = "photo"
                elif isinstance(msg.media, MessageMediaDocument): media_type = "document"
                else: media_type = "other_media"

            must_download = (dest_client != userbot)

            for dest in all_destinations:
                reply_to_id = dest_topic_ids.get(dest["dest_channel_id"])
                success = False

                # Monta kwargs de formatação (HTML ou entities diretas para custom emoji)
                fmt_kwargs = {}
                if msg_entities and not use_html:
                    fmt_kwargs["formatting_entities"] = msg_entities
                else:
                    fmt_kwargs["parse_mode"] = 'html'

                if not must_download:
                    try:
                        if msg.media:
                            await dest_client.send_message(dest["dest_channel_id"], message=text if text else None, file=msg.media, reply_to=reply_to_id, **fmt_kwargs)
                        elif text:
                            await dest_client.send_message(dest["dest_channel_id"], message=text, reply_to=reply_to_id, **fmt_kwargs)
                        logger.info(f"CLONE NATIVO | Canal {channel.id} → {dest['dest_channel_name']}")
                        success = True
                    except Exception as e:
                        logger.warning(f"Falha nativa, ativando fallback de download: {e}")
                        must_download = True

                if must_download and not success:
                    if msg.media and not downloaded_paths:
                        try:
                            path = await userbot.download_media(msg.media)
                            if path: downloaded_paths.append(path)
                        except Exception as e:
                            logger.error(f"Erro ao baixar mídia única: {e}")

                    try:
                        if downloaded_paths:
                            await dest_client.send_message(dest["dest_channel_id"], message=text if text else None, file=downloaded_paths[0], reply_to=reply_to_id, **fmt_kwargs)
                        elif text:
                            await dest_client.send_message(dest["dest_channel_id"], message=text, reply_to=reply_to_id, **fmt_kwargs)
                        logger.info(f"CLONE DOWNLOAD | Canal {channel.id} → {dest['dest_channel_name']}")
                    except Exception as e:
                        logger.error(f"CLONE ERRO | {dest['dest_channel_name']}: {e}")
                
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

    finally:
        for path in downloaded_paths:
            if os.path.exists(path):
                try:
                    os.remove(path)
                except:
                    pass

async def process_forward(userbot, channel, msg_group, db):
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

    dest_topic_ids = await resolve_dest_topics(userbot, dest_client, channel, first_msg, all_destinations, db)

    try:
        for dest in all_destinations:
            reply_to_id = dest_topic_ids.get(dest["dest_channel_id"])
            try:
                await dest_client.forward_messages(
                    dest["dest_channel_id"],
                    msg_ids,
                    int(channel.origin_channel_id),
                    reply_to=reply_to_id
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
    
    all_destinations = get_all_dest_channel_ids(channel, db)
    if not all_destinations:
        all_destinations = [{"dest_channel_id": int(bot_record.dest_channel_id), "dest_channel_name": "Bot Destino"}]

    dest_topic_ids = await resolve_dest_topics(userbot, bot_client, channel, first_msg, all_destinations, db)
    downloaded_paths = []

    try:
        if is_album:
            album_caption = None
            album_entities = None
            album_use_html = True
            for msg in msg_group:
                if album_caption is None:
                    text, ents, use_html = extract_text_and_entities(msg, channel)
                    if text:
                        album_caption = text
                        album_entities = ents
                        album_use_html = use_html
            
            bridge_msgs = None
            spy_fmt = {}
            if album_entities and not album_use_html:
                spy_fmt["formatting_entities"] = album_entities
            else:
                spy_fmt["parse_mode"] = 'html'
            
            try:
                # Tenta mandar instantaneamente pro canal ponte
                native_media = [m.media for m in msg_group if m.media]
                if native_media:
                    bridge_msgs = await userbot.send_file(bridge_channel_id, file=native_media, caption=album_caption if album_caption else None, **spy_fmt)
            except Exception as e:
                logger.warning(f"SPY ÁLBUM nativo falhou, baixando: {e}")
                if not downloaded_paths:
                    for m in msg_group:
                        if m.media:
                            try:
                                p = await userbot.download_media(m.media)
                                if p: downloaded_paths.append(p)
                            except: pass
                if downloaded_paths:
                    bridge_msgs = await userbot.send_file(bridge_channel_id, file=downloaded_paths, caption=album_caption if album_caption else None, **spy_fmt)

            if not bridge_msgs:
                return None

            await asyncio.sleep(1.5)

            if isinstance(bridge_msgs, list):
                bridge_msg_ids = [m.id for m in bridge_msgs]
            else:
                bridge_msg_ids = [bridge_msgs.id]

            for dest in all_destinations:
                reply_to_id = dest_topic_ids.get(dest["dest_channel_id"])
                try:
                    await bot_client.forward_messages(dest["dest_channel_id"], bridge_msg_ids, bridge_channel_id, reply_to=reply_to_id)
                    logger.info(f"SPY ÁLBUM PONTE | Canal {channel.id} → {dest['dest_channel_name']} ({dest['dest_channel_id']})")
                except Exception as e:
                    logger.error(f"SPY ÁLBUM ERRO | destino {dest['dest_channel_id']}: {e}")
                await asyncio.sleep(0.5)

            media_type = f"spy_album_{len(msg_group)}"

        else:
            msg = first_msg
            text, msg_entities, use_html = extract_text_and_entities(msg, channel)
            bridge_msg = None

            spy_fmt = {}
            if msg_entities and not use_html:
                spy_fmt["formatting_entities"] = msg_entities
            else:
                spy_fmt["parse_mode"] = 'html'

            try:
                if msg.media:
                    bridge_msg = await userbot.send_message(bridge_channel_id, message=text if text else None, file=msg.media, **spy_fmt)
                elif text:
                    bridge_msg = await userbot.send_message(bridge_channel_id, message=text, **spy_fmt)
            except Exception as e:
                logger.warning(f"SPY nativo falhou, baixando: {e}")
                if msg.media:
                    try:
                        p = await userbot.download_media(msg.media)
                        if p: downloaded_paths.append(p)
                    except: pass
                if downloaded_paths:
                    bridge_msg = await userbot.send_message(bridge_channel_id, message=text if text else None, file=downloaded_paths[0], **spy_fmt)

            if not bridge_msg:
                return None

            await asyncio.sleep(1)
            bridge_msg_ids = [bridge_msg.id]

            for dest in all_destinations:
                reply_to_id = dest_topic_ids.get(dest["dest_channel_id"])
                try:
                    await bot_client.forward_messages(dest["dest_channel_id"], bridge_msg_ids, bridge_channel_id, reply_to=reply_to_id)
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
                text_preview = apply_cta_replacement(t, channel.cta_find, channel.cta_replace, getattr(channel, 'cta_mode', 'exact'))[:200]
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

    finally:
        for path in downloaded_paths:
            if os.path.exists(path):
                try:
                    os.remove(path)
                except:
                    pass

# ==========================================
# PROCESSADOR PRINCIPAL DE UM CANAL
# ==========================================
async def process_channel(channel, session_record, db):
    if not is_within_schedule(channel):
        return 0

    last_sent = db.query(AutopostQueue).filter(
        AutopostQueue.channel_pair_id == channel.id,
        AutopostQueue.status == "sent"
    ).order_by(AutopostQueue.id.desc()).first()

    if last_sent and last_sent.sent_at:
        last_dt = last_sent.sent_at
        
        if last_dt.tzinfo is None:
            elapsed_local = (now_brazil().replace(tzinfo=None) - last_dt).total_seconds()
            elapsed_utc = (datetime.utcnow() - last_dt).total_seconds()
            
            if elapsed_local >= 0 and elapsed_utc >= 0:
                elapsed = min(elapsed_local, elapsed_utc)
            elif elapsed_utc >= 0:
                elapsed = elapsed_utc
            elif elapsed_local >= 0:
                elapsed = elapsed_local
            else:
                elapsed = abs(elapsed_local)
        else:
            elapsed = (now_brazil() - last_dt).total_seconds()

        interval_seconds = (channel.interval_minutes or 5) * 60
        if elapsed < interval_seconds:
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

        messages = await userbot.get_messages(
            origin_id,
            min_id=min_id,
            limit=20
        )

        if not messages:
            return 0

        new_messages = [m for m in messages if m.id > min_id and (m.message is not None or m.media is not None)]

        if not new_messages:
            return 0

        # 👇 CLONEX: Filtra mídias que excedem 50MB
        filtered_messages = []
        for m in new_messages:
            if check_media_size(m):
                filtered_messages.append(m)
            else:
                # Avança o last_post_id para não tentar de novo
                if m.id > (channel.last_post_id or 0):
                    channel.last_post_id = m.id
                    db.commit()
                create_log(db, channel.user_id, "media_skipped_size", {
                    "channel_id": channel.id,
                    "msg_id": m.id,
                    "reason": "Mídia excede limite de 50MB"
                })
        
        new_messages = filtered_messages

        if not new_messages:
            return 0

        new_messages.sort(key=lambda m: m.id)
        msg_groups = group_messages_by_album(new_messages)

        if channel.post_order == 'lifo':
            msg_groups.reverse()
        elif channel.post_order == 'random':
            import random
            random.shuffle(msg_groups)

        if not msg_groups:
            return 0

        group = msg_groups[0]

        if channel.channel_type == 'forward':
            result = await process_forward(userbot, channel, group, db)
        elif channel.channel_type == 'spy':
            result = await process_spy(userbot, channel, group, db)
        else:
            result = await process_clone(userbot, channel, group, db)

        group_max_id = max(m.id for m in group)
        if group_max_id > (channel.last_post_id or 0):
            channel.last_post_id = group_max_id
            db.commit()

        if result:
            channel.total_forwarded = (channel.total_forwarded or 0) + 1
            db.commit()

            is_album = len(group) > 1
            create_log(db, channel.user_id, "posts_sent", {
                "channel_id": channel.id,
                "origin": str(channel.origin_channel_id),
                "dest": str(channel.dest_channel_id),
                "mode": channel.channel_type,
                "count": 1,
                "is_album": is_album,
                "remaining": len(msg_groups) - 1
            })
            return 1
            
        return 0

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
    global _main_loop
    
    if _main_loop is None or _main_loop.is_closed():
        logger.warning("Event loop principal não disponível. Pulando tick.")
        return
    
    future = asyncio.run_coroutine_threadsafe(engine_tick(), _main_loop)
    try:
        # 👇 REMOVIDO O TIMEOUT! AGORA ELE ESPERA O DOWNLOAD TERMINAR PACIENTEMENTE
        future.result() 
    except Exception as e:
        logger.error(f"Erro no engine tick: {e}")


# ==========================================
# STARTUP / SHUTDOWN
# ==========================================
_scheduler = None
_main_loop = None


def start_engine():
    global _scheduler, _main_loop

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
    logger.info("🚀 AutoPost Engine v3 iniciado! Tick a cada 30s.")


def stop_engine():
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

    if _main_loop and not _main_loop.is_closed():
        try:
            asyncio.run_coroutine_threadsafe(_disconnect_all(), _main_loop)
        except Exception:
            pass
    
    _userbot_clients.clear()
    _bot_clients.clear()
    _main_loop = None


def get_engine_status():
    return {
        "running": _scheduler is not None and _scheduler.running if _scheduler else False,
        "userbots_connected": len(_userbot_clients),
        "bots_connected": len(_bot_clients),
        "next_tick": str(_scheduler.get_jobs()[0].next_run_time) if _scheduler and _scheduler.get_jobs() else None
    }