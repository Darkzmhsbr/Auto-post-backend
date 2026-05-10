import os
import httpx
import uuid
import json
import asyncio
import shutil
import tempfile
from fastapi import FastAPI, Depends, HTTPException, status, UploadFile, File, Form, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.orm import Session
from pydantic import BaseModel
from typing import List, Optional
from jose import jwt, JWTError

# Importações do Telethon (Automação do Telegram)
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import SessionPasswordNeededError

from database import init_db, SessionLocal, AutopostChannel, AutopostSession, AutopostBot, AutopostQueue, AutopostLog, AutopostDestination, AutopostAdmin, AutopostTopicMap, FerramentsJob, engine, Base
from engine import start_engine, stop_engine, get_engine_status

# =============================================================
# PATCH FFMPEG: static-ffmpeg garante o binario no Railway
# =============================================================
try:
    import static_ffmpeg
    static_ffmpeg.add_paths()
    import logging as _sfl
    _sfl.getLogger("autopost").info("static-ffmpeg registrado no PATH")
except Exception as _sfe:
    import logging as _sfl
    _sfl.getLogger("autopost").warning("static-ffmpeg indisponivel: " + str(_sfe))

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
# CONFIGURAÇÃO — FERRAMENTAS DE CRIATIVOS
# ==========================================
ZENYX_API_URL = os.getenv("ZENYX_API_URL", "https://api.zenyxvips.com")

# Diretório temporário para arquivos de upload/output
FERRAMENTAS_TMP_DIR = os.getenv("FERRAMENTAS_TMP_DIR", "/tmp/ferramentas")
os.makedirs(FERRAMENTAS_TMP_DIR, exist_ok=True)

# Cache de status Prime por user_id → (timestamp, is_unlocked)
# Evita chamar a Zenyx a cada request — TTL de 5 minutos
_prime_cache: dict = {}
PRIME_CACHE_TTL = 300  # segundos

# Extensões aceitas por tipo
EXTENSOES_IMAGEM = {".jpg", ".jpeg", ".png", ".webp", ".bmp", ".tiff"}
EXTENSOES_VIDEO  = {".mp4", ".mov", ".avi", ".mkv", ".webm", ".m4v"}
EXTENSOES_AUDIO  = {".mp3", ".wav", ".ogg", ".m4a", ".flac", ".aac"}
TAMANHO_MAX_UPLOAD = 500 * 1024 * 1024  # 500MB

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

def require_superadmin(user_id: str = Depends(get_current_user), db: Session = Depends(get_db)):
    """Verifica se o usuário é super admin"""
    admin = db.query(AutopostAdmin).filter(
        AutopostAdmin.user_id == user_id,
        AutopostAdmin.role == "superadmin"
    ).first()
    if not admin:
        raise HTTPException(status_code=403, detail="Acesso negado: privilégios de super admin necessários.")
    return user_id

# ==========================================
# HELPERS — FERRAMENTAS DE CRIATIVOS
# ==========================================

async def verificar_prime_clonador(token: str) -> bool:
    """
    Consulta a Zenyx API para verificar se o usuário tem o recurso
    'clonador_previas' desbloqueado. Cache local de 5 minutos por user.
    """
    import time
    now = time.time()

    # Tenta extrair user_id do token para usar como chave de cache
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM], options={"verify_aud": False})
        uid = str(payload.get("sub") or payload.get("id") or payload.get("user_id") or "")
    except Exception:
        uid = token[:32]  # fallback: usa prefixo do token

    # Verifica cache
    if uid in _prime_cache:
        ts, unlocked = _prime_cache[uid]
        if now - ts < PRIME_CACHE_TTL:
            return unlocked

    # Chama a Zenyx API
    try:
        async with httpx.AsyncClient(timeout=8.0) as client:
            resp = await client.get(
                f"{ZENYX_API_URL}/api/admin/recursos-prime",
                headers={"Authorization": f"Bearer {token}"}
            )
        if resp.status_code == 200:
            data = resp.json()
            recursos = data.get("recursos", [])
            for rec in recursos:
                if rec.get("id") == "clonador_previas" and rec.get("status") == "desbloqueado":
                    _prime_cache[uid] = (now, True)
                    return True
            _prime_cache[uid] = (now, False)
            return False
    except Exception:
        # Em caso de falha na consulta, libera se já estava em cache (mesmo expirado)
        if uid in _prime_cache:
            return _prime_cache[uid][1]
        return False

    _prime_cache[uid] = (now, False)
    return False


def get_extensao(filename: str) -> str:
    return os.path.splitext(filename.lower())[1]


def salvar_upload(file_bytes: bytes, filename: str) -> str:
    """Salva o arquivo recebido em FERRAMENTAS_TMP_DIR com nome único."""
    ext = get_extensao(filename)
    nome_unico = f"{uuid.uuid4().hex}{ext}"
    caminho = os.path.join(FERRAMENTAS_TMP_DIR, nome_unico)
    with open(caminho, "wb") as f:
        f.write(file_bytes)
    return nome_unico


def caminho_completo(filename: str) -> str:
    return os.path.join(FERRAMENTAS_TMP_DIR, filename)


async def _processar_imagem_sync(job: FerramentsJob, db: Session):
    """
    Processa jobs de imagem de forma síncrona em background thread.
    Atualiza o job no banco ao final (done ou error).
    """
    import time
    from PIL import Image
    import piexif

    try:
        job.status = "processing"
        db.commit()

        input_path  = caminho_completo(job.input_filename)
        ext         = get_extensao(job.input_filename)
        output_nome = f"{uuid.uuid4().hex}{ext}"
        output_path = caminho_completo(output_nome)
        params      = json.loads(job.parametros or "{}")

        if job.tipo == "limpar_metadados":
            # Remove EXIF/metadados de imagens preservando qualidade
            img = Image.open(input_path)
            # Cria nova imagem sem metadados
            dados = list(img.getdata())
            img_limpa = Image.new(img.mode, img.size)
            img_limpa.putdata(dados)
            # Salva sem info de metadata
            save_kwargs = {}
            if ext in (".jpg", ".jpeg"):
                save_kwargs = {"quality": 95, "optimize": True, "exif": b""}
            img_limpa.save(output_path, **save_kwargs)

        elif job.tipo == "conversor_proporcao":
            # Converte entre 9:16 (Stories/Reels) e 3:4 (Feed)
            proporcao_alvo = params.get("proporcao", "9:16")
            img = Image.open(input_path)
            w, h = img.size
            if proporcao_alvo == "9:16":
                alvo_w = min(w, int(h * 9 / 16))
                alvo_h = min(h, int(w * 16 / 9))
            else:  # 3:4
                alvo_w = min(w, int(h * 3 / 4))
                alvo_h = min(h, int(w * 4 / 3))
            # Recorta centralizado
            left = (w - alvo_w) // 2
            top  = (h - alvo_h) // 2
            img_crop = img.crop((left, top, left + alvo_w, top + alvo_h))
            img_crop.save(output_path, quality=95, optimize=True)

        elif job.tipo == "cloaker_criativo":
            # Hash único: altera 1 pixel invisível + limpa metadados + adiciona ruído mínimo
            import random
            img = Image.open(input_path).convert("RGBA")
            pixels = img.load()
            # Altera pixel no canto inferior direito — imperceptível
            px, py = img.size[0] - 1, img.size[1] - 1
            r, g, b, a = pixels[px, py]
            pixels[px, py] = (
                max(0, min(255, r + random.randint(-2, 2))),
                max(0, min(255, g + random.randint(-2, 2))),
                max(0, min(255, b + random.randint(-2, 2))),
                a
            )
            # Salva como RGB sem metadados
            img.convert("RGB").save(output_path, quality=95, optimize=True, exif=b"")

        elif job.tipo == "marca_dagua":
            # Adiciona marca d'água de texto ou imagem
            from PIL import ImageDraw, ImageFont
            texto_wm  = params.get("texto", "© Criativo")
            posicao   = params.get("posicao", "bottom_right")  # top_left, top_right, bottom_left, bottom_right, center
            opacidade = int(params.get("opacidade", 70))

            img = Image.open(input_path).convert("RGBA")
            overlay = Image.new("RGBA", img.size, (0, 0, 0, 0))
            draw = ImageDraw.Draw(overlay)

            try:
                font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 36)
            except Exception:
                font = ImageFont.load_default()

            bbox = draw.textbbox((0, 0), texto_wm, font=font)
            tw, th = bbox[2] - bbox[0], bbox[3] - bbox[1]
            iw, ih = img.size
            margem = 20

            pos_map = {
                "top_left":     (margem, margem),
                "top_right":    (iw - tw - margem, margem),
                "bottom_left":  (margem, ih - th - margem),
                "bottom_right": (iw - tw - margem, ih - th - margem),
                "center":       ((iw - tw) // 2, (ih - th) // 2),
            }
            pos_xy = pos_map.get(posicao, pos_map["bottom_right"])

            # Sombra sutil para legibilidade
            draw.text((pos_xy[0]+2, pos_xy[1]+2), texto_wm, font=font, fill=(0, 0, 0, opacidade))
            draw.text(pos_xy, texto_wm, font=font, fill=(255, 255, 255, opacidade))

            resultado = Image.alpha_composite(img, overlay).convert("RGB")
            resultado.save(output_path, quality=95)

        elif job.tipo == "gerador_preview":
            # Gera versão "censurada" — aplica blur no centro da imagem
            from PIL import ImageFilter
            img = Image.open(input_path)
            iw, ih = img.size
            # Área de blur: 60% central
            margem_x = int(iw * 0.20)
            margem_y = int(ih * 0.20)
            area_blur = img.crop((margem_x, margem_y, iw - margem_x, ih - margem_y))
            area_borrada = area_blur.filter(ImageFilter.GaussianBlur(radius=15))
            img.paste(area_borrada, (margem_x, margem_y))
            img.save(output_path, quality=95)

        else:
            raise ValueError(f"Tipo de job de imagem desconhecido: {job.tipo}")

        job.output_filename = output_nome
        job.status = "done"
        db.commit()

    except Exception as e:
        job.status = "error"
        job.error_msg = str(e)
        db.commit()


def _processar_video_sync(job_id: int):
    import subprocess, random, logging
    _log = logging.getLogger("autopost")
    db = SessionLocal()
    job = None
    try:
        job = db.query(FerramentsJob).filter(FerramentsJob.id == job_id).first()
        if not job or job.status != "pending":
            return
        job.status = "processing"
        db.commit()
        inp  = caminho_completo(job.input_filename)
        ext  = get_extensao(job.input_filename)
        onom = uuid.uuid4().hex + ext
        out  = caminho_completo(onom)
        p    = json.loads(job.parametros or "{}")
        cmd  = None
        if job.tipo == "conversor_proporcao":
            prop = p.get("proporcao", "9:16")
            filt = "crop=ih*9/16:ih:(iw-ih*9/16)/2:0,scale=1080:1920" if prop == "9:16" else "crop=ih*3/4:ih:(iw-ih*3/4)/2:0,scale=1080:1440"
            cmd = ["ffmpeg","-y","-i",inp,"-vf",filt,"-c:v","libx264","-crf","23","-preset","fast","-c:a","aac","-movflags","+faststart",out]
        elif job.tipo == "cloaker_video":
            crf = str(22 + random.randint(0, 3))
            cmd = ["ffmpeg","-y","-i",inp,"-c:v","libx264","-crf",crf,"-preset","fast","-c:a","aac","-b:a","128k","-movflags","+faststart","-map_metadata","-1",out]
        elif job.tipo == "cortar_video":
            ini = p.get("inicio","00:00:00")
            fim = p.get("fim","")
            cmd = ["ffmpeg","-y","-ss",ini,"-i",inp]
            if fim: cmd += ["-to",fim]
            cmd += ["-c","copy","-movflags","+faststart",out]
        elif job.tipo == "marca_dagua":
            texto = p.get("texto","© Criativo")
            opac  = float(p.get("opacidade",70)) / 100.0
            posm  = {"top_left":"x=20:y=20","top_right":"x=w-tw-20:y=20","bottom_left":"x=20:y=h-th-20","bottom_right":"x=w-tw-20:y=h-th-20","center":"x=(w-tw)/2:y=(h-th)/2"}
            pexp  = posm.get(p.get("posicao","bottom_right"),"x=w-tw-20:y=h-th-20")
            dt    = "drawtext=text='" + texto + "':fontsize=36:fontcolor=white@" + str(round(opac,2)) + ":shadowx=2:shadowy=2:shadowcolor=black@" + str(round(opac,2)) + ":" + pexp
            cmd = ["ffmpeg","-y","-i",inp,"-vf",dt,"-c:v","libx264","-crf","23","-preset","fast","-c:a","aac","-movflags","+faststart",out]
        elif job.tipo == "processamento_completo":
            crf = str(22 + random.randint(0, 3))
            cmd = ["ffmpeg","-y","-i",inp,"-c:v","libx264","-crf",crf,"-preset","fast","-c:a","aac","-b:a","128k","-movflags","+faststart","-map_metadata","-1",out]
        elif job.tipo == "gerador_preview":
            fb = "[0:v]split=2[orig][blur];[blur]crop=iw*0.6:ih*0.6:iw*0.2:ih*0.2,boxblur=20:20[blurred];[orig][blurred]overlay=iw*0.2:ih*0.2[out]"
            cmd = ["ffmpeg","-y","-i",inp,"-filter_complex",fb,"-map","[out]","-c:v","libx264","-crf","23","-preset","fast","-c:a","aac","-movflags","+faststart",out]
        elif job.tipo == "limpar_metadados":
            cmd = ["ffmpeg","-y","-i",inp,"-c","copy","-map_metadata","-1","-movflags","+faststart",out]
        else:
            raise ValueError("Tipo desconhecido: " + job.tipo)
        _log.info("[FERRAMENTAS] job#" + str(job_id) + " cmd=" + str(cmd[:5]))
        res = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        if res.returncode != 0:
            err = (res.stderr or "")[-2000:]
            _log.error("[FERRAMENTAS] ffmpeg erro job#" + str(job_id) + ": " + err)
            raise RuntimeError("ffmpeg falhou (" + str(res.returncode) + "): " + err[-500:])
        _log.info("[FERRAMENTAS] job#" + str(job_id) + " ok: " + onom)
        job.output_filename = onom
        job.status = "done"
        db.commit()
    except Exception as e:
        _log.error("[FERRAMENTAS] excecao job#" + str(job_id) + ": " + str(e))
        if job:
            job.status = "error"
            job.error_msg = str(e)[:1000]
            db.commit()
    finally:
        db.close()

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

# 👇 Modelo de Destino (para múltiplos destinos por canal) 👇
class DestinationCreate(BaseModel):
    dest_channel_id: int
    dest_channel_name: str

class DestinationResponse(BaseModel):
    id: int
    dest_channel_id: int
    dest_channel_name: str
    is_active: bool

    class Config:
        from_attributes = True

# 👇 Modelos de Canais (com múltiplos destinos + legenda personalizada + CTA inteligente) 👇
class ChannelCreate(BaseModel):
    bot_id: Optional[int] = None
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
    cta_mode: Optional[str] = "exact"    # "exact" ou "smart"
    custom_caption: Optional[str] = None
    use_custom_caption: Optional[bool] = False
    caption_mode: Optional[str] = "replace"
    caption_keep_title: Optional[bool] = False   # Mantém título original
    userbot_required: Optional[bool] = True      # False = só bot, sem userbot
    auto_topic_clone: Optional[bool] = False     # 👇 NOVO: Espelhamento Automático de Tópicos
    extra_destinations: Optional[List[DestinationCreate]] = None

# Modelo para edição (todos campos opcionais)
class ChannelUpdate(BaseModel):
    bot_id: Optional[int] = None
    origin_channel_id: Optional[int] = None
    origin_channel_name: Optional[str] = None
    dest_channel_id: Optional[int] = None
    dest_channel_name: Optional[str] = None
    channel_type: Optional[str] = None
    interval_minutes: Optional[int] = None
    schedule_start: Optional[str] = None
    schedule_end: Optional[str] = None
    post_order: Optional[str] = None
    cta_find: Optional[str] = None
    cta_replace: Optional[str] = None
    cta_mode: Optional[str] = None
    custom_caption: Optional[str] = None
    use_custom_caption: Optional[bool] = None
    caption_mode: Optional[str] = None
    caption_keep_title: Optional[bool] = None
    userbot_required: Optional[bool] = None
    auto_topic_clone: Optional[bool] = None      # 👇 NOVO

class ChannelResponse(BaseModel):
    id: int
    bot_id: Optional[int] = None
    bot_name: Optional[str] = None
    bot_username: Optional[str] = None
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
    cta_mode: Optional[str] = "exact"
    custom_caption: Optional[str] = None
    use_custom_caption: Optional[bool] = False
    caption_mode: Optional[str] = "replace"
    caption_keep_title: Optional[bool] = False
    userbot_required: Optional[bool] = True
    auto_topic_clone: Optional[bool] = False     # 👇 NOVO
    is_active: bool
    total_forwarded: int
    destinations: Optional[List[DestinationResponse]] = []

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

# Helper para serializar canal com dados do bot + destinos
def _serialize_channel(canal, db):
    """Converte AutopostChannel em dict com bot_name, bot_username e destinations"""
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
        "cta_mode": canal.cta_mode or "exact",
        "custom_caption": canal.custom_caption,
        "use_custom_caption": canal.use_custom_caption or False,
        "caption_mode": canal.caption_mode or "replace",
        "caption_keep_title": getattr(canal, 'caption_keep_title', False) or False,
        "userbot_required": getattr(canal, 'userbot_required', True) if getattr(canal, 'userbot_required', None) is not None else True,
        "auto_topic_clone": getattr(canal, 'auto_topic_clone', False) or False, # 👇 NOVO
        "is_active": canal.is_active,
        "total_forwarded": canal.total_forwarded or 0,
        "destinations": [],
    }
    # Puxa nome do bot se vinculado
    if canal.bot_id:
        bot = db.query(AutopostBot).filter(AutopostBot.id == canal.bot_id).first()
        if bot:
            data["bot_name"] = bot.bot_name
            data["bot_username"] = bot.bot_username
    
    # Puxa destinos adicionais
    dests = db.query(AutopostDestination).filter(AutopostDestination.channel_id == canal.id).all()
    data["destinations"] = [
        {"id": d.id, "dest_channel_id": d.dest_channel_id, "dest_channel_name": d.dest_channel_name, "is_active": d.is_active}
        for d in dests
    ]
    return data

@app.get("/api/autopost/channels")
def list_channels(user_id: str = Depends(get_current_user), db: Session = Depends(get_db)):
    canais = db.query(AutopostChannel).filter(AutopostChannel.user_id == user_id).all()
    return [_serialize_channel(c, db) for c in canais]

@app.post("/api/autopost/channels")
def create_channel(canal: ChannelCreate, user_id: str = Depends(get_current_user), db: Session = Depends(get_db)):
    from datetime import time as dt_time
    
    start_time = None
    end_time = None
    if canal.schedule_start:
        parts = canal.schedule_start.split(":")
        start_time = dt_time(int(parts[0]), int(parts[1]))
    if canal.schedule_end:
        parts = canal.schedule_end.split(":")
        end_time = dt_time(int(parts[0]), int(parts[1]))
    
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
        cta_replace=canal.cta_replace,
        cta_mode=canal.cta_mode or "exact",
        custom_caption=canal.custom_caption,
        use_custom_caption=canal.use_custom_caption or False,
        caption_mode=canal.caption_mode or "replace",
        caption_keep_title=canal.caption_keep_title or False,
        userbot_required=canal.userbot_required if canal.userbot_required is not None else True,
        auto_topic_clone=canal.auto_topic_clone or False, # 👇 NOVO
    )
    db.add(novo_canal)
    db.commit()
    db.refresh(novo_canal)
    
    # Cria destinos adicionais se fornecidos
    if canal.extra_destinations:
        for dest in canal.extra_destinations:
            new_dest = AutopostDestination(
                channel_id=novo_canal.id,
                dest_channel_id=dest.dest_channel_id,
                dest_channel_name=dest.dest_channel_name,
            )
            db.add(new_dest)
        db.commit()
    
    return _serialize_channel(novo_canal, db)

# Rota de edição completa de canal
@app.put("/api/autopost/channels/{channel_id}")
def update_channel(channel_id: int, data: ChannelUpdate, user_id: str = Depends(get_current_user), db: Session = Depends(get_db)):
    """Atualiza configuração de um canal existente"""
    from datetime import time as dt_time
    
    canal = db.query(AutopostChannel).filter(AutopostChannel.id == channel_id, AutopostChannel.user_id == user_id).first()
    if not canal:
        raise HTTPException(status_code=404, detail="Canal não encontrado.")
    
    # Atualiza apenas campos enviados (não None)
    update_data = data.dict(exclude_unset=True, exclude_none=True)
    
    # Tratamento especial para schedule (string HH:MM → time)
    if "schedule_start" in update_data:
        val = update_data.pop("schedule_start")
        if val:
            parts = val.split(":")
            canal.schedule_start = dt_time(int(parts[0]), int(parts[1]))
        else:
            canal.schedule_start = None
    
    if "schedule_end" in update_data:
        val = update_data.pop("schedule_end")
        if val:
            parts = val.split(":")
            canal.schedule_end = dt_time(int(parts[0]), int(parts[1]))
        else:
            canal.schedule_end = None
    
    # Permite limpar campos de texto com string vazia
    for field in ["cta_find", "cta_replace", "custom_caption"]:
        if field in update_data and update_data[field] == "":
            update_data[field] = None
    
    # Aplica os demais campos
    for key, value in update_data.items():
        if hasattr(canal, key):
            setattr(canal, key, value)
    
    db.commit()
    db.refresh(canal)
    return _serialize_channel(canal, db)

# CRUD de Destinos individuais (adicionar/remover destinos após criação)
@app.post("/api/autopost/channels/{channel_id}/destinations")
def add_destination(channel_id: int, dest: DestinationCreate, user_id: str = Depends(get_current_user), db: Session = Depends(get_db)):
    canal = db.query(AutopostChannel).filter(AutopostChannel.id == channel_id, AutopostChannel.user_id == user_id).first()
    if not canal:
        raise HTTPException(status_code=404, detail="Canal não encontrado.")
    new_dest = AutopostDestination(
        channel_id=channel_id,
        dest_channel_id=dest.dest_channel_id,
        dest_channel_name=dest.dest_channel_name,
    )
    db.add(new_dest)
    db.commit()
    db.refresh(new_dest)
    return {"id": new_dest.id, "dest_channel_id": new_dest.dest_channel_id, "dest_channel_name": new_dest.dest_channel_name, "is_active": new_dest.is_active}

@app.delete("/api/autopost/channels/{channel_id}/destinations/{dest_id}")
def remove_destination(channel_id: int, dest_id: int, user_id: str = Depends(get_current_user), db: Session = Depends(get_db)):
    canal = db.query(AutopostChannel).filter(AutopostChannel.id == channel_id, AutopostChannel.user_id == user_id).first()
    if not canal:
        raise HTTPException(status_code=404, detail="Canal não encontrado.")
    dest = db.query(AutopostDestination).filter(AutopostDestination.id == dest_id, AutopostDestination.channel_id == channel_id).first()
    if not dest:
        raise HTTPException(status_code=404, detail="Destino não encontrado.")
    db.delete(dest)
    db.commit()
    return {"message": "Destino removido!"}

# Rota para atualizar legenda personalizada
@app.put("/api/autopost/channels/{channel_id}/caption")
def update_caption(channel_id: int, user_id: str = Depends(get_current_user), db: Session = Depends(get_db), body: dict = None):
    canal = db.query(AutopostChannel).filter(AutopostChannel.id == channel_id, AutopostChannel.user_id == user_id).first()
    if not canal:
        raise HTTPException(status_code=404, detail="Canal não encontrado.")
    
    if body is None:
        raise HTTPException(status_code=400, detail="Corpo da requisição vazio.")
    
    canal.custom_caption = body.get("custom_caption", canal.custom_caption)
    canal.use_custom_caption = body.get("use_custom_caption", canal.use_custom_caption)
    canal.caption_mode = body.get("caption_mode", canal.caption_mode)
    db.commit()
    return {"message": "Legenda atualizada!", "custom_caption": canal.custom_caption, "use_custom_caption": canal.use_custom_caption, "caption_mode": canal.caption_mode}

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

@app.delete("/api/autopost/queue")
def clear_queue(user_id: str = Depends(get_current_user), db: Session = Depends(get_db)):
    """Limpa toda a fila do usuário"""
    user_channel_ids = [
        ch.id for ch in
        db.query(AutopostChannel).filter(AutopostChannel.user_id == user_id).all()
    ]
    if user_channel_ids:
        deleted = db.query(AutopostQueue).filter(
            AutopostQueue.channel_pair_id.in_(user_channel_ids)
        ).delete(synchronize_session=False)
        db.commit()
        return {"message": f"{deleted} itens removidos da fila!"}
    return {"message": "Nenhum item para limpar."}

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
# 8. SUPER ADMIN - PAINEL DE CONTROLE TOTAL
# ==========================================

@app.get("/api/admin/check")
def check_admin(user_id: str = Depends(get_current_user), db: Session = Depends(get_db)):
    """Verifica se o usuário é super admin"""
    admin = db.query(AutopostAdmin).filter(AutopostAdmin.user_id == user_id).first()
    return {"is_admin": admin is not None, "role": admin.role if admin else None}

@app.get("/api/admin/stats")
def admin_stats(user_id: str = Depends(require_superadmin), db: Session = Depends(get_db)):
    """Estatísticas globais do sistema — apenas super admin"""
    total_users = db.query(AutopostSession).count()
    active_sessions = db.query(AutopostSession).filter(AutopostSession.is_active == True).count()
    total_channels = db.query(AutopostChannel).count()
    active_channels = db.query(AutopostChannel).filter(AutopostChannel.is_active == True).count()
    total_bots = db.query(AutopostBot).count()
    total_queue = db.query(AutopostQueue).count()
    total_sent = db.query(AutopostQueue).filter(AutopostQueue.status == "sent").count()
    total_errors = db.query(AutopostQueue).filter(AutopostQueue.status == "error").count()
    total_destinations = db.query(AutopostDestination).count()
    
    return {
        "users": {"total": total_users, "active_sessions": active_sessions},
        "channels": {"total": total_channels, "active": active_channels},
        "bots": {"total": total_bots},
        "queue": {"total": total_queue, "sent": total_sent, "errors": total_errors},
        "destinations": {"total": total_destinations},
        "engine": get_engine_status()
    }

@app.get("/api/admin/users")
def admin_list_users(user_id: str = Depends(require_superadmin), db: Session = Depends(get_db)):
    """Lista todos os usuários com sessões e seus canais"""
    sessions = db.query(AutopostSession).all()
    result = []
    for s in sessions:
        channels = db.query(AutopostChannel).filter(AutopostChannel.user_id == s.user_id).all()
        bots = db.query(AutopostBot).filter(AutopostBot.user_id == s.user_id).all()
        total_sent = 0
        for ch in channels:
            total_sent += db.query(AutopostQueue).filter(
                AutopostQueue.channel_pair_id == ch.id,
                AutopostQueue.status == "sent"
            ).count()
        
        result.append({
            "user_id": s.user_id,
            "phone": s.phone_number,
            "is_active": s.is_active,
            "created_at": s.created_at.isoformat() if s.created_at else None,
            "total_channels": len(channels),
            "active_channels": sum(1 for c in channels if c.is_active),
            "total_bots": len(bots),
            "total_sent": total_sent,
        })
    return result

@app.get("/api/admin/users/{target_user_id}/channels")
def admin_user_channels(target_user_id: str, user_id: str = Depends(require_superadmin), db: Session = Depends(get_db)):
    """Lista canais de um usuário específico — super admin"""
    canais = db.query(AutopostChannel).filter(AutopostChannel.user_id == target_user_id).all()
    return [_serialize_channel(c, db) for c in canais]

@app.post("/api/admin/users/{target_user_id}/toggle")
def admin_toggle_user(target_user_id: str, user_id: str = Depends(require_superadmin), db: Session = Depends(get_db)):
    """Ativa/desativa sessão de um usuário — super admin"""
    session = db.query(AutopostSession).filter(AutopostSession.user_id == target_user_id).first()
    if not session:
        raise HTTPException(status_code=404, detail="Usuário não encontrado.")
    session.is_active = not session.is_active
    if not session.is_active:
        # Pausa todos os canais do usuário
        db.query(AutopostChannel).filter(AutopostChannel.user_id == target_user_id).update({"is_active": False})
    db.commit()
    return {"message": f"Usuário {'ativado' if session.is_active else 'desativado'}!", "is_active": session.is_active}

@app.delete("/api/admin/users/{target_user_id}")
def admin_delete_user(target_user_id: str, user_id: str = Depends(require_superadmin), db: Session = Depends(get_db)):
    """Remove um usuário e todos os seus dados — super admin"""
    session = db.query(AutopostSession).filter(AutopostSession.user_id == target_user_id).first()
    if not session:
        raise HTTPException(status_code=404, detail="Usuário não encontrado.")
    # Deleta em cascata: channels → destinations, queue
    channels = db.query(AutopostChannel).filter(AutopostChannel.user_id == target_user_id).all()
    for ch in channels:
        db.query(AutopostDestination).filter(AutopostDestination.channel_id == ch.id).delete()
        db.query(AutopostQueue).filter(AutopostQueue.channel_pair_id == ch.id).delete()
        db.query(AutopostTopicMap).filter(AutopostTopicMap.channel_id == ch.id).delete()
    db.query(AutopostChannel).filter(AutopostChannel.user_id == target_user_id).delete()
    db.query(AutopostBot).filter(AutopostBot.user_id == target_user_id).delete()
    db.query(AutopostLog).filter(AutopostLog.user_id == target_user_id).delete()
    db.delete(session)
    db.commit()
    return {"message": f"Usuário {target_user_id} removido com sucesso!"}

@app.post("/api/admin/promote/{target_user_id}")
def admin_promote(target_user_id: str, user_id: str = Depends(require_superadmin), db: Session = Depends(get_db)):
    """Promove um usuário a admin"""
    existing = db.query(AutopostAdmin).filter(AutopostAdmin.user_id == target_user_id).first()
    if existing:
        return {"message": "Usuário já é admin."}
    new_admin = AutopostAdmin(user_id=target_user_id, role="admin")
    db.add(new_admin)
    db.commit()
    return {"message": f"Usuário {target_user_id} promovido a admin!"}

@app.delete("/api/admin/demote/{target_user_id}")
def admin_demote(target_user_id: str, user_id: str = Depends(require_superadmin), db: Session = Depends(get_db)):
    """Remove admin de um usuário"""
    admin = db.query(AutopostAdmin).filter(AutopostAdmin.user_id == target_user_id).first()
    if not admin:
        raise HTTPException(status_code=404, detail="Usuário não é admin.")
    if admin.role == "superadmin":
        raise HTTPException(status_code=400, detail="Não é possível rebaixar um super admin.")
    db.delete(admin)
    db.commit()
    return {"message": f"Admin removido de {target_user_id}!"}

# Rota para mapeamento de tópicos
@app.get("/api/autopost/channels/{channel_id}/topics")
def list_topic_maps(channel_id: int, user_id: str = Depends(get_current_user), db: Session = Depends(get_db)):
    canal = db.query(AutopostChannel).filter(AutopostChannel.id == channel_id, AutopostChannel.user_id == user_id).first()
    if not canal:
        raise HTTPException(status_code=404, detail="Canal não encontrado.")
    topics = db.query(AutopostTopicMap).filter(AutopostTopicMap.channel_id == channel_id).all()
    return [{"id": t.id, "origin_topic_id": t.origin_topic_id, "origin_topic_name": t.origin_topic_name, "dest_topic_id": t.dest_topic_id, "dest_topic_name": t.dest_topic_name, "is_active": t.is_active} for t in topics]

@app.post("/api/autopost/channels/{channel_id}/topics")
def add_topic_map(channel_id: int, body: dict, user_id: str = Depends(get_current_user), db: Session = Depends(get_db)):
    canal = db.query(AutopostChannel).filter(AutopostChannel.id == channel_id, AutopostChannel.user_id == user_id).first()
    if not canal:
        raise HTTPException(status_code=404, detail="Canal não encontrado.")
    new_map = AutopostTopicMap(
        channel_id=channel_id,
        origin_topic_id=body.get("origin_topic_id"),
        origin_topic_name=body.get("origin_topic_name", ""),
        dest_topic_id=body.get("dest_topic_id"),
        dest_topic_name=body.get("dest_topic_name", ""),
    )
    db.add(new_map)
    db.commit()
    return {"id": new_map.id, "message": "Mapeamento de tópico criado!"}

@app.delete("/api/autopost/channels/{channel_id}/topics/{topic_id}")
def remove_topic_map(channel_id: int, topic_id: int, user_id: str = Depends(get_current_user), db: Session = Depends(get_db)):
    t = db.query(AutopostTopicMap).filter(AutopostTopicMap.id == topic_id, AutopostTopicMap.channel_id == channel_id).first()
    if not t:
        raise HTTPException(status_code=404, detail="Mapeamento não encontrado.")
    db.delete(t)
    db.commit()
    return {"message": "Mapeamento removido!"}

# ==========================================
# CLONEX — STATUS E CONFIGURAÇÃO
# ==========================================

@app.get("/api/clonex/status")
def clonex_status(user_id: str = Depends(get_current_user), db: Session = Depends(get_db)):
    """
    Retorna o status dos módulos Clonex para o usuário.
    - Clonex Prévias: sempre liberado
    - Clonex VIPs: bloqueado (exceto admin/superadmin)
    """
    admin = db.query(AutopostAdmin).filter(AutopostAdmin.user_id == user_id).first()
    is_admin = admin is not None
    
    return {
        "clonex_previas": {
            "unlocked": True,
            "label": "Clonex Prévias",
            "description": "Clone canais e grupos de prévias (seus ou de concorrentes). Limite: mídias até 50MB.",
            "max_media_mb": 50,
        },
        "clonex_vips": {
            "unlocked": is_admin,  # Só admin tem acesso
            "label": "Clonex VIPs",
            "description": "Clone canais VIPs completos sem limite de tamanho. Requer migração do backend para VPS dedicada.",
            "max_media_mb": None if is_admin else 50,
            "blocked_reason": None if is_admin else "Recurso será liberado após migração do backend para VPS dedicada (Contabo/Hetzner).",
        },
        "is_admin": is_admin,
    }

# ==========================================
# 9. FERRAMENTAS DE CRIATIVOS
# ==========================================
#
# Fluxo:
#   1. POST /api/ferramentas/processar  → recebe arquivo + tipo + params
#      • Verifica se o recurso 'clonador_previas' está desbloqueado na Zenyx
#      • Imagens: processa em background imediato → job fica 'done' em segundos
#      • Vídeos:  cria job 'pending' → APScheduler processa em background
#      • Retorna { job_id, status }
#
#   2. GET /api/ferramentas/status/{job_id} → polling do frontend (a cada 3s)
#      • Retorna { status, download_url } quando done
#
#   3. GET /api/ferramentas/download/{job_id} → download do arquivo processado
#
#   4. GET /api/ferramentas/jobs → lista jobs recentes do usuário
#
# Tipos suportados:
#   Imagem síncrona: limpar_metadados, conversor_proporcao, cloaker_criativo,
#                    marca_dagua (imagem), gerador_preview (imagem)
#   Vídeo assíncrono: conversor_proporcao, cloaker_video, cortar_video,
#                     marca_dagua (vídeo), processamento_completo,
#                     gerador_preview (vídeo), limpar_metadados (vídeo/áudio)
# ==========================================

# Tipos que processam imagem de forma síncrona
_TIPOS_IMAGEM_SYNC = {
    "limpar_metadados",
    "conversor_proporcao",
    "cloaker_criativo",
    "marca_dagua",
    "gerador_preview",
}

# Todos os tipos válidos
_TIPOS_VALIDOS = {
    "limpar_metadados",
    "conversor_proporcao",
    "cloaker_criativo",
    "processamento_completo",
    "cloaker_video",
    "cortar_video",
    "marca_dagua",
    "gerador_preview",
}


@app.post("/api/ferramentas/processar")
async def processar_ferramenta(
    background_tasks: BackgroundTasks,
    tipo: str = Form(...),
    parametros: Optional[str] = Form("{}"),
    arquivo: UploadFile = File(...),
    credentials: HTTPAuthorizationCredentials = Depends(security),
    user_id: str = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Recebe um arquivo e o tipo de ferramenta, cria um job e inicia o processamento.
    Para imagens: processa em background imediato (retorna done em segundos).
    Para vídeos/áudios: cria job pending e agenda processamento via thread.
    """
    # 1. Verificar acesso Prime
    token = credentials.credentials
    prime_ok = await verificar_prime_clonador(token)
    if not prime_ok:
        # Super admins do AutoPost têm acesso irrestrito
        admin = db.query(AutopostAdmin).filter(AutopostAdmin.user_id == user_id).first()
        if not admin:
            raise HTTPException(
                status_code=403,
                detail="Acesso negado: você precisa desbloquear o recurso 'Clonador de Prévias/VIPs' na plataforma Zenyx VIPs para usar as Ferramentas de Criativos."
            )

    # 2. Validar tipo
    if tipo not in _TIPOS_VALIDOS:
        raise HTTPException(status_code=400, detail=f"Tipo inválido: '{tipo}'. Tipos aceitos: {sorted(_TIPOS_VALIDOS)}")

    # 3. Validar e ler arquivo
    if arquivo.size and arquivo.size > TAMANHO_MAX_UPLOAD:
        raise HTTPException(status_code=413, detail="Arquivo muito grande. Limite: 500MB.")

    conteudo = await arquivo.read()
    if len(conteudo) > TAMANHO_MAX_UPLOAD:
        raise HTTPException(status_code=413, detail="Arquivo muito grande. Limite: 500MB.")

    ext = get_extensao(arquivo.filename or "arquivo.bin")
    if not ext:
        raise HTTPException(status_code=400, detail="Arquivo sem extensão reconhecida.")

    # 4. Salvar arquivo de entrada
    input_nome = salvar_upload(conteudo, arquivo.filename or f"input{ext}")

    # 5. Criar job no banco
    try:
        params_dict = json.loads(parametros or "{}")
    except json.JSONDecodeError:
        params_dict = {}

    job = FerramentsJob(
        user_id=user_id,
        tipo=tipo,
        status="pending",
        input_filename=input_nome,
        parametros=json.dumps(params_dict),
    )
    db.add(job)
    db.commit()
    db.refresh(job)

    # 6. Decidir: imagem síncrona ou vídeo assíncrono
    eh_imagem = ext in EXTENSOES_IMAGEM
    eh_video  = ext in EXTENSOES_VIDEO
    eh_audio  = ext in EXTENSOES_AUDIO

    if eh_imagem and tipo in _TIPOS_IMAGEM_SYNC:
        # Processa em background imediato (Pillow — milissegundos)
        background_tasks.add_task(_processar_imagem_sync, job, db)
        return {
            "job_id": job.id,
            "status": "processing",
            "tipo": tipo,
            "mensagem": "Processando imagem... consulte /api/ferramentas/status/{job_id}",
        }
    else:
        # Vídeo ou áudio → agenda em thread separada para não bloquear
        import threading
        t = threading.Thread(target=_processar_video_sync, args=(job.id,), daemon=True)
        t.start()
        return {
            "job_id": job.id,
            "status": "pending",
            "tipo": tipo,
            "mensagem": "Job criado! O processamento de vídeo pode levar alguns minutos. Consulte /api/ferramentas/status/{job_id}",
        }


@app.get("/api/ferramentas/status/{job_id}")
def status_ferramenta(
    job_id: int,
    user_id: str = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Retorna o status atual do job. O frontend faz polling a cada 3s."""
    job = db.query(FerramentsJob).filter(
        FerramentsJob.id == job_id,
        FerramentsJob.user_id == user_id,
    ).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job não encontrado.")

    resp = {
        "job_id": job.id,
        "tipo": job.tipo,
        "status": job.status,
        "created_at": job.created_at.isoformat() if job.created_at else None,
    }
    if job.status == "done":
        resp["download_url"] = f"/api/ferramentas/download/{job.id}"
    if job.status == "error":
        resp["error_msg"] = job.error_msg
    return resp


@app.get("/api/ferramentas/download/{job_id}")
def download_ferramenta(
    job_id: int,
    db: Session = Depends(get_db),
    token: str = None,
    credentials: HTTPAuthorizationCredentials = Depends(HTTPBearer(auto_error=False)),
):
    """Download do arquivo. Aceita token via header Authorization OU query param ?token=...
    Isso permite links diretos no browser sem precisar de JS para injetar o header."""
    # Resolve token: header tem prioridade, fallback para query param
    raw_token = None
    if credentials and credentials.credentials:
        raw_token = credentials.credentials
    elif token:
        raw_token = token
    if not raw_token:
        raise HTTPException(status_code=401, detail="Token nao fornecido.")
    try:
        from jose import jwt, JWTError
        payload = jwt.decode(raw_token, SECRET_KEY, algorithms=[ALGORITHM])
        user_id = str(payload.get("sub") or payload.get("id") or "")
        if not user_id:
            raise ValueError("user_id vazio")
    except Exception:
        raise HTTPException(status_code=401, detail="Token invalido.")
    # A partir daqui user_id esta validado
    job = db.query(FerramentsJob).filter(
        FerramentsJob.id == job_id,
        FerramentsJob.user_id == user_id,
    ).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job não encontrado.")
    if job.status != "done" or not job.output_filename:
        raise HTTPException(status_code=400, detail=f"Arquivo ainda não disponível. Status atual: {job.status}")

    output_path = caminho_completo(job.output_filename)
    if not os.path.exists(output_path):
        raise HTTPException(status_code=404, detail="Arquivo processado não encontrado no servidor.")

    # Nome de download amigável: tipo_ferramenta + extensão original
    ext = get_extensao(job.output_filename)
    nome_download = f"{job.tipo}{ext}"

    return FileResponse(
        path=output_path,
        filename=nome_download,
        media_type="application/octet-stream",
    )


@app.get("/api/ferramentas/jobs")
def listar_jobs_ferramentas(
    user_id: str = Depends(get_current_user),
    db: Session = Depends(get_db),
    limit: int = 20,
):
    """Lista os jobs mais recentes do usuário (para histórico na UI)."""
    jobs = (
        db.query(FerramentsJob)
        .filter(FerramentsJob.user_id == user_id)
        .order_by(FerramentsJob.created_at.desc())
        .limit(limit)
        .all()
    )
    return [
        {
            "job_id": j.id,
            "tipo": j.tipo,
            "status": j.status,
            "created_at": j.created_at.isoformat() if j.created_at else None,
            "download_url": f"/api/ferramentas/download/{j.id}" if j.status == "done" else None,
            "error_msg": j.error_msg if j.status == "error" else None,
        }
        for j in jobs
    ]


@app.delete("/api/ferramentas/jobs/{job_id}")
def deletar_job_ferramenta(
    job_id: int,
    user_id: str = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Remove um job e seus arquivos do disco."""
    job = db.query(FerramentsJob).filter(
        FerramentsJob.id == job_id,
        FerramentsJob.user_id == user_id,
    ).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job não encontrado.")

    # Remove arquivos do disco
    for fname in [job.input_filename, job.output_filename]:
        if fname:
            try:
                os.remove(caminho_completo(fname))
            except FileNotFoundError:
                pass

    db.delete(job)
    db.commit()
    return {"message": "Job removido com sucesso."}


# ==========================================
# 10. ROTA DE MIGRAÇÃO (Acessar via URL para aplicar novas colunas)
# ==========================================
@app.get("/api/migrate")
def run_migration(db: Session = Depends(get_db)):
    """
    Rota de migração manual. Acesse:
    https://api-autopost.zenyxvips.com/api/migrate
    
    Migrações:
    - Tabela autopost_bots (se não existir)
    - Coluna bot_id em autopost_channels_v2
    - Tabela autopost_destinations (múltiplos destinos)
    - Colunas custom_caption, use_custom_caption, caption_mode em autopost_channels_v2
    - Coluna auto_topic_clone
    - [CLONEX] Limite de 50MB aplicado no engine (sem alteração de schema)
    - [FERRAMENTAS] Tabela ferramentas_jobs (Ferramentas de Criativos)
    """
    from sqlalchemy import text, inspect
    
    results = []
    
    try:
        inspector = inspect(engine)
        existing_tables = inspector.get_table_names()
        
        # 1. Cria tabela autopost_bots se não existir
        if "autopost_bots" not in existing_tables:
            Base.metadata.tables["autopost_bots"].create(bind=engine)
            results.append("✅ Tabela 'autopost_bots' criada!")
        else:
            results.append("ℹ️ Tabela 'autopost_bots' já existe.")
        
        # 2. Colunas em autopost_channels_v2
        columns = [col["name"] for col in inspector.get_columns("autopost_channels_v2")]
        
        new_columns = {
            "bot_id": "INTEGER REFERENCES autopost_bots(id)",
            "custom_caption": "TEXT",
            "use_custom_caption": "BOOLEAN DEFAULT FALSE",
            "caption_mode": "VARCHAR DEFAULT 'replace'",
            "cta_mode": "VARCHAR DEFAULT 'exact'",
            "caption_keep_title": "BOOLEAN DEFAULT FALSE",
            "userbot_required": "BOOLEAN DEFAULT TRUE",
            "auto_topic_clone": "BOOLEAN DEFAULT FALSE", # 👇 NOVO: Espelhamento automático de tópicos
        }
        
        for col_name, col_def in new_columns.items():
            if col_name not in columns:
                db.execute(text(f"ALTER TABLE autopost_channels_v2 ADD COLUMN {col_name} {col_def}"))
                db.commit()
                results.append(f"✅ Coluna '{col_name}' adicionada em 'autopost_channels_v2'!")
            else:
                results.append(f"ℹ️ Coluna '{col_name}' já existe.")
        
        # 3. Cria tabela autopost_destinations se não existir
        if "autopost_destinations" not in existing_tables:
            Base.metadata.tables["autopost_destinations"].create(bind=engine)
            results.append("✅ Tabela 'autopost_destinations' criada!")
        else:
            results.append("ℹ️ Tabela 'autopost_destinations' já existe.")
        
        # 4. Cria tabela autopost_admins se não existir
        if "autopost_admins" not in existing_tables:
            Base.metadata.tables["autopost_admins"].create(bind=engine)
            results.append("✅ Tabela 'autopost_admins' criada!")
        else:
            results.append("ℹ️ Tabela 'autopost_admins' já existe.")
        
        # 5. Cria tabela autopost_topic_maps se não existir
        if "autopost_topic_maps" not in existing_tables:
            Base.metadata.tables["autopost_topic_maps"].create(bind=engine)
            results.append("✅ Tabela 'autopost_topic_maps' criada!")
        else:
            results.append("ℹ️ Tabela 'autopost_topic_maps' já existe.")

        # 6. Cria tabela ferramentas_jobs se não existir (Ferramentas de Criativos)
        if "ferramentas_jobs" not in existing_tables:
            Base.metadata.tables["ferramentas_jobs"].create(bind=engine)
            results.append("✅ Tabela 'ferramentas_jobs' criada! (Ferramentas de Criativos)")
        else:
            results.append("ℹ️ Tabela 'ferramentas_jobs' já existe.")
        
        return {"status": "success", "migrations": results}
    
    except Exception as e:
        db.rollback()
        return {"status": "error", "detail": str(e), "migrations": results}

@app.get("/api/setup-admin/{target_user_id}")
def setup_first_admin(target_user_id: str, db: Session = Depends(get_db)):
    """
    Rota de setup ÚNICA para criar o primeiro super admin.
    Acesse: https://api-autopost.zenyxvips.com/api/setup-admin/Stifler
    
    Depois de usar, essa rota não cria duplicatas.
    """
    existing = db.query(AutopostAdmin).filter(AutopostAdmin.user_id == target_user_id).first()
    if existing:
        return {"message": f"Usuário {target_user_id} já é {existing.role}."}
    
    new_admin = AutopostAdmin(user_id=target_user_id, role="superadmin")
    db.add(new_admin)
    db.commit()
    return {"message": f"🎉 {target_user_id} agora é SUPER ADMIN do AutoPost!"}