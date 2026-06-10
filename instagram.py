"""
instagram.py — Módulo de Instagram Farm
========================================
Todas as rotas e lógica do Instagram Farm ficam aqui,
completamente separadas do main.py (Telegram/AutoPost).

Registrado no main.py com:
    from instagram import router as instagram_router
    app.include_router(instagram_router)

Rotas expostas (prefixo /api/instagram/):
    POST   /api/instagram/accounts               → vincula nova conta
    GET    /api/instagram/accounts               → lista contas do usuário
    DELETE /api/instagram/accounts/{id}          → desvincula conta
    POST   /api/instagram/accounts/{id}/verify   → confirma código de challenge (2FA/email)
    GET    /api/instagram/accounts/{id}/status   → status de login da conta
    POST   /api/instagram/posts                  → agenda post
    GET    /api/instagram/posts                  → lista fila de posts
    DELETE /api/instagram/posts/{id}             → cancela/remove post da fila
    GET    /api/instagram/logs                   → histórico de ações
    DELETE /api/instagram/logs                   → limpa histórico

Verificação Prime:
    Assim como as Ferramentas de Criativos consultam o recurso 'clonador_previas',
    este módulo consulta o recurso 'instagram_farm' na Zenyx API.
    Super admins do AutoPost têm acesso irrestrito.

Segurança da sessão:
    A sessão do instagrapi é serializada para JSON e criptografada com Fernet
    (chave derivada da SECRET_KEY do ambiente) antes de ser salva como BYTEA.
    Senhas NUNCA são persistidas no banco.
"""

import os
import json
import uuid
import time
import logging
import threading
from datetime import datetime
from typing import List, Optional

import httpx
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form, BackgroundTasks
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import jwt, JWTError
from pydantic import BaseModel
from sqlalchemy.orm import Session

from database import (
    SessionLocal,
    InstagramAccount,
    InstagramPost,
    InstagramLog,
    AutopostAdmin,
    engine,
    Base,
    now_brazil,
)

# ==========================================
# CONFIGURAÇÃO
# ==========================================

logger = logging.getLogger("autopost")

router = APIRouter(prefix="/api/instagram", tags=["instagram"])
security = HTTPBearer()

SECRET_KEY = os.getenv("SECRET_KEY", "chave-secreta-padrao")
ALGORITHM  = "HS256"
ZENYX_API_URL = os.getenv("ZENYX_API_URL", "https://api.zenyxvips.com")

# Diretório de mídias — mesmo que as Ferramentas de Criativos
INSTA_TMP_DIR = os.getenv("FERRAMENTAS_TMP_DIR", "/tmp/ferramentas")
os.makedirs(INSTA_TMP_DIR, exist_ok=True)

# Cache de status Prime: user_id → (timestamp, is_unlocked)
_prime_insta_cache: dict = {}
PRIME_CACHE_TTL = 300  # 5 minutos

# Chave Fernet para criptografar session_data
# Derivada da SECRET_KEY para não precisar de nova variável de ambiente
def _get_fernet():
    """Retorna instância Fernet com chave derivada da SECRET_KEY."""
    import base64, hashlib
    from cryptography.fernet import Fernet
    chave_bytes = hashlib.sha256(SECRET_KEY.encode()).digest()
    chave_b64 = base64.urlsafe_b64encode(chave_bytes)
    return Fernet(chave_b64)

def _criptografar_sessao(session_json: str) -> bytes:
    """Criptografa a sessão do instagrapi antes de salvar no banco."""
    return _get_fernet().encrypt(session_json.encode())

def _descriptografar_sessao(session_bytes: bytes) -> str:
    """Descriptografa a sessão salva no banco."""
    return _get_fernet().decrypt(bytes(session_bytes)).decode()


# ==========================================
# HELPERS
# ==========================================

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)) -> str:
    token = credentials.credentials
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM], options={"verify_aud": False})
        user_id = payload.get("sub") or payload.get("id") or payload.get("user_id")
        if not user_id:
            raise HTTPException(status_code=401, detail="Token inválido: usuário não encontrado.")
        return str(user_id)
    except JWTError:
        raise HTTPException(status_code=401, detail="Token expirado ou inválido.")


async def verificar_prime_instagram(token: str) -> bool:
    """
    Consulta a Zenyx API para verificar se o usuário tem o recurso
    'instagram_farm' desbloqueado. Cache local de 5 minutos por user.
    Segue o mesmo padrão de verificar_prime_clonador() do main.py.
    """
    now = time.time()

    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM], options={"verify_aud": False})
        uid = str(payload.get("sub") or payload.get("id") or payload.get("user_id") or "")
    except Exception:
        uid = token[:32]

    if uid in _prime_insta_cache:
        ts, unlocked = _prime_insta_cache[uid]
        if now - ts < PRIME_CACHE_TTL:
            return unlocked

    try:
        async with httpx.AsyncClient(timeout=8.0) as client:
            resp = await client.get(
                f"{ZENYX_API_URL}/api/admin/recursos-prime",
                headers={"Authorization": f"Bearer {token}"}
            )
        if resp.status_code == 200:
            data = resp.json()
            for rec in data.get("recursos", []):
                if rec.get("id") == "instagram_farm" and rec.get("status") == "desbloqueado":
                    _prime_insta_cache[uid] = (now, True)
                    return True
            _prime_insta_cache[uid] = (now, False)
            return False
    except Exception:
        if uid in _prime_insta_cache:
            return _prime_insta_cache[uid][1]
        return False

    _prime_insta_cache[uid] = (now, False)
    return False


def _registrar_log(db: Session, user_id: str, action: str, account_id: int = None, details: dict = None):
    """Salva uma entrada no instagram_logs."""
    log = InstagramLog(
        user_id=user_id,
        account_id=account_id,
        action=action,
        details=details or {},
    )
    db.add(log)
    db.commit()


def _salvar_midia(conteudo: bytes, filename: str) -> str:
    """Salva arquivo de mídia em INSTA_TMP_DIR com nome único. Retorna o nome do arquivo."""
    ext = os.path.splitext(filename.lower())[1]
    nome = f"insta_{uuid.uuid4().hex}{ext}"
    with open(os.path.join(INSTA_TMP_DIR, nome), "wb") as f:
        f.write(conteudo)
    return nome


# ==========================================
# PYDANTIC MODELS
# ==========================================

class AccountCreate(BaseModel):
    ig_username: str
    ig_password: str          # Usado apenas para fazer login — NÃO é salvo
    proxy_url: Optional[str] = None   # socks5://user:pass@host:port ou http://...

class AccountResponse(BaseModel):
    id: int
    ig_username: str
    proxy_url: Optional[str] = None
    is_active: bool
    login_status: str
    last_login_at: Optional[str] = None
    created_at: Optional[str] = None

    class Config:
        from_attributes = True

class ChallengeVerify(BaseModel):
    code: str   # Código recebido por email/SMS do Instagram

class PostCreate(BaseModel):
    account_id: int
    post_type: str = "photo"   # 'photo', 'video', 'reel', 'story_photo', 'story_video'
    caption: Optional[str] = None
    scheduled_for: str         # ISO 8601: "2025-10-15T14:30:00"

class PostResponse(BaseModel):
    id: int
    account_id: int
    ig_username: Optional[str] = None
    post_type: str
    caption: Optional[str] = None
    media_filename: Optional[str] = None
    scheduled_for: str
    status: str
    sent_at: Optional[str] = None
    error_msg: Optional[str] = None
    ig_media_id: Optional[str] = None
    created_at: Optional[str] = None

    class Config:
        from_attributes = True


# ==========================================
# LÓGICA DE LOGIN (instagrapi)
# ==========================================

# Armazena clientes aguardando verificação de challenge: user_id → {client, account_id}
_pending_challenges: dict = {}


def _fazer_login_instagram(ig_username: str, ig_password: str, proxy_url: str = None):
    """
    Faz login no Instagram via instagrapi.
    Retorna (client, session_json, device_json) em caso de sucesso.
    Lança exceção com mensagem adequada em caso de erro.

    Exceções mapeadas:
    - ChallengeRequired  → Instagram pediu verificação (email/SMS)
    - BadPassword        → senha errada
    - TwoFactorRequired  → 2FA ativado na conta
    - FeedbackRequired   → conta bloqueada/suspensa temporariamente pelo Instagram
    - LoginRequired      → sessão inválida ou conta desativada
    - PleaseWaitFewMinutes → rate limit do Instagram (muitas tentativas)
    - Exception genérica → outro erro, mensagem repassada ao frontend
    """
    try:
        from instagrapi import Client
        from instagrapi.exceptions import (
            ChallengeRequired,
            BadPassword,
            TwoFactorRequired,
            FeedbackRequired,
            LoginRequired,
            PleaseWaitFewMinutes,
            ReloginAttemptExceeded,
        )
    except ImportError:
        raise RuntimeError("A biblioteca 'instagrapi' não está instalada. Verifique o requirements.txt.")

    cl = Client()

    # Configura proxy se fornecido
    if proxy_url:
        cl.set_proxy(proxy_url)

    # Device fingerprint e locale brasileiros
    cl.set_locale("pt_BR")
    cl.set_timezone_offset(-10800)  # UTC-3 (Brasília)

    # Desativa relogin automático para não mascarar erros
    cl.handle_exception = lambda client, e: (_ for _ in ()).throw(e)

    try:
        cl.login(ig_username, ig_password)

    except ChallengeRequired:
        raise ChallengeRequired("challenge_required")

    except BadPassword:
        raise ValueError("Senha incorreta. Verifique e tente novamente.")

    except TwoFactorRequired:
        raise ValueError("Esta conta tem verificação em duas etapas ativada. Desative temporariamente no app do Instagram e tente novamente.")

    except FeedbackRequired as e:
        raise RuntimeError(f"O Instagram bloqueou temporariamente esta conta. Abra o app e resolva o aviso pendente, depois tente novamente. Detalhe: {str(e)[:200]}")

    except LoginRequired as e:
        raise RuntimeError(f"O Instagram recusou o login. A conta pode estar desativada ou com restrições. Detalhe: {str(e)[:200]}")

    except PleaseWaitFewMinutes:
        raise RuntimeError("O Instagram está com rate limit ativo. Aguarde alguns minutos e tente novamente.")

    except ReloginAttemptExceeded:
        raise RuntimeError("Muitas tentativas de login. Aguarde alguns minutos antes de tentar novamente.")

    except Exception as e:
        erro_str = str(e)
        raise RuntimeError(f"Erro ao fazer login: {erro_str}")

    # Serializa sessão e device
    try:
        session_dict = cl.get_settings()
        session_json = json.dumps(session_dict)
    except Exception as e:
        raise RuntimeError(f"Login OK mas erro ao salvar sessão: {str(e)}")

    device_json = json.dumps({
        "device_type": getattr(cl, "device_type", "unknown"),
        "user_agent":  getattr(cl, "user_agent",  "unknown"),
    })

    return cl, session_json, device_json


def _executar_post(post_id: int):
    """
    Executa um post agendado. Chamado em thread separada pelo scheduler.
    Fluxo: busca post → carrega sessão → faz upload → atualiza status.
    """
    db = SessionLocal()
    post = None
    try:
        post = db.query(InstagramPost).filter(InstagramPost.id == post_id).first()
        if not post or post.status != "pending":
            return

        post.status = "processing"
        db.commit()

        account = db.query(InstagramAccount).filter(InstagramAccount.id == post.account_id).first()
        if not account or not account.session_data:
            raise RuntimeError("Conta não encontrada ou sessão expirada.")

        try:
            from instagrapi import Client
        except ImportError:
            raise RuntimeError("instagrapi não instalado.")

        cl = Client()

        # Restaura proxy se configurado
        if account.proxy_url:
            cl.set_proxy(account.proxy_url)

        # Restaura sessão
        session_str = _descriptografar_sessao(account.session_data)
        cl.set_settings(json.loads(session_str))
        cl.login_by_sessionid(cl.sessionid)

        media_path = os.path.join(INSTA_TMP_DIR, post.media_filename)
        if not os.path.exists(media_path):
            raise FileNotFoundError(f"Arquivo de mídia não encontrado: {post.media_filename}")

        caption = post.caption or ""
        media = None

        if post.post_type == "photo":
            media = cl.photo_upload(media_path, caption)
        elif post.post_type == "video":
            media = cl.video_upload(media_path, caption)
        elif post.post_type == "reel":
            media = cl.clip_upload(media_path, caption)
        elif post.post_type == "story_photo":
            media = cl.photo_upload_to_story(media_path)
        elif post.post_type == "story_video":
            media = cl.video_upload_to_story(media_path)
        else:
            raise ValueError(f"post_type inválido: {post.post_type}")

        post.status      = "done"
        post.sent_at     = now_brazil()
        post.ig_media_id = str(media.id) if media else None
        db.commit()

        _registrar_log(db, post.user_id, "post_done", post.account_id, {
            "post_id": post_id,
            "post_type": post.post_type,
            "ig_media_id": post.ig_media_id,
        })

    except Exception as e:
        logger.error(f"[INSTAGRAM] erro post#{post_id}: {e}")
        if post:
            post.status    = "error"
            post.error_msg = str(e)[:1000]
            db.commit()
            _registrar_log(db, post.user_id, "post_error", post.account_id, {
                "post_id": post_id,
                "error": str(e)[:500],
            })
    finally:
        db.close()


# ==========================================
# SCHEDULER — Job de execução automática
# ==========================================

def processar_posts_pendentes():
    """
    Verifica a cada minuto se há posts com status='pending'
    e scheduled_for <= agora, e dispara _executar_post() em thread separada.

    Este job é registrado no engine.py (start_engine) junto com os outros jobs.
    """
    db = SessionLocal()
    try:
        agora = now_brazil()
        posts = db.query(InstagramPost).filter(
            InstagramPost.status == "pending",
            InstagramPost.scheduled_for <= agora,
        ).all()

        for post in posts:
            logger.info(f"[INSTAGRAM] Disparando post#{post.id} para conta#{post.account_id}")
            t = threading.Thread(target=_executar_post, args=(post.id,), daemon=True)
            t.start()
    except Exception as e:
        logger.error(f"[INSTAGRAM] Erro no job de posts pendentes: {e}")
    finally:
        db.close()


# ==========================================
# ROTAS — CONTAS
# ==========================================

@router.post("/accounts", summary="Vincula nova conta Instagram")
async def vincular_conta(
    conta: AccountCreate,
    background_tasks: BackgroundTasks,
    credentials: HTTPAuthorizationCredentials = Depends(security),
    user_id: str = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Recebe username + senha, faz login via instagrapi,
    salva sessão criptografada e device fingerprint.
    A senha NÃO é persistida.

    Se o Instagram exigir challenge (verificação por email/SMS),
    retorna {'status': 'challenge_required', 'account_id': id}
    e o frontend deve chamar POST /accounts/{id}/verify com o código.
    """
    # 1. Verificar acesso Prime
    token = credentials.credentials
    prime_ok = await verificar_prime_instagram(token)
    if not prime_ok:
        admin = db.query(AutopostAdmin).filter(AutopostAdmin.user_id == user_id).first()
        if not admin:
            raise HTTPException(
                status_code=403,
                detail="Acesso negado: desbloqueie o recurso 'Instagram Farm' na plataforma Zenyx VIPs."
            )

    # 2. Verificar se conta já está vinculada
    existente = db.query(InstagramAccount).filter(
        InstagramAccount.user_id == user_id,
        InstagramAccount.ig_username == conta.ig_username.lower().strip(),
    ).first()
    if existente:
        raise HTTPException(status_code=400, detail="Esta conta do Instagram já está vinculada.")

    # 3. Criar registro no banco (status pending antes do login)
    nova_conta = InstagramAccount(
        user_id=user_id,
        ig_username=conta.ig_username.lower().strip(),
        proxy_url=conta.proxy_url,
        login_status="pending",
    )
    db.add(nova_conta)
    db.commit()
    db.refresh(nova_conta)

    # 4. Tentar login
    try:
        from instagrapi.exceptions import ChallengeRequired
        cl, session_json, device_json = _fazer_login_instagram(
            conta.ig_username, conta.ig_password, conta.proxy_url
        )
        # Login OK — salva sessão criptografada
        nova_conta.session_data  = _criptografar_sessao(session_json)
        nova_conta.device_json   = device_json
        nova_conta.login_status  = "ok"
        nova_conta.last_login_at = now_brazil()
        db.commit()

        _registrar_log(db, user_id, "account_added", nova_conta.id, {
            "ig_username": nova_conta.ig_username,
            "has_proxy": bool(conta.proxy_url),
        })

        return {
            "status": "ok",
            "account_id": nova_conta.id,
            "ig_username": nova_conta.ig_username,
            "message": "Conta vinculada com sucesso!",
        }

    except ChallengeRequired:
        # Armazena client temporário para verificação do código
        nova_conta.login_status = "challenge_required"
        db.commit()
        _pending_challenges[str(nova_conta.id)] = {
            "ig_username": conta.ig_username,
            "ig_password": conta.ig_password,
            "proxy_url": conta.proxy_url,
        }
        _registrar_log(db, user_id, "login_challenge", nova_conta.id, {
            "ig_username": nova_conta.ig_username,
        })
        return {
            "status": "challenge_required",
            "account_id": nova_conta.id,
            "message": "O Instagram pediu verificação. Insira o código recebido por email/SMS.",
        }

    except Exception as e:
        nova_conta.login_status = "error"
        db.commit()
        erro_msg = str(e)
        logger.error(f"[INSTAGRAM] Erro ao vincular conta '{conta.ig_username}': {erro_msg}")
        _registrar_log(db, user_id, "login_error", nova_conta.id, {"error": erro_msg[:300]})
        raise HTTPException(status_code=400, detail=erro_msg)


@router.post("/accounts/{account_id}/verify", summary="Confirma código de challenge do Instagram")
async def verificar_challenge(
    account_id: int,
    body: ChallengeVerify,
    user_id: str = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Quando o Instagram exige verificação (email/SMS), o frontend chama esta rota
    com o código recebido. Semelhante ao /api/telegram/verify-code do main.py.
    """
    account = db.query(InstagramAccount).filter(
        InstagramAccount.id == account_id,
        InstagramAccount.user_id == user_id,
    ).first()
    if not account:
        raise HTTPException(status_code=404, detail="Conta não encontrada.")

    pending = _pending_challenges.get(str(account_id))
    if not pending:
        raise HTTPException(status_code=400, detail="Nenhum challenge pendente para esta conta. Tente vincular novamente.")

    try:
        from instagrapi import Client
        from instagrapi.exceptions import ChallengeRequired

        cl = Client()
        if pending.get("proxy_url"):
            cl.set_proxy(pending["proxy_url"])

        cl.set_locale("pt_BR")
        cl.set_timezone_offset(-10800)
        cl.login(pending["ig_username"], pending["ig_password"], verification_code=body.code)

        session_json = json.dumps(cl.get_settings())
        device_json  = json.dumps({"device_type": cl.device_type, "user_agent": cl.user_agent})

        account.session_data  = _criptografar_sessao(session_json)
        account.device_json   = device_json
        account.login_status  = "ok"
        account.last_login_at = now_brazil()
        db.commit()

        del _pending_challenges[str(account_id)]

        _registrar_log(db, user_id, "account_added", account.id, {
            "ig_username": account.ig_username,
            "via": "challenge_verify",
        })

        return {"status": "ok", "message": "Conta verificada e vinculada com sucesso!"}

    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Erro ao verificar código: {str(e)}")


@router.get("/accounts", summary="Lista contas Instagram vinculadas")
async def listar_contas(
    user_id: str = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Lista todas as contas Instagram do usuário. Não expõe session_data."""
    contas = db.query(InstagramAccount).filter(
        InstagramAccount.user_id == user_id
    ).order_by(InstagramAccount.created_at.desc()).all()

    return [
        {
            "id": c.id,
            "ig_username": c.ig_username,
            "proxy_url": c.proxy_url,
            "is_active": c.is_active,
            "login_status": c.login_status,
            "last_login_at": c.last_login_at.isoformat() if c.last_login_at else None,
            "created_at": c.created_at.isoformat() if c.created_at else None,
        }
        for c in contas
    ]


@router.get("/accounts/{account_id}/status", summary="Status de login de uma conta")
async def status_conta(
    account_id: int,
    user_id: str = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    account = db.query(InstagramAccount).filter(
        InstagramAccount.id == account_id,
        InstagramAccount.user_id == user_id,
    ).first()
    if not account:
        raise HTTPException(status_code=404, detail="Conta não encontrada.")

    pending_posts = db.query(InstagramPost).filter(
        InstagramPost.account_id == account_id,
        InstagramPost.status == "pending",
    ).count()

    return {
        "id": account.id,
        "ig_username": account.ig_username,
        "login_status": account.login_status,
        "is_active": account.is_active,
        "has_session": account.session_data is not None,
        "has_proxy": account.proxy_url is not None,
        "pending_posts": pending_posts,
        "last_login_at": account.last_login_at.isoformat() if account.last_login_at else None,
    }


@router.delete("/accounts/{account_id}", summary="Desvincula conta Instagram")
async def desvincular_conta(
    account_id: int,
    user_id: str = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Remove a conta e todos os posts/logs associados (cascade)."""
    account = db.query(InstagramAccount).filter(
        InstagramAccount.id == account_id,
        InstagramAccount.user_id == user_id,
    ).first()
    if not account:
        raise HTTPException(status_code=404, detail="Conta não encontrada.")

    ig_username = account.ig_username
    db.delete(account)
    db.commit()

    _registrar_log(db, user_id, "account_removed", None, {"ig_username": ig_username})

    return {"message": f"Conta @{ig_username} desvinculada com sucesso!"}


# ==========================================
# ROTAS — POSTS
# ==========================================

@router.post("/posts", summary="Agenda um post para uma conta Instagram")
async def agendar_post(
    account_id: int = Form(...),
    post_type: str = Form("photo"),
    caption: Optional[str] = Form(None),
    scheduled_for: str = Form(...),
    arquivo: UploadFile = File(...),
    credentials: HTTPAuthorizationCredentials = Depends(security),
    user_id: str = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Agenda um post para uma conta Instagram vinculada.
    Recebe o arquivo de mídia via multipart/form-data.
    O post é salvo na fila (status=pending) e executado automaticamente
    pelo scheduler quando chegar o horário.
    """
    # 1. Verificar Prime
    token = credentials.credentials
    prime_ok = await verificar_prime_instagram(token)
    if not prime_ok:
        admin = db.query(AutopostAdmin).filter(AutopostAdmin.user_id == user_id).first()
        if not admin:
            raise HTTPException(
                status_code=403,
                detail="Acesso negado: desbloqueie o recurso 'Instagram Farm' na plataforma Zenyx VIPs."
            )

    # 2. Verificar conta
    account = db.query(InstagramAccount).filter(
        InstagramAccount.id == account_id,
        InstagramAccount.user_id == user_id,
        InstagramAccount.is_active == True,
    ).first()
    if not account:
        raise HTTPException(status_code=404, detail="Conta não encontrada ou inativa.")
    if account.login_status != "ok":
        raise HTTPException(
            status_code=400,
            detail=f"A conta @{account.ig_username} não está com login ativo (status: {account.login_status})."
        )

    # 3. Validar post_type
    tipos_validos = {"photo", "video", "reel", "story_photo", "story_video"}
    if post_type not in tipos_validos:
        raise HTTPException(status_code=400, detail=f"post_type inválido. Aceitos: {sorted(tipos_validos)}")

    # 4. Validar e parsear scheduled_for
    try:
        from pytz import timezone as tz
        # Aceita ISO 8601 com ou sem timezone
        if "T" in scheduled_for:
            dt_agendado = datetime.fromisoformat(scheduled_for.replace("Z", "+00:00"))
        else:
            dt_agendado = datetime.strptime(scheduled_for, "%Y-%m-%d %H:%M:%S")
            dt_agendado = tz("America/Sao_Paulo").localize(dt_agendado)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail="Formato de data inválido. Use ISO 8601: '2025-10-15T14:30:00' ou '2025-10-15 14:30:00'"
        )

    # 5. Salvar arquivo de mídia
    conteudo = await arquivo.read()
    if len(conteudo) == 0:
        raise HTTPException(status_code=400, detail="Arquivo de mídia vazio.")
    if len(conteudo) > 500 * 1024 * 1024:
        raise HTTPException(status_code=413, detail="Arquivo muito grande. Limite: 500MB.")

    media_nome = _salvar_midia(conteudo, arquivo.filename or "media.jpg")

    # 6. Criar post na fila
    novo_post = InstagramPost(
        account_id=account_id,
        user_id=user_id,
        post_type=post_type,
        caption=caption,
        media_filename=media_nome,
        scheduled_for=dt_agendado,
        status="pending",
    )
    db.add(novo_post)
    db.commit()
    db.refresh(novo_post)

    return {
        "post_id": novo_post.id,
        "status": "pending",
        "ig_username": account.ig_username,
        "post_type": post_type,
        "scheduled_for": dt_agendado.isoformat(),
        "message": "Post agendado com sucesso! Será publicado automaticamente no horário definido.",
    }


@router.get("/posts", summary="Lista fila de posts agendados")
async def listar_posts(
    user_id: str = Depends(get_current_user),
    db: Session = Depends(get_db),
    status_filter: Optional[str] = None,
    account_id: Optional[int] = None,
    limit: int = 50,
):
    """Lista posts do usuário, com filtros opcionais por status e conta."""
    query = db.query(InstagramPost).filter(InstagramPost.user_id == user_id)

    if status_filter:
        query = query.filter(InstagramPost.status == status_filter)
    if account_id:
        query = query.filter(InstagramPost.account_id == account_id)

    posts = query.order_by(InstagramPost.scheduled_for.desc()).limit(limit).all()

    result = []
    for p in posts:
        account = db.query(InstagramAccount).filter(InstagramAccount.id == p.account_id).first()
        result.append({
            "id": p.id,
            "account_id": p.account_id,
            "ig_username": account.ig_username if account else "—",
            "post_type": p.post_type,
            "caption": p.caption,
            "media_filename": p.media_filename,
            "scheduled_for": p.scheduled_for.isoformat() if p.scheduled_for else None,
            "status": p.status,
            "sent_at": p.sent_at.isoformat() if p.sent_at else None,
            "error_msg": p.error_msg,
            "ig_media_id": p.ig_media_id,
            "created_at": p.created_at.isoformat() if p.created_at else None,
        })
    return result


@router.delete("/posts/{post_id}", summary="Cancela/remove post da fila")
async def cancelar_post(
    post_id: int,
    user_id: str = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    post = db.query(InstagramPost).filter(
        InstagramPost.id == post_id,
        InstagramPost.user_id == user_id,
    ).first()
    if not post:
        raise HTTPException(status_code=404, detail="Post não encontrado.")
    if post.status == "processing":
        raise HTTPException(status_code=400, detail="Post está sendo processado agora. Aguarde.")

    # Remove arquivo de mídia do disco se existir
    if post.media_filename:
        try:
            os.remove(os.path.join(INSTA_TMP_DIR, post.media_filename))
        except FileNotFoundError:
            pass

    db.delete(post)
    db.commit()
    return {"message": "Post removido da fila com sucesso!"}


# ==========================================
# ROTAS — LOGS
# ==========================================

@router.get("/logs", summary="Histórico de ações das contas Instagram")
async def listar_logs(
    user_id: str = Depends(get_current_user),
    db: Session = Depends(get_db),
    account_id: Optional[int] = None,
    limit: int = 100,
):
    query = db.query(InstagramLog).filter(InstagramLog.user_id == user_id)
    if account_id:
        query = query.filter(InstagramLog.account_id == account_id)

    logs = query.order_by(InstagramLog.created_at.desc()).limit(limit).all()

    return [
        {
            "id": l.id,
            "account_id": l.account_id,
            "action": l.action,
            "details": l.details,
            "created_at": l.created_at.isoformat() if l.created_at else None,
        }
        for l in logs
    ]


@router.delete("/logs", summary="Limpa histórico de logs do Instagram")
async def limpar_logs(
    user_id: str = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    deleted = db.query(InstagramLog).filter(InstagramLog.user_id == user_id).delete()
    db.commit()
    return {"message": f"{deleted} registros de log removidos."}