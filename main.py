import os
from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.orm import Session
from pydantic import BaseModel
from typing import List, Optional
from datetime import time
from jose import jwt, JWTError

# Importações do nosso banco de dados
from database import init_db, SessionLocal, AutopostChannel, AutopostSession

# 1. INICIALIZAÇÃO
init_db()
app = FastAPI(title="Zenyx AutoPost API", version="1.0")

# 2. CONFIGURAÇÃO CORS (Permite o Frontend conversar com este Backend)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "https://autopost.zenyxvips.com"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==========================================
# 3. SISTEMA DE AUTENTICAÇÃO (O "SSO")
# ==========================================
security = HTTPBearer()
SECRET_KEY = os.getenv("SECRET_KEY", "chave-secreta-padrao") 
ALGORITHM = "HS256"

# Função que extrai o usuário do Token do Zenyx VIPs
def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    token = credentials.credentials
    try:
        # Tenta descriptografar o token usando a mesma chave da plataforma principal
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM], options={"verify_aud": False})
        
        # Extrai o ID do usuário (geralmente vem no 'sub' ou 'user_id')
        user_id = payload.get("sub") or payload.get("user_id") or payload.get("id")
        
        if user_id is None:
            raise HTTPException(status_code=401, detail="Token inválido: ID não encontrado")
            
        return int(user_id)
    except JWTError:
        raise HTTPException(status_code=401, detail="Token expirado ou inválido. Faça login novamente.")

# Função para conectar no banco em cada requisição
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# ==========================================
# 4. MODELOS DE DADOS (Pydantic - Validação)
# ==========================================
class ChannelCreate(BaseModel):
    origin_channel_id: int
    origin_channel_name: str
    dest_channel_id: int
    dest_channel_name: str
    channel_type: str
    interval_minutes: int
    cta_find: Optional[str] = None
    cta_replace: Optional[str] = None

class ChannelResponse(ChannelCreate):
    id: int
    is_active: bool
    total_forwarded: int

    class Config:
        from_attributes = True

# ==========================================
# 5. ROTAS DA API (Endpoints)
# ==========================================

@app.get("/")
def read_root():
    return {"status": "online", "message": "Zenyx AutoPost Backend operando com sucesso!"}

@app.get("/api/auth/me")
def verify_auth(user_id: int = Depends(get_current_user)):
    """Rota para o Frontend testar se o login do usuário é válido aqui no AutoPost"""
    return {"status": "success", "user_id": user_id, "message": "Autenticação via Zenyx VIPs confirmada!"}

# --- ROTAS DE CANAIS ---

@app.get("/api/autopost/channels", response_model=List[ChannelResponse])
def list_channels(user_id: int = Depends(get_current_user), db: Session = Depends(get_db)):
    """Lista todos os pares de canais configurados pelo usuário"""
    canais = db.query(AutopostChannel).filter(AutopostChannel.user_id == user_id).all()
    return canais

@app.post("/api/autopost/channels", response_model=ChannelResponse)
def create_channel(canal: ChannelCreate, user_id: int = Depends(get_current_user), db: Session = Depends(get_db)):
    """Cria uma nova regra de postagem (Origem -> Destino)"""
    novo_canal = AutopostChannel(
        user_id=user_id,
        origin_channel_id=canal.origin_channel_id,
        origin_channel_name=canal.origin_channel_name,
        dest_channel_id=canal.dest_channel_id,
        dest_channel_name=canal.dest_channel_name,
        channel_type=canal.channel_type,
        interval_minutes=canal.interval_minutes,
        cta_find=canal.cta_find,
        cta_replace=canal.cta_replace
    )
    db.add(novo_canal)
    db.commit()
    db.refresh(novo_canal)
    return novo_canal

@app.delete("/api/autopost/channels/{channel_id}")
def delete_channel(channel_id: int, user_id: int = Depends(get_current_user), db: Session = Depends(get_db)):
    """Deleta um par de canais"""
    canal = db.query(AutopostChannel).filter(AutopostChannel.id == channel_id, AutopostChannel.user_id == user_id).first()
    if not canal:
        raise HTTPException(status_code=404, detail="Configuração de canal não encontrada.")
    
    db.delete(canal)
    db.commit()
    return {"message": "Canal removido com sucesso!"}

@app.post("/api/autopost/channels/{channel_id}/toggle")
def toggle_channel(channel_id: int, user_id: int = Depends(get_current_user), db: Session = Depends(get_db)):
    """Pausa ou Continua as postagens de um canal específico"""
    canal = db.query(AutopostChannel).filter(AutopostChannel.id == channel_id, AutopostChannel.user_id == user_id).first()
    if not canal:
        raise HTTPException(status_code=404, detail="Configuração de canal não encontrada.")
    
    canal.is_active = not canal.is_active
    db.commit()
    return {"message": f"Canal {'ativado' if canal.is_active else 'pausado'} com sucesso!", "is_active": canal.is_active}

# --- ROTA DE STATUS GERAL ---
@app.get("/api/autopost/stats")
def get_stats(user_id: int = Depends(get_current_user), db: Session = Depends(get_db)):
    """Retorna dados para o Dashboard"""
    total_canais = db.query(AutopostChannel).filter(AutopostChannel.user_id == user_id).count()
    canais_ativos = db.query(AutopostChannel).filter(AutopostChannel.user_id == user_id, AutopostChannel.is_active == True).count()
    
    return {
        "total_canais_configurados": total_canais,
        "canais_ativos": canais_ativos,
        "status_sessao": "pendente" # Atualizaremos isso quando integrarmos o Telethon
    }