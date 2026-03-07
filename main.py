import os
from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.orm import Session
from pydantic import BaseModel
from typing import List, Optional
from datetime import time
from jose import jwt, JWTError

from database import init_db, SessionLocal, AutopostChannel, AutopostSession

init_db()
app = FastAPI(title="Zenyx AutoPost API", version="1.0")

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

# ==========================================
# CÉREBRO DA AUTENTICAÇÃO (Corrigido para aceitar Textos/Emails)
# ==========================================
def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    token = credentials.credentials
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM], options={"verify_aud": False})
        
        # Pega a informação que estiver no token (seja email, username ou ID)
        user_id = payload.get("sub") or payload.get("id") or payload.get("user_id")
        
        if not user_id:
            raise HTTPException(status_code=401, detail="Token inválido: Usuário não encontrado")
            
        return str(user_id) # <-- A MÁGICA AQUI: Retorna sempre como Texto Seguro!
    except JWTError:
        raise HTTPException(status_code=401, detail="Token expirado ou inválido.")

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

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
# ROTAS
# ==========================================
@app.get("/")
def read_root():
    return {"status": "online", "message": "Zenyx AutoPost Backend operando com sucesso!"}

@app.get("/api/auth/me")
def verify_auth(user_id: str = Depends(get_current_user)): # Mudou para str
    return {"status": "success", "user_id": user_id, "message": "Autenticação confirmada!"}

@app.get("/api/autopost/channels", response_model=List[ChannelResponse])
def list_channels(user_id: str = Depends(get_current_user), db: Session = Depends(get_db)): # Mudou para str
    canais = db.query(AutopostChannel).filter(AutopostChannel.user_id == user_id).all()
    return canais

@app.post("/api/autopost/channels", response_model=ChannelResponse)
def create_channel(canal: ChannelCreate, user_id: str = Depends(get_current_user), db: Session = Depends(get_db)):
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

@app.get("/api/autopost/stats")
def get_stats(user_id: str = Depends(get_current_user), db: Session = Depends(get_db)):
    total_canais = db.query(AutopostChannel).filter(AutopostChannel.user_id == user_id).count()
    canais_ativos = db.query(AutopostChannel).filter(AutopostChannel.user_id == user_id, AutopostChannel.is_active == True).count()
    
    return {
        "total_canais_configurados": total_canais,
        "canais_ativos": canais_ativos,
        "status_sessao": "pendente"
    }