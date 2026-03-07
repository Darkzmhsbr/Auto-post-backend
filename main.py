from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from database import init_db

# Inicializa o Banco de Dados ao iniciar a aplicação
init_db()

app = FastAPI(title="Zenyx AutoPost API", version="1.0")

# ==========================================
# CONFIGURAÇÃO DE CORS (Segurança de acesso)
# ==========================================
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",             # Para você testar localmente depois
        "https://autopost.zenyxvips.com",    # Seu frontend de produção!
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def read_root():
    return {
        "status": "online", 
        "message": "Zenyx AutoPost Backend operando com sucesso!",
        "database": "conectado e tabelas criadas"
    }

# Aqui ficarão as rotas de sessão, canais e logs que criaremos em seguida!