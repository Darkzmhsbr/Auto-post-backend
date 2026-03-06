from fastapi import FastAPI

app = FastAPI(title="Zenyx AutoPost API")

@app.get("/")
def read_root():
    return {"status": "online", "message": "Zenyx AutoPost Backend operando com sucesso!"}