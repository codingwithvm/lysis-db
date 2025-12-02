from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .api.v1.processes.router import router as processo_router
from .api.v1.status.router import router as status_router

app = FastAPI(
  title="Lysis DB API",
  version="1.0.0",
  docs_url="/docs",
  redoc_url="/redoc"
)

origins = [
    "http://localhost:4546",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(processo_router)
app.include_router(status_router)

@app.get("/")
def root():
    return {"message": "Lysis API DB working!"}
