# -*- coding: utf-8 -*-
from fastapi import FastAPI, Depends, HTTPException, Query
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.middleware.cors import CORSMiddleware
import mysql.connector
import jwt
import datetime
from typing import Optional

app = FastAPI(title="Emploi France API", version="1.0.0")

app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

SECRET_KEY = "emploi_france_secret_2026"
ALGORITHM = "HS256"
security = HTTPBearer()

DB_CONFIG = {
    "host": "localhost",
    "port": 3306,
    "user": "spark",
    "password": "spark123",
    "database": "emploi_france"
}

def get_db():
    conn = mysql.connector.connect(**DB_CONFIG)
    try:
        yield conn
    finally:
        conn.close()

def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    try:
        payload = jwt.decode(credentials.credentials, SECRET_KEY, algorithms=[ALGORITHM])
        return payload
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expire")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Token invalide")

@app.get("/")
def root():
    return {"message": "Emploi France API", "version": "1.0.0", "status": "running"}

@app.post("/auth/token")
def get_token(username: str = "admin", password: str = "admin123"):
    if username == "admin" and password == "admin123":
        payload = {
            "sub": username,
            "exp": datetime.datetime.utcnow() + datetime.timedelta(hours=24)
        }
        token = jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)
        return {"access_token": token, "token_type": "bearer"}
    raise HTTPException(status_code=401, detail="Identifiants invalides")

@app.get("/datamarts/top-metiers")
def get_top_metiers(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    country: Optional[str] = None,
    token=Depends(verify_token),
    db=Depends(get_db)
):
    offset = (page - 1) * page_size
    cursor = db.cursor(dictionary=True)
    
    where = "WHERE country = %s" if country else ""
    params_count = [country] if country else []
    params_data = ([country] if country else []) + [page_size, offset]
    
    cursor.execute(f"SELECT COUNT(*) as total FROM datamart_top_metiers {where}", params_count)
    total = cursor.fetchone()["total"]
    
    cursor.execute(f"""
        SELECT job_title, country, work_type, nb_offres, avg_demandeurs, rank_in_country
        FROM datamart_top_metiers {where}
        ORDER BY country, rank_in_country
        LIMIT %s OFFSET %s
    """, params_data)
    
    data = cursor.fetchall()
    return {
        "page": page,
        "page_size": page_size,
        "total": total,
        "total_pages": (total + page_size - 1) // page_size,
        "data": data
    }

@app.get("/datamarts/chomage-vs-offres")
def get_chomage_vs_offres(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    token=Depends(verify_token),
    db=Depends(get_db)
):
    offset = (page - 1) * page_size
    cursor = db.cursor(dictionary=True)
    cursor.execute("SELECT COUNT(*) as total FROM datamart_chomage_vs_offres")
    total = cursor.fetchone()["total"]
    cursor.execute("""
        SELECT country, nb_offres_total, avg_demandeurs, ratio_offres_demandeurs
        FROM datamart_chomage_vs_offres
        ORDER BY nb_offres_total DESC
        LIMIT %s OFFSET %s
    """, [page_size, offset])
    return {"page": page, "page_size": page_size, "total": total,
            "total_pages": (total + page_size - 1) // page_size,
            "data": cursor.fetchall()}

@app.get("/datamarts/profils-demandes")
def get_profils_demandes(
    token=Depends(verify_token),
    db=Depends(get_db)
):
    cursor = db.cursor(dictionary=True)
    cursor.execute("SELECT * FROM datamart_profils_demandes ORDER BY avg_offres_par_metier DESC")
    return {"data": cursor.fetchall()}
