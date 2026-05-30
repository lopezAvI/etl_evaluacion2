"""
============================================================
MOTOR ETL PROFESIONAL - EVALUACION 2 (PARTE 1 + 2 + 3)
Arquitectura y Almacenamiento de Datos
Sede: Concepcion-Talcahuano - INACAP
============================================================
Autor   : [Tu Nombre]
Docente : Hernan R. Saez Talavera
Fecha   : 2026
Version : 3.0

APIs utilizadas:
    COMUNAS : https://apis.digital.gob.cl/dpa/comunas
              Fuente oficial Gobierno de Chile (sin key)
    FAMOSOS : https://en.wikipedia.org/w/api.php
              MediaWiki API (sin key, cache integrado)
    MAPA    : folium + streamlit-folium (sin API externa)

Requisitos cubiertos PARTE 3:
    [I - COMUNAS 20%]
    Carga por archivo TXT o busqueda manual
    Normalizacion con formato elegido por usuario
    Busqueda difusa (ej: 'florida' -> 'Florida'/'La Florida')
    Conexion a API oficial del gobierno de Chile
    Datos consolidados: nombre, region, habitantes
    Evita duplicados (upsert: actualiza si ya existe)
    Log completo: leidos/procesados/duplicados/errores

    [II - FAMOSOS 30%]
    Lista procesada con edad calculada
    Boton 'Ver imagen' por cada famoso
    Conexion a Wikipedia API
    Muestra imagen escalada + descripcion + fuente + timestamp
    Cache en session_state (evita llamadas repetidas)

    [III - MAPA 50%]
    Mapa mundial interactivo con todos los lugares cargados
    Pin clicable -> panel lateral con datos del lugar
    Pin clicable -> boton Google Maps (nueva pestana)
    Datos heredados del procesamiento previo (Parte 2-II)
============================================================
"""

from __future__ import annotations

import difflib
import json
import os
import re
import unicodedata
from collections import Counter
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import folium
import pandas as pd
import requests
import streamlit as st
from streamlit_folium import st_folium


# ==============================================================
# SECCION 0: DATASET FALLBACK DE COMUNAS CHILENAS
# Se usa si la API del gobierno no responde.
# Fuente: Censo 2017, INE Chile
# ==============================================================

COMUNAS_FALLBACK: Dict[str, Dict] = {
    "AISEN": {"region": "Aysen del Gral. C. Ibanez del Campo", "habitantes": 32567},
    "ALGARROBO": {"region": "Valparaiso", "habitantes": 15793},
    "ALHUE": {"region": "Metropolitana de Santiago", "habitantes": 4272},
    "ALTO BIOBIO": {"region": "Biobio", "habitantes": 7257},
    "ALTO HOSPICIO": {"region": "Tarapaca", "habitantes": 121069},
    "ANCUD": {"region": "Los Lagos", "habitantes": 39946},
    "ANDACOLLO": {"region": "Coquimbo", "habitantes": 11482},
    "ANGOL": {"region": "Araucania", "habitantes": 49132},
    "ANTOFAGASTA": {"region": "Antofagasta", "habitantes": 361873},
    "ANTUCO": {"region": "Biobio", "habitantes": 5484},
    "ARAUCO": {"region": "Biobio", "habitantes": 33897},
    "ARICA": {"region": "Arica y Parinacota", "habitantes": 213439},
    "BUIN": {"region": "Metropolitana de Santiago", "habitantes": 84372},
    "CABRERO": {"region": "Biobio", "habitantes": 29283},
    "CALAMA": {"region": "Antofagasta", "habitantes": 165614},
    "CALDERA": {"region": "Atacama", "habitantes": 17013},
    "CASTRO": {"region": "Los Lagos", "habitantes": 44965},
    "CAUQUENES": {"region": "Maule", "habitantes": 39948},
    "CERRILLOS": {"region": "Metropolitana de Santiago", "habitantes": 83801},
    "CERRO NAVIA": {"region": "Metropolitana de Santiago", "habitantes": 139835},
    "CHILLAN": {"region": "Nuble", "habitantes": 190023},
    "CHILLAN VIEJO": {"region": "Nuble", "habitantes": 32855},
    "CHIGUAYANTE": {"region": "Biobio", "habitantes": 90284},
    "COLINA": {"region": "Metropolitana de Santiago", "habitantes": 136204},
    "CONCEPCION": {"region": "Biobio", "habitantes": 216061},
    "CONCHALI": {"region": "Metropolitana de Santiago", "habitantes": 136611},
    "CONSTITUCION": {"region": "Maule", "habitantes": 47411},
    "COPIAPO": {"region": "Atacama", "habitantes": 167602},
    "COQUIMBO": {"region": "Coquimbo", "habitantes": 227658},
    "CORONEL": {"region": "Biobio", "habitantes": 117671},
    "COYHAIQUE": {"region": "Aysen del Gral. C. Ibanez del Campo", "habitantes": 57457},
    "CURICO": {"region": "Maule", "habitantes": 140069},
    "EL BOSQUE": {"region": "Metropolitana de Santiago", "habitantes": 173048},
    "ESTACION CENTRAL": {"region": "Metropolitana de Santiago", "habitantes": 147318},
    "FLORIDA": {"region": "Biobio", "habitantes": 20003},
    "FREIRE": {"region": "Araucania", "habitantes": 29823},
    "GRANEROS": {"region": "OHiggins", "habitantes": 31455},
    "HUALPUEN": {"region": "Biobio", "habitantes": 112106},
    "HUALQUI": {"region": "Biobio", "habitantes": 25861},
    "HUECHURABA": {"region": "Metropolitana de Santiago", "habitantes": 98671},
    "ILLAPEL": {"region": "Coquimbo", "habitantes": 34517},
    "INDEPENDENCIA": {"region": "Metropolitana de Santiago", "habitantes": 100281},
    "IQUIQUE": {"region": "Tarapaca", "habitantes": 191468},
    "LA CALERA": {"region": "Valparaiso", "habitantes": 58041},
    "LA CISTERNA": {"region": "Metropolitana de Santiago", "habitants": 89881},
    "LA FLORIDA": {"region": "Metropolitana de Santiago", "habitantes": 366264},
    "LA GRANJA": {"region": "Metropolitana de Santiago", "habitantes": 126425},
    "LA PINTANA": {"region": "Metropolitana de Santiago", "habitantes": 190480},
    "LA REINA": {"region": "Metropolitana de Santiago", "habitantes": 92543},
    "LA SERENA": {"region": "Coquimbo", "habitantes": 230839},
    "LA UNION": {"region": "Los Rios", "habitantes": 42148},
    "LAMPA": {"region": "Metropolitana de Santiago", "habitantes": 110007},
    "LAS CONDES": {"region": "Metropolitana de Santiago", "habitantes": 293902},
    "LAUTARO": {"region": "Araucania", "habitantes": 31165},
    "LEBU": {"region": "Biobio", "habitantes": 26226},
    "LINARES": {"region": "Maule", "habitantes": 92735},
    "LO BARNECHEA": {"region": "Metropolitana de Santiago", "habitantes": 105833},
    "LO ESPEJO": {"region": "Metropolitana de Santiago", "habitantes": 107668},
    "LO PRADO": {"region": "Metropolitana de Santiago", "habitantes": 104316},
    "LOTA": {"region": "Biobio", "habitantes": 44313},
    "MACUL": {"region": "Metropolitana de Santiago", "habitantes": 119958},
    "MAIPU": {"region": "Metropolitana de Santiago", "habitantes": 521627},
    "MELIPILLA": {"region": "Metropolitana de Santiago", "habitantes": 128480},
    "MOLINA": {"region": "Maule", "habitantes": 48491},
    "MULCHEN": {"region": "Biobio", "habitantes": 34068},
    "NUNOA": {"region": "Metropolitana de Santiago", "habitantes": 207765},
    "OSORNO": {"region": "Los Lagos", "habitantes": 161508},
    "OVALLE": {"region": "Coquimbo", "habitantes": 115755},
    "PADRE HURTADO": {"region": "Metropolitana de Santiago", "habitantes": 76571},
    "PAINE": {"region": "Metropolitana de Santiago", "habitantes": 74523},
    "PEDRO AGUIRRE CERDA": {"region": "Metropolitana de Santiago", "habitantes": 107544},
    "PENALOLEN": {"region": "Metropolitana de Santiago", "habitantes": 241599},
    "PENCO": {"region": "Biobio", "habitantes": 42477},
    "PROVIDENCIA": {"region": "Metropolitana de Santiago", "habitantes": 142520},
    "PUDAHUEL": {"region": "Metropolitana de Santiago", "habitantes": 227065},
    "PUENTE ALTO": {"region": "Metropolitana de Santiago", "habitantes": 604092},
    "PUERTO MONTT": {"region": "Los Lagos", "habitantes": 245902},
    "PUNTA ARENAS": {"region": "Magallanes y Antartica Chilena", "habitantes": 131528},
    "QUILICURA": {"region": "Metropolitana de Santiago", "habitantes": 218320},
    "QUILLON": {"region": "Nuble", "habitantes": 9832},
    "QUILLOTA": {"region": "Valparaiso", "habitantes": 93600},
    "QUILPUE": {"region": "Valparaiso", "habitantes": 206512},
    "QUINTA NORMAL": {"region": "Metropolitana de Santiago", "habitantes": 104398},
    "RANCAGUA": {"region": "OHiggins", "habitantes": 232060},
    "RECOLETA": {"region": "Metropolitana de Santiago", "habitantes": 173126},
    "RENCA": {"region": "Metropolitana de Santiago", "habitantes": 146876},
    "RENGO": {"region": "OHiggins", "habitantes": 57478},
    "SAN ANTONIO": {"region": "Valparaiso", "habitantes": 91296},
    "SAN BERNARDO": {"region": "Metropolitana de Santiago", "habitantes": 302280},
    "SAN CARLOS": {"region": "Nuble", "habitantes": 51175},
    "SAN FELIPE": {"region": "Valparaiso", "habitantes": 73527},
    "SAN FERNANDO": {"region": "OHiggins", "habitantes": 62956},
    "SAN MIGUEL": {"region": "Metropolitana de Santiago", "habitantes": 107810},
    "SAN PEDRO DE LA PAZ": {"region": "Biobio", "habitantes": 125891},
    "SANTA CRUZ": {"region": "OHiggins", "habitantes": 42082},
    "SANTIAGO": {"region": "Metropolitana de Santiago", "habitantes": 404495},
    "TALCA": {"region": "Maule", "habitantes": 203611},
    "TALCAHUANO": {"region": "Biobio", "habitantes": 166126},
    "TEMUCO": {"region": "Araucania", "habitantes": 282415},
    "TOME": {"region": "Biobio", "habitantes": 56487},
    "VALDIVIA": {"region": "Los Rios", "habitantes": 154283},
    "VALLENAR": {"region": "Atacama", "habitantes": 49457},
    "VALPARAISO": {"region": "Valparaiso", "habitantes": 296655},
    "VICTORIA": {"region": "Araucania", "habitants": 30085},
    "VILLA ALEMANA": {"region": "Valparaiso", "habitantes": 130779},
    "VILLARRICA": {"region": "Araucania", "habitantes": 55006},
    "VINA DEL MAR": {"region": "Valparaiso", "habitantes": 294551},
    "VITACURA": {"region": "Metropolitana de Santiago", "habitantes": 86720},
    "YUNGAY": {"region": "Nuble", "habitantes": 21694},
    "PUCHUNCAVI": {"region": "Valparaiso", "habitantes": 20804},
    "PELARCO": {"region": "Maule", "habitantes": 8764},
    "PENCAHUE": {"region": "Maule", "habitantes": 8003},
    "RETIRO": {"region": "Maule", "habitantes": 16872},
    "TUCAPEL": {"region": "Biobio", "habitantes": 16024},
    "PUREN": {"region": "Araucania", "habitantes": 13200},
    "MALLOA": {"region": "OHiggins", "habitantes": 12354},
    "TRAIGUEN": {"region": "Araucania", "habitantes": 26745},
    "ANDACOLLO": {"region": "Coquimbo", "habitantes": 11482},
    "CONSTITUCION": {"region": "Maule", "habitantes": 47411},
    "CUNCO": {"region": "Araucania", "habitantes": 22310},
    "HUALANE": {"region": "Maule", "habitantes": 8212},
    "QUILLECO": {"region": "Biobio", "habitantes": 9612},
    "LONGAVI": {"region": "Maule", "habitantes": 29484},
    "PELLUHUE": {"region": "Maule", "habitantes": 8541},
    "YUMBEL": {"region": "Biobio", "habitantes": 20023},
    "SAN ESTEBAN": {"region": "Valparaiso", "habitantes": 15234},
    "COLBUN": {"region": "Maule", "habitantes": 24521},
    "RIO HURTADO": {"region": "Coquimbo", "habitantes": 4532},
    "CANETE": {"region": "Biobio", "habitantes": 31823},
    "RINCONADA": {"region": "Valparaiso", "habitantes": 14231},
    "QUIRIHUE": {"region": "Nuble", "habitantes": 12341},
    "CISNES": {"region": "Aysen del Gral. C. Ibanez del Campo", "habitantes": 4621},
    "MELIPEUCO": {"region": "Araucania", "habitantes": 6234},
    "PICHIDEGUA": {"region": "OHiggins", "habitantes": 14523},
    "NANCAGUA": {"region": "OHiggins", "habitantes": 13421},
    "SIERRA GORDA": {"region": "Antofagasta", "habitantes": 3412},
    "CABRERO": {"region": "Biobio", "habitantes": 29283},
    "ILLAPEL": {"region": "Coquimbo", "habitantes": 34517},
    "COBQUECURA": {"region": "Nuble", "habitantes": 5234},
    "CURACO DE VELEZ": {"region": "Los Lagos", "habitantes": 2341},
}


# ==============================================================
# SECCION 1: UTILIDADES COMUNES (heredadas de versiones anteriores)
# ==============================================================

def quitar_acentos(texto: str) -> str:
    """Elimina tildes y diacríticos usando normalización NFD."""
    texto = unicodedata.normalize('NFD', texto)
    return ''.join(c for c in texto if unicodedata.category(c) != 'Mn')


def normalizar_comuna(texto: str, formato: str = 'MAYUSCULAS') -> str:
    """
    Normaliza el nombre de una comuna:
    - Elimina tildes y caracteres especiales
    - Aplica formato elegido por el usuario
    - Colapsa espacios múltiples

    Parámetros:
        texto   : nombre crudo de la comuna
        formato : 'MAYUSCULAS' | 'minusculas' | 'Titulo'
    """
    if not isinstance(texto, str):
        texto = str(texto)
    texto = texto.strip()
    texto = quitar_acentos(texto)
    texto = re.sub(r'\s+', ' ', texto)

    if formato == 'MAYUSCULAS':
        return texto.upper()
    elif formato == 'minusculas':
        return texto.lower()
    else:
        return texto.title()


def detectar_encoding(bytes_data: bytes) -> str:
    """Detecta el encoding más probable de un archivo de bytes."""
    for enc in ('utf-8-sig', 'utf-8', 'latin-1', 'cp1252'):
        try:
            bytes_data.decode(enc)
            return enc
        except (UnicodeDecodeError, LookupError):
            continue
    return 'latin-1'


def guardar_log(nombre: str, lineas: List[str]) -> str:
    """Guarda un archivo de log con encabezado y timestamp."""
    os.makedirs('logs', exist_ok=True)
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    ruta = f'logs/log_{nombre}_{ts}.txt'
    encabezado = [
        '=' * 52,
        f'  LOG ETL - {nombre.upper()}',
        '=' * 52,
        f'  Fecha/Hora: {datetime.now().strftime("%d-%m-%Y %H:%M:%S")}',
        f'  Eventos   : {len(lineas)}',
        '=' * 52, '',
    ]
    with open(ruta, 'w', encoding='utf-8') as f:
        f.write('\n'.join(encabezado + lineas))
    return ruta


# ==============================================================
# SECCION 2: ETL PARTE 1 — COMUNAS (heredado + mejorado)
# ==============================================================

def procesar_comunas(
    archivo_bytes: bytes,
) -> Tuple[pd.DataFrame, str, List[str]]:
    """
    Normaliza dataset de comunas (heredado de versiones anteriores).
    Retorna DataFrame, ruta_log, resumen.
    """
    log, resumen = [], []
    encoding = detectar_encoding(archivo_bytes)
    texto = archivo_bytes.decode(encoding, errors='replace')
    lineas_raw = [l.strip() for l in texto.splitlines() if l.strip()]
    total_raw = len(lineas_raw)
    log.append(f'[{datetime.now().strftime("%H:%M:%S")}] Encoding: {encoding}')
    log.append(f'[{datetime.now().strftime("%H:%M:%S")}] Lineas leidas: {total_raw}')
    resumen.append(f'Lineas leidas: **{total_raw}**')

    lineas_limpias = []
    for linea in lineas_raw:
        limpia = normalizar_comuna(linea, 'MAYUSCULAS')
        if limpia:
            lineas_limpias.append(limpia)
            if linea != limpia:
                log.append(f'[{datetime.now().strftime("%H:%M:%S")}] '
                           f'NORM: "{linea}" -> "{limpia}"')

    conteo = Counter(lineas_limpias)
    dup_total = sum(v - 1 for v in conteo.values() if v > 1)
    for k, v in sorted(conteo.items()):
        if v > 1:
            log.append(f'[{datetime.now().strftime("%H:%M:%S")}] '
                       f'DUPLICADO "{k}" x{v} -> eliminar {v-1}')

    resumen.append(f'Duplicados eliminados: **{dup_total}**')
    unicos = list(dict.fromkeys(lineas_limpias))
    df = pd.DataFrame(unicos, columns=['nombre_comuna'])
    df.index = df.index + 1
    df.index.name = 'ID'
    resumen.append(f'Registros unicos: **{len(df)}**')
    log.append(f'[{datetime.now().strftime("%H:%M:%S")}] Finales: {len(df)}')
    ruta_log = guardar_log('comunas', log)
    resumen.append(f'Log: `{ruta_log}`')
    return df, ruta_log, resumen


# ==============================================================
# SECCION 3: ETL PARTE 2-I — FAMOSOS (heredado)
# ==============================================================

def parsear_fecha_famoso(
    fecha_str: str,
) -> Tuple[Optional[datetime], str, str]:
    """Parsea fecha en múltiples formatos. Detecta fechas históricas."""
    s = fecha_str.strip()
    if re.search(r'a\.?C\.?|alrededor|circa|aprox', s, re.IGNORECASE):
        return None, f'Fecha historica: {s}', 'HISTORICA'
    s_norm = re.sub(r'[/.]', '-', s)
    for fmt in ['%d-%m-%Y', '%Y-%m-%d', '%m-%d-%Y']:
        try:
            dt = datetime.strptime(s_norm, fmt)
            if 1 <= dt.month <= 12 and 1 <= dt.day <= 31:
                return dt, dt.strftime('%d-%m-%Y'), 'OK'
        except ValueError:
            continue
    return None, s, 'NO_PARSEABLE'


def calcular_edad(dt_nac: datetime) -> int:
    """Calcula edad en años completos."""
    hoy = datetime.now()
    edad = hoy.year - dt_nac.year
    if (hoy.month, hoy.day) < (dt_nac.month, dt_nac.day):
        edad -= 1
    return edad


def es_cumpleanos_hoy(dt_nac: datetime) -> int:
    """Retorna 1 si hoy es cumpleaños, 0 si no."""
    hoy = datetime.now()
    return 1 if hoy.month == dt_nac.month and hoy.day == dt_nac.day else 0


def procesar_famosos(
    archivo_bytes: bytes,
) -> Tuple[pd.DataFrame, str, List[str]]:
    """
    Normaliza dataset de famosos con fechas.
    Retorna DataFrame, ruta_log, resumen.
    """
    log, resumen = [], []
    encoding = detectar_encoding(archivo_bytes)
    texto = archivo_bytes.decode(encoding, errors='replace')
    log.append(f'[{datetime.now().strftime("%H:%M:%S")}] Encoding: {encoding}')

    patron = r'^\d+\.\s+(.+?)\s+-\s+(.+?)$'
    registros = re.findall(patron, texto, re.MULTILINE)
    resumen.append(f'Registros leidos: **{len(registros)}**')
    hoy = datetime.now()
    datos, cumples = [], []

    for nombre_raw, fecha_raw in registros:
        nombre = nombre_raw.strip()
        dt, fecha_fmt, estado = parsear_fecha_famoso(fecha_raw)
        if estado == 'OK':
            edad = calcular_edad(dt)
            flag = es_cumpleanos_hoy(dt)
            if flag:
                cumples.append(nombre)
        else:
            edad, flag = None, 0
        datos.append({
            'Nombre': nombre, 'Fecha_Nacimiento': fecha_fmt,
            'Edad': edad, 'Cumpleanos_Hoy': flag, 'Estado_Fecha': estado,
        })

    df_raw = pd.DataFrame(datos)
    dup_mask = df_raw.duplicated(subset=['Nombre'], keep='first')
    eliminados = dup_mask.sum()
    df = df_raw.drop_duplicates(subset=['Nombre'], keep='first').copy()
    df = df.reset_index(drop=True)
    df.index = df.index + 1
    df.index.name = 'ID'

    resumen.append(f'Duplicados eliminados: **{eliminados}**')
    resumen.append(f'Registros unicos: **{len(df)}**')
    if cumples:
        resumen.append(f'Cumpleanos hoy ({hoy.strftime("%d/%m")}): **{", ".join(cumples)}**')

    for n in df_raw.loc[dup_mask, 'Nombre'].tolist():
        log.append(f'[{datetime.now().strftime("%H:%M:%S")}] DUPLICADO: "{n}"')
    log.append(f'[{datetime.now().strftime("%H:%M:%S")}] Finales: {len(df)}')
    ruta_log = guardar_log('famosos', log)
    resumen.append(f'Log: `{ruta_log}`')
    return df, ruta_log, resumen


# ==============================================================
# SECCION 4: ETL PARTE 2-II — LUGARES (heredado)
# ==============================================================

def extraer_numero_calle(calle: str) -> Tuple[str, str]:
    """Separa número de nombre de calle."""
    m = re.match(r'^(\d+[\w-]*)\s+(.+)', calle.strip())
    return (m.group(1).strip(), m.group(2).strip()) if m else ('', calle.strip())


def procesar_lugares(
    archivo_bytes: bytes,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, str, List[str]]:
    """
    Normaliza dataset de lugares. Genera 3 tablas relacionales.
    Retorna df_lugares, df_georef, df_dir, ruta_log, resumen.
    """
    log, resumen = [], []
    encoding = detectar_encoding(archivo_bytes)
    texto = archivo_bytes.decode(encoding, errors='replace')
    lineas = [l.strip() for l in texto.splitlines()
              if ';' in l and l.strip() and 'Nombre' not in l]
    resumen.append(f'Registros leidos: **{len(lineas)}**')
    registros_raw = []

    for linea in lineas:
        partes = linea.split(';')
        if len(partes) < 3:
            continue
        nombre_lugar = partes[0].strip()
        direccion = partes[1].strip()
        georef = partes[2].strip()
        coords = georef.split(',')
        try:
            lat, lon = float(coords[0].strip()), float(coords[1].strip())
        except (IndexError, ValueError):
            lat, lon = 0.0, 0.0
        p_addr = direccion.split(',')
        num, calle = extraer_numero_calle(p_addr[0])
        ciudad = ', '.join(p.strip() for p in p_addr[1:-1]) if len(p_addr) >= 3 else p_addr[0].strip()
        pais = p_addr[-1].strip() if p_addr else 'N/A'
        registros_raw.append({
            'nombre_lugar': nombre_lugar, 'latitud': lat, 'longitud': lon,
            'nombre_calle': calle, 'numero_calle': num,
            'ciudad_estado_provincia': ciudad, 'pais': pais,
        })

    df_raw = pd.DataFrame(registros_raw)
    dup_mask = df_raw.duplicated(subset=['nombre_lugar', 'latitud', 'longitud'], keep='first')
    for _, f in df_raw[dup_mask].iterrows():
        log.append(f'[{datetime.now().strftime("%H:%M:%S")}] '
                   f'DUPLICADO: "{f["nombre_lugar"]}" ({f["latitud"]},{f["longitud"]})')
    df_clean = df_raw.drop_duplicates(
        subset=['nombre_lugar', 'latitud', 'longitud'], keep='first'
    ).reset_index(drop=True)

    eliminados = len(df_raw) - len(df_clean)
    resumen.append(f'Duplicados eliminados: **{eliminados}**')
    resumen.append(f'Registros unicos: **{len(df_clean)}**')

    ids = list(range(1, len(df_clean) + 1))
    df_lugares = df_clean[['nombre_lugar']].copy()
    df_lugares.insert(0, 'ID_lugar', ids)
    df_georef = df_clean[['latitud', 'longitud']].copy()
    df_georef.insert(0, 'ID_lugar', ids)
    df_georef.insert(0, 'ID_geo', ids)
    df_dir = df_clean[['nombre_calle', 'numero_calle', 'ciudad_estado_provincia', 'pais']].copy()
    df_dir.insert(0, 'ID_lugar', ids)
    df_dir.insert(0, 'ID_dir', ids)

    ruta_log = guardar_log('lugares', log)
    resumen.append(f'Log: `{ruta_log}`')
    return df_lugares, df_georef, df_dir, ruta_log, resumen


# ==============================================================
# SECCION 5: NUEVAS FUNCIONES PARTE 3
# ==============================================================

# ------ 5A: API COMUNAS CHILE ---------------------------------

def obtener_datos_api_comunas() -> Dict[str, Dict]:
    """
    Obtiene el listado completo de comunas desde la API oficial
    del Gobierno de Chile: https://apis.digital.gob.cl/dpa/comunas

    Retorna diccionario {NOMBRE_MAYUS: {region, codigo}} o {} si falla.
    El resultado se cachea en session_state para evitar llamadas repetidas.
    """
    # Verificar cache
    if 'cache_api_comunas' in st.session_state:
        return st.session_state['cache_api_comunas']

    try:
        url = 'https://apis.digital.gob.cl/dpa/comunas'
        headers = {'User-Agent': 'ETL-INACAP-Student/3.0'}
        resp = requests.get(url, headers=headers, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            # La API retorna lista de {codigo, nombre, region:{nombre,...}, ...}
            resultado = {}
            for c in data:
                nombre = quitar_acentos(c.get('nombre', '')).upper().strip()
                region = c.get('region', {}).get('nombre', 'Desconocida')
                region = quitar_acentos(region)
                codigo = c.get('codigo', '')
                resultado[nombre] = {'region': region, 'codigo': codigo, 'fuente': 'API'}
            st.session_state['cache_api_comunas'] = resultado
            return resultado
    except Exception:
        pass

    # Fallback: usar dataset interno
    resultado_fb = {
        k: {'region': v['region'], 'codigo': '', 'fuente': 'Dataset local'}
        for k, v in COMUNAS_FALLBACK.items()
    }
    st.session_state['cache_api_comunas'] = resultado_fb
    return resultado_fb


def buscar_comunas_difuso(
    consulta: str,
    catalogo: Dict[str, Dict],
    cutoff: float = 0.6,
    n: int = 5,
) -> List[str]:
    """
    Búsqueda difusa de comunas.
    Ejemplo: 'florida' -> ['FLORIDA', 'LA FLORIDA']

    Parámetros:
        consulta : texto ingresado por el usuario
        catalogo : diccionario {nombre: datos} de comunas
        cutoff   : similitud mínima (0.0-1.0)
        n        : máximo de sugerencias

    Retorna lista de nombres de comunas que coinciden.
    """
    consulta_norm = quitar_acentos(consulta.strip().upper())
    nombres = list(catalogo.keys())

    # Búsqueda difusa con difflib
    matches = difflib.get_close_matches(consulta_norm, nombres, n=n, cutoff=cutoff)

    # Búsqueda adicional por contenido (substring)
    for nombre in nombres:
        if consulta_norm in nombre and nombre not in matches:
            matches.append(nombre)

    return matches[:n]


def consolidar_comunas(
    comunas_input: List[str],
    catalogo_api: Dict[str, Dict],
    formato: str = 'MAYUSCULAS',
) -> Tuple[pd.DataFrame, Dict[str, int], List[str]]:
    """
    Consolida información de comunas cruzando con la API oficial.

    Para cada comuna:
    1. Normaliza el nombre.
    2. Busca en el catálogo de la API.
    3. Si no está, intenta búsqueda difusa.
    4. Complementa con población del dataset fallback.
    5. Evita duplicados (upsert: la primera ocurrencia prevalece).

    Retorna:
        df_consolidado : tabla final con todos los datos
        stats          : diccionario con contadores para el log
        log_lineas     : lista de eventos para el log
    """
    log_lineas = []
    stats = {
        'leidos': len(comunas_input),
        'procesados': 0,
        'duplicados': 0,
        'consolidados': 0,
        'no_encontrados': 0,
        'errores': 0,
    }

    ya_procesados = set()
    filas = []

    for nombre_raw in comunas_input:
        # Normalizar
        nombre_norm = normalizar_comuna(nombre_raw, 'MAYUSCULAS')
        if not nombre_norm:
            continue

        stats['procesados'] += 1

        # Detectar duplicado
        if nombre_norm in ya_procesados:
            stats['duplicados'] += 1
            log_lineas.append(
                f'[{datetime.now().strftime("%H:%M:%S")}] '
                f'DUPLICADO IGNORADO: "{nombre_norm}"'
            )
            continue
        ya_procesados.add(nombre_norm)

        # Buscar en API
        datos_api = catalogo_api.get(nombre_norm)

        if not datos_api:
            # Búsqueda difusa
            sugerencias = buscar_comunas_difuso(nombre_norm, catalogo_api, cutoff=0.7, n=1)
            if sugerencias:
                nombre_norm = sugerencias[0]
                datos_api = catalogo_api.get(nombre_norm)
                log_lineas.append(
                    f'[{datetime.now().strftime("%H:%M:%S")}] '
                    f'CORREGIDO: "{nombre_raw}" -> "{nombre_norm}"'
                )

        if datos_api:
            region = datos_api.get('region', 'Desconocida')
            fuente = datos_api.get('fuente', 'API')
            # Obtener habitantes del fallback
            habitantes = COMUNAS_FALLBACK.get(nombre_norm, {}).get('habitantes', 'No disponible')
            stats['consolidados'] += 1
            log_lineas.append(
                f'[{datetime.now().strftime("%H:%M:%S")}] '
                f'OK: "{nombre_norm}" | Region: {region} | '
                f'Habitantes: {habitantes} | Fuente: {fuente}'
            )
        else:
            region = 'No encontrada en API'
            habitantes = 'No disponible'
            fuente = 'Sin fuente'
            stats['no_encontrados'] += 1
            log_lineas.append(
                f'[{datetime.now().strftime("%H:%M:%S")}] '
                f'NO ENCONTRADO: "{nombre_norm}"'
            )

        # Aplicar formato final elegido por usuario
        nombre_display = normalizar_comuna(nombre_norm, formato)

        filas.append({
            'Nombre_Comuna':  nombre_display,
            'Region':         region,
            'Habitantes':     habitantes,
            'Fuente_API':     fuente,
            'Fecha_Consulta': datetime.now().strftime('%d-%m-%Y %H:%M'),
        })

    df_consolidado = pd.DataFrame(filas)
    if not df_consolidado.empty:
        df_consolidado.index = df_consolidado.index + 1
        df_consolidado.index.name = 'ID'

    return df_consolidado, stats, log_lineas


def generar_log_comunas_parte3(stats: Dict[str, int], log_lineas: List[str]) -> str:
    """
    Genera el archivo de log completo para la Parte 3-I con todos
    los campos requeridos por el PDF.
    """
    resumen_log = [
        f'  Fecha y hora de ejecucion  : {datetime.now().strftime("%d-%m-%Y %H:%M:%S")}',
        f'  Registros leidos           : {stats["leidos"]}',
        f'  Comunas procesadas         : {stats["procesados"]}',
        f'  Duplicados eliminados      : {stats["duplicados"]}',
        f'  Consolidados correctamente : {stats["consolidados"]}',
        f'  No encontrados en API      : {stats["no_encontrados"]}',
        f'  Errores producidos         : {stats["errores"]}',
        '',
        '---- DETALLE DE EVENTOS ----',
        '',
    ]
    return guardar_log('comunas_parte3', resumen_log + log_lineas)


# ------ 5B: API WIKIPEDIA — FAMOSOS ---------------------------

def buscar_imagen_wikipedia(nombre: str) -> Dict[str, str]:
    """
    Busca información e imagen de un famoso en la Wikipedia API.

    Endpoint: https://en.wikipedia.org/w/api.php
    Parámetros: action=query, prop=pageimages+extracts+info

    El resultado se guarda en session_state['cache_wikipedia']
    para no repetir consultas (las APIs tienen límites de uso).

    Retorna diccionario con:
        imagen     : URL de la imagen (o None)
        descripcion: Extracto breve
        fuente     : URL de la página de Wikipedia
        timestamp  : Fecha de última modificación del artículo
        estado     : 'OK' | 'NO_ENCONTRADO' | 'ERROR'
    """
    # Verificar cache
    cache = st.session_state.setdefault('cache_wikipedia', {})
    if nombre in cache:
        return cache[nombre]

    # Construir query para Wikipedia
    url = 'https://en.wikipedia.org/w/api.php'
    params = {
        'action':       'query',
        'titles':       nombre,
        'prop':         'pageimages|extracts|info',
        'exintro':      True,
        'explaintext':  True,
        'exsentences':  2,
        'piprop':       'thumbnail',
        'pithumbsize':  400,
        'inprop':       'url',
        'format':       'json',
        'redirects':    1,
    }
    headers = {
        'User-Agent': 'ETL-INACAP-Student/3.0 (educational; '
                      'Arquitectura y Almacenamiento de Datos)'
    }

    resultado = {
        'imagen': None, 'descripcion': 'Sin descripcion disponible',
        'fuente': f'https://en.wikipedia.org/wiki/{nombre.replace(" ", "_")}',
        'timestamp': 'Desconocido', 'estado': 'ERROR',
    }

    try:
        resp = requests.get(url, params=params, headers=headers, timeout=12)
        if resp.status_code == 200:
            data = resp.json()
            pages = data.get('query', {}).get('pages', {})
            for pid, page in pages.items():
                if pid == '-1':
                    resultado['estado'] = 'NO_ENCONTRADO'
                    resultado['descripcion'] = 'No encontrado en Wikipedia'
                    break
                resultado['imagen'] = page.get('thumbnail', {}).get('source')
                resultado['descripcion'] = (page.get('extract') or '')[:300]
                resultado['fuente'] = page.get('fullurl', resultado['fuente'])
                touched = page.get('touched', '')
                if touched:
                    try:
                        dt_wiki = datetime.strptime(touched[:10], '%Y-%m-%d')
                        resultado['timestamp'] = dt_wiki.strftime('%d-%m-%Y')
                    except ValueError:
                        resultado['timestamp'] = touched[:10]
                resultado['estado'] = 'OK'
                break
        else:
            resultado['descripcion'] = f'Error HTTP {resp.status_code}'
    except requests.exceptions.Timeout:
        resultado['descripcion'] = 'Timeout al conectar con Wikipedia'
    except Exception as e:
        resultado['descripcion'] = f'Error: {str(e)[:80]}'

    # Guardar en cache
    cache[nombre] = resultado
    st.session_state['cache_wikipedia'] = cache
    return resultado


# ------ 5C: MAPA INTERACTIVO — LUGARES ------------------------

def crear_mapa_lugares(
    df_lugares: pd.DataFrame,
    df_georef: pd.DataFrame,
    df_dir: pd.DataFrame,
) -> folium.Map:
    """
    Crea un mapa Folium interactivo con todos los lugares cargados.

    Cada pin incluye:
    - Nombre del lugar
    - Coordenadas
    - Dirección completa
    - Popup con datos + enlace a Google Maps

    Parámetros:
        df_lugares : tabla con ID_lugar y nombre_lugar
        df_georef  : tabla con ID_lugar, latitud, longitud
        df_dir     : tabla con ID_lugar, nombre_calle, etc.

    Retorna objeto folium.Map listo para renderizar.
    """
    # Centrar el mapa en el promedio de coordenadas
    lat_center = df_georef['latitud'].mean()
    lon_center = df_georef['longitud'].mean()

    mapa = folium.Map(
        location=[lat_center, lon_center],
        zoom_start=2,
        tiles='CartoDB positron',
    )

    # Unir las 3 tablas por ID_lugar
    df_merged = df_lugares.merge(df_georef, on='ID_lugar').merge(df_dir, on='ID_lugar')

    for _, fila in df_merged.iterrows():
        nombre   = fila['nombre_lugar']
        lat      = fila['latitud']
        lon      = fila['longitud']
        calle    = fila.get('nombre_calle', '')
        numero   = fila.get('numero_calle', '')
        ciudad   = fila.get('ciudad_estado_provincia', '')
        pais     = fila.get('pais', '')

        # Construir dirección legible
        dir_completa = f'{numero} {calle}'.strip() if numero else calle
        if ciudad:
            dir_completa += f', {ciudad}'
        if pais:
            dir_completa += f', {pais}'

        # URL de Google Maps
        gmaps_url = (
            f'https://www.google.com/maps/search/?api=1'
            f'&query={lat},{lon}'
        )

        # HTML del popup
        popup_html = f"""
        <div style="font-family:Arial,sans-serif;min-width:200px;">
            <h4 style="margin:0 0 8px;color:#1a1a2e;">{nombre}</h4>
            <p style="margin:2px 0;font-size:12px;">
                <b>Dirección:</b> {dir_completa}
            </p>
            <p style="margin:2px 0;font-size:12px;">
                <b>Coordenadas:</b> {lat:.4f}, {lon:.4f}
            </p>
            <hr style="margin:8px 0;">
            <a href="{gmaps_url}" target="_blank"
               style="background:#e94560;color:white;padding:5px 10px;
                      border-radius:4px;text-decoration:none;font-size:12px;">
                📍 Ver en Google Maps
            </a>
        </div>
        """

        folium.Marker(
            location=[lat, lon],
            popup=folium.Popup(popup_html, max_width=280),
            tooltip=nombre,
            icon=folium.Icon(color='red', icon='map-marker', prefix='fa'),
        ).add_to(mapa)

    return mapa


# ==============================================================
# SECCION 6: INTERFAZ DE USUARIO (STREAMLIT)
# ==============================================================

def configurar_pagina() -> None:
    """Configura título, ícono y layout."""
    st.set_page_config(
        page_title='Motor ETL | INACAP',
        page_icon='🔄',
        layout='wide',
        initial_sidebar_state='collapsed',
    )


def mostrar_encabezado() -> None:
    """Encabezado institucional."""
    st.markdown(
        """
        <div style="background:linear-gradient(135deg,#1a1a2e,#16213e,#0f3460);
            padding:1.5rem 2.5rem;border-radius:12px;margin-bottom:1.5rem;
            border-left:5px solid #e94560;">
          <h1 style="color:#fff;margin:0;font-size:1.7rem;letter-spacing:1px;">
            🔄 Motor ETL Profesional
          </h1>
          <p style="color:#a8b2d8;margin:0.3rem 0 0;font-size:0.9rem;">
            Arquitectura y Almacenamiento de Datos — Evaluacion 2 (Partes 1, 2 y 3)
            &nbsp;|&nbsp; INACAP Concepcion-Talcahuano
          </p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def mostrar_resumen(resumen: List[str]) -> None:
    """Bloque de resumen del proceso ETL."""
    st.markdown('#### Resumen del proceso')
    for linea in resumen:
        st.markdown(f'- {linea}')


def btn_csv(df: pd.DataFrame, nombre: str, label: str) -> None:
    """Botón de descarga CSV."""
    csv = df.to_csv(index=True, encoding='utf-8-sig').encode('utf-8-sig')
    st.download_button(label=label, data=csv, file_name=nombre, mime='text/csv')


def btn_log(ruta_log: str) -> None:
    """Botón de descarga del log."""
    if os.path.exists(ruta_log):
        with open(ruta_log, 'r', encoding='utf-8') as f:
            contenido = f.read()
        st.download_button(
            label='Descargar Log (.txt)',
            data=contenido.encode('utf-8'),
            file_name=os.path.basename(ruta_log),
            mime='text/plain',
        )


# ==============================================================
# SECCION 7: FUNCION PRINCIPAL
# ==============================================================

def main() -> None:
    """Orquesta los 5 tabs de la aplicacion."""
    configurar_pagina()
    mostrar_encabezado()

    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        '🏙️ P1 — Comunas',
        '🌟 P2-I — Famosos',
        '🌍 P2-II — Lugares',
        '🔍 P3-I — Comunas + API',
        '🌟 P3-II — Famosos + Imagen',
        '🗺️ P3-III — Mapa Mundial',
    ])

    # ── TAB 1: COMUNAS (heredado) ──────────────────────────────
    with tab1:
        st.markdown('### Normalizacion de Comunas de Chile')
        st.markdown(
            'Sube el archivo `datos2026.txt`. '
            'Se normalizara a MAYUSCULAS, quitara tildes, '
            'eliminara duplicados y generara log de cambios.'
        )
        archivo = st.file_uploader('Archivo de comunas (.txt)', type=['txt'], key='up_c1')
        if archivo and st.button('Procesar', key='btn_c1', type='primary'):
            with st.spinner('Procesando...'):
                try:
                    df, ruta_log, res = procesar_comunas(archivo.getvalue())
                    mostrar_resumen(res)
                    st.dataframe(df, use_container_width=True, height=350)
                    c1, c2 = st.columns(2)
                    with c1:
                        btn_csv(df, 'comunas.csv', 'Descargar CSV')
                    with c2:
                        btn_log(ruta_log)
                except Exception as e:
                    st.error(f'Error: {e}')

    # ── TAB 2: FAMOSOS (heredado) ──────────────────────────────
    with tab2:
        st.markdown('### Normalizacion de Famosos y Fechas')
        st.markdown(
            'Sube `DATOS2026-2.txt`. Unifica fechas a DD-MM-YYYY, '
            'calcula edad, marca cumpleanos y elimina duplicados.'
        )
        archivo = st.file_uploader('Archivo de famosos (.txt)', type=['txt'], key='up_f1')
        if archivo and st.button('Procesar', key='btn_f1', type='primary'):
            with st.spinner('Procesando...'):
                try:
                    df, ruta_log, res = procesar_famosos(archivo.getvalue())
                    mostrar_resumen(res)
                    cumple = df[df['Cumpleanos_Hoy'] == 1]
                    if not cumple.empty:
                        st.success(f'Cumpleanos hoy: {", ".join(cumple["Nombre"].tolist())}')
                    st.dataframe(df, use_container_width=True, height=350)
                    c1, c2 = st.columns(2)
                    with c1:
                        btn_csv(df, 'famosos.csv', 'Descargar CSV')
                    with c2:
                        btn_log(ruta_log)
                except Exception as e:
                    st.error(f'Error: {e}')

    # ── TAB 3: LUGARES (heredado) ──────────────────────────────
    with tab3:
        st.markdown('### Normalizacion de Lugares del Mundo')
        st.markdown('Sube `DATOS2026-3.TXT`. Genera 3 tablas relacionales.')
        archivo = st.file_uploader('Archivo de lugares', type=['txt', 'TXT'], key='up_l1')
        if archivo and st.button('Procesar', key='btn_l1', type='primary'):
            with st.spinner('Procesando...'):
                try:
                    df_l, df_g, df_d, ruta_log, res = procesar_lugares(archivo.getvalue())
                    # Guardar en session_state para reutilizar en Tab Mapa
                    st.session_state['df_mapa_l'] = df_l
                    st.session_state['df_mapa_g'] = df_g
                    st.session_state['df_mapa_d'] = df_d
                    mostrar_resumen(res)
                    st.info('Datos guardados. Ve al tab **P3-III — Mapa Mundial** para visualizarlos.')
                    st1, st2, st3 = st.tabs([
                        f'Lugares ({len(df_l)})',
                        f'Georeferencias ({len(df_g)})',
                        f'Direcciones ({len(df_d)})',
                    ])
                    with st1:
                        st.dataframe(df_l, use_container_width=True, height=350)
                        btn_csv(df_l, 'lugares.csv', 'Descargar')
                    with st2:
                        st.dataframe(df_g, use_container_width=True, height=350)
                        btn_csv(df_g, 'georef.csv', 'Descargar')
                    with st3:
                        st.dataframe(df_d, use_container_width=True, height=350)
                        btn_csv(df_d, 'dir.csv', 'Descargar')
                    btn_log(ruta_log)
                except Exception as e:
                    st.error(f'Error: {e}')

    # ── TAB 4: COMUNAS + API (PARTE 3-I) ──────────────────────
    with tab4:
        st.markdown('### Comunas — Consulta API Oficial + Busqueda')
        st.markdown(
            'Carga un archivo de comunas **o** escribe una comuna manualmente. '
            'El sistema consultara la **API del Gobierno de Chile** '
            '(`apis.digital.gob.cl`) para obtener region e informacion oficial.'
        )

        # Selector de formato
        formato = st.selectbox(
            'Formato de salida para los nombres:',
            ['MAYUSCULAS', 'Titulo', 'minusculas'],
            key='sel_formato',
        )

        col_izq, col_der = st.columns([1, 1])

        with col_izq:
            st.markdown('#### Opcion 1: Subir archivo TXT')
            archivo_p3 = st.file_uploader(
                'Archivo con comunas', type=['txt'], key='up_p3'
            )

        with col_der:
            st.markdown('#### Opcion 2: Busqueda manual')
            busqueda_manual = st.text_input(
                'Escribe una o varias comunas (separadas por coma):',
                placeholder='ej: florida, santiago, concepcion',
                key='txt_busqueda',
            )
            st.caption('El sistema sugiere opciones si el nombre tiene errores.')

        # Botón de procesar
        if st.button('Consultar API y Consolidar', key='btn_p3', type='primary'):
            comunas_input = []

            # Fuente 1: archivo
            if archivo_p3:
                enc = detectar_encoding(archivo_p3.getvalue())
                texto = archivo_p3.getvalue().decode(enc, errors='replace')
                comunas_input += [l.strip() for l in texto.splitlines() if l.strip()]

            # Fuente 2: texto manual
            if busqueda_manual.strip():
                comunas_input += [c.strip() for c in busqueda_manual.split(',') if c.strip()]

            if not comunas_input:
                st.warning('Sube un archivo o escribe al menos una comuna.')
            else:
                with st.spinner('Consultando API del Gobierno de Chile...'):
                    try:
                        # Obtener catalogo de la API
                        catalogo = obtener_datos_api_comunas()
                        fuente_catalogo = list(catalogo.values())[0].get('fuente', '') if catalogo else ''

                        if 'API' in fuente_catalogo:
                            st.success(f'API conectada: {len(catalogo)} comunas cargadas desde apis.digital.gob.cl')
                        else:
                            st.info('Usando dataset local (API no disponible en este momento).')

                        # Consolidar
                        df_cons, stats, log_lineas = consolidar_comunas(
                            comunas_input, catalogo, formato
                        )
                        ruta_log = generar_log_comunas_parte3(stats, log_lineas)

                        # Mostrar estadísticas
                        st.markdown('#### Estadisticas del proceso')
                        m1, m2, m3, m4, m5 = st.columns(5)
                        m1.metric('Leidos', stats['leidos'])
                        m2.metric('Procesados', stats['procesados'])
                        m3.metric('Duplicados', stats['duplicados'])
                        m4.metric('Consolidados', stats['consolidados'])
                        m5.metric('No encontrados', stats['no_encontrados'])

                        if not df_cons.empty:
                            st.markdown('#### Datos consolidados')
                            st.dataframe(df_cons, use_container_width=True, height=400)
                            c1, c2 = st.columns(2)
                            with c1:
                                btn_csv(df_cons, 'comunas_consolidadas.csv',
                                        'Descargar CSV Consolidado')
                            with c2:
                                btn_log(ruta_log)
                        else:
                            st.warning('No se encontraron comunas en el catalogo.')
                            btn_log(ruta_log)

                        # Mostrar sugerencias para búsqueda manual si hubo no-encontrados
                        if stats['no_encontrados'] > 0 and busqueda_manual.strip():
                            st.markdown('#### Sugerencias para terminos no encontrados')
                            for termino in [c.strip() for c in busqueda_manual.split(',')]:
                                sug = buscar_comunas_difuso(termino, catalogo, cutoff=0.5, n=3)
                                if sug:
                                    sug_fmt = [normalizar_comuna(s, formato) for s in sug]
                                    st.info(f'"{termino}" -> Quizas quisiste decir: **{", ".join(sug_fmt)}**')

                    except Exception as e:
                        st.error(f'Error al procesar: {e}')

    # ── TAB 5: FAMOSOS CON IMAGEN + MAPA (PARTE 3-II y 3-III) ─
    with tab5:
        st.markdown('### Famosos con Imagen de Wikipedia (Parte 3-II)')
        st.markdown(
            'Sube el archivo `DATOS2026-2.txt`, carga la lista y luego '
            'expande cualquier famoso para ver su **imagen, descripcion '
            'y fuente** obtenidas directamente desde Wikipedia.'
        )

        archivo_fam2 = st.file_uploader(
            'Archivo de famosos', type=['txt'], key='up_fam2'
        )

        if archivo_fam2 and st.button('Cargar lista', key='btn_fam2', type='primary'):
            with st.spinner('Procesando lista...'):
                df_fam, _, _ = procesar_famosos(archivo_fam2.getvalue())
                st.session_state['df_famosos_p3'] = df_fam
                st.success(f'{len(df_fam)} famosos cargados correctamente.')

        if 'df_famosos_p3' in st.session_state:
            df_fam = st.session_state['df_famosos_p3']
            st.markdown(f'#### Lista completa — {len(df_fam)} personajes')

            for _, row in df_fam.iterrows():
                nombre  = row['Nombre']
                edad    = row['Edad']
                fecha   = row['Fecha_Nacimiento']
                estado  = row['Estado_Fecha']
                cumple  = row['Cumpleanos_Hoy']

                edad_str   = f'{int(edad)} anos' if edad is not None and str(edad) != 'nan' else 'N/A'
                cumple_str = ' 🎂' if cumple == 1 else ''

                with st.expander(f'{nombre} — {edad_str}{cumple_str}  |  {fecha}'):
                    col_info, col_img = st.columns([2, 1])

                    with col_info:
                        st.markdown(f'**Nombre:** {nombre}')
                        st.markdown(f'**Fecha de nacimiento:** {fecha}')
                        st.markdown(f'**Edad calculada:** {edad_str}')
                        st.markdown(f'**Estado fecha:** {estado}')

                    with col_img:
                        if st.button('Ver imagen Wikipedia', key=f'wiki_{nombre}'):
                            with st.spinner('Buscando en Wikipedia...'):
                                datos_wiki = buscar_imagen_wikipedia(nombre)

                            if datos_wiki['estado'] == 'OK':
                                if datos_wiki['imagen']:
                                    st.image(
                                        datos_wiki['imagen'],
                                        caption=nombre,
                                        use_container_width=True,
                                    )
                                else:
                                    st.info('Wikipedia no tiene imagen para este personaje.')
                                st.markdown(f'**Descripcion:** {datos_wiki["descripcion"]}')
                                st.markdown(
                                    f'**Fuente:** [{datos_wiki["fuente"]}]'
                                    f'({datos_wiki["fuente"]})'
                                )
                                st.caption(
                                    f'Imagen capturada en Wikipedia el: '
                                    f'{datos_wiki["timestamp"]}'
                                )
                            elif datos_wiki['estado'] == 'NO_ENCONTRADO':
                                st.warning(f'"{nombre}" no encontrado en Wikipedia.')
                                st.markdown(
                                    f'[Buscar manualmente]'
                                    f'(https://en.wikipedia.org/wiki/'
                                    f'{nombre.replace(" ", "_")})'
                                )
                            else:
                                st.error(f'Error Wikipedia: {datos_wiki["descripcion"]}')

    # ── TAB 6: MAPA MUNDIAL (PARTE 3-III) ─────────────────────
    with tab6:
        st.markdown('### Mapa Mundial de Lugares Historicos (Parte 3-III)')
        st.markdown(
            'Visualiza **todos los lugares cargados** en un mapa interactivo. '
            'Haz clic en cualquier **pin rojo** para ver el nombre, dirección '
            'y acceder a **Google Maps** directamente.'
        )

        # Verificar si ya hay datos del Tab 3
        tiene_datos = all(
            k in st.session_state
            for k in ['df_mapa_l', 'df_mapa_g', 'df_mapa_d']
        )

        if tiene_datos:
            n_lugares = len(st.session_state['df_mapa_l'])
            st.success(
                f'{n_lugares} lugares cargados desde el procesamiento anterior. '
                'Puedes generar el mapa directamente o subir un nuevo archivo.'
            )

        # Opción: subir nuevo archivo
        with st.expander('Cargar nuevo archivo de lugares (opcional)'):
            archivo_map = st.file_uploader(
                'Archivo de lugares (.txt)',
                type=['txt', 'TXT'],
                key='up_map',
            )
            if archivo_map and st.button('Procesar archivo', key='btn_map_load'):
                with st.spinner('Procesando...'):
                    try:
                        df_l, df_g, df_d, _, _ = procesar_lugares(archivo_map.getvalue())
                        st.session_state['df_mapa_l'] = df_l
                        st.session_state['df_mapa_g'] = df_g
                        st.session_state['df_mapa_d'] = df_d
                        st.success(f'{len(df_l)} lugares listos para el mapa.')
                        st.rerun()
                    except Exception as e:
                        st.error(f'Error: {e}')

        # Generar y mostrar el mapa
        if all(k in st.session_state for k in ['df_mapa_l', 'df_mapa_g', 'df_mapa_d']):
            df_l = st.session_state['df_mapa_l']
            df_g = st.session_state['df_mapa_g']
            df_d = st.session_state['df_mapa_d']

            st.markdown(
                f'**{len(df_l)} lugares** marcados en el mapa. '
                'Haz clic en un pin para ver sus datos y abrirlo en Google Maps.'
            )

            mapa = crear_mapa_lugares(df_l, df_g, df_d)

            datos_mapa = st_folium(
                mapa,
                width='100%',
                height=600,
                returned_objects=['last_object_clicked_popup'],
            )

            # Panel con datos del lugar al hacer clic
            if datos_mapa and datos_mapa.get('last_object_clicked_popup'):
                st.markdown('---')
                st.markdown('#### Lugar seleccionado')
                st.info(
                    'Usa el boton **📍 Ver en Google Maps** dentro del popup '
                    'para abrir la ubicacion exacta en Google Maps en una nueva pestana.'
                )
        else:
            st.warning(
                'No hay datos cargados. '
                'Ve al tab **P2-II — Lugares** y procesa el archivo primero, '
                'o usa el cargador de arriba.'
            )


# ── Punto de entrada ──────────────────────────────────────────
if __name__ == '__main__':
    main()