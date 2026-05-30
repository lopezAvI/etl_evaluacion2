"""
============================================================
MOTOR ETL PROFESIONAL - EVALUACION 2
Arquitectura y Almacenamiento de Datos
Sede: Concepcion-Talcahuano - INACAP
============================================================
Autor   : Ignacio López, Arturo Díaz, Bayron Cerna
Docente : Hernan R. Saez Talavera
Fecha   : 28-05-2026
Version : 2.0

Descripcion:
    Aplicacion ETL (Extract, Transform, Load) que normaliza
    tres tipos de datasets:
    - PARTE 1: Comunas de Chile (COMUNAS_NORM)
    - PARTE 2-I: Famosos y fechas de nacimiento
    - PARTE 2-II: Lugares del mundo con georeferencias

Requisitos cubiertos:
    [PARTE 1]
    Unifica datos a formato MAYUSCULAS
    Quita caracteres especiales (tildes, enes, etc.)
    Elimina registros duplicados
    Genera log de cambios (archivo .txt con timestamp)
    Interfaz de usuario para cargar el archivo

    [PARTE 2-I]
    Unifica fechas al formato DD-MM-YYYY
    Agrega nombre del famoso
    Quita separadores no permitidos
    Elimina registros duplicados
    Calcula edad de cada personaje
    Flag de cumpleanos (1 si es hoy, 0 si no)
    Manejo de fechas historicas (a.C.)

    [PARTE 2-II]
    Elimina registros duplicados exactos
    Genera tabla Lugares       (ID_lugar, nombre_lugar)
    Genera tabla Georeferencias(ID_geo, ID_lugar, latitud, longitud)
    Genera tabla Direcciones   (ID_dir, ID_lugar, nombre_calle,
                                numero_calle, ciudad_estado_provincia, pais)
============================================================
"""

# -- Librerias estandar ----------------------------------------
from __future__ import annotations

import os
import re
import unicodedata
from collections import Counter
from datetime import datetime
from typing import Optional, Tuple, List

# -- Librerias de terceros -------------------------------------
import streamlit as st
import pandas as pd


# ==============================================================
# SECCION 1: UTILIDADES GLOBALES
# ==============================================================

def quitar_acentos(texto: str) -> str:
    """
    Elimina tildes, diéresis y otros diacríticos de un texto.
    Usa normalización Unicode NFD y filtra categoría 'Mn'.

    Ejemplo:
        'Concepción' -> 'Concepcion'
        'Ñuñoa'      -> 'Nunoa'
    """
    texto = unicodedata.normalize('NFD', texto)
    return ''.join(c for c in texto if unicodedata.category(c) != 'Mn')


def limpiar_texto_basico(texto: str) -> str:
    """
    Limpieza básica de un string:
    1. Strip de espacios al inicio y final.
    2. Convierte a MAYUSCULAS.
    3. Quita tildes y diacríticos.
    4. Colapsa espacios múltiples en uno.
    """
    if not isinstance(texto, str):
        texto = str(texto)
    texto = texto.strip().upper()
    texto = quitar_acentos(texto)
    texto = re.sub(r'\s+', ' ', texto)
    return texto


def detectar_encoding(bytes_data: bytes) -> str:
    """
    Detecta el encoding intentando decodificar con los más comunes.
    Retorna el primero que funcione sin error.
    """
    for enc in ('utf-8-sig', 'utf-8', 'latin-1', 'cp1252', 'iso-8859-1'):
        try:
            bytes_data.decode(enc)
            return enc
        except (UnicodeDecodeError, LookupError):
            continue
    return 'latin-1'


def guardar_log(nombre_proceso: str, lineas_log: List[str]) -> str:
    """
    Guarda el registro de cambios realizados durante un proceso ETL.

    Parámetros:
        nombre_proceso : nombre del proceso ('comunas', 'famosos', etc.)
        lineas_log     : lista de strings con cada cambio registrado

    Retorna:
        Ruta completa del archivo de log creado.
    """
    os.makedirs('logs', exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    nombre_archivo = f'logs/log_{nombre_proceso}_{timestamp}.txt'

    encabezado = [
        '=' * 50,
        f'  LOG ETL - PROCESO: {nombre_proceso.upper()}',
        '=' * 50,
        f'  Fecha/Hora : {datetime.now().strftime("%d-%m-%Y %H:%M:%S")}',
        f'  Eventos    : {len(lineas_log)}',
        '=' * 50,
        '',
    ]

    with open(nombre_archivo, 'w', encoding='utf-8') as f:
        f.write('\n'.join(encabezado + lineas_log))

    return nombre_archivo


# ==============================================================
# SECCION 2: ETL PARTE 1 — COMUNAS
# ==============================================================

def procesar_comunas(
    archivo_bytes: bytes,
) -> Tuple[pd.DataFrame, str, List[str]]:
    """
    Normaliza un dataset de comunas de Chile.

    Transformaciones:
        1. Detecta encoding automáticamente.
        2. Limpia cada línea: MAYUSCULAS + sin tildes.
        3. Registra y elimina duplicados.
        4. Genera log detallado.

    Retorna:
        df_resultado : DataFrame con columna 'nombre_comuna'
        ruta_log     : ruta del archivo de log generado
        resumen      : lista de strings para mostrar en UI
    """
    log = []
    resumen = []

    # Paso 1: Decodificar
    encoding = detectar_encoding(archivo_bytes)
    log.append(f'[{datetime.now().strftime("%H:%M:%S")}] Encoding detectado: {encoding}')
    texto = archivo_bytes.decode(encoding, errors='replace')

    # Paso 2: Leer líneas y limpiar
    lineas_raw = [l.strip() for l in texto.splitlines() if l.strip()]
    total_raw = len(lineas_raw)
    log.append(f'[{datetime.now().strftime("%H:%M:%S")}] Total lineas leidas: {total_raw}')
    resumen.append(f'Lineas leidas: **{total_raw}**')

    lineas_limpias = []
    for linea in lineas_raw:
        original = linea
        limpia = limpiar_texto_basico(linea)
        if limpia:
            lineas_limpias.append(limpia)
            if original != limpia:
                log.append(
                    f'[{datetime.now().strftime("%H:%M:%S")}] '
                    f'NORMALIZADO: "{original}" -> "{limpia}"'
                )

    # Paso 3: Detectar y registrar duplicados
    conteo = Counter(lineas_limpias)
    duplicados = {k: v for k, v in conteo.items() if v > 1}
    for nombre, cantidad in sorted(duplicados.items()):
        log.append(
            f'[{datetime.now().strftime("%H:%M:%S")}] '
            f'DUPLICADO: "{nombre}" x{cantidad} -> se mantiene 1, '
            f'se eliminan {cantidad - 1}'
        )

    total_dup = sum(v - 1 for v in duplicados.values())
    resumen.append(f'Duplicados eliminados: **{total_dup}**')
    log.append(f'[{datetime.now().strftime("%H:%M:%S")}] '
               f'Total duplicados eliminados: {total_dup}')

    # Paso 4: Construir DataFrame sin duplicados
    unicos = list(dict.fromkeys(lineas_limpias))
    df_resultado = pd.DataFrame(unicos, columns=['nombre_comuna'])
    df_resultado.index = df_resultado.index + 1
    df_resultado.index.name = 'ID'

    resumen.append(f'Registros unicos finales: **{len(df_resultado)}**')
    log.append(f'[{datetime.now().strftime("%H:%M:%S")}] '
               f'Proceso completado. Registros finales: {len(df_resultado)}')

    # Paso 5: Guardar log
    ruta_log = guardar_log('comunas', log)
    resumen.append(f'Log generado: `{ruta_log}`')

    return df_resultado, ruta_log, resumen


# ==============================================================
# SECCION 3: ETL PARTE 2-I — FAMOSOS
# ==============================================================

def parsear_fecha_famoso(
    fecha_str: str,
) -> Tuple[Optional[datetime], str, str]:
    """
    Parsea una fecha de nacimiento soportando múltiples formatos:
        DD-MM-YYYY  |  YYYY-MM-DD  |  DD/MM/YYYY  |  YYYY/MM/DD

    Fechas con 'a.C.', 'alrededor', 'circa' se marcan HISTORICA.

    Retorna:
        dt        : datetime o None
        fecha_fmt : string DD-MM-YYYY o descripción
        estado    : 'OK', 'HISTORICA' o 'NO_PARSEABLE'
    """
    s = fecha_str.strip()

    # Detectar fechas históricas
    if re.search(r'a\.?C\.?|alrededor|circa|aprox', s, re.IGNORECASE):
        return None, f'Fecha historica: {s}', 'HISTORICA'

    # Normalizar separadores
    s_norm = re.sub(r'[/.]', '-', s)

    # Intentar múltiples formatos
    formatos = ['%d-%m-%Y', '%Y-%m-%d', '%m-%d-%Y']
    for fmt in formatos:
        try:
            dt = datetime.strptime(s_norm, fmt)
            if 1 <= dt.month <= 12 and 1 <= dt.day <= 31:
                return dt, dt.strftime('%d-%m-%Y'), 'OK'
        except ValueError:
            continue

    return None, s, 'NO_PARSEABLE'


def calcular_edad(dt_nacimiento: datetime) -> int:
    """
    Calcula la edad en años. Descuenta 1 si el cumpleaños
    aún no ha ocurrido este año.
    """
    hoy = datetime.now()
    edad = hoy.year - dt_nacimiento.year
    if (hoy.month, hoy.day) < (dt_nacimiento.month, dt_nacimiento.day):
        edad -= 1
    return edad


def es_cumpleanos_hoy(dt_nacimiento: datetime) -> int:
    """Retorna 1 si hoy es el cumpleaños, 0 si no."""
    hoy = datetime.now()
    return 1 if (hoy.month == dt_nacimiento.month and
                 hoy.day == dt_nacimiento.day) else 0


def procesar_famosos(
    archivo_bytes: bytes,
) -> Tuple[pd.DataFrame, str, List[str]]:
    """
    Normaliza el dataset de famosos y fechas de nacimiento.

    Transformaciones:
        1. Extrae registros con regex robusta.
        2. Parsea fecha a DD-MM-YYYY (múltiples formatos).
        3. Calcula edad.
        4. Flag cumpleaños del día.
        5. Elimina duplicados por nombre.
        6. Genera log completo.
    """
    log = []
    resumen = []

    # Paso 1: Decodificar
    encoding = detectar_encoding(archivo_bytes)
    texto = archivo_bytes.decode(encoding, errors='replace')
    log.append(f'[{datetime.now().strftime("%H:%M:%S")}] Encoding: {encoding}')

    # Paso 2: Extraer registros
    patron = r'^\d+\.\s+(.+?)\s+-\s+(.+?)$'
    registros_raw = re.findall(patron, texto, re.MULTILINE)
    total_raw = len(registros_raw)
    log.append(f'[{datetime.now().strftime("%H:%M:%S")}] Registros extraidos: {total_raw}')
    resumen.append(f'Registros leidos: **{total_raw}**')

    # Paso 3: Procesar cada registro
    hoy = datetime.now()
    datos = []
    cumpleanos_hoy = []

    for nombre_raw, fecha_raw in registros_raw:
        nombre = nombre_raw.strip()
        dt, fecha_fmt, estado = parsear_fecha_famoso(fecha_raw)

        if estado == 'OK':
            edad = calcular_edad(dt)
            flag_cumple = es_cumpleanos_hoy(dt)
            if flag_cumple == 1:
                cumpleanos_hoy.append(nombre)
                log.append(f'[{datetime.now().strftime("%H:%M:%S")}] '
                           f'CUMPLEANOS HOY: {nombre} ({fecha_fmt})')
        elif estado == 'HISTORICA':
            edad = None
            flag_cumple = 0
            log.append(f'[{datetime.now().strftime("%H:%M:%S")}] '
                       f'HISTORICA: {nombre} - {fecha_raw}')
        else:
            edad = None
            flag_cumple = 0
            log.append(f'[{datetime.now().strftime("%H:%M:%S")}] '
                       f'ERROR_FECHA: {nombre} - "{fecha_raw}"')

        datos.append({
            'Nombre':           nombre,
            'Fecha_Nacimiento': fecha_fmt,
            'Edad':             edad,
            'Cumpleanos_Hoy':   flag_cumple,
            'Estado_Fecha':     estado,
        })

    # Paso 4: Eliminar duplicados
    df_raw = pd.DataFrame(datos)
    total_antes = len(df_raw)

    dup_mask = df_raw.duplicated(subset=['Nombre'], keep='first')
    for n in df_raw.loc[dup_mask, 'Nombre'].tolist():
        log.append(f'[{datetime.now().strftime("%H:%M:%S")}] '
                   f'DUPLICADO ELIMINADO: "{n}"')

    df_resultado = df_raw.drop_duplicates(
        subset=['Nombre'], keep='first'
    ).copy().reset_index(drop=True)
    df_resultado.index = df_resultado.index + 1
    df_resultado.index.name = 'ID'

    eliminados = total_antes - len(df_resultado)
    resumen.append(f'Duplicados eliminados: **{eliminados}**')
    resumen.append(f'Registros unicos finales: **{len(df_resultado)}**')

    if cumpleanos_hoy:
        resumen.append(
            f'Cumpleanos hoy ({hoy.strftime("%d/%m")}): '
            f'**{", ".join(cumpleanos_hoy)}**'
        )
    else:
        resumen.append(f'Sin cumpleanos hoy ({hoy.strftime("%d/%m")})')

    log.append(f'[{datetime.now().strftime("%H:%M:%S")}] '
               f'Proceso completado. Registros finales: {len(df_resultado)}')
    ruta_log = guardar_log('famosos', log)
    resumen.append(f'Log generado: `{ruta_log}`')

    return df_resultado, ruta_log, resumen


# ==============================================================
# SECCION 4: ETL PARTE 2-II — LUGARES
# ==============================================================

def extraer_numero_calle(calle: str) -> Tuple[str, str]:
    """
    Separa número de nombre de calle.
    '1600 Amphitheatre Pkwy' -> ('1600', 'Amphitheatre Pkwy')
    'Champ de Mars'          -> ('', 'Champ de Mars')
    """
    m = re.match(r'^(\d+[\w-]*)\s+(.+)', calle.strip())
    if m:
        return m.group(1).strip(), m.group(2).strip()
    return '', calle.strip()


def construir_ciudad_estado(partes_addr: List[str]) -> str:
    """
    Construye ciudad_estado_provincia tomando los elementos
    intermedios de la dirección (descarta primero=calle y último=país).
    """
    if len(partes_addr) >= 3:
        return ', '.join(p.strip() for p in partes_addr[1:-1])
    elif len(partes_addr) == 2:
        return partes_addr[0].strip()
    return 'N/A'


def procesar_lugares(
    archivo_bytes: bytes,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, str, List[str]]:
    """
    Normaliza el dataset de lugares y genera 3 tablas relacionales.

    Tablas:
        Lugares       (ID_lugar PK, nombre_lugar)
        Georeferencias(ID_geo PK, ID_lugar FK, latitud, longitud)
        Direcciones   (ID_dir PK, ID_lugar FK, nombre_calle,
                       numero_calle, ciudad_estado_provincia, pais)
    """
    log = []
    resumen = []

    # Paso 1: Decodificar
    encoding = detectar_encoding(archivo_bytes)
    texto = archivo_bytes.decode(encoding, errors='replace')
    log.append(f'[{datetime.now().strftime("%H:%M:%S")}] Encoding: {encoding}')

    # Paso 2: Filtrar líneas válidas (tienen ';', no son encabezado)
    lineas = [
        l.strip() for l in texto.splitlines()
        if ';' in l and l.strip() and 'Nombre' not in l
    ]
    total_raw = len(lineas)
    log.append(f'[{datetime.now().strftime("%H:%M:%S")}] Lineas leidas: {total_raw}')
    resumen.append(f'Registros leidos: **{total_raw}**')

    # Paso 3: Parsear cada línea
    registros_raw = []
    for linea in lineas:
        partes = linea.split(';')
        if len(partes) < 3:
            log.append(f'[{datetime.now().strftime("%H:%M:%S")}] '
                       f'LINEA INVALIDA: "{linea[:60]}"')
            continue

        nombre_lugar = partes[0].strip()
        direccion    = partes[1].strip()
        georef       = partes[2].strip()

        # Parsear coordenadas
        coords = georef.split(',')
        try:
            latitud  = float(coords[0].strip())
            longitud = float(coords[1].strip())
        except (IndexError, ValueError):
            latitud, longitud = 0.0, 0.0
            log.append(f'[{datetime.now().strftime("%H:%M:%S")}] '
                       f'ERROR_COORDS: "{nombre_lugar}" - "{georef}"')

        # Parsear dirección
        partes_addr = direccion.split(',')
        numero_calle, nombre_calle = extraer_numero_calle(partes_addr[0])
        ciudad_estado = construir_ciudad_estado(partes_addr)
        pais = partes_addr[-1].strip() if partes_addr else 'N/A'

        registros_raw.append({
            'nombre_lugar':            nombre_lugar,
            'latitud':                 latitud,
            'longitud':                longitud,
            'nombre_calle':            nombre_calle,
            'numero_calle':            numero_calle,
            'ciudad_estado_provincia': ciudad_estado,
            'pais':                    pais,
        })

    # Paso 4: Eliminar duplicados exactos (mismo nombre + coords)
    df_raw = pd.DataFrame(registros_raw)
    total_antes = len(df_raw)

    dup_mask = df_raw.duplicated(
        subset=['nombre_lugar', 'latitud', 'longitud'], keep='first'
    )
    for _, fila in df_raw[dup_mask].iterrows():
        log.append(f'[{datetime.now().strftime("%H:%M:%S")}] '
                   f'DUPLICADO ELIMINADO: "{fila["nombre_lugar"]}" '
                   f'({fila["latitud"]}, {fila["longitud"]})')

    df_clean = df_raw.drop_duplicates(
        subset=['nombre_lugar', 'latitud', 'longitud'], keep='first'
    ).reset_index(drop=True)

    eliminados = total_antes - len(df_clean)
    resumen.append(f'Duplicados exactos eliminados: **{eliminados}**')
    resumen.append(f'Registros unicos finales: **{len(df_clean)}**')
    log.append(f'[{datetime.now().strftime("%H:%M:%S")}] '
               f'Duplicados: {eliminados}. Finales: {len(df_clean)}')

    # Paso 5: Construir las 3 tablas con FK relacionado
    ids = list(range(1, len(df_clean) + 1))

    # Tabla Lugares
    df_lugares = df_clean[['nombre_lugar']].copy()
    df_lugares.insert(0, 'ID_lugar', ids)

    # Tabla Georeferencias
    df_georeferencias = df_clean[['latitud', 'longitud']].copy()
    df_georeferencias.insert(0, 'ID_lugar', ids)
    df_georeferencias.insert(0, 'ID_geo',   ids)

    # Tabla Direcciones
    df_direcciones = df_clean[[
        'nombre_calle', 'numero_calle',
        'ciudad_estado_provincia', 'pais',
    ]].copy()
    df_direcciones.insert(0, 'ID_lugar', ids)
    df_direcciones.insert(0, 'ID_dir',   ids)

    log.append(f'[{datetime.now().strftime("%H:%M:%S")}] '
               f'Tablas generadas: Lugares={len(df_lugares)}, '
               f'Georeferencias={len(df_georeferencias)}, '
               f'Direcciones={len(df_direcciones)}')

    ruta_log = guardar_log('lugares', log)
    resumen.append(f'Log generado: `{ruta_log}`')

    return df_lugares, df_georeferencias, df_direcciones, ruta_log, resumen


# ==============================================================
# SECCION 5: INTERFAZ DE USUARIO (STREAMLIT)
# ==============================================================

def configurar_pagina() -> None:
    """Configura título, ícono y layout de la página."""
    st.set_page_config(
        page_title='Motor ETL | INACAP',
        page_icon='🔄',
        layout='wide',
        initial_sidebar_state='collapsed',
    )


def mostrar_encabezado() -> None:
    """Muestra el encabezado institucional."""
    st.markdown(
        """
        <div style="
            background: linear-gradient(135deg,#1a1a2e,#16213e,#0f3460);
            padding:2rem 2.5rem; border-radius:12px;
            margin-bottom:1.5rem; border-left:5px solid #e94560;">
          <h1 style="color:#fff;margin:0;font-size:1.8rem;letter-spacing:1px;">
            🔄 Motor ETL Profesional
          </h1>
          <p style="color:#a8b2d8;margin:0.4rem 0 0;font-size:0.95rem;">
            Arquitectura y Almacenamiento de Datos — Evaluacion 2
            &nbsp;|&nbsp; INACAP Concepcion-Talcahuano
          </p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def mostrar_resumen(resumen: List[str]) -> None:
    """Muestra el resumen del proceso ETL."""
    st.markdown('#### Resumen del proceso')
    for linea in resumen:
        st.markdown(f'- {linea}')


def btn_csv(df: pd.DataFrame, nombre: str, label: str) -> None:
    """Botón de descarga de DataFrame como CSV."""
    csv = df.to_csv(index=True, encoding='utf-8-sig').encode('utf-8-sig')
    st.download_button(label=label, data=csv,
                       file_name=nombre, mime='text/csv')


def btn_log(ruta_log: str) -> None:
    """Botón de descarga del archivo de log."""
    if os.path.exists(ruta_log):
        with open(ruta_log, 'r', encoding='utf-8') as f:
            contenido = f.read()
        st.download_button(
            label='Descargar Log de Cambios (.txt)',
            data=contenido.encode('utf-8'),
            file_name=os.path.basename(ruta_log),
            mime='text/plain',
        )


# -- Aplicacion principal --------------------------------------

def main() -> None:
    """Orquesta la interfaz de usuario con tres tabs."""
    configurar_pagina()
    mostrar_encabezado()

    tab1, tab2, tab3 = st.tabs([
        'Parte 1 - Comunas',
        'Parte 2-I - Famosos',
        'Parte 2-II - Lugares',
    ])

    # ------ TAB 1: COMUNAS ------------------------------------
    with tab1:
        st.markdown('### Normalizacion de Comunas de Chile')
        st.markdown(
            'Sube el archivo `datos2026.txt`. '
            'El sistema lo normalizara a MAYUSCULAS, '
            'quitara tildes y caracteres especiales, '
            'eliminara duplicados y generara un log de cambios.'
        )
        archivo = st.file_uploader(
            'Selecciona el archivo de comunas (.txt)',
            type=['txt'], key='up_comunas',
        )
        if archivo and st.button('Procesar Comunas', key='btn_c', type='primary'):
            with st.spinner('Procesando...'):
                try:
                    df, ruta_log, resumen = procesar_comunas(archivo.getvalue())
                    mostrar_resumen(resumen)
                    st.markdown('#### Vista previa')
                    st.dataframe(df, use_container_width=True, height=400)
                    c1, c2 = st.columns(2)
                    with c1:
                        btn_csv(df, 'comunas_normalizadas.csv',
                                'Descargar CSV Normalizado')
                    with c2:
                        btn_log(ruta_log)
                except Exception as e:
                    st.error(f'Error: {e}')

    # ------ TAB 2: FAMOSOS ------------------------------------
    with tab2:
        st.markdown('### Normalizacion de Famosos y Fechas')
        st.markdown(
            'Sube el archivo `DATOS2026-2.txt`. '
            'El sistema unificara las fechas al formato DD-MM-YYYY, '
            'calculara la edad, marcara cumpleanos del dia '
            'y eliminara duplicados.'
        )
        archivo = st.file_uploader(
            'Selecciona el archivo de famosos (.txt)',
            type=['txt'], key='up_famosos',
        )
        if archivo and st.button('Procesar Famosos', key='btn_f', type='primary'):
            with st.spinner('Procesando...'):
                try:
                    df, ruta_log, resumen = procesar_famosos(archivo.getvalue())
                    mostrar_resumen(resumen)
                    cumple = df[df['Cumpleanos_Hoy'] == 1]
                    if not cumple.empty:
                        st.success(
                            f'Cumpleanos hoy: '
                            f'{", ".join(cumple["Nombre"].tolist())}'
                        )
                    st.markdown('#### Vista previa')
                    st.dataframe(df, use_container_width=True, height=400)
                    c1, c2 = st.columns(2)
                    with c1:
                        btn_csv(df, 'famosos_normalizados.csv',
                                'Descargar CSV Normalizado')
                    with c2:
                        btn_log(ruta_log)
                except Exception as e:
                    st.error(f'Error: {e}')

    # ------ TAB 3: LUGARES ------------------------------------
    with tab3:
        st.markdown('### Normalizacion de Lugares del Mundo')
        st.markdown(
            'Sube el archivo `DATOS2026-3.TXT`. '
            'El sistema generara **tres tablas relacionales** '
            'con clave foranea `ID_lugar`: '
            '`Lugares`, `Georeferencias` y `Direcciones`.'
        )
        archivo = st.file_uploader(
            'Selecciona el archivo de lugares (.txt)',
            type=['txt', 'TXT'], key='up_lugares',
        )
        if archivo and st.button('Procesar Lugares', key='btn_l', type='primary'):
            with st.spinner('Procesando...'):
                try:
                    df_l, df_g, df_d, ruta_log, resumen = procesar_lugares(
                        archivo.getvalue()
                    )
                    mostrar_resumen(resumen)

                    st1, st2, st3 = st.tabs([
                        f'Lugares ({len(df_l)})',
                        f'Georeferencias ({len(df_g)})',
                        f'Direcciones ({len(df_d)})',
                    ])
                    with st1:
                        st.markdown('**Tabla Lugares** — ID_lugar (PK), nombre_lugar')
                        st.dataframe(df_l, use_container_width=True, height=400)
                        btn_csv(df_l, 'tabla_lugares.csv', 'Descargar Lugares')
                    with st2:
                        st.markdown(
                            '**Tabla Georeferencias** — '
                            'ID_geo (PK), ID_lugar (FK), latitud, longitud'
                        )
                        st.dataframe(df_g, use_container_width=True, height=400)
                        btn_csv(df_g, 'tabla_georeferencias.csv',
                                'Descargar Georeferencias')
                    with st3:
                        st.markdown(
                            '**Tabla Direcciones** — '
                            'ID_dir (PK), ID_lugar (FK), nombre_calle, '
                            'numero_calle, ciudad_estado_provincia, pais'
                        )
                        st.dataframe(df_d, use_container_width=True, height=400)
                        btn_csv(df_d, 'tabla_direcciones.csv',
                                'Descargar Direcciones')

                    st.markdown('---')
                    btn_log(ruta_log)
                except Exception as e:
                    st.error(f'Error: {e}')


# -- Punto de entrada ------------------------------------------
if __name__ == '__main__':
    main()