"""
===============================================================================
  ETL - REPORTE MAESTRO DE PRODUCCIÓN INDUSTRIAL
  Autor: Ingeniero de Datos Senior
  Fecha: 2026-02-16
  Descripción:
    Script que procesa 3 archivos CSV con formatos heterogéneos,
    aplica limpieza, normalización y unificación, y genera un
    'Reporte_Maestro.csv' estandarizado.
===============================================================================
"""

import pandas as pd
import numpy as np
import os
import re
import sys
import io
from datetime import datetime

# Forzar codificación UTF-8 en la consola de Windows para soportar emojis
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# ──────────────────────────────────────────────────────────────────────────────
# CONFIGURACIÓN
# ──────────────────────────────────────────────────────────────────────────────

# Columnas finales estandarizadas
COLUMNAS_FINALES = ['Fecha', 'Producto', 'Cantidad', 'Maquina', 'Operador']

# Directorio donde se encuentran los CSV de entrada
DIRECTORIO_ENTRADA = os.path.dirname(os.path.abspath(__file__))

# Nombre del archivo de salida
ARCHIVO_SALIDA = os.path.join(DIRECTORIO_ENTRADA, 'Reporte_Maestro.csv')

# Archivos esperados
ARCHIVOS = {
    'enero':  'reporte_produccion_enero.csv',
    'febrero': 'produccion_feb_sucio.csv',
    'marzo':  'prod_marzo_v2.csv',
}


# ──────────────────────────────────────────────────────────────────────────────
# FUNCIONES AUXILIARES
# ──────────────────────────────────────────────────────────────────────────────

def log(mensaje: str, nivel: str = "INFO") -> None:
    """Imprime mensajes con timestamp y nivel para trazabilidad."""
    timestamp = datetime.now().strftime("%H:%M:%S")
    simbolo = {"INFO": "ℹ️", "OK": "✅", "WARN": "⚠️", "ERROR": "❌"}.get(nivel, "📌")
    print(f"  [{timestamp}] {simbolo} {mensaje}")


def limpiar_cantidad(valor) -> float:
    """
    Limpia valores de la columna Cantidad:
    - Remueve texto como 'pzas', 'pz', 'und', espacios, etc.
    - Convierte a float.
    - Retorna 0.0 si el valor es nulo, vacío o no convertible.
    """
    if pd.isna(valor):
        return 0.0
    texto = str(valor).strip().lower()
    # Eliminar cualquier sufijo de texto (pzas, pz, und, unidades, etc.)
    texto = re.sub(r'[a-záéíóúñ\s/]+', '', texto)
    try:
        return float(texto)
    except (ValueError, TypeError):
        return 0.0


def estandarizar_fecha(fecha_str, formato_origen: str = None) -> str:
    """
    Convierte cualquier formato de fecha al estándar YYYY-MM-DD.
    Intenta múltiples formatos si no se especifica uno.
    """
    if pd.isna(fecha_str):
        return None

    fecha_str = str(fecha_str).strip()

    # Lista de formatos a intentar (ordenados por prioridad)
    formatos = []
    if formato_origen:
        formatos.append(formato_origen)
    formatos.extend([
        '%Y-%m-%d',       # 2026-01-15
        '%d/%m/%Y',       # 15/01/2026
        '%m/%d/%Y',       # 01/15/2026
        '%d-%m-%Y',       # 15-01-2026
        '%Y/%m/%d',       # 2026/01/15
        '%d.%m.%Y',       # 15.01.2026
    ])

    for fmt in formatos:
        try:
            return datetime.strptime(fecha_str, fmt).strftime('%Y-%m-%d')
        except ValueError:
            continue

    # Si ningún formato coincide, devolver el valor original
    return fecha_str


def estandarizar_producto(nombre: str) -> str:
    """
    Normaliza los nombres de producto a formato Title Case consistente.
    Ejemplo: 'BUJE BRONCE' → 'Buje Bronce', 'eje principal' → 'Eje Principal'
    """
    if pd.isna(nombre):
        return nombre
    # Limpiar comillas dobles residuales y espacios extra
    nombre = str(nombre).strip().replace('"', '')
    # Casos especiales para mantener convenciones industriales
    nombre = nombre.title()
    # Corregir partículas que no deben ir en mayúscula
    nombre = nombre.replace(' De ', ' de ').replace(' Del ', ' del ')
    # Restaurar convenciones: "Hex" → "Hex", "1/2" queda tal cual
    return nombre


def estandarizar_maquina(nombre: str) -> str:
    """
    Normaliza los nombres de máquina.
    'CNC 01' → 'CNC-01', 'Fresadora B' → 'Fresadora-B', etc.
    """
    if pd.isna(nombre):
        return nombre
    nombre = str(nombre).strip()
    # Reemplazar espacios entre nombre y número por guión
    nombre = re.sub(r'(\D)\s+(\d)', r'\1-\2', nombre)
    # Reemplazar espacios entre nombre y letra por guión
    nombre = re.sub(r'(\w)\s+([A-Z]$)', r'\1-\2', nombre)
    return nombre


# ──────────────────────────────────────────────────────────────────────────────
# PROCESADORES POR ARCHIVO
# ──────────────────────────────────────────────────────────────────────────────

def procesar_enero(ruta: str) -> pd.DataFrame:
    """
    Procesa: reporte_produccion_enero.csv
    Características:
      - Separador: coma (,)
      - Fechas: YYYY-MM-DD (formato estándar)
      - Nulos en columna 'Cantidad'
      - Columnas ya nombradas correctamente
    """
    log("Leyendo archivo de ENERO...", "INFO")

    df = pd.read_csv(ruta, encoding='utf-8')
    filas_originales = len(df)
    log(f"Filas leídas: {filas_originales}", "INFO")

    # --- Limpieza de Cantidad: llenar nulos con 0 y convertir a numérico ---
    nulos_cantidad = df['Cantidad'].isna().sum()
    if nulos_cantidad > 0:
        log(f"Valores nulos encontrados en 'Cantidad': {nulos_cantidad} → Rellenando con 0", "WARN")
    df['Cantidad'] = df['Cantidad'].apply(limpiar_cantidad).astype(int)

    # --- Estandarizar fechas (ya están en formato correcto, pero validamos) ---
    df['Fecha'] = df['Fecha'].apply(lambda x: estandarizar_fecha(x))

    # --- Estandarizar nombres de producto ---
    df['Producto'] = df['Producto'].apply(estandarizar_producto)

    # --- Estandarizar nombres de máquina ---
    df['Maquina'] = df['Maquina'].apply(estandarizar_maquina)

    # --- Seleccionar solo columnas finales ---
    df = df[COLUMNAS_FINALES]

    log(f"Enero procesado exitosamente: {len(df)} filas limpias", "OK")
    return df, filas_originales


def procesar_febrero(ruta: str) -> pd.DataFrame:
    """
    Procesa: produccion_feb_sucio.csv
    Características:
      - Separador: punto y coma (;)
      - Fechas: DD/MM/YYYY
      - Columnas en spanglish: Dia de Produccion, Item, Qty, Machine_ID, Responsable
      - Cantidad con texto: "100 pzas", "250 pzas"
      - Nombres de producto en MAYÚSCULAS
    """
    log("Leyendo archivo de FEBRERO...", "INFO")

    df = pd.read_csv(ruta, sep=';', encoding='utf-8')
    filas_originales = len(df)
    log(f"Filas leídas: {filas_originales}", "INFO")

    # --- Renombrar columnas de spanglish al estándar ---
    mapeo_columnas = {
        'Dia de Produccion': 'Fecha',
        'Item':              'Producto',
        'Qty':               'Cantidad',
        'Machine_ID':        'Maquina',
        'Responsable':       'Operador',
    }
    df.rename(columns=mapeo_columnas, inplace=True)
    log("Columnas renombradas al estándar", "INFO")

    # --- Convertir fechas de DD/MM/YYYY a YYYY-MM-DD ---
    df['Fecha'] = df['Fecha'].apply(lambda x: estandarizar_fecha(x, '%d/%m/%Y'))
    log("Fechas convertidas a formato YYYY-MM-DD", "INFO")

    # --- Limpiar cantidad: quitar "pzas" y convertir a número ---
    registros_con_texto = df['Cantidad'].astype(str).str.contains(r'[a-zA-Z]', na=False).sum()
    if registros_con_texto > 0:
        log(f"Registros con texto en 'Cantidad': {registros_con_texto} → Limpiando", "WARN")
    df['Cantidad'] = df['Cantidad'].apply(limpiar_cantidad).astype(int)

    # --- Estandarizar nombres de producto (MAYÚSCULAS → Title Case) ---
    df['Producto'] = df['Producto'].apply(estandarizar_producto)

    # --- Estandarizar nombres de máquina ---
    df['Maquina'] = df['Maquina'].apply(estandarizar_maquina)

    # --- Seleccionar solo columnas finales ---
    df = df[COLUMNAS_FINALES]

    log(f"Febrero procesado exitosamente: {len(df)} filas limpias", "OK")
    return df, filas_originales


def procesar_marzo(ruta: str) -> pd.DataFrame:
    """
    Procesa: prod_marzo_v2.csv
    Características:
      - 3 filas de basura al inicio (título del reporte + línea vacía)
      - Headers reales en fila 4 (index 3)
      - Columnas en MAYÚSCULAS: FECHA, PRODUCTO, CANT_APROBADA, CANT_RECHAZADA, MAQUINA, OPERADOR
      - Columna extra CANT_RECHAZADA
      - Se debe calcular Cantidad_Neta = CANT_APROBADA - CANT_RECHAZADA
    """
    log("Leyendo archivo de MARZO...", "INFO")

    # Saltar las 3 primeras filas basura (fila 1: título, fila 2: generado por, fila 3: vacía)
    df = pd.read_csv(ruta, skiprows=3, encoding='utf-8')
    filas_originales = len(df)
    log(f"Filas leídas (post-skiprows): {filas_originales}", "INFO")
    log("Se saltaron 3 filas de encabezado basura", "WARN")

    # --- Calcular Cantidad Neta (Aprobada - Rechazada) ---
    df['CANT_APROBADA'] = pd.to_numeric(df['CANT_APROBADA'], errors='coerce').fillna(0).astype(int)
    df['CANT_RECHAZADA'] = pd.to_numeric(df['CANT_RECHAZADA'], errors='coerce').fillna(0).astype(int)
    df['Cantidad'] = df['CANT_APROBADA'] - df['CANT_RECHAZADA']
    log(f"Cantidad Neta calculada (Aprobada - Rechazada). "
        f"Total rechazadas: {df['CANT_RECHAZADA'].sum()} piezas", "INFO")

    # --- Renombrar columnas al estándar ---
    mapeo_columnas = {
        'FECHA':    'Fecha',
        'PRODUCTO': 'Producto',
        'MAQUINA':  'Maquina',
        'OPERADOR': 'Operador',
    }
    df.rename(columns=mapeo_columnas, inplace=True)

    # --- Estandarizar fechas (ya están en YYYY-MM-DD, validamos por seguridad) ---
    df['Fecha'] = df['Fecha'].apply(lambda x: estandarizar_fecha(x))

    # --- Estandarizar nombres de producto ---
    df['Producto'] = df['Producto'].apply(estandarizar_producto)

    # --- Estandarizar nombres de máquina ---
    df['Maquina'] = df['Maquina'].apply(estandarizar_maquina)

    # --- Seleccionar solo columnas finales ---
    df = df[COLUMNAS_FINALES]

    log(f"Marzo procesado exitosamente: {len(df)} filas limpias", "OK")
    return df, filas_originales


# ──────────────────────────────────────────────────────────────────────────────
# PIPELINE PRINCIPAL ETL
# ──────────────────────────────────────────────────────────────────────────────

def ejecutar_etl():
    """
    Ejecuta el pipeline ETL completo:
    1. Extract  → Lee los 3 archivos CSV
    2. Transform → Limpia, normaliza y estandariza cada uno
    3. Load     → Unifica y exporta a Reporte_Maestro.csv
    """
    print()
    print("=" * 72)
    print("  🏭 ETL - REPORTE MAESTRO DE PRODUCCIÓN INDUSTRIAL")
    print("=" * 72)
    print(f"  📅 Ejecución: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  📂 Directorio: {DIRECTORIO_ENTRADA}")
    print("=" * 72)

    dataframes = []
    resumen = {}  # {nombre_mes: (filas_originales, filas_procesadas)}
    errores = []

    # ── FASE 1: EXTRACT + TRANSFORM ──────────────────────────────────────
    procesadores = {
        'enero':   procesar_enero,
        'febrero': procesar_febrero,
        'marzo':   procesar_marzo,
    }

    for mes, funcion in procesadores.items():
        archivo = ARCHIVOS[mes]
        ruta = os.path.join(DIRECTORIO_ENTRADA, archivo)

        print(f"\n{'─' * 72}")
        print(f"  📄 Procesando: {archivo}")
        print(f"{'─' * 72}")

        try:
            # Verificar si el archivo existe
            if not os.path.exists(ruta):
                raise FileNotFoundError(f"Archivo no encontrado: {ruta}")

            # Verificar que no esté vacío
            if os.path.getsize(ruta) == 0:
                raise ValueError(f"El archivo está vacío: {archivo}")

            # Procesar archivo
            df, filas_orig = funcion(ruta)

            # Agregar columna de origen para trazabilidad
            df['Origen'] = mes.capitalize()

            dataframes.append(df)
            resumen[mes.capitalize()] = (filas_orig, len(df))

        except FileNotFoundError as e:
            log(str(e), "ERROR")
            errores.append(archivo)
        except pd.errors.EmptyDataError:
            log(f"El archivo '{archivo}' está vacío o corrupto", "ERROR")
            errores.append(archivo)
        except KeyError as e:
            log(f"Columna esperada no encontrada en '{archivo}': {e}", "ERROR")
            errores.append(archivo)
        except Exception as e:
            log(f"Error inesperado procesando '{archivo}': {type(e).__name__}: {e}", "ERROR")
            errores.append(archivo)

    # ── FASE 2: VALIDACIÓN PRE-CARGA ─────────────────────────────────────
    print(f"\n{'═' * 72}")
    print("  📊 VALIDACIÓN Y RESUMEN")
    print(f"{'═' * 72}")

    if not dataframes:
        log("No se procesó ningún archivo exitosamente. Abortando.", "ERROR")
        sys.exit(1)

    if errores:
        log(f"Archivos con errores ({len(errores)}): {', '.join(errores)}", "WARN")
        print()

    # ── FASE 3: LOAD (Unificación + Exportación) ─────────────────────────
    print(f"\n{'─' * 72}")
    print("  🔗 UNIFICACIÓN DE DATOS")
    print(f"{'─' * 72}")

    # Concatenar todos los DataFrames
    df_maestro = pd.concat(dataframes, ignore_index=True)
    log(f"DataFrames concatenados exitosamente", "OK")

    # Ordenar por fecha y resetear índice
    df_maestro.sort_values(by='Fecha', ascending=True, inplace=True)
    df_maestro.reset_index(drop=True, inplace=True)
    log("Datos ordenados cronológicamente por Fecha", "OK")

    # Eliminar posibles duplicados exactos
    duplicados = df_maestro.duplicated().sum()
    if duplicados > 0:
        df_maestro.drop_duplicates(inplace=True)
        log(f"Duplicados eliminados: {duplicados}", "WARN")
    else:
        log("Sin duplicados detectados", "OK")

    # ── EXPORTACIÓN ──────────────────────────────────────────────────────
    print(f"\n{'─' * 72}")
    print("  💾 EXPORTACIÓN")
    print(f"{'─' * 72}")

    df_maestro.to_csv(ARCHIVO_SALIDA, index=False, encoding='utf-8-sig')
    log(f"Archivo exportado: {ARCHIVO_SALIDA}", "OK")
    log(f"Tamaño: {os.path.getsize(ARCHIVO_SALIDA):,} bytes", "INFO")

    # ── REPORTE FINAL ────────────────────────────────────────────────────
    print(f"\n{'═' * 72}")
    print("  📋 REPORTE FINAL DE ETL")
    print(f"{'═' * 72}")

    total_originales = 0
    total_finales = 0

    print(f"\n  {'Mes':<12} {'Filas Orig.':<15} {'Filas Limpias':<15} {'Estado'}")
    print(f"  {'─' * 55}")

    for mes, (orig, final) in resumen.items():
        total_originales += orig
        total_finales += final
        estado = "✅ OK" if orig == final else f"⚠️ Δ{final - orig:+d}"
        print(f"  {mes:<12} {orig:<15} {final:<15} {estado}")

    print(f"  {'─' * 55}")
    print(f"  {'TOTAL':<12} {total_originales:<15} {total_finales:<15}")
    print()

    # Estadísticas adicionales del DataFrame final
    print(f"  📊 Estadísticas del Reporte Maestro:")
    print(f"     • Filas totales:      {len(df_maestro):,}")
    print(f"     • Columnas:           {list(df_maestro.columns)}")
    print(f"     • Rango de fechas:    {df_maestro['Fecha'].min()} → {df_maestro['Fecha'].max()}")
    print(f"     • Productos únicos:   {df_maestro['Producto'].nunique()}")
    print(f"     • Máquinas únicas:    {df_maestro['Maquina'].nunique()}")
    print(f"     • Operadores únicos:  {df_maestro['Operador'].nunique()}")
    print(f"     • Cantidad total:     {df_maestro['Cantidad'].sum():,.0f} piezas")
    print(f"     • Cantidad promedio:  {df_maestro['Cantidad'].mean():,.1f} piezas/registro")

    print(f"\n{'═' * 72}")
    print(f"  ✅ ETL COMPLETADO - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  📁 Resultado: {os.path.basename(ARCHIVO_SALIDA)}")
    print(f"{'═' * 72}\n")

    # Mostrar una vista previa de las primeras filas
    print("  👀 Vista previa (primeras 10 filas):")
    print(df_maestro.head(10).to_string(index=True))
    print()

    # ── BONUS: REPORTE ANALÍTICO ─────────────────────────────────────────
    print(f"\n{'█' * 72}")
    print(f"  🏆 BONUS - ANÁLISIS DE PRODUCCIÓN")
    print(f"{'█' * 72}")

    # ── 1. Total de Piezas Producidas por Máquina ────────────────────────
    print(f"\n  {'─' * 68}")
    print(f"  🔩 TOTAL DE PIEZAS PRODUCIDAS POR MÁQUINA")
    print(f"  {'─' * 68}\n")

    produccion_maquina = (
        df_maestro.groupby('Maquina')['Cantidad']
        .agg(['sum', 'count', 'mean'])
        .rename(columns={'sum': 'Total_Piezas', 'count': 'Registros', 'mean': 'Promedio'})
        .sort_values('Total_Piezas', ascending=False)
    )

    max_piezas = produccion_maquina['Total_Piezas'].max()

    for maquina, row in produccion_maquina.iterrows():
        # Barra visual proporcional (máximo 30 caracteres)
        barra_len = int((row['Total_Piezas'] / max_piezas) * 30)
        barra = '█' * barra_len + '░' * (30 - barra_len)
        porcentaje = (row['Total_Piezas'] / produccion_maquina['Total_Piezas'].sum()) * 100

        print(f"  {maquina:<14} {barra} {row['Total_Piezas']:>7,.0f} pzas "
              f"({porcentaje:>5.1f}%) │ {row['Registros']:>3.0f} registros │ "
              f"Prom: {row['Promedio']:>6.1f}")

    print(f"\n  {'Total general:':<14} {'':>30} {produccion_maquina['Total_Piezas'].sum():>7,.0f} pzas")

    # ── 2. Operador Más Productivo ───────────────────────────────────────
    print(f"\n  {'─' * 68}")
    print(f"  👷 ¿CUÁL FUE EL OPERADOR MÁS PRODUCTIVO?")
    print(f"  {'─' * 68}\n")

    produccion_operador = (
        df_maestro.groupby('Operador')['Cantidad']
        .agg(['sum', 'count', 'mean'])
        .rename(columns={'sum': 'Total_Piezas', 'count': 'Registros', 'mean': 'Promedio'})
        .sort_values('Total_Piezas', ascending=False)
    )

    # El operador #1
    top_operador = produccion_operador.index[0]
    top_piezas = produccion_operador.iloc[0]['Total_Piezas']
    top_registros = produccion_operador.iloc[0]['Registros']
    top_promedio = produccion_operador.iloc[0]['Promedio']

    print(f"  🥇 OPERADOR MÁS PRODUCTIVO: {top_operador}")
    print(f"     ├── Total producido:  {top_piezas:,.0f} piezas")
    print(f"     ├── Registros:        {top_registros:.0f} turnos/registros")
    print(f"     └── Promedio/turno:   {top_promedio:,.1f} piezas\n")

    # Ranking completo de operadores
    print(f"  {'Pos':<5} {'Operador':<18} {'Total Piezas':>14} {'Registros':>11} {'Promedio':>10}")
    print(f"  {'─' * 60}")

    medallas = {0: '🥇', 1: '🥈', 2: '🥉'}
    for i, (operador, row) in enumerate(produccion_operador.iterrows()):
        medalla = medallas.get(i, '  ')
        print(f"  {medalla} {i+1}  {operador:<18} {row['Total_Piezas']:>12,.0f}   "
              f"{row['Registros']:>9.0f}   {row['Promedio']:>8.1f}")

    # Desglose por mes del operador top
    print(f"\n  📅 Desglose mensual de {top_operador}:")
    desglose_top = (
        df_maestro[df_maestro['Operador'] == top_operador]
        .groupby('Origen')['Cantidad']
        .agg(['sum', 'count'])
        .rename(columns={'sum': 'Piezas', 'count': 'Registros'})
    )
    # Ordenar meses cronológicamente
    orden_meses = ['Enero', 'Febrero', 'Marzo']
    desglose_top = desglose_top.reindex([m for m in orden_meses if m in desglose_top.index])

    for mes, row in desglose_top.iterrows():
        print(f"     • {mes:<10} → {row['Piezas']:>6,.0f} piezas en {row['Registros']:.0f} registros")

    # Máquina favorita del operador top
    maquina_fav = (
        df_maestro[df_maestro['Operador'] == top_operador]
        .groupby('Maquina')['Cantidad'].sum()
        .idxmax()
    )
    piezas_maquina_fav = (
        df_maestro[df_maestro['Operador'] == top_operador]
        .groupby('Maquina')['Cantidad'].sum()
        .max()
    )
    print(f"     • Máquina favorita: {maquina_fav} ({piezas_maquina_fav:,.0f} piezas)")

    print(f"\n{'█' * 72}")
    print(f"  📊 Fin del reporte analítico")
    print(f"{'█' * 72}\n")

    return df_maestro


# ──────────────────────────────────────────────────────────────────────────────
# EJECUCIÓN
# ──────────────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    try:
        df_resultado = ejecutar_etl()
    except KeyboardInterrupt:
        print("\n\n  ⛔ Ejecución cancelada por el usuario.\n")
        sys.exit(0)
    except Exception as e:
        print(f"\n  ❌ ERROR FATAL: {type(e).__name__}: {e}\n")
        sys.exit(1)
