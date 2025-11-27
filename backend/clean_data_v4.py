import argparse
import os

import pandas as pd

# --- CONFIGURACION ---
# Rutas absolutas para usar los mismos archivos que la web consume
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "..", "Datasets crudos")

# Entradas (permiten override por variables de entorno para archivos subidos)
NOMBRE_ARCHIVO_ORIGINAL = os.getenv(
    "TRAINING_CSV_PATH",
    os.path.join(DATA_DIR, "entrenamiento.csv"),
)
NOMBRE_ARCHIVO_DOLAR = os.getenv(
    "DOLAR_CSV_PATH",
    os.path.join(DATA_DIR, "DOLAR OFICIAL - Cotizaciones historicas.csv"),
)

# Salidas
NOMBRE_ARCHIVO_LIMPIO = os.path.join(BASE_DIR, "PropiedadesLimpio_v4.csv")
NOMBRE_ARCHIVO_LIMPIO_ETAPA1 = os.path.join(BASE_DIR, "PropiedadesLimpio_v4_clean.csv")

# Columnas a eliminar y a validar contra nulos (logica v6)
COLUMNAS_A_ELIMINAR = ["description", "l5", "l6"]
COLUMNAS_SIN_NULOS = [
    "latitud",
    "longitud",
    "dormitorios",
    "banios",
    "superficie_total",
    "precio_dolares",
    "tipo_operacion",
    "tipo_propiedad",
    "superficie_cubierta",
    "pais",
    "provincia",
    "partido",
]

# Diccionario para renombrar a espanol (logica v6)
NUEVOS_NOMBRES_COLUMNAS = {
    "id": "id",
    "ad_type": "tipo_publicidad",
    "start_date": "fecha_inicio",
    "end_date": "fecha_fin",
    "created_on": "fecha_creacion",
    "lat": "latitud",
    "lon": "longitud",
    "l1": "pais",
    "l2": "provincia",
    "l3": "partido",
    "l4": "localidad",
    "rooms": "ambientes",
    "bedrooms": "dormitorios",
    "bathrooms": "banios",
    "surface_total": "superficie_total",
    "surface_covered": "superficie_cubierta",
    "currency": "moneda",
    "price_period": "periodo_precio",
    "title": "titulo",
    "property_type": "tipo_propiedad",
    "operation_type": "tipo_operacion",
    "price": "precio",
}

NOMBRE_COLUMNA_FECHA_DOLAR = "fecha"


def cargar_y_preparar_cotizaciones(archivo_dolar: str) -> pd.DataFrame:
    """Carga el archivo de cotizaciones y lo prepara para busquedas rapidas."""
    print(f"Cargando cotizaciones desde '{archivo_dolar}'...")
    df_dolar = pd.read_csv(archivo_dolar, encoding="utf-8")
    df_dolar[NOMBRE_COLUMNA_FECHA_DOLAR] = pd.to_datetime(
        df_dolar[NOMBRE_COLUMNA_FECHA_DOLAR], dayfirst=True
    )
    df_dolar["cierre"] = df_dolar["cierre"].astype(str).str.replace(",", ".").astype(float)
    df_dolar.set_index(NOMBRE_COLUMNA_FECHA_DOLAR, inplace=True)
    df_dolar.sort_index(inplace=True)
    df_dolar = df_dolar.asfreq("D").fillna(method="ffill")
    print("Cotizaciones cargadas y procesadas.")
    return df_dolar


def etapa_limpieza(df: pd.DataFrame) -> pd.DataFrame:
    """Etapa 1: limpieza y renombrado base (sin conversion)."""
    print(f"1. Eliminando columnas no deseadas: {COLUMNAS_A_ELIMINAR}...")
    df.drop(columns=COLUMNAS_A_ELIMINAR, inplace=True, errors="ignore")

    print("2. Intercambiando latitud y longitud...")
    df.rename(columns={"lat": "lon_temp", "lon": "lat"}, inplace=True)
    df.rename(columns={"lon_temp": "lon"}, inplace=True)

    print("3. Renombrando columnas al espanol...")
    df.rename(columns=NUEVOS_NOMBRES_COLUMNAS, inplace=True)

    print("4. Filtrando monedas: Eliminando UYU...")
    df = df[df["moneda"] != "UYU"].copy()

    return df


def etapa_conversion(df: pd.DataFrame, df_dolar: pd.DataFrame) -> pd.DataFrame:
    """Etapa 2: conversion de moneda, drop de nulos y outliers."""
    print("5. Convirtiendo precios en ARS a USD...")
    df["fecha_simple"] = pd.to_datetime(df["fecha_creacion"]).dt.normalize()
    df["tasa_cambio"] = df["fecha_simple"].map(df_dolar["cierre"])

    df["precio_dolares"] = df.apply(
        lambda row: row["precio"] / row["tasa_cambio"]
        if row["moneda"] == "ARS" and pd.notna(row["tasa_cambio"])
        else row["precio"],
        axis=1,
    )
    df.loc[df["moneda"] != "ARS", "precio_dolares"] = df["precio"]

    print("6. Eliminando filas con datos faltantes en columnas clave...")
    df.dropna(subset=COLUMNAS_SIN_NULOS, inplace=True)
    print(f"   Columnas revisadas: {COLUMNAS_SIN_NULOS}")

    print("7. Filtrando outliers de precios...")
    registros_antes = len(df)
    alquileres_caros = (df["tipo_operacion"] == "Alquiler") & (df["precio_dolares"] > 2000)
    ventas_caras = (df["tipo_operacion"] == "Venta") & (df["precio_dolares"] > 500000)
    df = df[~alquileres_caros]
    df = df[~ventas_caras]
    registros_despues = len(df)
    print(f"   Se eliminaron {registros_antes - registros_despues} registros por precios fuera de rango.")

    df.drop(columns=["precio", "moneda", "fecha_simple", "tasa_cambio"], inplace=True)

    return df


def guardar_csv(df: pd.DataFrame, ruta: str) -> None:
    df.to_csv(ruta, index=False, encoding="utf-8-sig")
    print(f"Archivo guardado en '{ruta}' con {len(df)} registros.")


def pipeline_completo() -> None:
    print("Iniciando pipeline completo (limpieza + conversion)...")
    df_original = pd.read_csv(NOMBRE_ARCHIVO_ORIGINAL, low_memory=False, encoding="utf-8")
    df_dolar = cargar_y_preparar_cotizaciones(NOMBRE_ARCHIVO_DOLAR)

    df_limpio = etapa_limpieza(df_original.copy())
    df_final = etapa_conversion(df_limpio, df_dolar)

    guardar_csv(df_final, NOMBRE_ARCHIVO_LIMPIO)


def pipeline_etapa_limpieza() -> None:
    print("Ejecutando SOLO etapa de limpieza...")
    df_original = pd.read_csv(NOMBRE_ARCHIVO_ORIGINAL, low_memory=False, encoding="utf-8")
    df_limpio = etapa_limpieza(df_original.copy())
    guardar_csv(df_limpio, NOMBRE_ARCHIVO_LIMPIO_ETAPA1)


def pipeline_etapa_conversion(input_path: str | None = None) -> None:
    ruta_entrada = input_path or NOMBRE_ARCHIVO_LIMPIO_ETAPA1
    if not os.path.exists(ruta_entrada):
        raise FileNotFoundError(
            f"No se encontro el archivo limpio para convertir: '{ruta_entrada}'. "
            "Ejecuta primero la etapa de limpieza o indica --input."
        )

    print(f"Ejecutando SOLO etapa de conversion usando '{ruta_entrada}'...")
    df_limpio = pd.read_csv(ruta_entrada, low_memory=False, encoding="utf-8")
    df_dolar = cargar_y_preparar_cotizaciones(NOMBRE_ARCHIVO_DOLAR)
    df_final = etapa_conversion(df_limpio, df_dolar)
    guardar_csv(df_final, NOMBRE_ARCHIVO_LIMPIO)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Pipeline de limpieza de propiedades (v6 logic, rutas v4).")
    parser.add_argument(
        "--step",
        choices=["clean", "convert", "all"],
        default="all",
        help="Etapa a ejecutar: clean (solo limpieza), convert (solo conversion), all (pipeline completo).",
    )
    parser.add_argument(
        "--input",
        help="Ruta de CSV limpio para la etapa de conversion (opcional). Si no se indica, usa el generado en la etapa de limpieza.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    try:
        args = parse_args()
        if args.step == "clean":
            pipeline_etapa_limpieza()
        elif args.step == "convert":
            pipeline_etapa_conversion(args.input)
        else:
            pipeline_completo()
    except FileNotFoundError as e:
        print(f"Error: No se encontro el archivo '{e.filename}'. Asegurate de que este en la carpeta esperada.")
    except KeyError as e:
        print(f"Error de columna: No se encontro la columna {e}. Revisa los nombres en tus CSV.")
        print(f"Asegurate de que la columna de fecha en el archivo de dolar se llama '{NOMBRE_COLUMNA_FECHA_DOLAR}'.")
    except Exception as e:
        print(f"Ocurrio un error inesperado: {e}")
