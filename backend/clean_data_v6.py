import pandas as pd
from datetime import datetime

# --- CONFIGURACIÓN ---
NOMBRE_ARCHIVO_ORIGINAL = 'PropiedadesOriginal.csv'
NOMBRE_ARCHIVO_DOLAR = 'DOLAR OFICIAL - Cotizaciones historicas.csv'
NOMBRE_ARCHIVO_LIMPIO = 'PropiedadesLimpio_v6.csv' # <-- CAMBIO DE NOMBRE

# Columnas a dropear
COLUMNAS_A_ELIMINAR = ['description', 'l5', 'l6']

# Columnas importantes para verificar valores nulos
# SE HAN AÑADIDO 'superficie_cubierta', 'pais', 'provincia', 'partido'
COLUMNAS_SIN_NULOS = [
    'latitud', 'longitud', 'dormitorios', 'banios', 
    'superficie_total', 'precio_dolares', 'tipo_operacion', 'tipo_propiedad',
    'superficie_cubierta', 'pais', 'provincia', 'partido'
]

# Mapeo de nombres de columnas a español
NUEVOS_NOMBRES_COLUMNAS = {
    'id': 'id',
    'ad_type': 'tipo_publicidad',
    'start_date': 'fecha_inicio',
    'end_date': 'fecha_fin',
    'created_on': 'fecha_creacion',
    'lat': 'latitud',
    'lon': 'longitud',
    'l1': 'pais',
    'l2': 'provincia',
    'l3': 'partido',
    'l4': 'localidad',
    'rooms': 'ambientes',
    'bedrooms': 'dormitorios',
    'bathrooms': 'banios',
    'surface_total': 'superficie_total',
    'surface_covered': 'superficie_cubierta',
    'currency': 'moneda',
    'price_period': 'periodo_precio',
    'title': 'titulo',
    'property_type': 'tipo_propiedad',
    'operation_type': 'tipo_operacion',
    'price': 'precio'
}

# IMPORTANTE: Ajusta este nombre si la columna de fecha en tu CSV de dólar se llama diferente
NOMBRE_COLUMNA_FECHA_DOLAR = 'fecha' 

# --- FUNCIONES ---

def cargar_y_preparar_cotizaciones(archivo_dolar):
    """
    Carga el archivo de cotizaciones y lo prepara para búsquedas rápidas.
    """
    print(f"Cargando cotizaciones desde '{archivo_dolar}'...")
    df_dolar = pd.read_csv(archivo_dolar)
    
    # Convertir la columna de fecha a datetime
    df_dolar[NOMBRE_COLUMNA_FECHA_DOLAR] = pd.to_datetime(df_dolar[NOMBRE_COLUMNA_FECHA_DOLAR], dayfirst=True)
    
    # Asegurarse de que la columna 'cierre' sea numérica
    df_dolar['cierre'] = df_dolar['cierre'].astype(str).str.replace(',', '.').astype(float)
    
    # Establecer la fecha como índice
    df_dolar.set_index(NOMBRE_COLUMNA_FECHA_DOLAR, inplace=True)
    
    # Ordenar por fecha
    df_dolar.sort_index(inplace=True)
    
    # Rellenar fechas faltantes con el último valor conocido
    df_dolar = df_dolar.asfreq('D').fillna(method='ffill')
    
    print("Cotizaciones cargadas y procesadas.")
    return df_dolar

def limpiar_y_convertir(df, df_dolar):
    """
    Aplica toda la lógica de limpieza, filtrado y conversión de moneda.
    """
    print(f"1. Eliminando columnas no deseadas: {COLUMNAS_A_ELIMINAR}...")
    df.drop(columns=COLUMNAS_A_ELIMINAR, inplace=True, errors='ignore')

    print("2. Intercambiando latitud y longitud...")
    # Se realiza el intercambio antes de renombrar
    df.rename(columns={'lat': 'lon_temp', 'lon': 'lat'}, inplace=True)
    df.rename(columns={'lon_temp': 'lon'}, inplace=True)

    print("3. Renombrando columnas al español...")
    df.rename(columns=NUEVOS_NOMBRES_COLUMNAS, inplace=True)

    print("4. Filtrando monedas: Eliminando UYU...")
    df = df[df['moneda'] != 'UYU'].copy()

    print("5. Convirtiendo precios en ARS a USD...")
    
    df['fecha_simple'] = pd.to_datetime(df['fecha_creacion']).dt.normalize()
    df['tasa_cambio'] = df['fecha_simple'].map(df_dolar['cierre'])

    df['precio_dolares'] = df.apply(
        lambda row: row['precio'] / row['tasa_cambio'] if row['moneda'] == 'ARS' and pd.notna(row['tasa_cambio']) else row['precio'],
        axis=1
    )
    df.loc[df['moneda'] != 'ARS', 'precio_dolares'] = df['precio']

    print(f"6. Eliminando filas con datos faltantes en columnas clave...")
    # La lista COLUMNAS_SIN_NULOS ahora es más larga
    df.dropna(subset=COLUMNAS_SIN_NULOS, inplace=True)
    print(f"   Columnas revisadas: {COLUMNAS_SIN_NULOS}")
    
    print("7. Filtrando outliers de precios...")
    # Guardar el número de filas antes de filtrar
    registros_antes = len(df)

    # Filtrar alquileres caros
    alquileres_caros = (df['tipo_operacion'] == 'Alquiler') & (df['precio_dolares'] > 2000)
    df = df[~alquileres_caros]
    
    # Filtrar ventas caras
    ventas_caras = (df['tipo_operacion'] == 'Venta') & (df['precio_dolares'] > 500000)
    df = df[~ventas_caras]
    
    registros_despues = len(df)
    print(f"   Se eliminaron {registros_antes - registros_despues} registros por precios fuera de rango.")

    # Limpieza final de columnas
    df.drop(columns=['precio', 'moneda', 'fecha_simple', 'tasa_cambio'], inplace=True)

    return df

# --- EJECUCIÓN PRINCIPAL ---

if __name__ == "__main__":
    try:
        print("Iniciando proceso de limpieza de datos (v6)...")
        df_original = pd.read_csv(NOMBRE_ARCHIVO_ORIGINAL, low_memory=False)
        df_dolar = cargar_y_preparar_cotizaciones(NOMBRE_ARCHIVO_DOLAR)
        
        df_limpio = limpiar_y_convertir(df_original.copy(), df_dolar)

        print(f"\nGuardando el archivo limpio en '{NOMBRE_ARCHIVO_LIMPIO}'...")
        df_limpio.to_csv(NOMBRE_ARCHIVO_LIMPIO, index=False)

        print("\n¡Proceso completado exitosamente!")
        print(f"Se ha guardado el archivo '{NOMBRE_ARCHIVO_LIMPIO}' con {len(df_limpio)} registros.")

    except FileNotFoundError as e:
        print(f"Error: No se encontró el archivo '{e.filename}'. Asegúrate de que esté en la misma carpeta.")
    except KeyError as e:
        print(f"Error de columna: No se encontró la columna {e}. Revisa los nombres de las columnas en tus archivos CSV.")
        print(f"Asegúrate de que la columna de fecha en el archivo de dólar se llama '{NOMBRE_COLUMNA_FECHA_DOLAR}'. Si no, ajústala en el script.")
    except Exception as e:
        print(f"Ocurrió un error inesperado: {e}")
