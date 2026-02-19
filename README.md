# Horizonte Inmobiliaria - Panel de Control del Pipeline de Datos

Este proyecto proporciona una interfaz web simple y profesional para ejecutar un pipeline de limpieza de datos de Python.

La industria inmobiliaria enfrenta una paradoja crítica: mientras el volumen de datos disponibles crece exponencialmente, la toma de decisiones sigue dependiendo de métodos tradicionales que generan ineficiencias operativas y comerciales significativas.



La aplicacion consta de dos componentes principales:
1.  **Backend (Flask):** Un servidor en Python que ejecuta el script de limpieza y comunica el estado del proceso en tiempo real.
2.  **Frontend (React):** Una moderna interfaz de usuario web que permite a un usuario final iniciar el pipeline y ver su progreso.

## Requisitos Previos

- Python (version 3.8 o superior)
- Node.js (version 18 o superior) y npm

## Estructura de Archivos

```
/
├── backend/
│   ├── clean_data_v4.py        # El script de limpieza de datos
│   ├── server.py               # El servidor Flask
│   ├── requirements.txt        # Dependencias de Python
│   ├── uploads/                # Archivos CSV subidos desde la UI
│   └── (Archivos de datos CSV) # Asegurate de que tus CSV esten aqui o subelos desde la UI
│
├── frontend/
│   ├── src/
│   │   └── App.tsx             # Componente principal de React
│   ├── public/                 # Estaticos (logo, etc.)
│   ├── package.json            # Dependencias de Node.js
│   └── ...                     # Otros archivos de React
│
└── README.md                   # Este archivo
```

IMPORTANTE: Antes de comenzar, asegúrate de que los CSV requeridos por `clean_data_v4.py` (como el de propiedades y el de dolar) esten accesibles en `backend/uploads/` (subidos desde la UI) o en la carpeta `Datasets crudos/` si usas rutas por defecto.

## Pasos para la Instalacion y Ejecucion

Debes abrir dos terminales separadas, una para el backend y otra para el frontend.

### 1. Backend (Servidor Flask)

**Terminal 1:**

```bash
# 1. Navega a la carpeta del backend
cd backend

# 2. (Recomendado) Crea y activa un entorno virtual
python -m venv venv
# En Windows:
venv\Scripts\activate
# En macOS/Linux:
# source venv/bin/activate

# 3. Instala las dependencias de Python
pip install -r requirements.txt

# 4. Ejecuta el servidor del backend
python server.py
```

El servidor Flask ahora estara corriendo en `http://localhost:5000`. Deberias ver un mensaje indicando que el servidor se ha iniciado.

---

### 2. Frontend (Interfaz de React)

**Terminal 2:**

```bash
# 1. Navega a la carpeta del frontend
cd frontend

# 2. Instala las dependencias de Node.js
npm install

# 3. Inicia la aplicacion de desarrollo de React
npm run dev
```

La aplicacion de React se iniciara y te proporcionara una URL, generalmente `http://localhost:5173`.

### 3. Uso

Abre la URL del frontend (ej. `http://localhost:5173`) en tu navegador web. Veras la interfaz de "Horizonte Inmobiliaria".

- **Para ejecutar el pipeline completo o por etapas:** Usa los botones de la UI (Limpieza, Conversion, Pipeline completo).
- **Para configurar fuentes de datos:** Sube los CSV en la tarjeta “Fuentes de datos”. El backend los guardara en `backend/uploads/`.
- **Para ver el progreso:** Los mensajes de estado aparecen en la UI, con barra de progreso y mensajes en lenguaje de negocio.
- **Para ver la ultima ejecucion:** La fecha y hora de la ultima modificacion del archivo de salida se muestra en el encabezado.

Ambas terminales (backend y frontend) deben permanecer abiertas mientras usas la aplicacion.

## Conclusión

###Colaboradores 
-Nicolás Amarilla
-Nicolás Djandjikian
-Nicolás Fernández
-Carlos Hernández



