from flask import Flask, jsonify, request
from flask_cors import CORS
from flask_socketio import SocketIO
from werkzeug.utils import secure_filename
import os
import subprocess
import sys
from datetime import datetime

app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "*"}})
socketio = SocketIO(app, cors_allowed_origins="*")

# --- Constantes ---
OUTPUT_FILE_NAME = "PropiedadesLimpio_v4.csv"
SCRIPT_NAME = "clean_data_v4.py"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_FILE_PATH = os.path.join(BASE_DIR, OUTPUT_FILE_NAME)
SCRIPT_PATH = os.path.join(BASE_DIR, SCRIPT_NAME)
UPLOADS_DIR = os.path.join(BASE_DIR, "uploads")
os.makedirs(UPLOADS_DIR, exist_ok=True)
TRAINING_UPLOAD = os.path.join(UPLOADS_DIR, "entrenamiento.csv")
DOLAR_UPLOAD = os.path.join(UPLOADS_DIR, "DOLAR OFICIAL - Cotizaciones historicas.csv")


@app.route("/api/last-execution", methods=["GET"])
def get_last_execution_time():
    """Endpoint para obtener la fecha de última modificación del archivo de salida."""
    if not os.path.exists(OUTPUT_FILE_PATH):
        return jsonify(last_execution_date=None, file_exists=False)

    try:
        last_mod_time = os.path.getmtime(OUTPUT_FILE_PATH)
        last_execution_date = datetime.fromtimestamp(last_mod_time).strftime("%d/%m/%Y, %H:%M:%S")
        return jsonify(last_execution_date=last_execution_date, file_exists=True)
    except Exception as e:
        return jsonify(error=str(e)), 500


@app.route("/api/sources", methods=["GET"])
def get_sources():
    """Devuelve qué archivos fuente están configurados actualmente."""
    return jsonify({
        "training_path": TRAINING_UPLOAD if os.path.exists(TRAINING_UPLOAD) else None,
        "dolar_path": DOLAR_UPLOAD if os.path.exists(DOLAR_UPLOAD) else None,
        "defaults": True  # indicador simple por ahora
    })


@app.route("/api/upload-source", methods=["POST"])
def upload_source():
    """Permite subir los CSV de entrenamiento y cotización dolar."""
    if "training" not in request.files and "dolar" not in request.files:
        return jsonify(error="Envía al menos un archivo con claves 'training' o 'dolar'"), 400

    saved = {}
    for key, dest in [("training", TRAINING_UPLOAD), ("dolar", DOLAR_UPLOAD)]:
        file = request.files.get(key)
        if file:
            filename = secure_filename(file.filename)
            if not filename.lower().endswith(".csv"):
                return jsonify(error=f"El archivo de {key} debe ser CSV"), 400
            file.save(dest)
            saved[key] = dest

    return jsonify(message="Archivos cargados correctamente", saved=saved)


def _emit_pipeline_finished(action: str):
    """Envía el evento de finalización con la fecha del archivo si existe."""
    last_execution_date = None
    if os.path.exists(OUTPUT_FILE_PATH):
        last_mod_time = os.path.getmtime(OUTPUT_FILE_PATH)
        last_execution_date = datetime.fromtimestamp(last_mod_time).strftime("%d/%m/%Y, %H:%M:%S")

    socketio.emit(
        "pipeline_finished",
        {
            "message": f"Proceso {action} completado.",
            "last_execution_date": last_execution_date,
            "action": action,
        },
    )


def run_step(step: str):
    """Ejecuta el script con la etapa solicitada: clean, convert o all."""
    socketio.emit("status", {"message": f"Iniciando etapa: {step}..."})
    try:
        env = os.environ.copy()
        env["PYTHONIOENCODING"] = "utf-8"
        # Si existen uploads, indicarlos al script
        if os.path.exists(TRAINING_UPLOAD):
            env["TRAINING_CSV_PATH"] = TRAINING_UPLOAD
        if os.path.exists(DOLAR_UPLOAD):
            env["DOLAR_CSV_PATH"] = DOLAR_UPLOAD

        args = [sys.executable, "-u", SCRIPT_PATH]
        if step != "all":
            args.extend(["--step", step])

        process = subprocess.Popen(
            args,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            cwd=BASE_DIR,
            env=env,
        )

        for line in iter(process.stdout.readline, ""):
            clean_line = line.strip()
            if clean_line:
                print(f"Output: {clean_line}")
                socketio.emit("status", {"message": clean_line, "action": step})

        process.stdout.close()
        return_code = process.wait()

        if return_code == 0:
            socketio.emit("status", {"message": f"Etapa {step} finalizada con éxito.", "action": step})
            _emit_pipeline_finished(step)
        else:
            socketio.emit("status", {"message": f"Error en la etapa {step}. Código de salida: {return_code}", "action": step})

    except FileNotFoundError:
        error_msg = f"Error: El script '{SCRIPT_NAME}' no se encontró en la ruta esperada."
        print(error_msg)
        socketio.emit("status", {"message": error_msg, "action": step})
    except Exception as e:
        error_msg = f"Ocurrió un error inesperado al ejecutar la etapa {step}: {str(e)}"
        print(error_msg)
        socketio.emit("status", {"message": error_msg, "action": step})


@socketio.on("connect")
def handle_connect():
    """Confirma la conexión del cliente."""
    print("Cliente conectado")
    socketio.emit("status", {"message": "Conectado al servidor."})


@socketio.on("disconnect")
def handle_disconnect():
    """Maneja la desconexión del cliente."""
    print("Cliente desconectado")


@socketio.on("run_pipeline")
def handle_run_pipeline():
    """Compatibilidad: ejecuta el pipeline completo."""
    run_step("all")


@socketio.on("run_full")
def handle_run_full():
    """Ejecuta el pipeline completo."""
    run_step("all")


@socketio.on("run_clean")
def handle_run_clean():
    """Ejecuta solo la etapa de limpieza."""
    run_step("clean")


@socketio.on("run_convert")
def handle_run_convert():
    """Ejecuta solo la etapa de conversión."""
    run_step("convert")


if __name__ == "__main__":
    print("Iniciando servidor Flask...")
    socketio.run(app, debug=True, port=5000, host="0.0.0.0")
