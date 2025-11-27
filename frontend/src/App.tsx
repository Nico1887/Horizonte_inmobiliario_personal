import { useEffect, useRef, useState } from 'react';
import io from 'socket.io-client';
import './App.css';
import logo from '/logoHorizonte.png';

const BACKEND_URL = 'http://localhost:5000';
const socket = io(BACKEND_URL);

type ActionType = 'full' | 'clean' | 'convert';

const ACTION_LABELS: Record<ActionType, string> = {
  full: 'Pipeline completo',
  clean: 'Limpieza',
  convert: 'Conversión',
};

const ACTION_EVENT: Record<ActionType, string> = {
  full: 'run_full',
  clean: 'run_clean',
  convert: 'run_convert',
};

function App() {
  const [lastExecutionDate, setLastExecutionDate] = useState<string | null>('Cargando...');
  const [statusMessages, setStatusMessages] = useState<string[]>([]);
  const [isRunning, setIsRunning] = useState<boolean>(false);
  const [progress, setProgress] = useState<number>(0);
  const [currentAction, setCurrentAction] = useState<ActionType | null>(null);
  const [trainingFile, setTrainingFile] = useState<File | null>(null);
  const [dolarFile, setDolarFile] = useState<File | null>(null);
  const [uploadStatus, setUploadStatus] = useState<string>('');
  const [uploading, setUploading] = useState<boolean>(false);
  const [sources, setSources] = useState<{ training_path?: string | null; dolar_path?: string | null }>({});
  const statusLogRef = useRef<HTMLDivElement>(null);

  const businessMessage = (raw: string) => {
    const lower = raw.toLowerCase();
    const mappings: { match: string; text: string }[] = [
      { match: 'iniciando', text: 'Arrancando el proceso de depuración y control de datos.' },
      { match: 'cargando cotizaciones', text: 'Tomando la referencia del dólar para convertir precios.' },
      { match: 'eliminando columnas', text: 'Depurando campos innecesarios de la base.' },
      { match: 'intercambiando latitud', text: 'Reordenando ubicaciones geográficas.' },
      { match: 'renombrando columnas', text: 'Normalizando nombres para análisis.' },
      { match: 'filtrando monedas', text: 'Quitando publicaciones en moneda no soportada.' },
      { match: 'convirtiendo precios', text: 'Unificando precios en dólares.' },
      { match: 'eliminando filas con datos faltantes', text: 'Conservando solo avisos con datos completos.' },
      { match: 'filtrando outliers', text: 'Descartando valores fuera de mercado.' },
      { match: 'guardando', text: 'Guardando el archivo final para el panel.' },
      { match: 'pipeline finalizado', text: 'Pipeline listo. Datos actualizados.' },
      { match: 'etapa clean finalizada', text: 'Limpieza lista. Datos depurados.' },
      { match: 'etapa convert finalizada', text: 'Conversión lista. Precios unificados.' },
      { match: 'error', text: 'Se detectó un problema. Revisar los datos de origen.' },
    ];

    const found = mappings.find(item => lower.includes(item.match));
    return found ? found.text : raw;
  };

  useEffect(() => {
    const fetchLastExecution = () => {
      fetch(`${BACKEND_URL}/api/last-execution`)
        .then(res => res.json())
        .then(data => {
          if (data.file_exists) {
            setLastExecutionDate(data.last_execution_date);
          } else {
            setLastExecutionDate('No se ha ejecutado aún');
          }
        })
        .catch(() => {
          setLastExecutionDate('Error al contactar al servidor');
        });
    };

    const fetchSources = () => {
      fetch(`${BACKEND_URL}/api/sources`)
        .then(res => res.json())
        .then(data => setSources(data))
        .catch(() => setSources({}));
    };

    fetchLastExecution();
    fetchSources();

    const onConnect = () => {
      console.log('Conectado al servidor de Socket.IO');
    };

    const onStatusMessage = (data: { message: string; action?: ActionType }) => {
      const friendly = businessMessage(data.message);
      setStatusMessages(prevMessages => [...prevMessages, friendly]);
      setProgress(prev => Math.min(90, prev + 8));
    };

    const onPipelineFinished = (data: { last_execution_date: string | null; action?: ActionType }) => {
      if (data.last_execution_date) {
        setLastExecutionDate(data.last_execution_date);
      }
      setIsRunning(false);
      setCurrentAction(null);
      setProgress(100);
      setStatusMessages(prev => [...prev, 'Proceso finalizado. Archivo actualizado y listo para el negocio.']);
    };

    socket.on('connect', onConnect);
    socket.on('status', onStatusMessage);
    socket.on('pipeline_finished', onPipelineFinished);

    return () => {
      socket.off('connect', onConnect);
      socket.off('status', onStatusMessage);
      socket.off('pipeline_finished', onPipelineFinished);
    };
  }, []);

  useEffect(() => {
    if (statusLogRef.current) {
      statusLogRef.current.scrollTop = statusLogRef.current.scrollHeight;
    }
  }, [statusMessages]);

  const handleUploadSources = () => {
    if (!trainingFile && !dolarFile) {
      setUploadStatus('Selecciona al menos un archivo para cargar.');
      return;
    }
    const formData = new FormData();
    if (trainingFile) formData.append('training', trainingFile);
    if (dolarFile) formData.append('dolar', dolarFile);
    setUploading(true);
    setUploadStatus('Cargando archivos...');

    fetch(`${BACKEND_URL}/api/upload-source`, {
      method: 'POST',
      body: formData,
    })
      .then(res => res.json())
      .then(data => {
        if (data.error) {
          setUploadStatus(data.error);
        } else {
          setUploadStatus('Fuentes guardadas. Ejecuta la etapa que necesites.');
          setSources((prev) => ({
            ...prev,
            ...data.saved && {
              training_path: data.saved.training ?? prev.training_path,
              dolar_path: data.saved.dolar ?? prev.dolar_path,
            },
          }));
        }
      })
      .catch(() => setUploadStatus('No se pudo cargar. Reintenta.'))
      .finally(() => setUploading(false));
  };

  const handleRun = (action: ActionType) => {
    if (isRunning) return;
    setCurrentAction(action);
    setStatusMessages([`Preparando ejecución de ${ACTION_LABELS[action].toLowerCase()}.`]);
    setIsRunning(true);
    setProgress(10);
    socket.emit(ACTION_EVENT[action]);
  };

  const progressLabel = isRunning
    ? `Ejecutando ${ACTION_LABELS[currentAction || 'full']}`
    : progress === 100
      ? 'Datos listos y actualizados'
      : 'Listo para ejecutar el pipeline';

  return (
    <div className="page">
      <div className="hero">
        <div className="hero-content">
          <div className="brand">
            <img src={logo} className="logo" alt="Logo Horizonte Inmobiliaria" />
            <div>
              <p className="eyebrow">Panel de Datos</p>
              <h1>Horizonte Inmobiliaria</h1>
              <p className="subtitle">
                Control y actualización del pipeline de datos para el equipo comercial.
              </p>
              <div className="meta-card">
                <p className="card-label small">Última actualización</p>
                <p className="card-value small">{lastExecutionDate}</p>
                <p className="card-hint">Archivo limpio listo para el panel.</p>
              </div>
            </div>
          </div>
          <div className="hero-image" aria-hidden="true" />
        </div>
      </div>

      <main className="content">
        <section className="split">
          <div className="card upload">
            <p className="card-label">Fuentes de datos</p>
            <h2>Configurar archivos CSV</h2>
            <p className="card-hint">
              Sube el CSV de propiedades y el de cotización del dólar. Usaremos estos archivos en las próximas ejecuciones.
            </p>
            <div className="file-row">
              <label>CSV de propiedades (entrenamiento)</label>
              <input
                type="file"
                accept=".csv"
                onChange={e => setTrainingFile(e.target.files?.[0] || null)}
              />
              <p className="mini">
                Actual: {sources.training_path ? sources.training_path : 'por defecto en Datasets crudos'}
              </p>
            </div>
            <div className="file-row">
              <label>CSV de dólar</label>
              <input
                type="file"
                accept=".csv"
                onChange={e => setDolarFile(e.target.files?.[0] || null)}
              />
              <p className="mini">
                Actual: {sources.dolar_path ? sources.dolar_path : 'por defecto en Datasets crudos'}
              </p>
            </div>
            <button
              className="secondary-button"
              onClick={handleUploadSources}
              disabled={uploading}
            >
              {uploading ? 'Cargando...' : 'Guardar fuentes'}
            </button>
            <p className="mini status">{uploadStatus}</p>
          </div>

          <div className="card action">
            <div className="action-header">
              <div>
                <p className="card-label">Pipeline de limpieza</p>
                <h2>Ejecutar proceso</h2>
                <p className="card-hint">
                  Limpia datos, convierte precios a USD y elimina valores fuera de mercado.
                </p>
              </div>
              <div className="button-group">
                <button onClick={() => handleRun('clean')} disabled={isRunning} className="secondary-button">
                  {isRunning && currentAction === 'clean' ? 'Procesando...' : 'Solo limpieza'}
                </button>
                <button onClick={() => handleRun('convert')} disabled={isRunning} className="secondary-button">
                  {isRunning && currentAction === 'convert' ? 'Procesando...' : 'Solo conversión'}
                </button>
                <button onClick={() => handleRun('full')} disabled={isRunning} className="run-button">
                  {isRunning && currentAction === 'full' ? 'Procesando...' : 'Pipeline completo'}
                </button>
              </div>
            </div>

            <div className="progress-card">
              <div className="progress-head">
                <div>
                  <p className="card-label">Estado</p>
                  <p className="progress-title">{progressLabel}</p>
                </div>
                <span className="pill">{progress}%</span>
              </div>
              <div className="progress-track">
                <div className="progress-fill" style={{ width: `${progress}%` }} />
              </div>
              <p className="card-hint">
                {isRunning
                  ? 'Puedes seguir trabajando: te avisaremos cuando esté listo.'
                  : 'Elige qué etapa ejecutar según lo que necesites actualizar.'}
              </p>
            </div>
          </div>
        </section>

        <section className="status-section">
          <div className="section-header">
            <div>
              <p className="card-label">Actividad</p>
              <h3>Detalle del proceso</h3>
              <p className="card-hint">
                Mensajes en lenguaje de negocio para seguir el avance paso a paso.
              </p>
            </div>
          </div>

          <div className="status-log" ref={statusLogRef}>
            {statusMessages.length === 0 ? (
              <p className="placeholder-text">Aún no hay ejecuciones. Inicia alguna etapa para ver el detalle.</p>
            ) : (
              statusMessages.map((msg, index) => (
                <div key={index} className="log-message">
                  <span className="dot" />
                  <p>{msg}</p>
                </div>
              ))
            )}
          </div>
        </section>
      </main>
    </div>
  );
}

export default App;
