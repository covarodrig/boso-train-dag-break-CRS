# boso-train-dag-break-CRS

Proyecto de entrenamiento para depurar y refactorizar un DAG de Apache Airflow a partir de un ejemplo con malas prácticas.  
El objetivo es: (1) levantar Airflow en Docker, (2) identificar errores usando la UI y los logs, y (3) refactorizar el DAG aplicando buenas prácticas.

## Contenido

- `docker-compose.yml`: stack de Airflow (webserver, scheduler, worker, postgres, redis).
- `dags/`: DAGs de Airflow.
- `logs/`: logs de Airflow (montado como volumen).
- `plugins/`: plugins (montado como volumen).
- `config/`: configuración adicional del proyecto (variables de Airflow, notas, etc.).

## DAG refactorizado

El DAG implementa un pipeline ETL sencillo:

1. **extract_data**: llama a la API pública de Futurama y guarda la respuesta en JSON.
2. **validate_json**: valida estructura y contenido mínimo del JSON (lista no vacía y campos requeridos).
3. **transform_to_csv**: genera un CSV a partir del JSON usando un schema dinámico (unión de keys presentes en los registros).
4. **pipeline_summary**: registra un resumen con rutas de ficheros y métricas.

### Decisiones de diseño destacadas

- **Variables de Airflow** para configuración:
  - `futurama_api_url` (por defecto: `https://api.sampleapis.com/futurama/characters`)
  - `futurama_base_dir` (por defecto: `/tmp/data_api_bad`)
- **Idempotencia por DAG Run**: los nombres de fichero se basan en `execution_date` (no `datetime.now()`).
- **Logging estructurado** con `logging.getLogger(__name__)`.
- **Separación de responsabilidades** por tasks para mejorar observabilidad, reintentos y debugging.

## Requisitos

- Docker Desktop
- Docker Compose (v2)
- (Opcional) VS Code


