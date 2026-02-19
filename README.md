# Datos Grandes - Analisis de Criptomonedas (ETH)

Proyecto de Big Data para ingestar, almacenar y transformar datos historicos de ETH, con soporte de descarga via TradingView y pipeline en AWS (S3 + Glue).

**Estructura**
- `data/` CSV historicos de ETH por anio.
- `src/trading_view_data/` libreria y notebooks para descarga y exploracion.
- `src/s3/` scripts de carga a S3 (capa bronze).
- `src/aws_glue/` crawler y jobs ETL (bronze -> silver -> gold).
- `scrum/` documentacion del proyecto y sprints.

**Flujo**
1. Descargar datos con `src/trading_view_data/` y guardarlos en `data/`.
2. Subir CSVs a S3 con `src/s3/s3_setup.py` (bronze por anio).
3. Crear y ejecutar el crawler con `src/aws_glue/crawler/crawler_setup.py`.
4. Ejecutar `src/aws_glue/etl_jobs/bronze2silver.py` para generar Parquet en silver.
5. Ejecutar `src/aws_glue/etl_jobs/silver2gold.py` para calcular indicadores y escribir en gold.

**Requisitos**
- Python con dependencias en `requirements.txt`.
- Credenciales AWS configuradas localmente.
- Region AWS usada en scripts: `eu-south-2`.

**Uso rapido**
```powershell
pip install -r requirements.txt
python src/s3/s3_setup.py
python src/aws_glue/crawler/crawler_setup.py
```

**Notas**
- El bucket definido en `src/s3/s3_setup.py` es `datos-grandes-eth-project`.
- Los notebooks de ejemplo estan en `src/trading_view_data/`.
