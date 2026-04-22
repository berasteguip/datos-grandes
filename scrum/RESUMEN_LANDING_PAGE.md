# TradeData

## Resumen ejecutivo

TradeData es un proyecto de datos orientado al analisis de mercados cripto, construido sobre un caso real de ETH/USDT. El trabajo recogido en este repositorio muestra la evolucion de una plataforma capaz de cubrir tanto analitica historica como procesamiento near real time, con una arquitectura apoyada en AWS, Kafka y Spark.

Desde una perspectiva de consultoria, el proyecto demuestra la capacidad de disenar e implementar una solucion end to end para datos financieros: captacion de fuentes, almacenamiento por capas, gobierno del dato, transformaciones analiticas, calculo de indicadores y explotacion operativa de streaming.

## Que se ha desarrollado

### 1. Captacion y preparacion del dato historico

- Extraccion de historicos de ETH desde TradingView mediante una libreria propia y notebooks de apoyo.
- Consolidacion de ficheros CSV anuales en el repositorio para el periodo 2022-2026.
- Preparacion de una base de trabajo reproducible para analisis y posteriores cargas a cloud.

El dataset historico disponible en `data/` cubre cinco anos naturales y alrededor de 1.460 registros diarios, lo que permite construir una base consistente para analitica financiera y pruebas de pipeline.

### 2. Data lake en AWS con enfoque por capas

- Carga automatizada de datos a Amazon S3 en capa bronze, particionando por ano.
- Definicion de una estructura orientada a escalabilidad y gobierno del dato.
- Preparacion del lago de datos para procesos ETL y consumo analitico posterior.

Esta capa queda reflejada en los scripts de carga y organizacion sobre S3, sentando las bases de una arquitectura moderna de datos.

### 3. Gobierno del dato y catalogacion

- Automatizacion de AWS Glue Crawler.
- Creacion de base de datos y tablas catalogadas en AWS Glue Data Catalog.
- Preparacion del entorno para consulta, descubrimiento y trazabilidad del dato almacenado.

La documentacion de `scrum/s2` y los scripts de `src/aws_glue/crawler/` muestran una fase centrada en ordenar el dato y convertir almacenamiento en capacidad real de consulta y explotacion.

### 4. Pipeline ETL batch bronze -> silver -> gold

- Transformacion de datos crudos a formatos mas eficientes en parquet.
- Limpieza de particiones tecnicas generadas por catalogacion.
- Generacion de una capa gold con indicadores analiticos sobre ETH.

Entre los indicadores implementados destacan:

- SMA 200
- EMA 50
- RSI 14
- MACD

Esta parte del proyecto materializa una logica analitica util para equipos de negocio, analistas cuantitativos o casos de reporting avanzado.

### 5. Streaming de mercado en tiempo real

- Conexion con Binance para recibir velas de mercado en tiempo real.
- Publicacion de eventos en Kafka con autenticacion SASL/PLAIN.
- Scripts de productor, consumidor y utilidades operativas para probar el flujo end to end.

La carpeta `src/streaming/` refleja una evolucion clara hacia casos de uso mas operativos, donde el dato deja de ser solo historico y pasa a alimentar procesos continuos.

### 6. Analitica streaming con Spark y AWS Glue Streaming

- Lectura de eventos desde Kafka con Spark Structured Streaming.
- Calculo de VWAP en ventanas moviles de 5 minutos con refresco por minuto.
- Publicacion del indicador calculado en un topic de salida dedicado.
- Adaptacion del procesamiento para su despliegue como Glue Streaming Job en AWS.

Este bloque es especialmente relevante desde un punto de vista de consultoria porque conecta arquitectura cloud, procesamiento distribuido y analitica financiera en tiempo casi real.

### 7. Persistencia operativa en Amazon Timestream

- Consumo coordinado de cotizacion y VWAP desde Kafka.
- Escritura de medidas en Amazon Timestream para consultas temporales de baja latencia.
- Preparacion de una base preparada para cuadros operativos, monitorizacion o futuras APIs analiticas.

Con ello, el proyecto no se queda en el calculo del indicador, sino que avanza hacia su disponibilidad para consumo operativo.

## Evolucion por fases

### Sprint 1. Definicion del caso de uso y base del proyecto

La carpeta `scrum/s1` recoge el arranque del proyecto: alineamiento del contexto, definicion de historias de usuario y preparacion del marco de trabajo. Esta fase fija el problema de negocio y el alcance tecnico inicial.

### Sprint 2. Gobierno del dato y catalogacion en AWS

Se consolida el uso de Glue Data Catalog y Glue Crawler para convertir el almacenamiento en S3 en un activo gobernado, localizable y consultable. Es la fase en la que el proyecto deja de ser una coleccion de ficheros y pasa a tener una base de datos de metadatos utilizable.

### Sprint 3. Industrializacion batch del dato historico

Se formaliza el pipeline bronze -> silver -> gold y se incorporan transformaciones analiticas sobre el historico de ETH. Es el primer gran salto desde la ingesta a la generacion de valor analitico.

### Sprint 4. Apertura del frente streaming

La documentacion de `scrum/s4` y la evolucion del repositorio apuntan a una nueva fase centrada en streaming: conexion con fuentes en tiempo real, primeras pruebas con Kafka y preparacion de procesos continuos.

### Sprint 5. VWAP en tiempo real y persistencia temporal

La documentacion de `scrum/s5` y los commits recientes muestran una etapa ya orientada a producto tecnico: calculo de VWAP, ajuste de ventanas, despliegue en Glue Streaming y escritura en Timestream.

### Fase final del repositorio

La carpeta `scrum/s6` no contiene documentacion funcional en este estado, pero el codigo mas reciente evidencia trabajo de ajuste, validacion y despliegue sobre la parte streaming.

## Arquitectura resultante

El repositorio refleja una arquitectura de datos con dos velocidades:

- Flujo batch para historico: TradingView -> CSV -> Amazon S3 bronze -> Glue Catalog/Crawler -> ETL bronze/silver/gold -> capa analitica.
- Flujo streaming para tiempo real: Binance -> Kafka -> Spark/Glue Streaming -> VWAP -> Kafka de salida -> Amazon Timestream.

Esta combinacion permite cubrir tanto analisis retrospectivo como casos operativos de reaccion sobre mercado.

## Stack y servicios utilizados

- Python
- Pandas
- TradingView data extraction
- Binance WebSocket
- Apache Kafka
- Apache Spark Structured Streaming
- Amazon S3
- AWS Glue
- AWS Glue Data Catalog
- AWS Glue Crawler
- Amazon Timestream
- Athena como capa natural de consulta sobre datos catalogados

## Entregables visibles en el repositorio

- Libreria y notebooks para descarga y exploracion de datos.
- Scripts de carga a S3.
- Automatizacion de crawler y catalogacion.
- Jobs ETL para capas silver y gold.
- Scripts de productor y consumidor Kafka.
- Jobs Spark para calculo de VWAP.
- Job de despliegue para AWS Glue Streaming.
- Script de escritura en Timestream.
- Documentacion Scrum por sprint y entregables parciales.

## Valor aportado al cliente

### De dato aislado a plataforma

El proyecto transforma datos de mercado en una plataforma organizada, explotable y preparada para crecer. No solo almacena informacion: la estructura, la gobierna y la convierte en indicadores utiles.

### De historico a tiempo real

La solucion cubre tanto la explotacion de historicos como la capacidad de reaccion sobre streaming, un punto diferencial para contextos de trading, monitorizacion o analitica avanzada.

### De prueba tecnica a base para producto

El trabajo realizado deja una base reutilizable para evolucionar hacia dashboards, alertas, reporting financiero, motores de decision o APIs de datos.

## Mensaje comercial para landing

Desarrollamos una plataforma de datos para mercados financieros capaz de integrar historico y tiempo real en una misma arquitectura. Sobre un caso real de analitica cripto, construimos un data lake en AWS, pipelines ETL por capas, gobierno del dato con Glue, procesamiento streaming con Kafka y Spark, calculo de indicadores avanzados y persistencia temporal lista para consumo operativo.

El resultado es una solucion escalable, modular y orientada a negocio: preparada para convertir flujos de mercado en informacion accionable.

## Claims reutilizables

- Arquitectura de datos end to end sobre AWS para analitica financiera.
- Integracion de procesamiento batch y streaming en una misma solucion.
- Gobierno del dato, catalogacion y explotacion analitica desde origen hasta consumo.
- Calculo de KPIs de mercado en tiempo real con tecnologias distribuidas.
- Base tecnica preparada para cuadros de mando, alertas y nuevos productos de datos.

## Nota de alcance

Este resumen se ha elaborado tomando como base la carpeta `scrum`, la estructura del repositorio y los entregables tecnicos presentes en `src/`. En las fases finales, el nivel de detalle documental por sprint es desigual, por lo que la narrativa incorpora tanto la evidencia documental como el trabajo efectivamente implementado en el codigo.
