# Sistema de Detección de Transacciones Fraudulentas

## 1. Descripción del proyecto

El proyecto implementa una solución de ingeniería de datos para la generación, procesamiento, detección y almacenamiento de transacciones financieras sospechosas.

La solución combina procesamiento batch y procesamiento de eventos en tiempo real mediante Apache Kafka. Los datos son procesados con Python, almacenados en Amazon S3 y posteriormente integrados en un Data Warehouse desarrollado en PostgreSQL.

El sistema aplica reglas de negocio para identificar posibles operaciones fraudulentas y genera alertas que son procesadas de manera independiente mediante un consumidor Kafka.

## 2. Objetivos

- Generar transacciones financieras simuladas para alimentar el sistema.
- Implementar un proceso de limpieza y transformación de datos.
- Identificar transacciones sospechosas mediante reglas de detección.
- Almacenar los datos originales y procesados en Amazon S3.
- Implementar un flujo de mensajería mediante Apache Kafka.
- Construir un Data Warehouse en PostgreSQL.
- Registrar las alertas de fraude y generar métricas agregadas por minuto.
- Evitar registros duplicados durante la carga y el procesamiento de eventos.

## 3. Arquitectura general

La solución está compuesta por dos flujos principales:

**Procesamiento batch y Data Warehouse**

El proceso genera lotes de 100 transacciones cada 60 segundos. Los datos se almacenan inicialmente en archivos CSV, se cargan en Amazon S3 y pasan por procesos de limpieza, transformación, enriquecimiento y detección de transacciones sospechosas.

Los resultados se almacenan en archivos procesados y sospechosos. Posteriormente, `warehouse_loader.py` carga los datos procesados al modelo dimensional de PostgreSQL.

**Procesamiento de eventos con Kafka**

Las transacciones sospechosas se publican en el tópico `suspicious-transactions`. Un consumidor independiente recibe los eventos, genera alertas y registra la información en PostgreSQL. También actualiza las estadísticas de fraude agrupadas por minuto.

## 4. Diseño de arquitectura

<img width="1774" height="887" alt="image" src="https://github.com/user-attachments/assets/4cb96408-64a8-4480-a41e-1b17742afa14" />


**Descripción de los componentes:**

| Componente | Responsabilidad |
|---|---|
| Python | Generación y procesamiento de transacciones. |
| Pandas y NumPy | Limpieza, transformación y manipulación de datos. |
| Amazon S3 | Almacenamiento de archivos originales y procesados. |
| Apache Kafka | Distribución de eventos de transacciones sospechosas. |
| PostgreSQL | Almacenamiento del Data Warehouse, alertas y métricas. |
| SQLAlchemy y psycopg2 | Conexión e integración con PostgreSQL. |

## 5. Diseño de la base de datos

El proyecto utiliza PostgreSQL como repositorio central de información.

Se implementó un modelo dimensional bajo el esquema `fraud_dw`, orientado al almacenamiento y análisis de las transacciones financieras.

El modelo contempla dimensiones para usuarios, comercios, métodos de pago y tiempo, además de una tabla de hechos para almacenar las transacciones.

También incorpora tablas de staging para preparar los datos antes de su integración al modelo dimensional.

### Diagrama del modelo de datos

<img width="760" height="560" alt="diagrama" src="https://github.com/user-attachments/assets/970c029b-da4d-4dba-a240-52e3ca458f28" />

### Tablas principales

| Tabla | Descripción |
|---|---|
| `dim_users` | Información de los usuarios. |
| `dim_merchants` | Información de los comercios. |
| `dim_payment_methods` | Catálogo de métodos de pago. |
| `dim_time` | Dimensión temporal. |
| `fact_transactions` | Registro central de transacciones financieras. |
| `fraud_alerts` | Registro de alertas de fraude detectadas. |
| `fraud_metrics_minute` | Métricas agregadas de fraude por minuto. |
| `stg_users` | Preparación de datos de usuarios. |
| `stg_merchants` | Preparación de datos de comercios. |
| `stg_payment_methods` | Preparación de métodos de pago. |
| `stg_time` | Preparación de la dimensión temporal. |
| `stg_transactions` | Preparación de transacciones antes de su carga al modelo dimensional. |

## 6. Procesamiento y detección de fraude

El procesamiento de transacciones se realiza mediante `main.py`.

El proceso genera 100 transacciones por lote y ejecuta las siguientes operaciones:

1. Generación de transacciones simuladas.
2. Almacenamiento de archivos CSV originales.
3. Carga de archivos originales a Amazon S3.
4. Limpieza y validación de registros.
5. Enriquecimiento mediante información maestra de usuarios y comercios.
6. Evaluación de reglas de detección de fraude.
7. Separación de transacciones normales y sospechosas.
8. Almacenamiento de resultados procesados en Amazon S3.
9. Publicación de eventos sospechosos en Apache Kafka.

Entre las reglas implementadas se consideran:

- Múltiples intentos de transacción.
- Operaciones rechazadas.
- Tiempos de procesamiento inusuales.
- Verificación 3DS no completada.
- Estado de cuenta del usuario.
- Estado de verificación KYC.
- Nivel de riesgo del usuario.
- Estado y nivel de riesgo del comercio.
- Métodos de pago bloqueados.
- Indicadores relacionados con el riesgo de pago.

## 7. Carga del Data Warehouse

El proceso de integración al Data Warehouse se implementa mediante `warehouse_loader.py`.

Este componente lee los archivos CSV ubicados en el directorio `processed/` y realiza las siguientes operaciones:

1. Lectura de archivos procesados.
2. Normalización y validación de campos.
3. Conversión de tipos de datos y fechas.
4. Eliminación de registros inválidos.
5. Eliminación de transacciones duplicadas.
6. Carga de dimensiones mediante tablas de staging.
7. Actualización de dimensiones de usuarios y comercios.
8. Inserción de métodos de pago y dimensión temporal.
9. Integración de transacciones en `fact_transactions`.

La carga utiliza operaciones SQL con `ON CONFLICT` para controlar duplicados y actualizar determinados registros dimensionales.

## 8. Procesamiento de eventos y alertas

El procesamiento de eventos se implementa mediante `kafka_consumer.py`.

El consumidor escucha continuamente el tópico `suspicious-transactions` y procesa los mensajes recibidos.

Cuando identifica un evento marcado como fraudulento:

1. Deserializa el mensaje JSON.
2. Genera una alerta en consola.
3. Registra la transacción en `fraud_dw.fraud_alerts`.
4. Calcula el monto sospechoso.
5. Actualiza las métricas agrupadas por minuto en `fraud_dw.fraud_metrics_minute`.
6. Confirma el mensaje en Kafka después de completar el procesamiento.

El sistema utiliza confirmación manual de mensajes y restricciones de unicidad en PostgreSQL para controlar la duplicidad de alertas.

## 9. Estructura del proyecto

```text
fraud-detection/
│
├── main.py
│   └── Generación, procesamiento y detección de fraude.
│
├── warehouse_loader.py
│   └── Integración de datos al Data Warehouse.
│
├── kafka_consumer.py
│   └── Consumo de eventos y registro de alertas.
│
├── transactions/
│   └── Archivos CSV de transacciones originales.
│
├── processed/
│   └── Transacciones procesadas.
│
├── suspicious/
│   └── Transacciones sospechosas.
│
├── data/
│   └── Datos maestros y archivos de referencia.
│
└── README.md
    └── Documentación general del proyecto.
```

## 10. Tecnologías utilizadas

| Tecnología | Aplicación |
|---|---|
| Python | Lenguaje principal del proyecto. |
| Pandas | Manipulación y transformación de datos. |
| NumPy | Operaciones y transformaciones numéricas. |
| Apache Kafka | Procesamiento y distribución de eventos. |
| PostgreSQL | Base de datos y Data Warehouse. |
| Amazon S3 | Almacenamiento de datos en la nube. |
| SQLAlchemy | Integración de Python con PostgreSQL. |
| Psycopg2 | Conexión y ejecución de consultas SQL. |
| Boto3 | Integración con Amazon S3. |
| Docker | Ejecución de servicios de infraestructura, incluido Kafka. |

## 11. Ejecución del proyecto

Los componentes se ejecutan de manera independiente.

**1. Generación y procesamiento de transacciones**

```bash
python main.py
```

**2. Carga del Data Warehouse**

```bash
python warehouse_loader.py
```

**3. Inicio del consumidor Kafka**

```bash
python kafka_consumer.py
```

Para el correcto funcionamiento, PostgreSQL y el broker de Kafka deben estar disponibles. Asimismo, deben existir previamente las tablas del esquema `fraud_dw` y el tópico correspondiente de Kafka.

## 12. Resultados del proyecto

La solución permite implementar un flujo integral de ingeniería de datos que combina procesamiento batch y procesamiento de eventos.

Entre los resultados obtenidos se encuentran:

- Generación continua de transacciones financieras simuladas.
- Almacenamiento de datos originales y procesados en Amazon S3.
- Identificación de transacciones sospechosas mediante reglas de negocio.
- Distribución de eventos mediante Apache Kafka.
- Integración de información en un Data Warehouse dimensional.
- Registro de alertas de fraude en PostgreSQL.
- Cálculo de métricas de fraude por minuto.
- Control de duplicidad durante la integración de datos y el procesamiento de eventos.

---

**Estado de implementación:** solución desarrollada con Python, Apache Kafka, Amazon S3 y PostgreSQL, integrando procesos batch, almacenamiento analítico y procesamiento de alertas mediante eventos.
