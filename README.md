# Movie-IMDb-Analyzer - Trabajo Practico Sistemas Distribuidos
Realizado para el 1C2025, para la materia sistemas distribuidos de la facultad de ingenieria de la universidad de Buenos Aires. 

Este proyecto implementa un sistema distribuido para procesamiento de datos de películas con capacidades de tolerancia a fallos. El sistema incluye herramientas para generar configuraciones dinámicas, ejecutar pruebas de resiliencia y validar resultados automáticamente.

### Integrantes
| Nombre             | Padrón | Email               |
|--------------------|--------|---------------------|
| Joaquin Pandolfi   | 108215 | jpandolfi@fi.uba.ar |
| Theo Lijs          | 109472 | tlijs@fi.uba.ar     |
| Francisco Juarez   | 107748 | fjuarez@fi.uba.ar   |


## Tabla de Contenidos
- [Documento de arquitectura](#Documentacion-de-Arquitectura)
- [Configuración del Sistema](#configuración-del-sistema)
- [Generación de Docker Compose](#generación-de-docker-compose)
- [Ejecución Normal](#ejecución-normal)
- [Validación de Resultados](#validación-de-resultados)
- [Pruebas de Resiliencia](#pruebas-de-resiliencia)
- [Comandos Disponibles](#comandos-disponibles)

## Documentacion de Arquitectura
Ver el PDF sobre la documentacion del proyecto [link al pdf en el repositorio]()

## Configuración del Sistema

### Archivo `config-script.json`

El sistema utiliza un archivo de configuración JSON que define la topología y parámetros del sistema distribuido que luego es utilizado por otros scripts:

```json
{
  "logLevel": "DEBUG",
  "clients": 5,
  "joiners": 5,
  "healer": 3,
  "nodes": {
    "preprocessor": 3,
    "production-filter": 2,
    "year-filter": 2,
    "sentiment-analyzer": 2,
    "reducer": 4
  },
  "files": {
    "movies": [
      "archive/movies_metadata.csv"
    ],
    "reviews": [
      "archive/ratings_small.csv",
      "archive/ratings.csv"
    ],
    "credits": [
      "archive/credits.csv"
    ]
  }
}
```

**Parámetros de configuración:**
- `logLevel`: Nivel de logging (`DEBUG` o `INFO`)
- `clients`: Número de clientes que envian consultas al sistema. 
- `joiners`: Número de nodos joiner para combinar resultados, es importante especificarlos debido al sharding que hay en el sistema 
- `healer`: Número de nodos healer para monitoreo y recuperación
- `nodes`: Cantidad de cada tipo de nodo de procesamiento, los nodos son los que aparecen en el ejemplo. 
- `files`: Archivos de datos que utilizará cada cliente (se asignan cíclicamente). Es decir, el cliente 1 tendra el archivo de `ratings_small.csv`, el cliente 2 tendra el archivo de `ratings.csv`, y el 3 tendra `rating`.

### Archivos de datos soportados

Los archivos de datos que estan integrados en el sistema con los varios scripts son:
- `movies_metadata.csv`: Metadatos de películas ([link de kaggle](https://www.kaggle.com/datasets/rounakbanik/the-movies-dataset?select=movies_metadata.csv))
- `ratings_small.csv`: Conjunto reducido de ratings de películas ([link de kaggle](https://www.kaggle.com/datasets/rounakbanik/the-movies-dataset?select=ratings_small.csv))
- `ratings.csv`: Conjunto completo de ratings de películas ([link de kaggle](https://www.kaggle.com/datasets/rounakbanik/the-movies-dataset?select=ratings.csv))
- `credits.csv`: Información de créditos de películas ([link de kaggle](https://www.kaggle.com/datasets/rounakbanik/the-movies-dataset?select=credits.csv))

> Se pueden utilizar otros archivos pero estoss tiene que cumplir con el formato esperado, es decir, que tengan las columnas necesarias para las queries que se ejecutan.
> De lo contrario el cliente tirara error de que los headers de los CSV no son los esperados.

## Generación de Docker Compose

### Script `generate-compose.sh`

Este script genera automáticamente el archivo `docker-compose.yaml` basado en la configuración:

```bash
./generate-compose.sh config-script.json
```

**Funcionalidades:**
- **Generación dinámica**: Crea servicios según la cantidad especificada en el config
- **Asignación de archivos**: Distribuye los archivos de datos entre los clientes cíclicamente
- **Variables de entorno**: Configura automáticamente las variables necesarias para cada servicio
- **Dependencias**: Establece las dependencias correctas entre servicios

**Servicios generados:**
- **Gateway**: Punto de entrada del sistema
- **RabbitMQ**: Sistema de mensajería
- **Nodos de procesamiento**: Según especificación en `nodes`
- **Final reducers**: Uno por cada query (2-5)
- **Joiners**: Para combinar resultados parciales
- **Healers**: Para monitoreo y recuperación automática
- **Clientes**: Que procesan los archivos de datos

##  Ejecución Normal

### Comando `make run`

Ejecuta el sistema completo automáticamente:

```bash
make run
```

**Lo que hace internamente:**
1. Verifica que existe `config-script.json`
2. Ejecuta `generate-compose.sh` para crear el docker-compose
3. Limpia contenedores y volúmenes anteriores (`docker compose down -v`)
4. Elimina directorios de datos anteriores
5. Construye las imágenes Docker
6. Levanta todos los servicios (`docker compose up -d`)

> **Nota**: Ya que se utilizan volumenes para el manejo de datos y tolerancia a fallos, es necesario tener permisos de administrador ya que se hace un `sudo` para borrar el directorio de datos.

## Validación de Resultados

### Script `compare_outputs.sh`

Valida automáticamente los resultados generados por los clientes:

```bash
./compare-outputs.sh config-script.json
```

**Funcionalidades de validación:**
- **Archivos faltantes**: Detecta si algún cliente no generó resultados
- **Archivos vacíos**: Identifica archivos de resultado sin contenido
- **Duplicados**: Detecta y reporta resultados duplicados en las queries
- **Comparación de contenido**: Verifica que los resultados coincidan con los esperados
- **Resultados faltantes**: Identifica queries que no se ejecutaron completamente

**Tipos de archivos soportados:**
- Resultados para `movies_metadata.csv` + `credits.csv`
- Diferentes conjuntos de reviews (`ratings_small.csv` vs `ratings.csv`)
- Resultados de archivos no conocidos son ignorados

**Salida del script:**
```bash
client-results/queries-results-1.txt: ✅
client-results/queries-results-2.txt: ❌
   Query 1: Found duplicate results!
   Duplicate: La Cienaga | Genres: [Comedy, Drama]
   Query 1: Results don't match: ❌
   Missing results:
  - Roma | Genres: [Drama, Foreign]
```

## Pruebas de Resiliencia

### Script `kill-containers.sh`

Implementa diferentes estrategias para probar la tolerancia a fallos del sistema:

```bash
./kill-containers.sh [modo]
```

**Modos disponibles:**

#### Modo 0 - Kill Random (por defecto)
```bash
./kill-containers.sh 0
# o simplemente:
./kill-containers.sh
```
- Mata contenedores aleatoriamente con 10% de probabilidad cada iteración
- Preserva siempre al menos un healer activo
- Ejecuta hasta que no queden clientes

#### Modo 1 - Atomic Bomb
```bash
./kill-containers.sh 1
```
- Espera 30 segundos después del inicio
- Mata **todos** los contenedores de la whitelist simultáneamente
- Preserva un healer para la recuperación
- Simula una falla masiva del sistema

#### Modo 2 - Determinístico
```bash
./kill-containers.sh 2
```
- Mata cada contenedor de la whitelist **mínimo una vez**
- Selección aleatoria entre contenedores no matados aún
- Resetea el ciclo cuando todos fueron matados al menos una vez
- Garantiza que todos los componentes sean probados

**Contenedores en la whitelist:**
- `joiner-*`
- `reducer-*` 
- `final-reducer-q*`
- `preprocessor-*`
- `production-filter-*`
- `year-filter-*`
- `sentiment-analyzer-*`
- `healer-*` (siempre preserva uno)

**Contenedores excluidos (críticos):**
- `gateway`
- `rabbitmq`
- `client*`

## Mas funcionalidades en el Makefile

### Comandos básicos:
```bash
make run                   # Ejecución normal completa
make run-kill              # Ejecución + kill random
make run-atomic-bomb       # Ejecución + atomic bomb
make run-deterministic     # Ejecución + kill determinístico
make down                  # Detener todo
```

### Comandos de prueba continua:
```bash
make run-kill-inf          # Bucle infinito de run-kill + validación
```

**¿Qué hace `run-kill-inf`?**
1. Ejecuta `make run-kill`
2. Valida resultados con `compare-outputs.sh`
3. Si falla la validación, termina con error
4. Si encuentra ❌ en la salida, termina con error  
5. Si todo está bien, espera 5 segundos y repite

### Comandos de desarrollo:
```bash
make run-local             
```

Permite correr un nodo localmente en vez de en Docker, esta por default con el final reducer 3. 
Deja que el final reducer se conecte a RabbitMQ para que pueda recibir mensajes de prueba en sus colas correspondientes. 

##  Flujo de Trabajo Típico

1. **Configurar el sistema:**
   ```bash
   # Editar config-script.json según necesidades
   vim config-script.json
   ```

2. **Prueba básica:**
   ```bash
   make run
   ./compare-outputs.sh config-script.json
   ```

3. **Prueba de resiliencia:**
   ```bash
   make run-kill                                # Una ejecución con fallas
   ./compare-outputs.sh config-script.json      # Validación de resultados
   make run-atomic-bomb                         # Prueba de falla masiva
    ./compare-outputs.sh config-script.json     # Validación de resultados
   make run-deterministic                       # Prueba exhaustiva
    ./compare-outputs.sh config-script.json     # Validación de resultados
   ```

4. **Prueba de estrés continua:**
   ```bash
   make run-kill-inf          # Hasta que falle o Ctrl+C
   ```

> Notar que para en las "pruebas" de resiliencia no se valida el resultado automaticamente. Por eso es importante validar los resultados manualmente luego de cada prueba. 