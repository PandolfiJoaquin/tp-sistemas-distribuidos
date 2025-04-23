import json
import sys

YAML_FILE = "docker-compose.yaml"

# nodos que inyectan JOINER_SHARDS automáticamente
NEEDS_SHARDS = {"preprocessor", "production-filter"}

QUERY_AMNT = 5

# plantillas
BASE_NODE = """
  {svc_name}:
    build:
      dockerfile: ./server/Dockerfile
      args:
        NODE: {node}
    container_name: {svc_name}
    environment:
      - RABBITMQ_DEFAULT_USER=monke
      - RABBITMQ_DEFAULT_PASS=joaco1{extra_env}
    depends_on:
      rabbitmq:
        condition: service_healthy
        restart: true
"""

CLIENT_NODE = """
  client{idx}:
    container_name: client{idx}
    build:
      dockerfile: ./client/Dockerfile
    environment:
      - CLI_ID={idx}
    depends_on:
      - gateway
    volumes:
      - ./archive/:/home/app/archive/
"""

FINAL_REDUCER_NODE = """
  final-reducer-q{idx}:
    build:
      dockerfile: ./server/Dockerfile
      args:
        NODE: final-reducer
    container_name: final-reducer-q{idx}
    environment:
      - RABBITMQ_DEFAULT_USER=monke
      - RABBITMQ_DEFAULT_PASS=joaco1
      - QUERY_NUM={idx}
      - JOINER_SHARDS={joiners}
    depends_on:
      rabbitmq:
        condition: service_healthy
        restart: true
"""

JOINER_NODE = """
  joiner-{idx}:
    build:
      dockerfile: ./server/Dockerfile
      args:
        NODE: joiner
    container_name: joiner-{idx}
    environment:
      - RABBITMQ_DEFAULT_USER=monke
      - RABBITMQ_DEFAULT_PASS=joaco1
      - JOINER_ID={idx}
    depends_on:
      rabbitmq:
        condition: service_healthy
        restart: true
"""

RABBITMQ_SERVICE = """
  rabbitmq:
    image: rabbitmq:3-management
    container_name: rabbitmq
    ports:
      - "5672:5672"
      - "15672:15672"
    environment:
      - RABBITMQ_DEFAULT_USER=monke
      - RABBITMQ_DEFAULT_PASS=joaco1
    volumes:
      - ./rabbitmq.conf:/etc/rabbitmq/rabbitmq.conf
    healthcheck:
      test: ["CMD", "rabbitmq-diagnostics", "check_port_connectivity"]
      interval: 2s
      timeout: 5s
      retries: 3
"""

def create_compose(cfg):
    clients = cfg["clients"]
    joiners = cfg["joiners"]
    nodes    = cfg["nodes"]    # dict: { "preprocessor": n, "production-filter": m, ... }

    compose = "name: tp-dist\nservices:\n"

    # Gateway
    compose += BASE_NODE.format(
        svc_name="gateway",
        node="gateway",
        extra_env=""
    )

    # RabbitMQ
    compose += RABBITMQ_SERVICE

    # Nodos Dinamicos
    for node, count in nodes.items():
        for i in range(1, count+1):
            svc_name = f"{node}-{i}" if count > 1 else node
            extra = f"\n      - JOINER_SHARDS={joiners}" if node in NEEDS_SHARDS else ""
            compose += BASE_NODE.format(svc_name=svc_name, node=node, extra_env=extra)

    # Final Reducer
    for q in range(2, QUERY_AMNT+1):
        compose += FINAL_REDUCER_NODE.format(idx=q, joiners=joiners)

    # Joiners
    for j in range(1, joiners+1):
        compose += JOINER_NODE.format(idx=j)

    # Clients
    for c in range(1, clients+1):
        compose += CLIENT_NODE.format(idx=c)

    with open(YAML_FILE, "w") as f:
        f.write(compose)

    print(f"   • joiners ×{joiners}")
    for node, count in nodes.items():
        print(f"   • {node} ×{count}")

def main():
    if len(sys.argv) != 2:
        print("Uso: python generate-compose.py <config.json>")
        sys.exit(1)

    try:
        cfg = json.load(open(sys.argv[1]))
    except Exception as e:
        print(f"Error al leer config.json: {e}")
        sys.exit(1)

    for key in ("clients", "joiners", "nodes"):
        if key not in cfg:
            print(f"Falta la clave '{key}' en el JSON")
            sys.exit(1)

    create_compose(cfg)

if __name__ == "__main__":
    main()
