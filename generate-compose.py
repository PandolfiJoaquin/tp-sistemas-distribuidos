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
      - MOVIES_FILE={movies_file}
      - REVIEWS_FILE={reviews_file}
      - CREDITS_FILE={credits_file}
    depends_on:
      - gateway
    volumes:
      - ./archive/:/home/app/archive/
      - ./client-results/:/home/app/results/
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

def get_node_env(node_type, node_id=None, joiners=None):
    """Generate environment variables for a node based on its type"""
    env_vars = []
    
    if node_type == "final-reducer":
        env_vars.extend([
            f"\n      - QUERY_NUM={node_id}",
            f"\n      - JOINER_SHARDS={joiners}"
        ])
    elif node_type == "joiner":
        env_vars.append(f"\n      - JOINER_ID={node_id}")
    elif node_type in NEEDS_SHARDS:
        env_vars.append(f"\n      - JOINER_SHARDS={joiners}")
    
    return "".join(env_vars)

def create_compose(cfg):
    clients = cfg["clients"]
    joiners = cfg["joiners"]
    nodes    = cfg["nodes"]    # dict: { "preprocessor": n, "production-filter": m, ... }
    files    = cfg["files"]    # dict: { "movies": [paths], "reviews": [paths], ... }

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
            extra_env = get_node_env(node, joiners=joiners)
            compose += BASE_NODE.format(
                svc_name=svc_name,
                node=node,
                extra_env=extra_env
            )

    # Final Reducer
    for q in range(2, QUERY_AMNT+1):
        svc_name = f"final-reducer-q{q}"
        extra_env = get_node_env("final-reducer", node_id=q, joiners=joiners)
        compose += BASE_NODE.format(
            svc_name=svc_name,
            node="final-reducer",
            extra_env=extra_env
        )

    # Joiners
    for j in range(1, joiners+1):
        svc_name = f"joiner-{j}"
        extra_env = get_node_env("joiner", node_id=j)
        compose += BASE_NODE.format(
            svc_name=svc_name,
            node="joiner",
            extra_env=extra_env
        )

    # Clients
    print(f"   • clients ×{clients}")
    for c in range(1, clients+1):
        # Cycle through the file arrays using modulo
        movies_file = files["movies"][(c-1) % len(files["movies"])]
        reviews_file = files["reviews"][(c-1) % len(files["reviews"])]
        credits_file = files["credits"][(c-1) % len(files["credits"])]
        
        compose += CLIENT_NODE.format(
            idx=c,
            movies_file=movies_file,
            reviews_file=reviews_file,
            credits_file=credits_file
        )

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

    for key in ("clients", "joiners", "nodes", "files"):
        if key not in cfg:
            print(f"Falta la clave '{key}' en el JSON")
            sys.exit(1)
        
    for file_type, file_list in cfg["files"].items():
        if not isinstance(file_list, list):
            print(f"Error: '{file_type}' debe ser una lista de archivos")
            sys.exit(1)
        if len(file_list) == 0:
            print(f"Error: '{file_type}' no puede estar vacío")
            sys.exit(1)

    create_compose(cfg)

if __name__ == "__main__":
    main()
