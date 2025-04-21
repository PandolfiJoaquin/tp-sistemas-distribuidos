import sys

QUERY_AMNT = 5

YAML_FILE = "docker-compose.yaml"

SKELETON = """
name: tp-dist
services:
  gateway:
    build:
      dockerfile: ./server/Dockerfile
      args:
        NODE: gateway
    container_name: gateway
    environment:
      - RABBITMQ_DEFAULT_USER=monke
      - RABBITMQ_DEFAULT_PASS=joaco1
    depends_on:
      rabbitmq:
        condition: service_healthy
        restart: true
  preprocessor:
    build:
      dockerfile: ./server/Dockerfile
      args:
        NODE: preprocessor
    container_name: preprocessor
    environment:
      - RABBITMQ_DEFAULT_USER=monke
      - RABBITMQ_DEFAULT_PASS=joaco1
      - JOINER_SHARDS={joiners_amnt}
    depends_on:
      rabbitmq:
        condition: service_healthy
        restart: true
  production-filter:
    build:
      dockerfile: ./server/Dockerfile
      args:
          NODE: production-filter
    container_name: production-filter
    environment:
      - RABBITMQ_DEFAULT_USER=monke
      - RABBITMQ_DEFAULT_PASS=joaco1
      - JOINER_SHARDS={joiners_amnt}
    depends_on:
      rabbitmq:
        condition: service_healthy
        restart: true
  year-filter:
    build:
      dockerfile: ./server/Dockerfile
      args:
          NODE: year-filter
    container_name: year-filter
    environment:
      - RABBITMQ_DEFAULT_USER=monke
      - RABBITMQ_DEFAULT_PASS=joaco1
    depends_on:
      rabbitmq:
        condition: service_healthy
        restart: true
  sentiment-analyzer:
    build:
      dockerfile: ./server/Dockerfile
      args:
          NODE: sentiment-analyzer
    container_name: sentiment-analyzer
    environment:
      - RABBITMQ_DEFAULT_USER=monke
      - RABBITMQ_DEFAULT_PASS=joaco1
    depends_on:
      rabbitmq:
        condition: service_healthy
        restart: true
  reducer:
    build:
      dockerfile: ./server/Dockerfile
      args:
          NODE: reducer
    container_name: reducer
    environment:
      - RABBITMQ_DEFAULT_USER=monke
      - RABBITMQ_DEFAULT_PASS=joaco1
    depends_on:
      rabbitmq:
        condition: service_healthy
        restart: true
  rabbitmq:
    image: rabbitmq:3-management
    container_name: rabbitmq
    ports:
      - "5672:5672"   # AMQP protocol port
      - "15672:15672" # Management UI port
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

def create_compose(client_amnt, joiners_amnt):
  compose_str = SKELETON.format(joiners_amnt=joiners_amnt)

  for i in range(2, QUERY_AMNT + 1):
    compose_str += f"""
  final-reducer-q{i}:
    build:
      dockerfile: ./server/Dockerfile
      args:
        NODE: final-reducer
    container_name: final-reducer-q{i}
    environment:
      - RABBITMQ_DEFAULT_USER=monke
      - RABBITMQ_DEFAULT_PASS=joaco1
      - QUERY_NUM={i}
      - JOINER_SHARDS={joiners_amnt}
    depends_on:
        rabbitmq:
          condition: service_healthy
          restart: true
    """
  
  for i in range(1, joiners_amnt + 1):
    compose_str += f"""
  joiner-{i}:
    build:
      dockerfile: ./server/Dockerfile
      args:
          NODE: joiner
    container_name: joiner-{i}
    environment:
      - RABBITMQ_DEFAULT_USER=monke
      - RABBITMQ_DEFAULT_PASS=joaco1
      - JOINER_ID={i}
    depends_on:
      rabbitmq:
        condition: service_healthy
        restart: true
    """


  for i in range(1, client_amnt + 1):
    compose_str += f"""
  client{i}:
    container_name: client{i}
    build:
      dockerfile: ./client/Dockerfile
    environment:
      - CLI_ID={i}
    depends_on:
      - gateway
    volumes:
        - ./archive/:/home/app/archive/
"""
  
  with open(YAML_FILE, "w") as file:
      file.write(compose_str)

  return

def main():
    if len(sys.argv) != 3:
        print("Invalid number of arguments: <client_amount> <joiners_amount>")
        sys.exit(1)

    try:
        client_amnt = int(sys.argv[1])
        if client_amnt < 0:
            raise ValueError
        joiners_amnt = int(sys.argv[2])
        if joiners_amnt < 0:
            raise ValueError
    except ValueError:
        print("Error: La cantidad de clientes debe ser un número mayor a 0.")
        sys.exit(1)

    create_compose(client_amnt, joiners_amnt)

if __name__ == "__main__":
    main()