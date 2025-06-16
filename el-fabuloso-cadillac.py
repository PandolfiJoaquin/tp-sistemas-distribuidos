import random
from random import randint

import time
import docker
import re

# Define tu whitelist de nombres o IDs de contenedores
WHITELIST = [
    # "gateway",
     r"joiner-\d+$",
     r"reducer-\d+$",
    r"final-reducer-q\d+$",
    r"preprocessor-\d+$",
     r"production-filter-\d+$",
     r"year-filter-\d+$",
     r"sentiment-analyzer-\d+$",
]
REGEXS = [re.compile(exp) for exp in WHITELIST]

REGEX_CLIENT = re.compile(r"client\d+$")

def is_whitelisted(name_or_id, regex_list):
    return any(pattern.match(name_or_id) for pattern in regex_list)


def main():
    client = docker.from_env()
    print("Esperando a que aparezcan contenedores...")
    while True:
        running_containers = client.containers.list(filters={"status": "running"})
        if len(running_containers) > 0:
            break
        time.sleep(5)
    
    print("Contenedores detectados. Iniciando...")
    while any(REGEX_CLIENT.match(c.name) for c in running_containers) and len(running_containers) > 0:
        print("iteration")
        containers_to_kill = list(filter(lambda c: is_whitelisted(c.name, REGEXS), running_containers))
        for container in containers_to_kill:
            if random.random() < 0.9:
                continue

            container_name = container.name
            print(f"Matando contenedor: {container_name}")
            try:
                container.kill()
            except Exception as e:
                print(f"Error al matar {container_name}: {e}")

        time.sleep(random.randint(10, 20))
        running_containers = client.containers.list(filters={"status": "running"})
    print("No more clients. Terminating...")


if __name__ == "__main__":
    main()