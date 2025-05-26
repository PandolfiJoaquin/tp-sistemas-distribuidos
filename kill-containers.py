import random
from random import randint

import time
import docker
import re

# Define tu whitelist de nombres o IDs de contenedores
WHITELIST = [
    #"gateway"
    "joiner-\d+$",
    "reducer-\d+$",
    "final-reducer-q\d+$",
    "preprocessor-\d+$",
    "production-filter-\d+$",
    "year-filter-\d+$",
    "sentiment-analyzer-\d+$",
]
REGEXS = [re.compile(exp) for exp in WHITELIST]



def is_whitelisted(name_or_id, regex_list):
    return any(pattern.match(name_or_id) for pattern in regex_list)


def main():
    client = docker.from_env()
    while True:
        print("iteration")
        running_containers = client.containers.list(filters={"status": "running"})
        containers_to_kill = list(filter(lambda c: is_whitelisted(c.name, REGEXS), running_containers))
        if len(containers_to_kill) <= 0:
            print("tuki :)")
            return
        for container in containers_to_kill:
            if random.random() < 0.9:
                continue

            container_name = container.name
            print(f"Matando contenedor: {container_name}")
            try:
                container.kill()
            except Exception as e:
                print(f"Error al matar {container_name}: {e}")
        time.sleep(random.randint(1, 5))


if __name__ == "__main__":
    main()