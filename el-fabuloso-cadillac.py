import random
from random import randint
import argparse
import time
import docker
import re

WHITELIST_PATTERNS = {
    "joiner": r"joiner-\d+$",
    "reducer": r"reducer-\d+$",
    "final_reducer": r"final-reducer-q\d+$",
    "preprocessor": r"preprocessor-\d+$",
    "production_filter": r"production-filter-\d+$",
    "year_filter": r"year-filter-\d+$",
    "sentiment_analyzer": r"sentiment-analyzer-\d+$",
    "healer": r"healer-\d+$",
}

REGEXS_WHITELIST = {
    name: re.compile(pattern) for name, pattern in WHITELIST_PATTERNS.items()
}

REGEX_CLIENT = re.compile(r"client\d+$")


def is_whitelisted(name_or_id, regex_list):
    return any(pattern.match(name_or_id) for pattern in regex_list)

def remove_first_healer_container(containers_to_kill):
    """
    Remove the first healer container from the list of containers to kill.
    """
    healer_containers = [
        c for c in containers_to_kill if REGEXS_WHITELIST["healer"].match(c.name)
    ]
    if healer_containers:
        healer_to_remove = healer_containers[randint(0, len(healer_containers) - 1)]
        containers_to_kill.remove(healer_to_remove)
    else:
        print("No healer containers found. Skipping...")

def run_kill_mode(client, running_containers):
    while (
        any(REGEX_CLIENT.match(c.name) for c in running_containers)
        and len(running_containers) > 0
    ):
        print("iteration")
        containers_to_kill = list(
            filter(
                lambda c: is_whitelisted(c.name, REGEXS_WHITELIST.values()), running_containers
            )
        )

        # Take out of the list just 1 healer container so we dont kill all of them
        remove_first_healer_container(containers_to_kill)

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

def run_atomic_bomb_mode(client):
    #sleep for a random time between 10 and 20 seconds before killing the containers
    print(f"Waiting perfect time to drop DA BOMB (1 minute)")
    time.sleep(60)

    #Kill all containers in whitelist except 1 healer container
    running_containers = client.containers.list(filters={"status": "running"})
    containers_to_kill = list(
        filter(
            lambda c: is_whitelisted(c.name, REGEXS_WHITELIST.values()), running_containers
        )
    )
    remove_first_healer_container(containers_to_kill)

    for container in containers_to_kill:
        print(f"Matando contenedor: {container.name}")
        try:
            container.kill()
        except Exception as e:
            print(f"Error al matar {container.name}: {e}")
    
    print("Atomic bomb mode completed. Terminating...")
    return

def run_deterministic_mode(client, running_containers):
    """Kill exactly one container per regex in REGEXS_WHITELIST at every iteration."""
    killed = set()
    
    while (
        any(REGEX_CLIENT.match(c.name) for c in running_containers)
        and len(running_containers) > 0
    ):
        print("iteration")
        running_containers = client.containers.list(filters={"status": "running"})
        
        # Get all containers that match whitelist patterns
        all_whitelisted = []
        for container in running_containers:
            if is_whitelisted(container.name, REGEXS_WHITELIST.values()):
                all_whitelisted.append(container)
        
        # Remove one healer to keep at least one alive
        remove_first_healer_container(all_whitelisted)
        
        # Filter containers that haven't been killed yet
        not_killed_yet = [c for c in all_whitelisted if c.name not in killed]
        
        # If all containers have been killed at least once, reset the killed set
        if not not_killed_yet:
            print("✅ All containers in whitelist have been killed at least once. Resetting...")
            killed.clear()
            not_killed_yet = all_whitelisted
        
        if not_killed_yet:
            target = random.choice(not_killed_yet)
            try:
                print(f"Killing container: {target.name}")
                target.kill()
                killed.add(target.name)
            except Exception as e:
                print(f"Error killing {target.name}: {e}")
        
        time.sleep(1)  
        
    print("No more clients. Terminating deterministic mode...")


def main():
    parser = argparse.ArgumentParser(description="Container killer script")
    parser.add_argument(
        "--mode",
        type=int,
        default=0,
        choices=[0, 1, 2],
        help="Mode: 0=kill-random, 1=atomic-bomb, 2=deterministic (default: 0)",
    )
    args = parser.parse_args()

    client = docker.from_env()

    print("Waiting for containers to appear...")
    while True:
        running_containers = client.containers.list(filters={"status": "running"})
        if len(running_containers) > 0:
            break
        time.sleep(3)

    mode_dispatch = {
        0: ("kill", run_kill_mode),
        1: ("atomic-bomb", run_atomic_bomb_mode),
        2: ("deterministic", run_deterministic_mode),
    }

    mode_name, mode_func = mode_dispatch.get(args.mode, mode_dispatch[0])

    print(f"Containers detected. Initializing with mode: {mode_name}")
    mode_func(client, running_containers)


if __name__ == "__main__":
    main()
