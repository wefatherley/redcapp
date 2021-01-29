from argparse import ArgumentParser
from logging import basicConfig, INFO, getLogger, WARN
from os import environ, getenv, putenv


basicConfig(
    format="%(asctime)s - %(name)s - %(message)s",
    level=INFO
)


parser = ArgumentParser(prog="redcapp")
parser.add_argument(
    "command",
    choices=["set-env"],
    help="Run or test services"
)
args = parser.parse_args()


if args.command == "set-env":
    putenv("REDCAP_API_HOST", input("Enter API host: "))
    putenv("REDCAP_API_PATH", input("Enter API path: "))
    putenv("REDCAP_API_TOKEN", input("Enter API token: "))
    

