"""Command-lin interface"""
def cli_parse(args):
    from argparse import ArgumentParser

    parser = ArgumentParser(
        prog=args[0],
        usage="",
        
    )
    parser.add_argument(
        "--version", action="store_true", help="show version number."
    )

    # TODO: listen daemon
    # TODO: GET/POST/PATCH analysis queries
    # ... SO MUCH TO DO

    cli_args = parser.parse_args(args[1:])

    return cli_args, parser


if __name__ == "__main__":
    cli_parse(sys.argv) # or similar