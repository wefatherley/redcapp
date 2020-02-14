"""Command-lin interface"""
def cli_parse(args):
    from argparse import ArgumentParser

    parser = ArgumentParser(
        prog=args[0],
        usage="Alter the state of a REDCap instance",
        
    )
    parser.add_argument(
        "--version", action="store_true", help="show version number."
    )

    cli_args = parser.parse_args(args[1:])

    return cli_args, parser


if __name__ == "__main__":
    cli_parse(sys.argv)