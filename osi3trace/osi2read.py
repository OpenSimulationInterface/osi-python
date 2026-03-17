"""
This program converts serialized osi trace files into a human readable output.

Supported output formats:
    txth - Human readable text (default)
    json - JSON format (machine parsable, protobuf compatible)

Example usage:
    python3 osi2read.py -d trace.osi -o myreadableosifile
    python3 osi2read.py -d trace.osi --format json
"""

from osi3trace.osi_trace import OSITrace
from google.protobuf.json_format import MessageToJson
import argparse
import json
import pathlib


def command_line_arguments():
    """Define and handle command line interface"""

    parser = argparse.ArgumentParser(
        description="Convert a serialized osi trace file to a readable txth or json output.",
        prog="osi2read",
    )
    parser.add_argument(
        "--data",
        "-d",
        help="Path to the file with serialized data.",
        type=str,
        required=True,
    )
    parser.add_argument(
        "--type",
        "-t",
        help="Name of the type used to serialize data.",
        choices=OSITrace.message_types(),
        default="SensorView",
        type=str,
        required=False,
    )
    parser.add_argument(
        "--output",
        "-o",
        help="Output name of the file.",
        type=str,
        required=False,
    )
    parser.add_argument(
        "--format",
        "-f",
        help="Output format.",
        choices=["txth", "json"],
        default="txth",
        type=str,
        required=False,
    )

    return parser.parse_args()


def main():
    # Handling of command line arguments
    args = command_line_arguments()

    # Initialize the OSI trace class
    trace = OSITrace(args.data, args.type)

    if not args.output:
        suffix = ".json" if args.format == "json" else ".txth"
        path = pathlib.Path(args.data).with_suffix(suffix)
        args.output = str(path)

    with open(args.output, "wt") as f:
        if args.format == "json":
            f.write("[\n")
            first = True
            for message in trace:
                if not first:
                    f.write(",\n")
                first = False
                f.write(MessageToJson(message))
            f.write("\n]\n")
        else:
            for message in trace:
                f.write(str(message))

    trace.close()


if __name__ == "__main__":
    main()
