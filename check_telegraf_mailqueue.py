#!/usr/bin/env python3

# check_influx2 | (c) 2022 NETWAYS | MIT

import math

from pprint import pformat
from humanfriendly import format_timespan
from influxdb.plugin import Plugin, PluginError


class CheckDisk(Plugin):
    def __init__(self):
        super().__init__()

        self.parser.add_argument(
            "-I", "--instance", dest="instance", type=str, default="", required=True
        )

    def main(self):
        host = self.args.host
        instance = self.args.instance

        result = self.query(
            f"""|> filter(fn: (r) => 
                r["_measurement"] == "postfix_queue" 
                or r["_measurement"] == "msexchange.transport"
            )
            |> filter(fn: (r) => r["host"] == "{host}")
            |> filter(fn: (r) => 
                r["_field"] == "{instance}"
                or r["queue"] == "{instance}"
            )"""
        )

        try:
            latest = result.peekitem()[1]

            if "length" in latest:
                self.logger.debug("Detected postfix")
                length = int(latest["length"])
            else:
                self.logger.debug("Detected msexchange")
                length = math.floor(float(latest[instance]))

            self.perfdata["length"] = length

            time_delta_seconds = self.timedelta_seconds(latest["_time"])

            self.logger.debug("Influx data={0}", pformat(latest))

            self.statusline = (
                f" <strong>{instance}</strong>"
                + f" with {length} items"
                + f" ({format_timespan(time_delta_seconds)} ago)"
            )

            return length

        except IndexError as orig_exc:
            raise PluginError(f"No data for {host}") from orig_exc


CheckDisk.run()
