#!/usr/bin/env python3

# check_influx2 | (c) 2022 NETWAYS | MIT

from influxdb.plugin import Plugin, PluginError, STATE
from humanfriendly import format_size


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
            f"""|> filter(fn: (r) => r["_measurement"] == "disk" or r["_measurement"] == "win_disk")
            |> filter(fn: (r) => r["host"] == "{host}")
            |> filter(fn: (r) => r["instance"] == "{instance}" or r["device"] == "{instance}" or r["path"] == "{instance}")"""
        )

        try:
            latest = result.peekitem()[1]

            if "inodes_free" in latest:
                self.logger.debug("Detected *nix metric")

                used = latest["used"]
                total = latest["total"]
                free = latest["free"]
                used_percent = latest["used_percent"]
                free_percent = 100.0 - used_percent

            else:
                self.logger.debug("Detected windows metric")
                free_percent = latest["Percent_Free_Space"]
                free = latest["Free_Megabytes"] * 1000000

                used_percent = 100.0 - free_percent
                used = (free / free_percent) * used_percent
                total = free + used

            self.perfdata["used"] = f"{used}b"
            self.perfdata["free"] = f"{free}b"
            self.perfdata["total"] = f"{total}b"
            self.perfdata["used_percent"] = f"{used_percent}b"
            self.perfdata["free_percent"] = f"{free_percent}b"

            instance = self.args.instance

            self.statusline = f" <strong>{instance}</strong> {used_percent:.2f}% used ({format_size(used)} of {format_size(total)})"

            return used_percent

        except IndexError:
            raise PluginError(f"No data for {host}")


CheckDisk.run()
