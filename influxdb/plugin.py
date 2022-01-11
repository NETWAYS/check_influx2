# check_influx2 | (c) 2022 NETWAYS | MIT

import sys
import logging
import pathlib
import configparser
import datetime
import pathlib

from argparse import ArgumentParser
from logbook import Logger, StreamHandler
from logbook.compat import redirect_logging
from enum import Enum
from collections import namedtuple

from influxdb_client import InfluxDBClient
from sortedcontainers import SortedDict

StreamHandler(sys.stdout).push_application()
redirect_logging()


class STATE(Enum):
    OK = 0
    WARNING = 1
    CRITICAL = 2
    UNKNOWN = 3


class PluginError(RuntimeError):
    pass


class Plugin(object):
    def __init__(self):
        self.prog = pathlib.Path(sys.argv[0]).stem
        self.status = STATE.UNKNOWN
        self.statusline = "<EMPTY>"
        self.perfdata = SortedDict()

        self.parser = self.__parser__()

    def __logger__(self, verbose: bool = False):
        level = logging.ERROR

        if verbose is True:
            level = logging.DEBUG

        logger = Logger(type(self).__name__, level=level)

        return logger

    def __parser__(self):

        parser = ArgumentParser(prog=self.prog)

        parser.add_argument(
            "-v", "--verbose", action="store_true", dest="verbose", default=False
        )

        parser.add_argument("-w", "--warning", dest="warning", type=str, required=True)

        parser.add_argument(
            "-c", "--critical", dest="critical", type=str, required=True
        )

        parser.add_argument("-H", "--host", dest="host", type=str, required=True)

        return parser

    def __client__(
        self, url: str, token: str, org: str, verify_ssl: bool = True, **kwargs
    ):
        return InfluxDBClient(url=url, token=token, org=org, verify_ssl=verify_ssl)

    def __config__(self, config_dir: str = None):
        if config_dir is None:
            config_dir = pathlib.Path(__file__).parent.parent.resolve()

        config_file = f"{config_dir}/config.ini"

        config = configparser.ConfigParser()
        config.read(config_file)

        if "url" in config["influx2"]:
            return config

    def build_result(self, result):
        results = SortedDict()
        for table in result:
            for record in table.records:
                timestamp = int(record.get_time().timestamp())

                if not timestamp in results:
                    results[timestamp] = {"_time": record.get_time()}

                results[timestamp][record.get_field()] = record.get_value()

        return results

    def build_from_query(self, filter: str):
        config = self.config["influx2"]
        query = f"""
        from(bucket: "{ config["bucket"] }")
          |> range(start: { config["range"] })
          {filter}
        """

        return query

    def build_perfdata(self):
        items = []

        for k, v in self.perfdata.items():
            items.append(f"'{k}'={v}")

        return " ".join(items)

    def query(self, filter: str):
        query = self.build_from_query(filter)

        return self.build_result(self.client.query_api().query(query=query))

    def check_threshold(self, value: float, threshold: str) -> bool:
        if threshold[0] == "@":
            inside = True
            threshold = threshold[1:]
        elif threshold[0] == "~":
            inside = False
            threshold = threshold[1:]
        else:
            inside = False

        values = threshold.split(":")

        if len(values) == 1:
            val1 = 0.0
            val2 = float(values[0])
        elif len(values) == 2:
            val1 = float(values[0]) if values[0] else float("-inf")
            val2 = float(values[1]) if values[1] else float("+inf")

        else:
            raise PluginError("Error parsing thresholds")

        if inside is True and value >= val1 and value <= val2:
            return True
        elif inside is False and (value < val1 or value > val2):
            return True

        return False

    def main(self):
        raise NotImplementedError()

    def __run__(self):
        self.args = self.parser.parse_args()

        self.logger = self.__logger__(self.args.verbose)

        self.config = self.__config__()

        client_config = dict(self.config["influx2"])
        client_config["verify_ssl"] = self.config["influx2"].getboolean(
            "verify_ssl", fallback=True
        )

        if client_config["verify_ssl"] is False:
            import urllib3

            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            self.logger.warn("SSL verification is disabled")

        self.client = self.__client__(**client_config)

        self.logger.debug("Run {0}", self.main)

        try:
            time_start = datetime.datetime.now()
            value = self.main()

            if self.check_threshold(value, self.args.critical) is True:
                self.status = STATE.CRITICAL
            elif self.check_threshold(value, self.args.warning) is True:
                self.status = STATE.WARNING
            else:
                self.status = STATE.OK

        except PluginError as e:
            self.statusline = str(e)
        finally:
            time_end = datetime.datetime.now()
            time_delta = time_end - time_start
            self.perfdata["runtime"] = f"{time_delta.total_seconds()}s"

        perfdata = self.build_perfdata()

        print(f"{self.prog}: {repr(self.status)} {self.statusline}|{perfdata}")

        sys.exit(self.status.value)

    @classmethod
    def run(cls):
        plugin = cls()
        plugin.__run__()
