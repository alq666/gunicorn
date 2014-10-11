# -*- coding: utf-8 -
#
# This file is part of gunicorn released under the MIT license.
# See the NOTICE for more information.

"Bare-bones implementation of statsD's protocol, client-side"

import socket
import logging
from gunicorn.glogging import Logger

# Instrumentation constants
STATSD_DEFAULT_PORT = 8125

METRIC_VAR     = "metric"
VALUE_VAR      = "value"
MTYPE_VAR      = "mtype"
TAGS_VAR       = "tags"
GAUGE_TYPE     = "gauge"
COUNTER_TYPE   = "counter"
HISTOGRAM_TYPE = "histogram"

class Statsd(Logger):
    """statsD-based instrumentation, that passes as a logger
    """
    def __init__(self, cfg):
        """host, port: statsD server
        """
        Logger.__init__(self, cfg)
        # Defensive initialization
        self.statsd_use_tags = False
        self.proc_name = None
        self.sock = None

        try:
          # Use proc_name to decorate metric names
          self.proc_name = cfg.proc_name
          # Use statsD tags or stick the proc name in the metric name
          self.statsd_use_tags = cfg.statsd_use_tags
        except cfg.AttributeError:
          pass

        # Should anything fail
        try:
            host, port = cfg.statsd_host

            # Connect to the statsD server
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.sock.connect((host, int(port)))
        except Exception:
            self.sock = None

    # Log errors and warnings
    def critical(self, msg, *args, **kwargs):
        Logger.critical(self, msg, *args, **kwargs)
        self.increment("log.critical", 1, None)

    def error(self, msg, *args, **kwargs):
        Logger.error(self, msg, *args, **kwargs)
        self.increment("log.error", 1, None)

    def warning(self, msg, *args, **kwargs):
        Logger.warning(self, msg, *args, **kwargs)
        self.increment("log.warning", 1, None)

    def exception(self, msg, *args, **kwargs):
        Logger.exception(self, msg, *args, **kwargs)
        self.increment("log.exception", 1, None)

    # Special treatement for info, the most common log level
    def info(self, msg, *args, **kwargs):
        self.log(logging.INFO, msg, *args, **kwargs)

    # skip the run-of-the-mill logs
    def debug(self, msg, *args, **kwargs):
        self.log(logging.DEBUG, msg, *args, **kwargs)

    def log(self, lvl, msg, *args, **kwargs):
        """Log a given statistic if metric, value and type are present
        """
        try:
            extra = kwargs.get("extra", None)
            if extra is not None:
                metric = extra.get(METRIC_VAR, None)
                value = extra.get(VALUE_VAR, None)
                typ = extra.get(MTYPE_VAR, None)
                tags = extra.get(TAGS_VAR, None)
                if metric and value and typ:
                    if typ == GAUGE_TYPE:
                        self.gauge(metric, value, tags)
                    elif typ == COUNTER_TYPE:
                        self.increment(metric, value, tags)
                    elif typ == HISTOGRAM_TYPE:
                        self.histogram(metric, value, tags)
                    else:
                        pass

            # Log to parent logger only if there is something to say
            if msg is not None and len(msg) > 0:
                Logger.log(self, lvl, msg, *args, **kwargs)
        except Exception:
            pass

    # access logging
    def access(self, resp, req, environ, request_time):
        """Measure request duration
        request_time is a datetime.timedelta
        """
        Logger.access(self, resp, req, environ, request_time)
        duration_in_ms = request_time.seconds * 1000 + float(request_time.microseconds)/10**3
        self.histogram("request.duration", duration_in_ms, None)
        self.increment("requests", 1, None)
        self.increment("request.status.%d" % int(resp.status.split()[0]), 1, None)

    # statsD methods
    # you can use those directly if you want
    def gauge(self, name, value, tags=None):
        try:
            if self.sock:
                self.sock.send("{0}:{1}|g{2}".format(self._metric_name(name),
                                                     value,
                                                     self._meta(None, tags)))
        except Exception:
            pass

    def increment(self, name, value, tags, sampling_rate=1.0):
        try:
            if self.sock:
                self.sock.send("{0}:{1}|c{2}".format(self._metric_name(name),
                                                     value,
                                                     self._meta(sampling_rate, tags)))
        except Exception:
            pass

    def decrement(self, name, value, tags, sampling_rate=1.0):
        try:
            if self.sock:
                self.sock.send("{0}:-{1}|c{2}".format(self._metric_name(name),
                                                      value,
                                                      self._meta(sampling_rate, tags)))
        except Exception:
            pass

    def histogram(self, name, value, tags):
        try:
            if self.sock:
                self.sock.send("{0}:{1}|ms{2}".format(self._metric_name(name),
                                                      value,
                                                      self._meta(None, tags)))
        except Exception:
            pass

    # datagram-formatting method
    def _metric_name(self, name):
        """Updates the metric name to follow a convention:
        gunicorn.<app_name>.metric when not using statsd tags
        and
        gunicorn.metric when using statsd tags
        """
        if not self.statsd_use_tags and\
           self.proc_name is not None and\
           len(self.proc_name) > 0 and\
           self.proc_name != "gunicorn":
            name_parts = ("gunicorn", self.proc_name, name)
        else:
            name_parts = ("gunicorn", name)
        return ".".join(name_parts)

    def _meta(self, sampling_rate, tags):
        """Serialize metadata
        sampling rate, tags are the only metadata supported right now
        """
        meta = ""
        if sampling_rate is not None and sampling_rate > 0.0 and sampling_rate <= 1.0:
            meta = "|@{0}".format(sampling_rate)
        if self.statsd_use_tags:
            if self.proc_name is not None and self.proc_name != "gunicorn":
                if tags is None:
                    tags = ["app:{0}".format(self.proc_name)]
                else:
                    tags.append("app:{0}".format(self.proc_name))
            meta += "|#{0}".format(",".join(tags))
        return meta
