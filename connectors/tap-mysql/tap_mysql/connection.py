#!/usr/bin/env python3

import backoff

import pymysql
from pymysql.constants import CLIENT

import re
import singer
import ssl

LOGGER = singer.get_logger()

CONNECT_TIMEOUT_SECONDS = 30
READ_TIMEOUT_SECONDS = 3600


def _match_hostname_fallback(cert, hostname):
    """Fallback for ssl.match_hostname when not in stdlib (e.g. Python 3.12+)."""
    if not cert:
        raise ssl.CertificateError("empty or no certificate")
    dnsnames = []
    san = cert.get("subjectAltName", ())
    for key, value in san:
        if key == "DNS":
            if _dnsname_to_pattern(value).match(hostname):
                return
            dnsnames.append(value)
    if not dnsnames:
        # Subject common name only if no SAN
        for key, value in cert.get("subject", ()):
            if key == "commonName":
                if _dnsname_to_pattern(value).match(hostname):
                    return
                dnsnames.append(value)
                break
    if not dnsnames:
        raise ssl.CertificateError(
            "no appropriate subjectAltName or commonName for hostname %r" % (hostname,)
        )
    raise ssl.CertificateError(
        "hostname %r doesn't match %r" % (hostname, dnsnames[0] if len(dnsnames) == 1 else dnsnames)
    )


def _dnsname_to_pattern(dn):
    """Convert DNS name to a regex pattern for wildcard matching."""
    pats = []
    for part in dn.split("."):
        if part == "*":
            pats.append("[^.]+")
        else:
            pats.append(re.escape(part))
    return re.compile(r"\A" + r"\.".join(pats) + r"\Z", re.IGNORECASE)


# ssl.match_hostname was removed in Python 3.12; use fallback when missing
try:
    match_hostname = ssl.match_hostname
except AttributeError:
    match_hostname = _match_hostname_fallback

@backoff.on_exception(backoff.expo,
                      (pymysql.err.OperationalError),
                      max_tries=5,
                      factor=2)
def connect_with_backoff(connection):
    connection.connect()

    warnings = []
    with connection.cursor() as cur:
        try:
            cur.execute('SET @@session.time_zone="+0:00"')
        except pymysql.err.InternalError as e:
            warnings.append('Could not set session.time_zone. Error: ({}) {}'.format(*e.args))

        try:
            cur.execute('SET @@session.wait_timeout=2700')
        except pymysql.err.InternalError as e:
             warnings.append('Could not set session.wait_timeout. Error: ({}) {}'.format(*e.args))

        try:
            cur.execute("SET @@session.net_read_timeout={}".format(READ_TIMEOUT_SECONDS))
        except pymysql.err.InternalError as e:
             warnings.append('Could not set session.net_read_timeout. Error: ({}) {}'.format(*e.args))


        try:
            cur.execute('SET @@session.innodb_lock_wait_timeout=2700')
        except pymysql.err.InternalError as e:
            warnings.append(
                'Could not set session.innodb_lock_wait_timeout. Error: ({}) {}'.format(*e.args)
                )

        if warnings:
            LOGGER.info(("Encountered non-fatal errors when configuring MySQL session that could "
                         "impact performance:"))
        for w in warnings:
            LOGGER.warning(w)

    return connection


def parse_internal_hostname(hostname):
    # special handling for google cloud
    if ":" in hostname:
        parts = hostname.split(":")
        if len(parts) == 3:
            return parts[0] + ":" + parts[2]
        return parts[0] + ":" + parts[1]

    return hostname


class MySQLConnection(pymysql.connections.Connection):
    def __init__(self, config):
        # Google Cloud's SSL involves a self-signed certificate. This certificate's
        # hostname matches the form {instance}:{box}. The hostname displayed in the
        # Google Cloud UI is of the form {instance}:{region}:{box} which
        # necessitates the "parse_internal_hostname" function to get the correct
        # hostname to match.
        # The "internal_hostname" config variable allows for matching the SSL
        # against a host that doesn't match the host we are connecting to. In the
        # case of Google Cloud, we will be connecting to an IP, not the hostname
        # the SSL certificate expects.
        # The "ssl.match_hostname" function is patched to check against the
        # internal hostname rather than the host of the connection. In the event
        # that the connection fails, the patch is reverted by reassigning the
        # patched out method to it's original spot.

        args = {
            "user": config["user"],
            "password": config["password"],
            "host": config["host"],
            "port": int(config["port"]),
            "cursorclass": config.get("cursorclass") or pymysql.cursors.SSCursor,
            "connect_timeout": CONNECT_TIMEOUT_SECONDS,
            "read_timeout": READ_TIMEOUT_SECONDS,
            "charset": "utf8",
        }

        ssl_arg = None

        if config.get("database"):
            args["database"] = config["database"]

        use_ssl = config.get('ssl') == 'true'

        # Attempt self-signed SSL if config vars are present
        use_self_signed_ssl = config.get("ssl_ca")

        if use_ssl and use_self_signed_ssl:
            LOGGER.info("Using custom certificate authority")

            # Config values MUST be paths to files for the SSL module to read them correctly.
            ssl_arg = {
                "ca": config["ssl_ca"],
                "check_hostname": config.get("check_hostname", "true") == "true"
            }

            # If using client authentication, cert and key are required
            if config.get("ssl_cert") and config.get("ssl_key"):
                ssl_arg["cert"] = config["ssl_cert"]
                ssl_arg["key"] = config["ssl_key"]

            # override match hostname for google cloud (Python <3.12 had ssl.match_hostname)
            if config.get("internal_hostname"):
                parsed_hostname = parse_internal_hostname(config["internal_hostname"])
                _match = match_hostname
                try:
                    ssl.match_hostname = lambda cert, hostname: _match(cert, parsed_hostname)
                except AttributeError:
                    pass

        super().__init__(defer_connect=True, ssl=ssl_arg, **args)

        # Configure SSL without custom CA
        # Manually create context to override default behavior of
        # CERT_NONE without a CA supplied
        if use_ssl and not use_self_signed_ssl:
            LOGGER.info("Attempting SSL connection")
            # For compatibility with previous version, verify mode is off by default
            verify_mode = config.get("verify_mode", "false") == 'true'
            if not verify_mode:
                LOGGER.warn("Not verifying server certificate. The connection is encrypted, but the server hasn't been verified. Please provide a root CA certificate to enable verification.")
            self.ssl = True
            self.ctx = ssl.create_default_context()
            check_hostname = config.get("check_hostname", "false") == 'true'
            self.ctx.check_hostname = check_hostname
            self.ctx.verify_mode = ssl.CERT_REQUIRED if verify_mode else ssl.CERT_NONE
            self.client_flag |= CLIENT.SSL


    def __enter__(self):
        return self


    def __exit__(self, *exc_info):
        del exc_info
        self.close()


def make_connection_wrapper(config):
    class ConnectionWrapper(MySQLConnection):
        def __init__(self, *args, **kwargs):
            config["cursorclass"] = kwargs.get('cursorclass')
            super().__init__(config)

            connect_with_backoff(self)

    return ConnectionWrapper
