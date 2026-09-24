#!/usr/bin/env python3
"""
Shared wire format and helpers for the CSE 434 DHT project (Group 77).

Messages are UTF-8 strings in a single UDP datagram, fields joined by '|'.
See MESSAGES.md for the full format. Both manager.py and peer.py import
this module so the two programs cannot drift apart.
"""

import socket

# ---------------------------------------------------------------- constants

DELIM = "|"        # separates fields of a message
TUPLE_DELIM = ","  # separates name,ip,p-port inside one 3-tuple
REC_DELIM = "\x1f"  # separates the 14 fields of a storm record
ENCODING = "utf-8"
BUFSIZE = 65535    # max UDP payload; storm records are far smaller

SUCCESS = "SUCCESS"
FAILURE = "FAILURE"

DEFAULT_TIMEOUT = 5.0  # seconds to wait for a reply
DEFAULT_RETRIES = 3    # attempts before giving up on a request

# ------------------------------------------------------------ encode/decode


def encode(parts):
    """['register', 'alice', ...] -> bytes ready for sendto()."""
    return DELIM.join(str(p) for p in parts).encode(ENCODING)


def decode(data):
    """bytes from recvfrom() -> ['register', 'alice', ...]."""
    return data.decode(ENCODING).split(DELIM)


def fmt_tuple(name, ip, p_port):
    """(name, ip, port) -> 'name,ip,port' for embedding in a message."""
    return TUPLE_DELIM.join([name, ip, str(p_port)])


def parse_tuple(text):
    """'name,ip,port' -> (name, ip, int(port))."""
    name, ip, p_port = text.split(TUPLE_DELIM)
    return (name, ip, int(p_port))


def fmt_record(fields):
    """The 14 CSV fields of a storm event -> one message field."""
    return REC_DELIM.join(str(f) for f in fields)


def parse_record(text):
    """Inverse of fmt_record."""
    return text.split(REC_DELIM)


# ------------------------------------------------------------------- tracing

TRACE_MAXLEN = 110  # keep lines short enough to read in the demo video


def trace(who, arrow, other, parts):
    """
    Print a labelled trace line for every datagram.

    The project spec grades on the output being "a well-labelled trace of the
    messages transmitted and received", so every send and receive in this
    codebase goes through here.
    """
    body = DELIM.join(str(p) for p in parts)
    body = body.replace(REC_DELIM, "~")  # unit separators are invisible
    if len(body) > TRACE_MAXLEN:
        body = body[:TRACE_MAXLEN] + " ...(truncated)"
    print("[{:<8}] {} {:<22} {}".format(who, arrow, other, body), flush=True)


def trace_sent(who, other, parts):
    trace(who, "SENT -->", other, parts)


def trace_recv(who, other, parts):
    trace(who, "RECV <--", other, parts)


def trace_info(who, text):
    print("[{:<8}] {} {}".format(who, "   ..   ", text), flush=True)


# ------------------------------------------------------------------ requests


class Timeout(Exception):
    """A request got no reply after all retries."""


def send(sock, addr, parts, who="?", label=None):
    """Fire and forget one datagram, with a trace line."""
    trace_sent(who, label or "{}:{}".format(*addr), parts)
    sock.sendto(encode(parts), addr)


def request(sock, addr, parts, who="?", label=None,
            timeout=DEFAULT_TIMEOUT, retries=DEFAULT_RETRIES):
    """
    Send a request and block until a reply arrives.

    UDP gives no delivery guarantee, so the datagram is retransmitted up to
    `retries` times. Every request in this protocol is idempotent at the
    application level, so a duplicate caused by a retry is harmless.

    Returns the decoded reply as a list of fields. Raises Timeout on failure.
    """
    name = label or "{}:{}".format(*addr)
    old = sock.gettimeout()
    try:
        sock.settimeout(timeout)
        for attempt in range(1, retries + 1):
            if attempt > 1:
                trace_info(who, "retry {}/{} -> {}".format(attempt, retries, name))
            trace_sent(who, name, parts)
            sock.sendto(encode(parts), addr)
            try:
                data, src = sock.recvfrom(BUFSIZE)
            except socket.timeout:
                continue
            reply = decode(data)
            trace_recv(who, name, reply)
            return reply
        raise Timeout("no reply from {} after {} attempts".format(name, retries))
    finally:
        sock.settimeout(old)


# ------------------------------------------------------------------- hashing


def first_prime_after(x):
    """
    Smallest prime strictly greater than x.

    Used for the local hash table size: s = first prime > 2 * l, where l is
    the number of storm event records in the dataset.
    """
    def is_prime(k):
        if k < 2:
            return False
        if k % 2 == 0:
            return k == 2
        d = 3
        while d * d <= k:
            if k % d == 0:
                return False
            d += 2
        return True

    candidate = int(x) + 1
    while not is_prime(candidate):
        candidate += 1
    return candidate


def hash_record(event_id, table_size, ring_size):
    """
    The two hash functions from the project spec.

        pos = event_id mod s
        id  = pos mod n

    Returns (pos, id): the slot in the local hash table, and the ring
    identifier of the node that owns that slot.
    """
    pos = int(event_id) % table_size
    node_id = pos % ring_size
    return pos, node_id
