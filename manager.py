#!/usr/bin/env python3
"""
DHT manager — CSE 434 Socket Project, Group 77.

An always-on process that tracks registered peers and coordinates construction
of the DHT. Listens on one UDP port given on the command line.

Milestone scope: register, setup-dht, dht-complete.

    python3 manager.py <port>
"""

import random
import socket
import sys

from protocol import (BUFSIZE, FAILURE, SUCCESS, decode, encode, fmt_tuple,
                      trace_info, trace_recv, trace_sent)

WHO = "manager"

# Peer states from the spec
FREE = "Free"
LEADER = "Leader"
IN_DHT = "InDHT"

# Manager's view of the DHT itself
DHT_NONE = "none"          # no DHT exists
DHT_BUILDING = "building"  # setup-dht answered, waiting for dht-complete
DHT_READY = "complete"     # DHT is built


class Manager:
    def __init__(self, port):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("", port))
        self.port = port

        # name -> {"ip", "m_port", "p_port", "state"}
        self.peers = {}

        self.dht_status = DHT_NONE
        self.dht_leader = None
        self.dht_members = []  # peer names, index == ring identifier

    # ------------------------------------------------------------- utilities

    def free_peers(self, exclude=None):
        return [n for n, p in self.peers.items()
                if p["state"] == FREE and n != exclude]

    def endpoint_taken(self, ip, port):
        """
        The spec allows several peers per host but requires each process's
        ports to be unique, so the constraint is on the (ip, port) pair.
        """
        for p in self.peers.values():
            if p["ip"] == ip and port in (p["m_port"], p["p_port"]):
                return True
        return False

    def dump_state(self):
        trace_info(WHO, "--- state ---------------------------------------")
        if not self.peers:
            trace_info(WHO, "    (no peers registered)")
        for name, p in self.peers.items():
            trace_info(WHO, "    {:<10} {:<15} m={} p={} {}".format(
                name, p["ip"], p["m_port"], p["p_port"], p["state"]))
        trace_info(WHO, "    dht: {} leader={} members={}".format(
            self.dht_status, self.dht_leader, self.dht_members or "-"))
        trace_info(WHO, "-------------------------------------------------")

    # -------------------------------------------------------------- handlers

    def handle_register(self, args):
        if len(args) != 4:
            return [FAILURE, "register", "expected 4 arguments"]
        name, ip, m_port, p_port = args

        if not name.isalpha() or len(name) > 15:
            return [FAILURE, "register", "name must be alphabetic, <= 15 chars"]
        if name in self.peers:
            return [FAILURE, "register", "name already registered"]
        try:
            m_port, p_port = int(m_port), int(p_port)
        except ValueError:
            return [FAILURE, "register", "ports must be integers"]
        if m_port == p_port:
            return [FAILURE, "register", "m-port and p-port must differ"]
        if self.endpoint_taken(ip, m_port) or self.endpoint_taken(ip, p_port):
            return [FAILURE, "register", "port already in use on that host"]

        self.peers[name] = {"ip": ip, "m_port": m_port,
                            "p_port": p_port, "state": FREE}
        trace_info(WHO, "registered {} at {} (m={} p={}) as {}".format(
            name, ip, m_port, p_port, FREE))
        return [SUCCESS, "register"]

    def handle_setup_dht(self, args):
        if len(args) != 3:
            return [FAILURE, "setup-dht", "expected 3 arguments"]
        name, n, year = args

        if name not in self.peers:
            return [FAILURE, "setup-dht", "peer not registered"]
        try:
            n = int(n)
        except ValueError:
            return [FAILURE, "setup-dht", "n must be an integer"]
        if n < 3:
            return [FAILURE, "setup-dht", "n must be at least 3"]
        if len(self.peers) < n:
            return [FAILURE, "setup-dht",
                    "only {} peers registered, need {}".format(len(self.peers), n)]
        if self.dht_status != DHT_NONE:
            return [FAILURE, "setup-dht", "a DHT already exists"]

        others = self.free_peers(exclude=name)
        if len(others) < n - 1:
            return [FAILURE, "setup-dht",
                    "only {} free peers available, need {}".format(len(others), n - 1)]

        chosen = random.sample(others, n - 1)

        self.peers[name]["state"] = LEADER
        for other in chosen:
            self.peers[other]["state"] = IN_DHT

        self.dht_leader = name
        self.dht_members = [name] + chosen
        self.dht_status = DHT_BUILDING

        trace_info(WHO, "leader={} ring={} (waiting for dht-complete)".format(
            name, self.dht_members))

        tuples = [fmt_tuple(m, self.peers[m]["ip"], self.peers[m]["p_port"])
                  for m in self.dht_members]
        return [SUCCESS, "setup-dht", n] + tuples

    def handle_dht_complete(self, args):
        if len(args) != 1:
            return [FAILURE, "dht-complete", "expected 1 argument"]
        name = args[0]

        if name != self.dht_leader:
            return [FAILURE, "dht-complete", "peer is not the DHT leader"]

        self.dht_status = DHT_READY
        trace_info(WHO, "DHT of size {} is complete".format(len(self.dht_members)))
        self.dump_state()
        return [SUCCESS, "dht-complete"]

    # ------------------------------------------------------------- main loop

    def dispatch(self, parts):
        command, args = parts[0], parts[1:]

        # While a DHT is under construction the manager answers FAILURE to
        # everything except the dht-complete it is waiting for.
        if self.dht_status == DHT_BUILDING and command != "dht-complete":
            return [FAILURE, command, "DHT is under construction"]

        if command == "register":
            return self.handle_register(args)
        if command == "setup-dht":
            return self.handle_setup_dht(args)
        if command == "dht-complete":
            return self.handle_dht_complete(args)

        # Commands deferred to the full project (due 10/18).
        if command in ("query-dht", "leave-dht", "join-dht", "dht-rebuilt",
                       "deregister", "teardown-dht", "teardown-complete"):
            return [FAILURE, command, "not implemented until the full project"]

        return [FAILURE, command, "unknown command"]

    def run(self):
        trace_info(WHO, "listening on UDP port {}".format(self.port))
        while True:
            data, src = self.sock.recvfrom(BUFSIZE)
            label = "{}:{}".format(*src)
            try:
                parts = decode(data)
            except UnicodeDecodeError:
                trace_info(WHO, "dropped undecodable datagram from " + label)
                continue

            trace_recv(WHO, label, parts)
            try:
                reply = self.dispatch(parts)
            except Exception as exc:  # never let one bad message kill the manager
                reply = [FAILURE, parts[0] if parts else "?", "internal error: " + str(exc)]
            trace_sent(WHO, label, reply)
            self.sock.sendto(encode(reply), src)


def main():
    if len(sys.argv) != 2:
        print("usage: python3 manager.py <port>", file=sys.stderr)
        sys.exit(1)
    try:
        port = int(sys.argv[1])
    except ValueError:
        print("port must be an integer", file=sys.stderr)
        sys.exit(1)
    if not 38500 <= port <= 38999:
        print("warning: port {} is outside group 77's range 38500-38999".format(port),
              file=sys.stderr)

    try:
        Manager(port).run()
    except KeyboardInterrupt:
        print()
        trace_info(WHO, "shutting down")


if __name__ == "__main__":
    main()
