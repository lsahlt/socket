#!/usr/bin/env python3
"""Always on UDP manager"""

import random
import socket
import sys

from protocol import (BUFSIZE, FAILURE, SUCCESS, decode, encode, fmt_tuple,
                      trace_info, trace_recv, trace_sent)

WHO = "manager"
#peer states from the spec
FREE = "Free"
LEADER = "Leader"
IN_DHT = "InDHT"
#manager view of the dht
DHT_NONE = "none"
DHT_BUILDING = "building"
DHT_READY = "complete"
DHT_REBUILDING = "rebuilding"
DHT_TEARING_DOWN = "tearing-down"


class Manager:
    """ keep authoritative registration and DHT membership state """

    def __init__(self, port, sock=None):
        """ create the manager and bind its UDP socket to the given port"""
        self.sock = sock or socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        if sock is None:
            self.sock.bind(("", port))
        self.port = port
        self.peers = {}
        self.dht_status = DHT_NONE
        self.dht_leader = None
        self.dht_members = []
        self.dht_year = None
        self.pending_action = None
        self.pending_peer = None
        self.pending_members = None

    def free_peers(self, exclude=None):
        """Return the names of all Free peers except  optional name """
        return [name for name, peer in self.peers.items()
                if peer["state"] == FREE and name != exclude]

    def endpoint_taken(self, ip, port):
        """check whether a registered peer already uses this IP/port pair"""
        return any(peer["ip"] == ip and port in (peer["m_port"], peer["p_port"])
                   for peer in self.peers.values())

    def tuple_for(self, name):
        """Convert a registered peer into the name, IP, p port wire format"""
        peer = self.peers[name]
        return fmt_tuple(name, peer["ip"], peer["p_port"])

    def dump_state(self):
        """Print the manager's current peer and DHT state for the demo trace."""
        trace_info(WHO, "--- state ---------------------------------------")
        if not self.peers:
            trace_info(WHO, "    (no peers registered)")
        for name, peer in self.peers.items():
            trace_info(WHO, "    {:<10} {:<15} m={} p={} {}".format(
                name, peer["ip"], peer["m_port"], peer["p_port"], peer["state"]))
        trace_info(WHO, "    dht: {} leader={} year={} members={}".format(
            self.dht_status, self.dht_leader, self.dht_year,
            self.dht_members or "-"))
        trace_info(WHO, "-------------------------------------------------")




    def handle_register(self, args):
        """validate a registration and add the new peer as Free."""
        if len(args) != 4:
            return [FAILURE, "register", "expected 4 arguments"]
        name, ip, m_port, p_port = args
        if not name.isalpha() or len(name) > 15:
            return [FAILURE, "register", "name must be alphabetic, <= 15 chars"]
        if name in self.peers:
            return [FAILURE, "register", "name already registered"]
        try:
            socket.inet_aton(ip)
            m_port, p_port = int(m_port), int(p_port)
        except OSError:
            return [FAILURE, "register", "invalid IPv4 address"]
        except ValueError:
            return [FAILURE, "register", "ports must be integers"]
        if not all(1 <= port <= 65535 for port in (m_port, p_port)):
            return [FAILURE, "register", "ports must be in 1..65535"]
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
        """ hoose DHT members, assign their roles, and return their tuples."""
        if len(args) != 3:
            return [FAILURE, "setup-dht", "expected 3 arguments"]
        name, n_text, year_text = args
        if name not in self.peers:
            return [FAILURE, "setup-dht", "peer not registered"]
        if self.peers[name]["state"] != FREE:
            return [FAILURE, "setup-dht", "peer is not Free"]
        try:
            n, year = int(n_text), int(year_text)
        except ValueError:
            return [FAILURE, "setup-dht", "n and YYYY must be integers"]
        if n < 3:
            return [FAILURE, "setup-dht", "n must be at least 3"]
        if not 1950 <= year <= 2019:
            return [FAILURE, "setup-dht", "YYYY must be between 1950 and 2019"]
        if len(self.peers) < n:
            return [FAILURE, "setup-dht", "only {} peers registered, need {}".format(
                len(self.peers), n)]
        if self.dht_status != DHT_NONE:
            return [FAILURE, "setup-dht", "a DHT already exists"]
        others = self.free_peers(exclude=name)
        if len(others) < n - 1:
            return [FAILURE, "setup-dht", "only {} free peers available, need {}".format(
                len(others), n - 1)]
        chosen = random.sample(others, n - 1)
        self.dht_members = [name] + chosen
        self.dht_leader = name
        self.dht_year = year
        self.dht_status = DHT_BUILDING
        self.peers[name]["state"] = LEADER
        for other in chosen:
            self.peers[other]["state"] = IN_DHT
        trace_info(WHO, "leader={} ring={} year={} (waiting for dht-complete)".format(
            name, self.dht_members, year))
        return [SUCCESS, "setup-dht", n] + [self.tuple_for(m) for m in self.dht_members]

    def handle_dht_complete(self, args):
        """mark the initial DHT build complete when its leader reports back."""
        if len(args) != 1:
            return [FAILURE, "dht-complete", "expected 1 argument"]
        name = args[0]
        if self.dht_status == DHT_READY and name == self.dht_leader:
            return [SUCCESS, "dht-complete"]
        if self.dht_status != DHT_BUILDING or name != self.dht_leader:
            return [FAILURE, "dht-complete", "peer is not the building DHT leader"]
        self.dht_status = DHT_READY
        trace_info(WHO, "DHT of size {} is complete".format(len(self.dht_members)))
        self.dump_state()
        return [SUCCESS, "dht-complete"]

    def handle_query_dht(self, args):
        """give a Free querying peer random DHT member to contact first"""
        if len(args) != 1:
            return [FAILURE, "query-dht", "expected 1 argument"]
        name = args[0]
        if self.dht_status != DHT_READY:
            return [FAILURE, "query-dht", "DHT is not ready"]
        if name not in self.peers:
            return [FAILURE, "query-dht", "peer not registered"]
        if self.peers[name]["state"] != FREE:
            return [FAILURE, "query-dht", "querying peer must be Free"]
        target = random.choice(self.dht_members)
        trace_info(WHO, "query from {} starts at {}".format(name, target))
        return [SUCCESS, "query-dht", self.tuple_for(target)]

    def handle_leave_dht(self, args):
        """Start a leave operation and remember the expected smaller ring."""
        if len(args) != 1:
            return [FAILURE, "leave-dht", "expected 1 argument"]
        name = args[0]
        if self.dht_status != DHT_READY:
            return [FAILURE, "leave-dht", "DHT does not exist or is busy"]
        if name not in self.dht_members:
            return [FAILURE, "leave-dht", "peer is not in the DHT"]
        if len(self.dht_members) <= 1:
            return [FAILURE, "leave-dht", "cannot remove the final DHT peer"]
        leaving_id = self.dht_members.index(name)
        # The leaving peer's right neighbour becomes id 0/new leader
        self.pending_action = "leave"
        self.pending_peer = name
        self.pending_members = (self.dht_members[leaving_id + 1:] +
                                self.dht_members[:leaving_id])
        self.dht_status = DHT_REBUILDING
        trace_info(WHO, "{} is leaving; waiting for dht-rebuilt".format(name))
        return [SUCCESS, "leave-dht"]

    def handle_join_dht(self, args):
        """start a join operation and return the current ring to the joiner"""
        if len(args) != 1:
            return [FAILURE, "join-dht", "expected 1 argument"]
        name = args[0]
        if self.dht_status != DHT_READY:
            return [FAILURE, "join-dht", "DHT does not exist or is busy"]
        if name not in self.peers or self.peers[name]["state"] != FREE:
            return [FAILURE, "join-dht", "peer is not registered and Free"]
        old_members = list(self.dht_members)
        self.pending_action = "join"
        self.pending_peer = name
        self.pending_members = [name] + old_members
        self.dht_status = DHT_REBUILDING
        trace_info(WHO, "{} is joining; waiting for dht-rebuilt".format(name))
        return ([SUCCESS, "join-dht", self.dht_year] +
                [self.tuple_for(member) for member in old_members])

    def handle_dht_rebuilt(self, args):
        """finish leave/join processing and publish the rebuilt ring state"""
        if len(args) != 2:
            return [FAILURE, "dht-rebuilt", "expected 2 arguments"]
        name, new_leader = args
        if self.dht_status != DHT_REBUILDING or name != self.pending_peer:
            return [FAILURE, "dht-rebuilt", "peer did not initiate the rebuild"]
        if new_leader not in self.pending_members:
            return [FAILURE, "dht-rebuilt", "new leader is not in rebuilt DHT"]
        old_members = set(self.dht_members)
        self.dht_members = list(self.pending_members)
        self.dht_leader = new_leader
        for member in old_members | set(self.dht_members):
            self.peers[member]["state"] = FREE
        for member in self.dht_members:
            self.peers[member]["state"] = (LEADER if member == new_leader else IN_DHT)
        action, initiator = self.pending_action, self.pending_peer
        self.pending_action = self.pending_peer = self.pending_members = None
        self.dht_status = DHT_READY
        trace_info(WHO, "{} by {} complete; new leader={} ring={}".format(
            action, initiator, new_leader, self.dht_members))
        self.dump_state()
        return [SUCCESS, "dht-rebuilt"]

    def handle_deregister(self, args):
        """remove a Free peer from the manager's registration table."""
        if len(args) != 1:
            return [FAILURE, "deregister", "expected 1 argument"]
        name = args[0]
        if name not in self.peers:
            return [FAILURE, "deregister", "peer not registered"]
        if self.peers[name]["state"] != FREE:
            return [FAILURE, "deregister", "only a Free peer may deregister"]
        del self.peers[name]
        trace_info(WHO, "deregistered {}".format(name))
        return [SUCCESS, "deregister"]

    def handle_teardown_dht(self, args):
        """allow the current leader to begin deleting the DHT."""
        if len(args) != 1:
            return [FAILURE, "teardown-dht", "expected 1 argument"]
        name = args[0]
        if self.dht_status != DHT_READY or name != self.dht_leader:
            return [FAILURE, "teardown-dht", "peer is not the DHT leader"]
        self.dht_status = DHT_TEARING_DOWN
        trace_info(WHO, "teardown initiated by {}; waiting for teardown-complete".format(name))
        return [SUCCESS, "teardown-dht"]

    def handle_teardown_complete(self, args):
        """Finish DHT deletion and return every former member to Free."""
        if len(args) != 1:
            return [FAILURE, "teardown-complete", "expected 1 argument"]
        name = args[0]
        if self.dht_status != DHT_TEARING_DOWN or name != self.dht_leader:
            return [FAILURE, "teardown-complete", "peer is not the tearing-down leader"]
        for member in self.dht_members:
            self.peers[member]["state"] = FREE
        self.dht_status = DHT_NONE
        self.dht_leader = None
        self.dht_members = []
        self.dht_year = None
        trace_info(WHO, "DHT teardown complete; all members are Free")
        self.dump_state()
        return [SUCCESS, "teardown-complete"]

    def dispatch(self, parts):
        """Validate manager busy state and route one message to its handler"""
        if not parts or not parts[0]:
            return [FAILURE, "?", "empty message"]
        command, args = parts[0], parts[1:]
        allowed = None
        if self.dht_status == DHT_BUILDING:
            allowed = "dht-complete"
        elif self.dht_status == DHT_REBUILDING:
            allowed = "dht-rebuilt"
        elif self.dht_status == DHT_TEARING_DOWN:
            allowed = "teardown-complete"
        if allowed and command != allowed:
            return [FAILURE, command, "DHT is busy; waiting for " + allowed]
        handlers = {
            "register": self.handle_register,
            "setup-dht": self.handle_setup_dht,
            "dht-complete": self.handle_dht_complete,
            "query-dht": self.handle_query_dht,
            "leave-dht": self.handle_leave_dht,
            "join-dht": self.handle_join_dht,
            "dht-rebuilt": self.handle_dht_rebuilt,
            "deregister": self.handle_deregister,
            "teardown-dht": self.handle_teardown_dht,
            "teardown-complete": self.handle_teardown_complete,
        }
        handler = handlers.get(command)
        return handler(args) if handler else [FAILURE, command, "unknown command"]

    def run(self):
        """Receive UDP requests forever and send one response for each."""
        trace_info(WHO, "listening on UDP port {}".format(self.port))
        while True:
            data, src = self.sock.recvfrom(BUFSIZE)
            label = "{}:{}".format(*src)
            parts = []
            try:
                parts = decode(data)
                trace_recv(WHO, label, parts)
                reply = self.dispatch(parts)
            except (UnicodeDecodeError, ValueError, IndexError) as exc:
                reply = [FAILURE, "?", "malformed message: " + str(exc)]
            except Exception as exc:
                reply = [FAILURE, parts[0] if parts else "?", "internal error: " + str(exc)]
            trace_sent(WHO, label, reply)
            self.sock.sendto(encode(reply), src)


def main():
    """read the manager port from the command line and start the server."""
    if len(sys.argv) != 2:
        print("usage: python3 manager.py <port>", file=sys.stderr)
        sys.exit(1)
    try:
        port = int(sys.argv[1])
    except ValueError:
        print("port must be an integer", file=sys.stderr)
        sys.exit(1)
    if not 39500 <= port <= 39999:
        print("warning: port {} is outside group 77's range 39500-39999".format(port),
              file=sys.stderr)
    try:
        Manager(port).run()
    except KeyboardInterrupt:
        print()
        trace_info(WHO, "shutting down")


if __name__ == "__main__":
    main()
