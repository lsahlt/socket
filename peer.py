#!/usr/bin/env python3
"""
DHT peer — CSE 434 Socket Project, Group 77.

Reads commands from stdin, talks to the manager over its m-port and to other
peers over its p-port. Both are UDP.

    python3 peer.py <manager-ipv4> <manager-port>

Commands:
    register <peer-name> <IPv4-address> <m-port> <p-port>
    setup-dht <peer-name> <n> <YYYY>
    state                 (local: print this peer's view of the world)
    quit

Status:
    register    working
    setup-dht   working through ring construction (set-id)
    dataset     loaded, l and hash table size computed
    store       STUB -- distribution around the ring, next commit
"""

import csv
import os
import select
import socket
import sys

from protocol import (BUFSIZE, FAILURE, SUCCESS, Timeout, decode, encode,
                      first_prime_after, fmt_tuple, hash_record, parse_tuple,
                      request, trace_info, trace_recv, trace_sent)

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")

# Local peer states, mirroring what the manager tracks
UNREGISTERED = "Unregistered"
FREE = "Free"
LEADER = "Leader"
IN_DHT = "InDHT"


class Peer:
    def __init__(self, mgr_ip, mgr_port):
        self.mgr = (mgr_ip, mgr_port)

        # identity, set by the register command
        self.name = None
        self.ip = None
        self.m_port = None
        self.p_port = None
        self.m_sock = None
        self.p_sock = None
        self.state = UNREGISTERED

        # ring membership, set by setup-dht / set-id
        self.id = None          # this peer's ring identifier
        self.n = None           # ring size
        self.ring = []          # [(name, ip, p_port)] indexed by identifier
        self.right = None       # (name, ip, p_port) of the right neighbour

        # local hash table
        self.table = {}         # pos -> record (list of 14 fields)
        self.hash_size = None   # s, the first prime > 2*l

    # -------------------------------------------------------------- utilities

    @property
    def who(self):
        return self.name or "peer"

    def label_of(self, tup):
        return "{} {}:{}".format(tup[0], tup[1], tup[2])

    def print_state(self):
        trace_info(self.who, "--- local state ---------------------------------")
        trace_info(self.who, "    name={} state={}".format(self.name, self.state))
        trace_info(self.who, "    m-port={} p-port={}".format(self.m_port, self.p_port))
        trace_info(self.who, "    ring id={} n={}".format(self.id, self.n))
        trace_info(self.who, "    right neighbour={}".format(
            self.label_of(self.right) if self.right else "-"))
        trace_info(self.who, "    records held={} table size={}".format(
            len(self.table), self.hash_size))
        trace_info(self.who, "-------------------------------------------------")

    def adopt_ring(self, my_id, n, tuples):
        """Store our identifier, the ring size, and our right neighbour."""
        self.id = my_id
        self.n = n
        self.ring = tuples
        self.right = tuples[(my_id + 1) % n]
        trace_info(self.who, "ring id={} of n={}, right neighbour is {}".format(
            my_id, n, self.label_of(self.right)))

    # -------------------------------------------------------- stdin commands

    def cmd_register(self, args):
        if self.state != UNREGISTERED:
            trace_info(self.who, "already registered as " + self.name)
            return
        if len(args) != 4:
            print("usage: register <peer-name> <IPv4-address> <m-port> <p-port>")
            return
        name, ip, m_port, p_port = args
        try:
            m_port, p_port = int(m_port), int(p_port)
        except ValueError:
            print("ports must be integers")
            return
        for port in (m_port, p_port):
            if not 38500 <= port <= 38999:
                print("warning: port {} is outside group 77's range 38500-38999".format(port))

        # Bind both sockets before contacting the manager, so that the ports we
        # advertise are ports we actually hold.
        m_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        p_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            m_sock.bind((ip, m_port))
            p_sock.bind((ip, p_port))
        except OSError as exc:
            m_sock.close()
            p_sock.close()
            print("could not bind {}: {}".format(ip, exc))
            return

        try:
            reply = request(m_sock, self.mgr,
                            ["register", name, ip, m_port, p_port],
                            who=name, label="manager")
        except Timeout as exc:
            m_sock.close()
            p_sock.close()
            print("register failed: " + str(exc))
            return

        if reply[0] != SUCCESS:
            m_sock.close()
            p_sock.close()
            trace_info(name, "register refused: " + (reply[2] if len(reply) > 2 else "?"))
            return

        self.name, self.ip = name, ip
        self.m_port, self.p_port = m_port, p_port
        self.m_sock, self.p_sock = m_sock, p_sock
        self.state = FREE
        trace_info(self.who, "registered, state={}".format(FREE))

    def cmd_setup_dht(self, args):
        if self.state == UNREGISTERED:
            print("register first")
            return
        if len(args) != 3:
            print("usage: setup-dht <peer-name> <n> <YYYY>")
            return
        name, n, year = args

        try:
            reply = request(self.m_sock, self.mgr, ["setup-dht", name, n, year],
                            who=self.who, label="manager")
        except Timeout as exc:
            print("setup-dht failed: " + str(exc))
            return

        if reply[0] != SUCCESS:
            trace_info(self.who, "setup-dht refused: " + (reply[2] if len(reply) > 2 else "?"))
            return

        ring_size = int(reply[2])
        tuples = [parse_tuple(t) for t in reply[3:]]
        self.state = LEADER
        trace_info(self.who, "leader of a ring of {}: {}".format(
            ring_size, [t[0] for t in tuples]))

        # Step 1 of 1.2.1: assign identifiers and neighbours.
        self.adopt_ring(0, ring_size, tuples)
        if not self.assign_identifiers():
            trace_info(self.who, "ring construction failed, not sending dht-complete")
            return

        # Step 2 of 1.2.1: populate the local hash tables.
        if not self.build_local_dhts(year):
            trace_info(self.who, "DHT population failed, not sending dht-complete")
            return

        # Step 3 of 1.2.1: report and tell the manager we are done.
        self.print_ring_counts()
        try:
            reply = request(self.m_sock, self.mgr, ["dht-complete", self.name],
                            who=self.who, label="manager")
        except Timeout as exc:
            print("dht-complete failed: " + str(exc))
            return
        if reply[0] == SUCCESS:
            trace_info(self.who, "DHT setup complete")

    # ------------------------------------------------- leader: ring assembly

    def assign_identifiers(self):
        """
        Send set-id to peers 1..n-1. Each carries the peer's identifier, the
        ring size, and the full tuple table so the receiver can work out its
        own right neighbour.
        """
        for i in range(1, self.n):
            target = self.ring[i]
            msg = ["set-id", i, self.n] + [fmt_tuple(*t) for t in self.ring]
            try:
                reply = request(self.p_sock, (target[1], target[2]), msg,
                                who=self.who, label=self.label_of(target))
            except Timeout as exc:
                trace_info(self.who, "set-id to {} failed: {}".format(target[0], exc))
                return False
            if reply[0] != SUCCESS:
                trace_info(self.who, "set-id refused by " + target[0])
                return False
        trace_info(self.who, "all {} identifiers assigned".format(self.n))
        return True

    # ----------------------------------------------- leader: populate the DHT

    def load_records(self, year):
        """
        Read data/details-<year>.csv and return its storm event records.

        The first line of the file holds the field names and is skipped, so
        len(records) is l, the number of storm events, as defined in the spec.
        Rows whose event_id is not an integer are reported and skipped rather
        than crashing the leader mid-build.
        """
        path = os.path.join(DATA_DIR, "details-{}.csv".format(year))
        if not os.path.isfile(path):
            trace_info(self.who, "dataset not found: " + path)
            return None

        records, skipped = [], 0
        with open(path, newline="", encoding="utf-8", errors="replace") as handle:
            reader = csv.reader(handle)
            header = next(reader, None)
            if header is None:
                trace_info(self.who, "dataset is empty: " + path)
                return None
            for row in reader:
                if not row or not any(field.strip() for field in row):
                    continue  # trailing blank line
                try:
                    int(row[0])
                except (ValueError, IndexError):
                    skipped += 1
                    continue
                records.append(row)

        if skipped:
            trace_info(self.who, "skipped {} rows with a non-numeric event_id".format(skipped))
        return records

    def build_local_dhts(self, year):
        """
        Step 2 of section 1.2.1: populate the DHT with data.

        This commit covers reading the dataset and sizing the hash table. The
        distribution of records around the ring via store is the next commit;
        see handle_store().
        """
        records = self.load_records(year)
        if records is None:
            return False

        self.records = records
        length = len(records)                          # l
        self.hash_size = first_prime_after(2 * length)  # s

        trace_info(self.who, "dataset details-{}.csv: l={} records".format(year, length))
        trace_info(self.who, "hash table size s = first prime > 2*{} = {}".format(
            length, self.hash_size))

        # Dry run of the two hash functions. This proves the hashing is right
        # before any of it goes over the wire, and the same tally becomes the
        # real per-node count once store is implemented -- the leader computes
        # id for every record, so it never has to ask the other nodes.
        self.expected_counts = {i: 0 for i in range(self.n)}
        for row in records:
            _pos, node_id = hash_record(row[0], self.hash_size, self.n)
            self.expected_counts[node_id] += 1

        trace_info(self.who, "expected distribution: " + ", ".join(
            "id={}:{}".format(i, self.expected_counts[i]) for i in range(self.n)))
        trace_info(self.who, "[STUB] records not yet distributed -- store is the next commit")
        return True

    def print_ring_counts(self):
        """Step 3: the leader reports how many records each node holds."""
        trace_info(self.who, "--- records stored per node ---------------------")
        for i, tup in enumerate(self.ring):
            if i == self.id:
                count = len(self.table)
            else:
                count = "?"  # real once store is implemented
            trace_info(self.who, "    id={} {:<10} records={}".format(i, tup[0], count))
        trace_info(self.who, "-------------------------------------------------")

    # ------------------------------------------------- peer-to-peer handlers

    def handle_set_id(self, parts, src):
        my_id = int(parts[1])
        n = int(parts[2])
        tuples = [parse_tuple(t) for t in parts[3:]]
        self.state = IN_DHT
        self.adopt_ring(my_id, n, tuples)
        return [SUCCESS, "set-id", my_id]

    def handle_store(self, parts, src):
        """
        STUB -- next commit.

        Expected shape:
            target = int(parts[1]); pos = int(parts[2])
            record = protocol.parse_record(parts[3])
            if target == self.id:  self.table[pos] = record
            else:                  forward the same message to self.right

        Watch out: several thousand records go out back-to-back and UDP has no
        flow control, so the receiver's socket buffer can overflow and drop
        them silently. Verify the per-node counts match expected_counts; if
        they do not, ack each store or add a small delay between sends.
        """
        trace_info(self.who, "[STUB] store received but not handled yet")
        return [FAILURE, "store", "not implemented yet"]

    def dispatch_peer(self, parts, src):
        command = parts[0]
        if command == "set-id":
            return self.handle_set_id(parts, src)
        if command == "store":
            return self.handle_store(parts, src)
        return [FAILURE, command, "unknown peer command"]

    # ------------------------------------------------------------- main loop

    def handle_stdin(self, line):
        parts = line.split()
        if not parts:
            return True
        command, args = parts[0], parts[1:]

        if command in ("quit", "exit"):
            return False
        if command == "state":
            self.print_state()
        elif command == "register":
            self.cmd_register(args)
        elif command == "setup-dht":
            self.cmd_setup_dht(args)
        elif command in ("query-dht", "leave-dht", "join-dht",
                         "deregister", "teardown-dht"):
            print("{}: not implemented until the full project (10/18)".format(command))
        else:
            print("unknown command: " + command)
        return True

    def run(self):
        trace_info("peer", "manager at {}:{}. Type 'register ...' to begin.".format(*self.mgr))
        while True:
            # One thread watching stdin and the peer socket together. recvfrom
            # is blocking by default, so select tells us which source is ready
            # and we only read one that will not block.
            watch = [sys.stdin]
            if self.p_sock is not None:
                watch.append(self.p_sock)
            ready, _, _ = select.select(watch, [], [])

            for source in ready:
                if source is sys.stdin:
                    line = sys.stdin.readline()
                    if not line:  # EOF
                        return
                    if not self.handle_stdin(line.strip()):
                        return
                else:
                    data, src = self.p_sock.recvfrom(BUFSIZE)
                    parts = decode(data)
                    label = "{}:{}".format(*src)
                    trace_recv(self.who, label, parts)
                    reply = self.dispatch_peer(parts, src)
                    trace_sent(self.who, label, reply)
                    self.p_sock.sendto(encode(reply), src)


def main():
    if len(sys.argv) != 3:
        print("usage: python3 peer.py <manager-ipv4> <manager-port>", file=sys.stderr)
        sys.exit(1)
    try:
        mgr_port = int(sys.argv[2])
    except ValueError:
        print("manager port must be an integer", file=sys.stderr)
        sys.exit(1)

    try:
        Peer(sys.argv[1], mgr_port).run()
    except KeyboardInterrupt:
        print()


if __name__ == "__main__":
    main()
