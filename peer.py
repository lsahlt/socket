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
    store       working -- records distributed hop-by-hop around the ring
"""

import csv
import os
import select
import socket
import sys

from protocol import (BUFSIZE, FAILURE, SUCCESS, Timeout, decode, encode,
                      first_prime_after, fmt_record, fmt_tuple, hash_record,
                      parse_record, parse_tuple, request, trace_info,
                      trace_recv, trace_sent)

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

        # local hash table: pos -> list of records that hashed to that slot.
        # Chaining, because pos = event_id mod s is not injective -- with 223
        # records in 449 slots collisions are likely, and overwriting would
        # silently lose data. The spec's query wording ("examines position pos
        # to see if it holds the record with the given event id") assumes a
        # slot may hold something other than the record you want, so a chain
        # is also what find-event will need for the full project.
        self.table = {}
        self.record_count = 0   # records stored here, counting chained ones
        self.forwarded = 0      # store messages passed along the ring
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
        trace_info(self.who, "    records held={} in {} slots, table size s={}".format(
            self.record_count, len(self.table), self.hash_size))
        trace_info(self.who, "    store messages forwarded={}".format(self.forwarded))
        trace_info(self.who, "-------------------------------------------------")

    def store_locally(self, pos, record):
        """Append to the chain at slot pos."""
        self.table.setdefault(pos, []).append(record)
        self.record_count += 1

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

        The leader reads the dataset, sizes the hash table, then places each
        record. Records belonging to the leader go straight into its own
        table; the rest are handed to the right neighbour with a store message
        and travel hop-by-hop around the ring to their owner. The leader never
        sends a record directly to its target node -- the spec requires ring
        topology to be used for all management traffic.
        """
        records = self.load_records(year)
        if records is None:
            return False

        length = len(records)                           # l
        self.hash_size = first_prime_after(2 * length)  # s

        trace_info(self.who, "dataset details-{}.csv: l={} records".format(year, length))
        trace_info(self.who, "hash table size s = first prime > 2*{} = {}".format(
            length, self.hash_size))

        # Dry run of the two hash functions before anything goes over the
        # wire. The leader computes id for every record, so it knows the
        # correct per-node totals without having to ask the other nodes; the
        # actual counts are checked against these at the end.
        self.expected_counts = {i: 0 for i in range(self.n)}
        for row in records:
            _pos, node_id = hash_record(row[0], self.hash_size, self.n)
            self.expected_counts[node_id] += 1
        trace_info(self.who, "expected distribution: " + ", ".join(
            "id={}:{}".format(i, self.expected_counts[i]) for i in range(self.n)))

        # Distribute. One record is in flight at a time: the leader waits for
        # the end-to-end acknowledgement before sending the next. That is
        # stop-and-wait flow control, and it is deliberate. UDP has no flow
        # control of its own, so firing hundreds of datagrams back-to-back
        # overruns the receiver's socket buffer and records vanish with no
        # error anywhere. Waiting for each ack also means only one datagram
        # can be outstanding on a peer's p-port, so a reply can never be
        # mistaken for an unrelated message. At a few hundred records on a LAN
        # this costs well under a second.
        right_addr = (self.right[1], self.right[2])
        self.actual_counts = {i: 0 for i in range(self.n)}
        sent = 0

        trace_info(self.who, "distributing {} records around the ring via {}".format(
            length, self.right[0]))

        for row in records:
            pos, node_id = hash_record(row[0], self.hash_size, self.n)

            if node_id == self.id:
                self.store_locally(pos, row)
                self.actual_counts[node_id] += 1
                continue

            message = ["store", node_id, pos, fmt_record(row)]
            try:
                # The first few are traced in full so the message format is
                # visible in the demo; the rest are summarised.
                loud = sent < 2
                reply = request(self.p_sock, right_addr, message,
                                who=self.who, label=self.label_of(self.right),
                                timeout=2.0, quiet=not loud)
            except Timeout as exc:
                trace_info(self.who, "store for id={} failed: {}".format(node_id, exc))
                return False

            if reply[0] != SUCCESS:
                trace_info(self.who, "store for id={} refused: {}".format(
                    node_id, reply[2] if len(reply) > 2 else "?"))
                return False

            self.actual_counts[node_id] += 1
            sent += 1
            if sent % 50 == 0:
                trace_info(self.who, "  ... {} records sent around the ring".format(sent))

        trace_info(self.who, "distribution done: {} stored locally, {} sent".format(
            self.record_count, sent))
        return True

    def print_ring_counts(self):
        """
        Step 3: the leader reports how many records each node holds.

        The counts are the leader's own tally, since it computed the owning id
        for every record as it distributed them. They are cross-checked
        against the dry run: a shortfall means a store was lost on the wire.
        """
        trace_info(self.who, "--- records stored per node ---------------------")
        total = 0
        mismatch = False
        for i, tup in enumerate(self.ring):
            actual = self.actual_counts.get(i, 0)
            expected = self.expected_counts.get(i, 0)
            flag = ""
            if actual != expected:
                flag = "   <-- MISMATCH, expected {}".format(expected)
                mismatch = True
            trace_info(self.who, "    id={} {:<10} records={}{}".format(
                i, tup[0], actual, flag))
            total += actual
        trace_info(self.who, "    total={} (l={})".format(total, sum(
            self.expected_counts.values())))
        if mismatch:
            trace_info(self.who, "    WARNING: records were lost in transit")
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
        Receive a record travelling around the ring.

        If we are the target, store it and acknowledge. Otherwise pass the
        message unchanged to our right neighbour and wait for its answer
        before acknowledging, so the leader's acknowledgement is end-to-end:
        by the time it returns, the record really is in the target's table.
        """
        if self.id is None:
            return [FAILURE, "store", "peer is not part of a DHT"]
        try:
            target = int(parts[1])
            pos = int(parts[2])
            record = parse_record(parts[3])
        except (IndexError, ValueError):
            return [FAILURE, "store", "malformed store message"]

        if target == self.id:
            self.store_locally(pos, record)
            if self.record_count <= 2 or self.record_count % 25 == 0:
                trace_info(self.who, "stored event {} at pos {} ({} records held)".format(
                    record[0], pos, self.record_count))
            return [SUCCESS, "store", target]

        # Not ours: keep it moving around the ring.
        right_addr = (self.right[1], self.right[2])
        self.forwarded += 1
        if self.forwarded <= 2 or self.forwarded % 25 == 0:
            trace_info(self.who, "forwarding event {} for id={} to {} ({} forwarded)".format(
                record[0], target, self.right[0], self.forwarded))
        try:
            return request(self.p_sock, right_addr, parts,
                           who=self.who, label=self.label_of(self.right),
                           timeout=2.0, quiet=True)
        except Timeout:
            return [FAILURE, "store", "right neighbour did not respond"]

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
