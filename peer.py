#!/usr/bin/env python3
"""Interactive UDP peer"""

import csv
import os
import random
import select
import socket
import sys

from protocol import (BUFSIZE, FAILURE, SUCCESS, Timeout, decode, encode,
                      first_prime_after, fmt_record, fmt_tuple, hash_record,
                      parse_record, parse_tuple, request, trace_info,
                      trace_recv, trace_sent)

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
UNREGISTERED = "Unregistered"
FREE = "Free"
LEADER = "Leader"
IN_DHT = "InDHT"

RECORD_FIELDS = (
    "event id", "state", "year", "month name", "event type", "cz type",
    "cz name", "injuries direct", "injuries indirect", "deaths direct",
    "deaths indirect", "damage property", "damage crops", "tor f scale",
)


class Peer:
    def __init__(self, mgr_ip, mgr_port):
        """create an unregistered peer and remember the manager's address"""
        self.mgr = (mgr_ip, mgr_port)
        self.name = None
        self.ip = None
        self.m_port = None
        self.p_port = None
        self.m_sock = None
        self.p_sock = None
        self.state = UNREGISTERED
        self.id = None
        self.n = None
        self.ring = []
        self.right = None
        self.year = None
        self.hash_size = None
        self.table = {}
        self._stored_records = set()
        self.record_count = 0
        self.forwarded = 0
        self.query_target = None

    @property
    def who(self):
        """Return  peers name or  generic label before registration"""
        return self.name or "peer"

    def label_of(self, peer_tuple):
        """Make a readable name/IP/port label for trace output."""
        return "{} {}:{}".format(peer_tuple[0], peer_tuple[1], peer_tuple[2])

    def own_tuple(self):
        """Return this peer's name, IP address, and peer-to-peer port."""
        return (self.name, self.ip, self.p_port)

    def manager_request(self, parts):
        """Send one request to the manager and report a timeout cleanly."""
        try:
            return request(self.m_sock, self.mgr, parts,
                           who=self.who, label="manager")
        except Timeout as exc:
            trace_info(self.who, "{} failed: {}".format(parts[0], exc))
            return None

    def require_own_name(self, args, usage):
        """Check that a command contains this peer's registered name."""
        if len(args) != 1:
            print("usage: " + usage)
            return False
        if args[0] != self.name:
            trace_info(self.who, "command name must match registered name {}".format(self.name))
            return False
        return True

    def print_state(self):
        """Print this peer's identity, ring information, and record counts."""
        trace_info(self.who, "--- local state ---------------------------------")
        trace_info(self.who, "    name={} state={}".format(self.name, self.state))
        trace_info(self.who, "    m-port={} p-port={}".format(self.m_port, self.p_port))
        trace_info(self.who, "    ring id={} n={} year={}".format(self.id, self.n, self.year))
        trace_info(self.who, "    right neighbour={}".format(
            self.label_of(self.right) if self.right else "-"))
        trace_info(self.who, "    records held={} in {} slots, table size s={}".format(
            self.record_count, len(self.table), self.hash_size))
        trace_info(self.who, "    store messages forwarded={}".format(self.forwarded))
        trace_info(self.who, "-------------------------------------------------")

    def clear_table(self):
        """Delete all locally stored records and reset traffic counters."""
        self.table = {}
        self._stored_records = set()
        self.record_count = 0
        self.forwarded = 0

    def forget_ring(self):
        """Clear the local table and forget all DHT membership information."""
        self.clear_table()
        self.id = self.n = None
        self.ring = []
        self.right = None
        self.year = self.hash_size = None
        self.query_target = None

    def store_locally(self, pos, record):
        """Store idempotently so a retransmitted UDP request cannot duplicate data."""
        key = (pos, tuple(record))
        if key in self._stored_records:
            return False
        self.table.setdefault(pos, []).append(record)
        self._stored_records.add(key)
        self.record_count += 1
        return True

    def adopt_ring(self, my_id, n, tuples, year, hash_size):
        """Install a new ring configuration and calculate the right neighbor."""
        if len(tuples) != n or not 0 <= my_id < n:
            raise ValueError("inconsistent ring configuration")
        if tuples[my_id][0] != self.name:
            raise ValueError("ring id does not identify this peer")
        self.id, self.n, self.ring = my_id, n, tuples
        self.year, self.hash_size = int(year), int(hash_size)
        self.right = tuples[(my_id + 1) % n]
        self.state = LEADER if my_id == 0 else IN_DHT
        trace_info(self.who, "ring id={} of n={}, right neighbour is {}".format(
            my_id, n, self.label_of(self.right)))

    # ---------- commands entered at stdin ----------

    def cmd_register(self, args):
        """Bind the two peer sockets and register this process with the manager."""
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
            if not 39500 <= port <= 39999:
                print("warning: port {} is outside group 77's range 39500-39999".format(port))
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

    def load_records(self, year):
        """Read and validate the 14-column CSV dataset for one year."""
        path = os.path.join(DATA_DIR, "details-{}.csv".format(year))
        if not os.path.isfile(path):
            trace_info(self.who, "dataset not found: " + path)
            return None
        records, skipped = [], 0
        with open(path, newline="", encoding="utf-8", errors="replace") as handle:
            reader = csv.reader(handle)
            if next(reader, None) is None:
                trace_info(self.who, "dataset is empty: " + path)
                return None
            for row in reader:
                if not row or not any(field.strip() for field in row):
                    continue
                if len(row) != 14:
                    skipped += 1
                    continue
                try:
                    int(row[0])
                except ValueError:
                    skipped += 1
                    continue
                records.append(row)
        if skipped:
            trace_info(self.who, "skipped {} malformed dataset rows".format(skipped))
        return records

    def prepare_dataset(self, year):
        """Load a dataset and calculate the required prime hash-table size."""
        records = self.load_records(year)
        if records is None:
            return None
        self.year = int(year)
        self.hash_size = first_prime_after(2 * len(records))
        trace_info(self.who, "dataset details-{}.csv: l={} records".format(year, len(records)))
        trace_info(self.who, "hash table size s = first prime > 2*{} = {}".format(
            len(records), self.hash_size))
        return records

    def cmd_setup_dht(self, args):
        """Ask the manager to create a DHT, then build and populate its ring."""
        if self.state == UNREGISTERED:
            print("register first")
            return
        if len(args) != 3:
            print("usage: setup-dht <peer-name> <n> <YYYY>")
            return
        name, n, year = args
        if name != self.name:
            trace_info(self.who, "command name must match registered name {}".format(self.name))
            return
        records = self.prepare_dataset(year)
        if records is None:
            return
        reply = self.manager_request(["setup-dht", name, n, year])
        if not reply:
            return
        if reply[0] != SUCCESS:
            trace_info(self.who, "setup-dht refused: " + (reply[2] if len(reply) > 2 else "?"))
            return
        ring_size = int(reply[2])
        tuples = [parse_tuple(text) for text in reply[3:]]
        self.adopt_ring(0, ring_size, tuples, int(year), self.hash_size)
        trace_info(self.who, "leader of a ring of {}: {}".format(
            ring_size, [peer[0] for peer in tuples]))
        if not self.assign_identifiers():
            trace_info(self.who, "ring construction failed, not sending dht-complete")
            return
        if not self.build_local_dhts(year, records):
            trace_info(self.who, "DHT population failed, not sending dht-complete")
            return
        self.print_ring_counts()
        reply = self.manager_request(["dht-complete", self.name])
        if reply and reply[0] == SUCCESS:
            trace_info(self.who, "DHT setup complete")

    def assign_identifiers(self):
        """Send each selected member its id and complete ring configuration."""
        for node_id in range(1, self.n):
            target = self.ring[node_id]
            msg = (["set-id", node_id, self.n, self.year, self.hash_size] +
                   [fmt_tuple(*peer) for peer in self.ring])
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

    def build_local_dhts(self, year, records=None):
        """Hash every dataset record and distribute it through the ring."""
        if records is None:
            records = self.prepare_dataset(year)
        if records is None:
            return False
        self.clear_table()
        self.expected_counts = {node_id: 0 for node_id in range(self.n)}
        for row in records:
            _pos, node_id = hash_record(row[0], self.hash_size, self.n)
            self.expected_counts[node_id] += 1
        self.actual_counts = {node_id: 0 for node_id in range(self.n)}
        sent = 0
        trace_info(self.who, "distributing {} records around the ring via {}".format(
            len(records), self.right[0]))
        for row in records:
            pos, node_id = hash_record(row[0], self.hash_size, self.n)
            if node_id == self.id:
                self.store_locally(pos, row)
                self.actual_counts[node_id] += 1
                continue
            message = ["store", node_id, pos, fmt_record(row)]
            try:
                reply = request(self.p_sock, (self.right[1], self.right[2]), message,
                                who=self.who, label=self.label_of(self.right),
                                timeout=2.0, quiet=sent >= 2)
            except Timeout as exc:
                trace_info(self.who, "store for id={} failed: {}".format(node_id, exc))
                return False
            if reply[0] != SUCCESS:
                trace_info(self.who, "store for id={} refused: {}".format(
                    node_id, reply[2] if len(reply) > 2 else "?"))
                return False
            self.actual_counts[node_id] += 1
            sent += 1
            if sent % 1000 == 0:
                trace_info(self.who, "  ... {} records sent around the ring".format(sent))
        trace_info(self.who, "distribution done: {} stored locally, {} sent".format(
            self.record_count, sent))
        return True

    def print_ring_counts(self):
        """Print the number of records assigned to every ring member."""
        trace_info(self.who, "--- records stored per node ---------------------")
        total = 0
        for node_id, peer in enumerate(self.ring):
            actual = self.actual_counts.get(node_id, 0)
            expected = self.expected_counts.get(node_id, 0)
            suffix = "" if actual == expected else " <-- expected {}".format(expected)
            trace_info(self.who, "    id={} {:<10} records={}{}".format(
                node_id, peer[0], actual, suffix))
            total += actual
        trace_info(self.who, "    total={}".format(total))
        trace_info(self.who, "-------------------------------------------------")

    def cmd_query_dht(self, args):
        """Ask the manager for the starting peer of the next event query."""
        if not self.require_own_name(args, "query-dht <peer-name>"):
            return
        reply = self.manager_request(["query-dht", self.name])
        if not reply:
            return
        if reply[0] != SUCCESS:
            trace_info(self.who, "query-dht refused: " + (reply[2] if len(reply) > 2 else "?"))
            return
        self.query_target = parse_tuple(reply[2])
        trace_info(self.who, "query accepted; use 'find-event <event-id>' (start peer {})".format(
            self.query_target[0]))

    def cmd_find_event(self, args):
        """Search for one event and print either its fields or not-found text."""
        if len(args) != 1:
            print("usage: find-event <event-id>")
            return
        if self.query_target is None:
            trace_info(self.who, "issue query-dht first")
            return
        try:
            int(args[0])
        except ValueError:
            trace_info(self.who, "event-id must be an integer")
            return
        msg = ["find-event", args[0], fmt_tuple(*self.own_tuple()), "-", "-"]
        try:
            reply = request(self.p_sock, (self.query_target[1], self.query_target[2]), msg,
                            who=self.who, label=self.label_of(self.query_target))
        except Timeout as exc:
            trace_info(self.who, "find-event failed: " + str(exc))
            return
        sequence = reply[3] if len(reply) > 3 else "?"
        if reply[0] != SUCCESS:
            print("Storm event {} not found in the DHT.".format(args[0]))
            trace_info(self.who, "id-seq=" + sequence)
            return
        try:
            record = parse_record(reply[4])
        except IndexError:
            trace_info(self.who, "malformed find-event response")
            return
        print("Storm event {}:".format(args[0]))
        for label, value in zip(RECORD_FIELDS, record):
            print("  {:<20} {}".format(label + ":", value))
        print("  {:<20} {}".format("id-seq:", sequence))

    def teardown_ring(self):
        """Circulate teardown through the other members, then clear locally."""
        if self.n is None:
            return False
        if self.n > 1:
            try:
                reply = request(self.p_sock, (self.right[1], self.right[2]),
                                ["teardown", self.n - 1], who=self.who,
                                label=self.label_of(self.right))
            except Timeout as exc:
                trace_info(self.who, "teardown failed: " + str(exc))
                return False
            if reply[0] != SUCCESS:
                trace_info(self.who, "teardown refused: " + (reply[2] if len(reply) > 2 else "?"))
                return False
        self.clear_table()
        return True

    def cmd_teardown_dht(self, args):
        """As leader, delete the DHT and report completion to the manager."""
        if not self.require_own_name(args, "teardown-dht <peer-name>"):
            return
        reply = self.manager_request(["teardown-dht", self.name])
        if not reply or reply[0] != SUCCESS:
            if reply:
                trace_info(self.who, "teardown-dht refused: " + (reply[2] if len(reply) > 2 else "?"))
            return
        if not self.teardown_ring():
            return
        reply = self.manager_request(["teardown-complete", self.name])
        if reply and reply[0] == SUCCESS:
            self.state = FREE
            self.forget_ring()
            trace_info(self.who, "DHT teardown complete; state=Free")

    def cmd_leave_dht(self, args):
        """Leave the ring, renumber survivors, and ask the new leader to rebuild."""
        if not self.require_own_name(args, "leave-dht <peer-name>"):
            return
        if self.id is None or self.state not in (LEADER, IN_DHT):
            trace_info(self.who, "this peer is not in the DHT")
            return
        reply = self.manager_request(["leave-dht", self.name])
        if not reply or reply[0] != SUCCESS:
            if reply:
                trace_info(self.who, "leave-dht refused: " + (reply[2] if len(reply) > 2 else "?"))
            return
        old_ring = list(self.ring)
        old_id, old_n = self.id, self.n
        year, hash_size = self.year, self.hash_size
        if not self.teardown_ring():
            return
        # Rotate so the leaver's old right neighbour becomes id 0, exactly as
        # section 1.2.3 requires.
        new_ring = old_ring[old_id + 1:] + old_ring[:old_id]
        new_leader = new_ring[0]
        reset = (["reset-id", 0, old_n - 1, year, hash_size, old_n - 1] +
                 [fmt_tuple(*peer) for peer in new_ring])
        try:
            result = request(self.p_sock, (new_leader[1], new_leader[2]), reset,
                             who=self.who, label=self.label_of(new_leader))
        except Timeout as exc:
            trace_info(self.who, "reset-id failed: " + str(exc))
            return
        if result[0] != SUCCESS:
            trace_info(self.who, "reset-id was refused")
            return
        try:
            # Rebuilding a large year can legitimately take several minutes.
            # Use one long attempt: retrying the whole rebuild would duplicate
            # work while the first request may still be running.
            result = request(self.p_sock, (new_leader[1], new_leader[2]),
                             ["rebuild-dht", year], who=self.who,
                             label=self.label_of(new_leader), timeout=600.0,
                             retries=1)
        except Timeout as exc:
            trace_info(self.who, "rebuild-dht failed: " + str(exc))
            return
        if result[0] != SUCCESS:
            trace_info(self.who, "rebuild-dht was refused")
            return
        reply = self.manager_request(["dht-rebuilt", self.name, new_leader[0]])
        if reply and reply[0] == SUCCESS:
            self.state = FREE
            self.forget_ring()
            trace_info(self.who, "left DHT; state=Free; new leader={}".format(new_leader[0]))

    def cmd_join_dht(self, args):
        """Join as the new leader and rebuild an expanded DHT ring."""
        if not self.require_own_name(args, "join-dht <peer-name>"):
            return
        reply = self.manager_request(["join-dht", self.name])
        if not reply or reply[0] != SUCCESS:
            if reply:
                trace_info(self.who, "join-dht refused: " + (reply[2] if len(reply) > 2 else "?"))
            return
        year = int(reply[2])
        old_ring = [parse_tuple(text) for text in reply[3:]]
        records = self.prepare_dataset(year)
        if records is None:
            return
        new_ring = [self.own_tuple()] + old_ring
        old_leader = old_ring[0]
        prepare = (["prepare-join", year, self.hash_size] +
                   [fmt_tuple(*peer) for peer in new_ring])
        try:
            result = request(self.p_sock, (old_leader[1], old_leader[2]), prepare,
                             who=self.who, label=self.label_of(old_leader), timeout=30.0)
        except Timeout as exc:
            trace_info(self.who, "join preparation failed: " + str(exc))
            return
        if result[0] != SUCCESS:
            trace_info(self.who, "join preparation was refused")
            return
        self.adopt_ring(0, len(new_ring), new_ring, year, self.hash_size)
        if not self.build_local_dhts(year, records):
            return
        self.print_ring_counts()
        reply = self.manager_request(["dht-rebuilt", self.name, self.name])
        if reply and reply[0] == SUCCESS:
            trace_info(self.who, "joined DHT as new leader")

    def cmd_deregister(self, args):
        """Deregister a Free peer, close its sockets, and request program exit."""
        if not self.require_own_name(args, "deregister <peer-name>"):
            return True
        reply = self.manager_request(["deregister", self.name])
        if not reply or reply[0] != SUCCESS:
            if reply:
                trace_info(self.who, "deregister refused: " + (reply[2] if len(reply) > 2 else "?"))
            return True
        trace_info(self.who, "deregistered; exiting")
        self.m_sock.close()
        self.p_sock.close()
        return False

    # ---------- peer-to-peer handlers ----------

    def handle_set_id(self, parts, _src):
        """Accept the initial id, metadata, and ring sent by the setup leader."""
        if len(parts) < 7:
            return [FAILURE, "set-id", "malformed set-id message"]
        my_id, n = int(parts[1]), int(parts[2])
        year, hash_size = int(parts[3]), int(parts[4])
        tuples = [parse_tuple(text) for text in parts[5:]]
        self.clear_table()
        self.adopt_ring(my_id, n, tuples, year, hash_size)
        return [SUCCESS, "set-id", my_id]

    def handle_store(self, parts, _src):
        """Store a record at its owner or forward it to the right neighbor."""
        if self.id is None:
            return [FAILURE, "store", "peer is not part of a DHT"]
        try:
            if len(parts) != 4:
                raise ValueError("wrong field count")
            target, pos = int(parts[1]), int(parts[2])
            record = parse_record(parts[3])
            if len(record) != 14 or not 0 <= target < self.n:
                raise ValueError("invalid record or target")
        except (ValueError, IndexError):
            return [FAILURE, "store", "malformed store message"]
        if target == self.id:
            inserted = self.store_locally(pos, record)
            if inserted and (self.record_count <= 2 or self.record_count % 1000 == 0):
                trace_info(self.who, "stored event {} at pos {} ({} records held)".format(
                    record[0], pos, self.record_count))
            return [SUCCESS, "store", target]
        self.forwarded += 1
        if self.forwarded <= 2 or self.forwarded % 1000 == 0:
            trace_info(self.who, "forwarded {} store messages; latest target id={}".format(
                self.forwarded, target))
        try:
            return request(self.p_sock, (self.right[1], self.right[2]), parts,
                           who=self.who, label=self.label_of(self.right),
                           timeout=2.0, quiet=True)
        except Timeout:
            return [FAILURE, "store", "right neighbour did not respond"]

    def handle_find_event(self, parts, _src):
        """Process one hot-potato query hop and return the eventual result."""
        if self.id is None or self.hash_size is None:
            return [FAILURE, "find-event", parts[1] if len(parts) > 1 else "?", "-"]
        try:
            event_id = int(parts[1])
            parse_tuple(parts[2])  # requester tuple is carried as required by the spec
            sequence = [] if parts[3] == "-" else [int(x) for x in parts[3].split(",")]
            remaining = (set(range(self.n)) if parts[4] == "-" else
                         {int(x) for x in parts[4].split(",") if x})
        except (ValueError, IndexError):
            return [FAILURE, "find-event", "?", "-"]
        if self.id not in sequence:
            sequence.append(self.id)
        remaining.discard(self.id)
        pos, owner = hash_record(event_id, self.hash_size, self.n)
        seq_text = ",".join(str(node_id) for node_id in sequence)
        if self.id == owner:
            for record in self.table.get(pos, []):
                if record and record[0] == str(event_id):
                    return [SUCCESS, "find-event", event_id, seq_text, fmt_record(record)]
            return [FAILURE, "find-event", event_id, seq_text]
        if not remaining:
            return [FAILURE, "find-event", event_id, seq_text]
        next_id = random.choice(sorted(remaining))
        target = self.ring[next_id]
        message = ["find-event", event_id, parts[2], seq_text,
                   ",".join(str(node_id) for node_id in sorted(remaining))]
        trace_info(self.who, "hot potato event {}: id {} -> id {}".format(
            event_id, self.id, next_id))
        try:
            return request(self.p_sock, (target[1], target[2]), message,
                           who=self.who, label=self.label_of(target))
        except Timeout:
            return [FAILURE, "find-event", event_id, seq_text]

    def handle_teardown(self, parts, _src):
        """Delete this local table and continue teardown around the ring."""
        try:
            remaining = int(parts[1])
        except (ValueError, IndexError):
            return [FAILURE, "teardown", "malformed teardown message"]
        if self.id is None or remaining < 1:
            return [FAILURE, "teardown", "peer is not in a DHT"]
        next_peer = self.right
        self.clear_table()
        self.state = FREE
        trace_info(self.who, "local hash table deleted")
        if remaining == 1:
            return [SUCCESS, "teardown"]
        try:
            return request(self.p_sock, (next_peer[1], next_peer[2]),
                           ["teardown", remaining - 1], who=self.who,
                           label=self.label_of(next_peer))
        except Timeout:
            return [FAILURE, "teardown", "right neighbour did not respond"]

    def handle_reset_id(self, parts, _src):
        """Adopt a rebuilt ring id and pass the next id to the right."""
        try:
            my_id, n = int(parts[1]), int(parts[2])
            year, hash_size = int(parts[3]), int(parts[4])
            remaining = int(parts[5])
            tuples = [parse_tuple(text) for text in parts[6:]]
            self.adopt_ring(my_id, n, tuples, year, hash_size)
        except (ValueError, IndexError) as exc:
            return [FAILURE, "reset-id", "malformed reset-id: " + str(exc)]
        if remaining == 1:
            return [SUCCESS, "reset-id"]
        next_peer = self.right
        forwarded = (["reset-id", my_id + 1, n, year, hash_size, remaining - 1] +
                     [fmt_tuple(*peer) for peer in tuples])
        try:
            return request(self.p_sock, (next_peer[1], next_peer[2]), forwarded,
                           who=self.who, label=self.label_of(next_peer))
        except Timeout:
            return [FAILURE, "reset-id", "right neighbour did not respond"]

    def handle_rebuild_dht(self, parts, _src):
        """As the new leader, reload the year and repopulate the DHT."""
        if self.id != 0 or len(parts) != 2:
            return [FAILURE, "rebuild-dht", "receiver is not the new leader"]
        if not self.build_local_dhts(parts[1]):
            return [FAILURE, "rebuild-dht", "could not rebuild DHT"]
        self.print_ring_counts()
        return [SUCCESS, "rebuild-dht", self.name]

    def handle_prepare_join(self, parts, _src):
        """Tear down and renumber the old ring before a new leader joins."""
        try:
            year, hash_size = int(parts[1]), int(parts[2])
            new_ring = [parse_tuple(text) for text in parts[3:]]
        except (ValueError, IndexError):
            return [FAILURE, "prepare-join", "malformed prepare-join message"]
        if self.id != 0 or self.state != LEADER or len(new_ring) != self.n + 1:
            return [FAILURE, "prepare-join", "receiver is not current leader"]
        old_n = self.n
        if not self.teardown_ring():
            return [FAILURE, "prepare-join", "old DHT teardown failed"]
        #the joiner is id 0; the old leader and the remaining old ring become
        # ids 1..n, Do not forward reset id to the joiner because it is
        # waiting for this response


        #####TODO
        self.adopt_ring(1, len(new_ring), new_ring, year, hash_size)
        if old_n > 1:
            target = self.right
            reset = (["reset-id", 2, len(new_ring), year, hash_size, old_n - 1] +
                     [fmt_tuple(*peer) for peer in new_ring])
            try:
                reply = request(self.p_sock, (target[1], target[2]), reset,
                                who=self.who, label=self.label_of(target))
            except Timeout:
                return [FAILURE, "prepare-join", "renumbering timed out"]
            if reply[0] != SUCCESS:
                return [FAILURE, "prepare-join", "renumbering failed"]
        return [SUCCESS, "prepare-join"]

    def dispatch_peer(self, parts, src):
        """rfoute one peer to peer message to the matching handler"""
        if not parts:
            return [FAILURE, "?", "empty peer message"]
        handlers = {
            "set-id": self.handle_set_id,
            "store": self.handle_store,
            "find-event": self.handle_find_event,
            "teardown": self.handle_teardown,
            "reset-id": self.handle_reset_id,
            "rebuild-dht": self.handle_rebuild_dht,
            "prepare-join": self.handle_prepare_join,
        }
        handler = handlers.get(parts[0])
        return handler(parts, src) if handler else [FAILURE, parts[0], "unknown peer command"]

    # ---------- event loop ----------

    def handle_stdin(self, line):
        """parse and execute one command typed by the user"""
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
        elif self.state == UNREGISTERED:
            print("register first")
        elif command == "setup-dht":
            self.cmd_setup_dht(args)
        elif command == "query-dht":
            self.cmd_query_dht(args)
        elif command == "find-event":
            self.cmd_find_event(args)
        elif command == "leave-dht":
            self.cmd_leave_dht(args)
        elif command == "join-dht":
            self.cmd_join_dht(args)
        elif command == "teardown-dht":
            self.cmd_teardown_dht(args)
        elif command == "deregister":
            return self.cmd_deregister(args)
        else:
            print("unknown command: " + command)
        return True

    def run(self):
        """wait for typed commands and peer datagrams in one event loop"""
        trace_info("peer", "manager at {}:{}. Type 'register ...' to begin.".format(*self.mgr))
        while True:
            watch = [sys.stdin]
            if self.p_sock is not None:
                watch.append(self.p_sock)
            ready, _, _ = select.select(watch, [], [])
            for source in ready:
                if source is sys.stdin:
                    line = sys.stdin.readline()
                    if not line or not self.handle_stdin(line.strip()):
                        return
                else:
                    data, src = self.p_sock.recvfrom(BUFSIZE)
                    label = "{}:{}".format(*src)
                    try:
                        parts = decode(data)
                        # Full-project years contain tens of thousands of
                        # records. Show representative store datagrams and
                        # periodic summaries without flooding the terminal.
                        store_number = self.record_count + self.forwarded + 1
                        loud = (parts[0] != "store" or store_number <= 2 or
                                store_number % 1000 == 0)
                        if loud:
                            trace_recv(self.who, label, parts)
                        reply = self.dispatch_peer(parts, src)
                    except (UnicodeDecodeError, ValueError, IndexError) as exc:
                        reply = [FAILURE, "?", "malformed message: " + str(exc)]
                        loud = True
                    if loud:
                        trace_sent(self.who, label, reply)
                    self.p_sock.sendto(encode(reply), src)


def main():
    """read the manager address from the command line and start peer"""
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
