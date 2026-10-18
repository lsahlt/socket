# CSE 434 Socket Project — Group 77

A complete distributed hash table over UDP sockets. A central manager tracks
membership; peers form a logical ring for construction and churn, and use a
randomized hot-potato protocol for queries.

#
for cooper
run one manager and multiple peer programs. The manager tracks registrations, DHT membership, and whether the DHT is being built or modified. The peers actually store and query the storm records.
Start the manager:

```bash
python3 manager.py <manager-port>
```
Start each peer:

```bash
python3 peer.py <manager-ip> <manager-port>
```
Then register each peer using its own IP and two unique ports:

```text
register <name> <peer-ip> <manager-port-for-peer> <peer-port>
```
Example:

```text
register alice 127.0.0.1 39501 39502
```
To build the DHT:

```text
setup-dht alice 3 1950
```
For the final demo, it must be:

```text
setup-dht alice 5 1996
```

The manager randomly selects the other DHT members. Use this on each peer to check its role:

```text
state
```
Only a Free peer can query. Querying takes two commands:

```text
query-dht <free-peer-name>
find-event <event-id>
```
Example:

```text
query-dht frank
find-event 5536849
```

`query-dht` asks the manager for a random starting node. `find-event` then travels between random unvisited DHT peers until it reaches the node responsible for that event ID.

To leave the DHT, run this from a DHT member:

```text
leave-dht <name>
```

The ring is deleted, renumbered, and rebuilt without that peer. The leaving peer becomes Free and can query afterward.

To join, run this from a Free peer:

```text
join-dht <name>
```

The DHT is rebuilt with that peer included. In our implementation, the joining peer becomes ID 0 and the new leader.

Only the current leader can delete the DHT:

```text
teardown-dht <leader-name>
```

After teardown, every peer becomes Free. Gracefully terminate each peer with:

```text
deregister <name>
```

Important notes:

- Use ports 39500–39999.
- Every IP/port pair must be unique.
- Use real host IP addresses when running on different ASU machines; don’t use `127.0.0.1` across machines.
- The selected `details-YYYY.csv` must be inside the `data/` directory on every machine that could become leader.
- `MESSAGES.md` explains the network-message formats but is not required to run the program.
- `state` is useful for determining the leader, DHT members, and Free peers.

## Team

- Rogelio Corrales (ASU ID: 1229167630)
- Cooper Lake (ASU ID: 1231074158)

## Requirements

- Python 3.8 or newer; standard library only
- Ports 39500–39999 (Group 77's assigned range)
- `data/details-YYYY.csv` on every host that may become a leader

The repository contains the 1950 milestone dataset and the 1996 full-demo
dataset. Copy the repository's `data/` directory to every host that may become
a leader. See `data/README.md` for provenance.

## Start the programs

```text
python3 manager.py <manager-port>
python3 peer.py <manager-ipv4> <manager-port>
```

Each peer must register before using another command:

```text
register <peer-name> <IPv4-address> <m-port> <p-port>
```

Use the IP address of the peer's host—not `127.0.0.1`—when processes run on
different machines. The manager port, every m-port, and every p-port must be
unique on a host and remain in the assigned range.

## Commands

| Command | Who may issue it | Result |
|---|---|---|
| `setup-dht <name> <n> <YYYY>` | Free peer | Builds a ring of at least three peers and distributes the year dataset. |
| `query-dht <name>` | Free peer | Asks the manager for a random starting peer. |
| `find-event <event-id>` | Same querying peer | Runs the hot-potato search and prints the record and id sequence. |
| `leave-dht <name>` | DHT member | Tears down, renumbers, and rebuilds without that peer. |
| `join-dht <name>` | Free peer | Joins as id 0/new leader and rebuilds the expanded DHT. |
| `teardown-dht <name>` | Current leader | Deletes the DHT and makes all members Free. |
| `deregister <name>` | Free peer | Removes registration and exits cleanly. |
| `state` | Any peer | Prints local state for debugging/demo purposes. |

`quit` exits locally without deregistering and is intended only for emergency
or debugging use.

## Full demo sequence

Start the manager and six peers on the required hosts, then register all six.
Assuming the names `alice` through `frank`, a typical command flow is:

```text
# On alice
setup-dht alice 5 1996

# On the one Free peer (suppose frank), repeat for each required event
query-dht frank
find-event 5536849
query-dht frank
find-event 2402920
query-dht frank
find-event 5539287
query-dht frank
find-event 55770111

# On a non-leader DHT member (suppose carol)
leave-dht carol
query-dht carol
find-event 5536849

# On the other Free peer (suppose frank)
join-dht frank

# Query from the remaining Free peer, carol
query-dht carol
find-event 2402920

# frank is the leader after joining
teardown-dht frank

# On every peer
deregister <that-peer-name>
```

The manager chooses setup members randomly, so use `state` or the manager's
trace to identify the Free peer and DHT members. After a leave, the leaver's
old right neighbor becomes leader. After a join, the joiner becomes leader.

## Design notes

- `pos = event_id mod s`, where `s` is the first prime greater than `2*l`.
- `owner = pos mod n`.
- Ring-management records move only to the right neighbor.
- A query visits randomly selected, previously unvisited peer ids until it
  reaches the computed owner.
- Hash slots use chaining, and duplicate UDP deliveries are stored
  idempotently.
- See [MESSAGES.md](MESSAGES.md) for every wire format and churn exchange.
