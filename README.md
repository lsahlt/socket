# CSE 434 Socket Project — Group 77

Distributed hash table over UDP sockets. Storm event records are spread across
a logical ring of peers, coordinated by a central manager.

## Team

- Rogelio Corrales
- Cooper _____

## Ports

Group 77 is assigned **38500–38999**. All sockets must bind inside this range.


| manager | 38500 
| peer 1 | 38501 | 38502 |
| peer 2 | 38503 | 38504 |
| peer 3 | 38505 | 38506 |

## Requirements

Python 3.8+, standard library only. Dataset at `data/details-1950.csv`.

## Running

```
python3 manager.py <manager-port>
python3 peer.py <manager-ipv4> <manager-port>
```

Peers read commands from stdin:

```
register <peer-name> <IPv4-address> <m-port> <p-port>
setup-dht <peer-name> <n> <YYYY>
```

Example with the manager on host A at 10.0.0.1:

```
python3 manager.py 38500                  # host A
python3 peer.py 10.0.0.1 38500            # host A
  register alice 10.0.0.1 38501 38502
python3 peer.py 10.0.0.1 38500            # host B
  register bob 10.0.0.2 38503 38504
python3 peer.py 10.0.0.1 38500            # host B
  register carol 10.0.0.2 38505 38506
  
# then at alice:
setup-dht alice 3 1950
```

## Milestone scope (due 09/27/2026)

Implemented: `register`, `setup-dht`, `dht-complete`, plus peer-to-peer
`set-id` and `store`.

Everything else (`query-dht`, `leave-dht`, `join-dht`, `teardown-dht`,
`deregister`) is deferred to the full project on 10/18/2026. Graceful
termination is not implemented yet.

## How setup-dht works

1. Manager marks the caller `Leader`, picks `n-1` random `Free` peers, and
   returns all `n` tuples of `(name, IP, p-port)`, leader first.
2. Leader takes id 0 and sends `set-id` to each other peer with its id, the
   ring size, and the tuple table. Each peer stores peer `(i+1) mod n` as its
   right neighbour.
3. Leader reads the CSV, counts `l` records, sets hash table size `s` to the
   first prime greater than `2*l`, then for each record computes
   `pos = event_id mod s` and `id = pos mod n`. Records for other nodes are
   sent hop-by-hop around the ring via `store` — never directly to the target.
4. Leader prints per-node record counts and sends `dht-complete`.

## Links

- Design document: `docs/`
- Video demo: `TODO`
