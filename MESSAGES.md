# Wire protocol — Group 77

Every message is one UTF-8 UDP datagram. Top-level fields use `|`, peer
tuples use `,`, and the 14 fields of a storm record use ASCII unit separator
(`\x1f`). Replies begin with `SUCCESS` or `FAILURE` and echo the command name.

## Peer ↔ manager

| Command | Request | Successful response |
|---|---|---|
| register | `register|name|ip|m-port|p-port` | `SUCCESS|register` |
| setup-dht | `setup-dht|name|n|YYYY` | `SUCCESS|setup-dht|n|tuple0|...|tupleN-1` |
| dht-complete | `dht-complete|name` | `SUCCESS|dht-complete` |
| query-dht | `query-dht|name` | `SUCCESS|query-dht|start-tuple` |
| leave-dht | `leave-dht|name` | `SUCCESS|leave-dht` |
| join-dht | `join-dht|name` | `SUCCESS|join-dht|YYYY|old-tuple0|...` |
| dht-rebuilt | `dht-rebuilt|name|new-leader` | `SUCCESS|dht-rebuilt` |
| teardown-dht | `teardown-dht|name` | `SUCCESS|teardown-dht` |
| teardown-complete | `teardown-complete|name` | `SUCCESS|teardown-complete` |
| deregister | `deregister|name` | `SUCCESS|deregister` |

The manager enters a busy state after `setup-dht`, `leave-dht`, `join-dht`,
or `teardown-dht`. Until the matching completion message arrives, every other
manager request receives `FAILURE`.

## Peer ↔ peer

| Command | Format and purpose |
|---|---|
| set-id | `set-id|id|n|YYYY|s|tuple0|...` assigns initial ring state. |
| store | `store|target-id|pos|record` travels right around the ring. |
| find-event | `find-event|event-id|requester-tuple|id-seq|remaining-ids` follows randomly selected, unvisited peers. |
| teardown | `teardown|remaining` deletes each local table while moving right. |
| reset-id | `reset-id|id|n|YYYY|s|remaining|tuple0|...` renumbers a rebuilt ring. |
| rebuild-dht | `rebuild-dht|YYYY` tells the new leader to reload and distribute the dataset. |
| prepare-join | `prepare-join|YYYY|s|new-tuple0|...` makes the old leader tear down and renumber the existing members for a joining peer. |

`s` is the first prime greater than twice the number of records. A peer tuple
is `name,ip,p-port`. A record is
`f1\x1ff2\x1f...\x1ff14`.

## Churn protocols

### Leave

1. The leaver receives manager approval and circulates `teardown` through all
   other members.
2. Its old right neighbor becomes id 0. `reset-id` travels through the
   remaining ring, incrementing identifiers.
3. The leaver sends `rebuild-dht` to that new leader.
4. After the rebuild acknowledgment, it sends `dht-rebuilt` to the manager
   and becomes Free.

### Join

1. The manager returns the current ring to the Free joining peer.
2. The joiner becomes id 0 and sends `prepare-join` to the old leader.
3. The old leader tears down the old table and propagates new ids through the
   old members without forwarding back to the synchronously waiting joiner.
4. The joiner loads the dataset, rebuilds the expanded ring, and sends
   `dht-rebuilt` naming itself as the new leader.

## Reliability

Requests normally wait five seconds and retry up to three times. Store
requests use a two-second timeout. The long-running leave rebuild uses one
ten-minute attempt so a still-running rebuild is never started twice. Local
storage is idempotent: an identical retransmitted record is not inserted a
second time.
