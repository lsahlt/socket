# Wire format — Group 77

All messages are UTF-8 strings sent as single UDP datagrams. Fields are joined
with `|`. A 3-tuple is `name,ip,p-port` (commas inside the tuple, never `|`).
Storm record fields are joined with `\x1f` (ASCII unit separator) because the
NWS data contains commas inside quoted fields.

The first field is always the command name, or `SUCCESS` / `FAILURE` for a
reply. Every reply echoes the command it answers, so a peer can tell which
request a late datagram belongs to.

## Peer to manager

| Message | Format |
|---|---|
| register | `register\|<name>\|<ip>\|<m-port>\|<p-port>` |
| setup-dht | `setup-dht\|<name>\|<n>\|<YYYY>` |
| dht-complete | `dht-complete\|<name>` |

Sent from the peer's **m-port** to the manager's port. Strictly
request/response: the peer blocks until it gets a reply before issuing another
command.

## Manager to peer

| Message | Format |
|---|---|
| ok | `SUCCESS\|<command>` |
| error | `FAILURE\|<command>\|<reason>` |
| setup-dht ok | `SUCCESS\|setup-dht\|<n>\|<tuple0>\|<tuple1>\|...\|<tuple n-1>` |

`<tuple0>` is always the leader. The reason string on a FAILURE is for humans
and the trace log; peers do not parse it.

## Peer to peer

Sent from **p-port** to **p-port**.

| Message | Format |
|---|---|
| set-id | `set-id\|<id>\|<n>\|<tuple0>\|...\|<tuple n-1>` |
| set-id ack | `SUCCESS\|set-id\|<id>` |
| store | `store\|<target-id>\|<pos>\|<f1>\x1f<f2>\x1f...\x1f<f14>` |
| store ack | `SUCCESS\|store\|<target-id>` |

`store` carries `target-id` so each hop can decide "mine, or forward right?"
without recomputing the hash. It is forwarded hop-by-hop around the ring and
never sent directly to the target.

## Reliability

UDP may drop datagrams. Every request blocks on a 5 second timeout and is
retried up to 3 times before the peer reports failure. Requests are idempotent
at the application level, so a duplicate caused by a retry is harmless.

## Example exchange

```
alice -> manager   register|alice|10.0.0.1|38501|38502
manager -> alice   SUCCESS|register

alice -> manager   setup-dht|alice|3|1950
manager -> alice   SUCCESS|setup-dht|3|alice,10.0.0.1,38502|bob,10.0.0.2,38504|carol,10.0.0.2,38506

alice -> bob       set-id|1|3|alice,10.0.0.1,38502|bob,10.0.0.2,38504|carol,10.0.0.2,38506
bob -> alice       SUCCESS|set-id|1
alice -> carol     set-id|2|3|...
carol -> alice     SUCCESS|set-id|2

alice -> bob       store|1|1487|383097\x1fGEORGIA\x1f1950\x1f...
bob -> alice       SUCCESS|store|1

alice -> manager   dht-complete|alice
manager -> alice   SUCCESS|dht-complete
```
