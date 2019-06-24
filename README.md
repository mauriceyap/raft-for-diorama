# Raft for Diorama

This is a Python 3 implementation of the Raft consensus algorithm for the *Diorama* distributed algorithm simulator.
Raft achieves consensus by electing a leader from among the nodes in a Raft cluster, and then replicating a log of data
(for example, transactions or commands) across all these nodes. Raft was created by Diego Ongaro and John Ousterhout and
is [presented in their paper](https://raft.github.io/raft.pdf).

This repository contains two node program code files:
- `raft_node.py` is for a node in running the Raft algorithm in a cluster.
- `client.py` is for an example client interacting with a Raft cluster.

On the top line of `raft_node.py`, you can set `PRINT_LOGS` to false to not see the committed and pending logs outputted
periodically.

The main function for both these node programs are `main`.

## Network topology requirements
All nodes in a Raft cluster must be connected to each and every other (a fully-connected topology). Nodes should **not**
be self-connected.

All nodes in a Raft cluster must have a node ID (nid) beginning with `raft`, for example `raft0` or
`raft_cluster_node_12`.

All clients must be connected to every node in the Raft cluster, since any one of them could be the leader at any one
time. As should be obvious, client nodes IDs should not begin with `raft`. 