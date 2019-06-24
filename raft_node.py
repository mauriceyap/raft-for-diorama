PRINT_LOGS: bool = True

import json
from random import random
from typing import Any, Callable, List, Dict
from threading import Timer

dummy_timer: Timer = Timer(123, lambda: None)
ELECTION_TIMEOUT_MIN: float = 1
ELECTION_TIMEOUT_MAX: float = 1.6
HEARTBEAT_INTERVAL: float = 0.7
RequestVote: str = 'RequestVote'
RespondVote: str = 'RespondVote'
AppendEntries: str = 'AppendEntries'
RespondAppendEntries: str = 'RespondAppendEntries'
RequestAddEntry: str = 'RequestAddEntry'
CommitAddEntry: str = 'CommitAddEntry'

FOLLOWER: str = 'FOLLOWER'
CANDIDATE: str = 'CANDIDATE'
LEADER: str = 'LEADER'


send: Callable[[str, Any, str], None] = lambda message_type, payload, recipient_nid: None

raft_peers: List[str] = []
diorama_nid: str = ''

state: str = FOLLOWER
current_term: int = 1
voted_for: str = None
election_timer: Timer = dummy_timer
heartbeat_timer: Timer = dummy_timer
received_votes: set = set()
diorama_storage = None
leader: str = None


committed_log: List[Dict[str, Any]] = []
pending_entries: List[Dict[str, Any]] = []
pending_entries_acknowledgements: Dict[str, List[Dict[str, Any]]] = {}


def output_logs():
    if PRINT_LOGS:
        print(f'Committed: {committed_log}')
        print(f'Pending: {pending_entries}')


def get_election_timeout() -> float:
    return ELECTION_TIMEOUT_MIN + (random() * (ELECTION_TIMEOUT_MAX - ELECTION_TIMEOUT_MIN))


def is_consensus(n: int) -> bool:
    return n > ((len(raft_peers) + 1) / 2)


def is_raft_node_nid(nid: str) -> bool:
    return nid.startswith('raft')


def step_down(new_term):
    global current_term
    global state
    global voted_for
    global received_votes
    global pending_entries_acknowledgements
    global raft_peers

    current_term = new_term
    state = FOLLOWER
    voted_for = None
    reset_election_timer()
    received_votes = set()
    pending_entries_acknowledgements = {peer: [] for peer in raft_peers}


def commit_pending_values_with_consensus():
    global raft_peers
    global committed_log
    global pending_entries
    global pending_entries_acknowledgements

    for pending_entry in pending_entries:
        acknowledged_by_nodes: List[str] = [
            peer for peer in raft_peers if pending_entry in pending_entries_acknowledgements[peer]]
        number_acknowledgements: int = len(acknowledged_by_nodes)
        if is_consensus(number_acknowledgements):
            send_commit_add_entry(pending_entry['client_nid'], pending_entry['command'])
            committed_log.append(pending_entry)
            pending_entries.remove(pending_entry)
            for acknowledged_by_node in acknowledged_by_nodes:
                pending_entries_acknowledgements[acknowledged_by_node].remove(pending_entry)


def continue_sending_append_entries_to_all_nodes():
    global heartbeat_timer

    if state == LEADER:
        commit_pending_values_with_consensus()
        for peer in raft_peers:
            send_append_entries(peer)

        heartbeat_timer.cancel()
        heartbeat_timer = Timer(HEARTBEAT_INTERVAL, continue_sending_append_entries_to_all_nodes)
        heartbeat_timer.start()
    else:
        heartbeat_timer.cancel()


def count_votes_for_self():
    global received_votes
    global raft_peers
    global state
    global leader

    if is_consensus(len(received_votes)):
        state = LEADER
        leader = diorama_nid

        continue_sending_append_entries_to_all_nodes()


def become_candidate_and_request_votes():
    global state
    global current_term
    global received_votes
    global voted_for

    state = CANDIDATE
    current_term += 1
    voted_for = diorama_nid
    received_votes = {diorama_nid}
    for peer in raft_peers:
        send_request_vote(peer)


def send_request_vote(peer: str):
    send(RequestVote, {'term': current_term}, peer)


def handle_request_vote(sender: str, payload: Dict[str, Any]):
    global voted_for

    if payload['term'] > current_term:
        step_down(payload['term'])
    if payload['term'] == current_term and voted_for is None:
        voted_for = sender
        send_respond_vote(True, sender)
        reset_election_timer()


def send_respond_vote(granted: bool, peer: str):
    send(RespondVote, {'granted': granted, 'term': current_term}, peer)


def handle_respond_vote(sender: str, payload: Dict[str, Any]):
    global received_votes

    if payload['term'] > current_term:
        step_down(payload['term'])
    if state == CANDIDATE and payload['term'] == current_term and payload['granted']:
        received_votes.add(sender)
        count_votes_for_self()


def send_append_entries(peer: str):
    output_logs()
    send(AppendEntries,
         {
             'term': current_term,
             'committed_log': committed_log,
             'pending_entries': pending_entries
         },
         peer)


def handle_append_entries(sender: str, payload: Dict[str, Any]):
    global leader
    global state
    global committed_log
    global pending_entries

    output_logs()

    if current_term < payload['term']:
        step_down(payload['term'])

    if current_term == payload['term']:
        leader = sender
        state = FOLLOWER
        reset_election_timer()
        committed_log = payload['committed_log']
        pending_entries = payload['pending_entries']
        diorama_storage.put('committed_log', committed_log)

    send_respond_append_entries(sender)


def send_respond_append_entries(peer):
    send(RespondAppendEntries,
         {
             'term': current_term,
             'committed_log': committed_log,
             'pending_entries': pending_entries
         },
         peer)


def handle_respond_append_entries(sender: str, payload: Dict[str, Any]):
    global committed_log
    global pending_entries
    global pending_entries_acknowledgements

    if current_term < payload['term']:
        step_down(payload['term'])

    if current_term == payload['term'] and state == LEADER:
        for acknowledged_pending_entry in payload['pending_entries']:
            if (acknowledged_pending_entry not in pending_entries_acknowledgements[sender] and
                    acknowledged_pending_entry in pending_entries):
                pending_entries_acknowledgements[sender].append(acknowledged_pending_entry)


def handle_request_add_entry(sender: str, payload: Dict[str, Any]):
    global pending_entries

    pending_entries.append({'term': current_term, 'command': payload['command'], 'client_nid': sender})


def send_commit_add_entry(client: str, command: Any):
    send(CommitAddEntry, {'command': command}, client)


def reset_election_timer():
    global election_timer

    election_timeout: float = get_election_timeout()

    election_timer.cancel()
    election_timer = Timer(election_timeout, become_candidate_and_request_votes)
    election_timer.start()


def initialise_diorama_info(diorama_peer_nids, my_nid, raw_send, storage):
    global diorama_nid
    global raft_peers
    global send
    global diorama_storage
    global committed_log
    global pending_entries_acknowledgements

    diorama_nid = my_nid
    raft_peers = [nid for nid in diorama_peer_nids if is_raft_node_nid(nid)]
    diorama_storage = storage
    pending_entries_acknowledgements = {peer: [] for peer in raft_peers}

    if diorama_storage.contains_key('committed_log'):
        committed_log = diorama_storage.get('committed_log')

    send = lambda _message_type, _payload, recipient_nid: raw_send(
        json.dumps({'type': _message_type, 'payload': _payload}).encode(),
        recipient_nid)


def main(diorama_peer_nids, my_nid, raw_send, raw_receive, storage):
    initialise_diorama_info(diorama_peer_nids, my_nid, raw_send, storage)

    def receive():
        bytes_message, _sender_nid = raw_receive()
        message = json.loads(bytes_message.decode('utf8'))
        return message['type'], message['payload'], _sender_nid

    reset_election_timer()

    handlers: Dict[str, Callable] = {
        RequestVote: handle_request_vote,
        RespondVote: handle_respond_vote,
        AppendEntries: handle_append_entries,
        RespondAppendEntries: handle_respond_append_entries,
        RequestAddEntry: handle_request_add_entry
    }

    while True:
        message_type, payload, sender_nid = receive()
        handlers[message_type](sender_nid, payload)
