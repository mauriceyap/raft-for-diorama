NUMBER_OF_COMMANDS_TO_REQUEST_TO_ADD: int = 50
ADD_COMMAND_REQUEST_INTERVAL: float = 3.5  # seconds

import json
import random
from threading import Timer

RequestAddEntry: str = 'RequestAddEntry'
CommitAddEntry: str = 'CommitAddEntry'


def is_raft_node_nid(nid: str) -> bool:
    return nid.startswith('raft')


def main(diorama_peer_nids, my_nid, raw_send, raw_receive, storage):
    raft_peers = [nid for nid in diorama_peer_nids if is_raft_node_nid(nid)]

    def send(_message_type, _payload, recipient_nid):
        raw_send(json.dumps({'type': _message_type, 'payload': _payload}).encode(), recipient_nid)

    def receive():
        bytes_message, _sender_nid = raw_receive()
        message = json.loads(bytes_message.decode('utf8'))
        return message['type'], message['payload'], _sender_nid

    def request_add_entry():
        command: str = random.choice(['UP', 'DOWN', 'LEFT', 'RIGHT'])
        print(f'Requesting to add {command}')
        for raft_peer in raft_peers:
            send(RequestAddEntry, {'command': command}, raft_peer)

    for i in range(0, NUMBER_OF_COMMANDS_TO_REQUEST_TO_ADD):
        Timer(ADD_COMMAND_REQUEST_INTERVAL * i, request_add_entry).start()

    while True:
        message_type, payload, sender_nid = receive()
        if message_type == CommitAddEntry:
            print(f'Committed new entry by {sender_nid}: {payload["command"]}')
