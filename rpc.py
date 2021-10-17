#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-


import argparse
import socket
import json
import sys


class BaseRPC:
    def __init__(self, app_id, optional_arguments, mode):
        self.id = app_id
        self.args = optional_arguments
        self.mode = mode
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._connect_socket()

    def _connect_socket(self):
        try:
            self.sock.connect("./socket_" + self.mode + self.id)
        except FileNotFoundError:
            sys.stderr.write(f"Could not connect to {self.mode} - it may not be running\n")
            exit(1)


class PeerRPC(BaseRPC):
    def __init__(self, app_id, optional_arguments):
        super().__init__(app_id, optional_arguments, "peer")

    def send_command(self, command):
        if command == "message":
            self.send_message()
        elif command == "getlist":
            self.get_list()
        elif command == "peers":
            self.peers()
        elif command == "reconnect":
            self.reconnect()
        else:
            print("Unknown command\n", file=sys.stderr)
        self.sock.close()

    def send_message(self):
        parser = argparse.ArgumentParser(allow_abbrev=False, add_help=False)
        parser.add_argument('--from', type=str, required=True, dest="sender")
        parser.add_argument('--to', type=str, required=True)
        parser.add_argument('--message', type=str, required=True)
        args = parser.parse_args(self.args)
        self.sock.sendall(
            bytes(json.dumps(
                {"command": "message", "from": args.sender, "to": args.to, "message": args.message}
            ), encoding="utf8")
        )

    def get_list(self):
        self.sock.sendall(bytes(json.dumps({"command": "getlist"}), encoding="utf8"))

    def peers(self):
        self.sock.sendall(bytes(json.dumps({"command": "peers"}), encoding="utf8"))

    def reconnect(self):
        parser = argparse.ArgumentParser(allow_abbrev=False, add_help=False)
        parser.add_argument('--reg-ipv4', type=str, required=True)
        parser.add_argument('--reg-port', type=str, required=True)
        args = parser.parse_args(self.args)
        self.sock.sendall(
            bytes(json.dumps(
                {"command": "reconnect", "node_ip": args.reg_ipv4, "node_port": args.reg_port}
            ), encoding="utf8")
        )


class NodeRPC(BaseRPC):
    def __init__(self, app_id, optional_arguments):
        super().__init__(app_id, optional_arguments, "node")

    def send_command(self, command):
        if command == "database":
            self.database()
        elif command == "neighbors":
            self.neighbors()
        elif command == "connect":
            self.connect()
        elif command == "disconnect":
            self.disconnect()
        elif command == "sync":
            self.sync()
        else:
            print("Unknown command", file=sys.stderr)
        self.sock.close()

    def database(self):
        self.sock.sendall(bytes(json.dumps({"command": "database"}), encoding="utf8"))

    def neighbors(self):
        self.sock.sendall(bytes(json.dumps({"command": "neighbors"}), encoding="utf8"))

    def connect(self):
        parser = argparse.ArgumentParser(allow_abbrev=False, add_help=False)
        parser.add_argument('--reg-ipv4', type=str, required=True)
        parser.add_argument('--reg-port', type=str, required=True)
        args = parser.parse_args(self.args)
        self.sock.sendall(bytes(json.dumps({"command": "connect", "ip": args.reg_ipv4, "port": args.reg_port}),
                                encoding="utf8"))

    def disconnect(self):
        self.sock.sendall(bytes(json.dumps({"command": "disconnect"}), encoding="utf8"))

    def sync(self):
        self.sock.sendall(bytes(json.dumps({"command": "sync"}), encoding="utf8"))


def parse_arguments():
    parser = argparse.ArgumentParser(allow_abbrev=False, add_help=False)
    parser.add_argument('--id', type=str, required=True)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--peer', dest='is_peer', action='store_true')
    group.add_argument('--node', dest='is_peer', action='store_false')
    parser.add_argument('--command', type=str, required=True,
                        choices=["message", "getlist", "peers", "reconnect", "database",
                                 "neighbors", "connect", "disconnect", "sync"])
    return parser.parse_known_args()


def main():
    args = parse_arguments()
    if args[0].is_peer:
        rpc = PeerRPC(args[0].id, args[1])
        rpc.send_command(args[0].command)

    else:
        rpc = NodeRPC(args[0].id, args[1])
        rpc.send_command(args[0].command)


if __name__ == "__main__":
    main()
