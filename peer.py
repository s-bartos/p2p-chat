#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-

import argparse
import sys
import threading
import os
import json
import socket

from base import Base
from bencode import Bencoder


class Peer(Base):
    def __init__(self, identifier, ip, port, username, node_ip, node_port):
        super().__init__(identifier, ip, port)
        try:
            os.unlink("./socket_peer"+identifier)
        except OSError:
            pass
        self.rpc_sock.bind("./socket_peer"+identifier)
        self.username = username
        self.node_ip = node_ip
        self.node_port = node_port
        self.reset_helloing = threading.Event()
        self.list_waiting = []
        self.current_list = dict()
        self._list_waiting_lock = threading.Lock()
        self._current_list_lock = threading.Lock()
        self.daemonize(self.send_hello)
        self.daemonize(self.listen)


    def send_hello(self):
        while True:
            self.sock.sendto(self._hello_msg(), (self.node_ip, self.node_port))
            self.reset_helloing.wait(10)
            self.reset_helloing.clear()

    def get_list(self):
        msg, txid = self._get_list_msg()
        event = self.add_msg_ack_wait(txid)
        list_event = threading.Event()
        with self._list_waiting_lock:
            self.list_waiting.append(list_event)
        self.sock.sendto(msg, (self.node_ip, self.node_port))
        ret = True
        if event.wait(2):
            if self.msg_ack_wait[txid][1]:
                sys.stderr.write("Error response: " + self.msg_ack_wait[txid][1] + "\n")
                ret = False
            else:
                if not list_event.wait(2):
                    sys.stderr.write(f"List has not arrived.\n")
                    ret = False
        else:
            sys.stderr.write(f"Ack for getlist txid:{txid} has not arrived.\n")
            ret = False
        self.rm_msg_ack_wait(txid)
        with self._list_waiting_lock:
            self.list_waiting.remove(list_event)
        return ret

    def send_message(self, username, message, sender):
        if not self.get_list():
            return
        with self._current_list_lock:
            if username not in self.current_list:
                sys.stderr.write(f"User was not found in the network\n")
                return
            else:
                addr = self.current_list[username]
        if sender != self.username:
            sys.stderr.write("Warning: sending message with different username -> receiver may not be able to reply.\n")
        msg, txid = self._message_msg(username, message, sender)
        event = self.add_msg_ack_wait(txid)
        try:
            self.sock.sendto(msg, addr)
            if event.wait(2):
                if self.msg_ack_wait[txid][1]:
                    sys.stderr.write("Error response: " + self.msg_ack_wait[txid][1] + "\n")
            else:
                sys.stderr.write(f"Ack for message txid:{txid} has not arrived.\n")
        except socket.error:
            sys.stderr.write(f"Failed sending message:{msg} to {addr}.\n")
        self.rm_msg_ack_wait(txid)

    def reconnect(self, node_ip, node_port):
        self.sock.sendto(self._logout_msg(), (self.node_ip, self.node_port))
        self.node_ip = node_ip
        self.node_port = node_port
        self.reset_helloing.set()

    def listen_rpc(self):
        self.rpc_sock.listen()
        while True:
            connection, addr = self.rpc_sock.accept()
            self.daemonize(self.handle_rpc_command, connection)

    def handle_rpc_command(self, connection):
        recv = str(connection.recv(4096), encoding="utf8")
        if recv == "":
            connection.close()
            return
        command = json.loads(recv)
        if command["command"] == "message":
            self.send_message(command["to"], command["message"], command["from"])
        elif command["command"] == "getlist":
            self.get_list()
        elif command["command"] == "peers":
            self.get_list()
            self._print_peers()
        elif command["command"] == "reconnect":
            self.reconnect(command["node_ip"], int(command["node_port"]))
        else:
            sys.stderr.write(f"Unknown rpc command: {command['command']}\n")
        connection.close()

    def exit(self):
        self.sock.sendto(self._logout_msg(), (self.node_ip, self.node_port))
        self.rpc_sock.close()
        try:
            os.unlink("./socket_peer" + self.id)
        except OSError:
            pass
        self.sock.close()

    def _parse_message(self, message_dict, addr):
        try:
            self.logger.debug(f"{message_dict['type']} addr:{addr}")
            if message_dict["type"] == "message":
                try:
                    print(f"From: {message_dict['from']}\nTo:{message_dict['to']}\n{message_dict['message']}")
                except KeyError:
                    sys.stderr.write("Missing required field in message body.\n")
                    self.send_error(addr, message_dict["txid"], "Missing required field in message body.")
                else:
                    self.send_ack(addr, message_dict["txid"])
            elif message_dict["type"] == "list":
                self.send_ack(addr, txid=message_dict["txid"])
                self._list_arrived(message_dict["peers"])
            elif message_dict["type"] == "ack":
                self.ack_arrived(message_dict["txid"])
            else:
                sys.stderr.write("Wrong type\n")
        except KeyError:
            sys.stderr.write("Wrong format of incoming message.\n")
            if "txid" in message_dict:
                self.send_error(addr, message_dict["txid"], "Wrong format\n")

    def _list_arrived(self, peer_list):
        with self._current_list_lock:
            self.current_list = dict()
            for peer in peer_list.values():
                self.current_list[peer["username"]] = (peer["ipv4"], peer["port"])
        with self._list_waiting_lock:
            for event in self.list_waiting:
                event.set()

    def _print_peers(self):
        with self._current_list_lock:
            print("Peers:")
            for username, addr in self.current_list.items():
                print("\t" + username + ": " + addr[0] + ", " + str(addr[1]))

    def _hello_msg(self):
        msg = {"type": "hello", "txid": self.get_message_id(), "username": self.username,
               "ipv4": self.ip, "port": self.port}
        return bytes(Bencoder.encode(msg), encoding="utf8")

    def _logout_msg(self):
        msg = {"type": "hello", "txid": self.get_message_id(), "username": self.username,
               "ipv4": "0.0.0.0", "port": 0}
        return bytes(Bencoder.encode(msg), encoding="utf8")

    def _get_list_msg(self):
        txid = self.get_message_id()
        msg = {"type": "getlist", "txid": txid}
        return bytes(Bencoder.encode(msg), encoding="utf8"), txid

    def _message_msg(self, username, message, sender):
        txid = self.get_message_id()
        msg = {"type": "message", "txid": txid, "from": sender, "to": username, "message": message}
        return bytes(Bencoder.encode(msg), encoding="utf8"), txid

def parse_arguments():
    parser = argparse.ArgumentParser(allow_abbrev=False, add_help=False)
    parser.add_argument('--id', type=str, required=True)
    parser.add_argument('--username', type=str, required=True)
    parser.add_argument('--chat-ipv4', type=str, required=True)
    parser.add_argument('--chat-port', type=int, required=True)
    parser.add_argument('--reg-ipv4', type=str, required=True)
    parser.add_argument('--reg-port', type=int, required=True)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_arguments()
    peer = Peer(args.id, args.chat_ipv4, args.chat_port, args.username, args.reg_ipv4, args.reg_port)
    try:
        peer.listen_rpc()
    except KeyboardInterrupt:
        print("\n\nTerminating peer..\n")
    finally:
        peer.exit()
