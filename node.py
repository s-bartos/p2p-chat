#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-

import argparse
import threading
import sys
import json
import os
import socket

from base import Base
from bencode import Bencoder
from database import Database


class Node(Base):
    def __init__(self, identifier, ip, port):
        super().__init__(identifier, ip, port)
        try:
            os.unlink("./socket_node" + identifier)
        except OSError:
            pass
        self.rpc_sock.bind("./socket_node" + identifier)
        self.database = Database(ip, port)
        self.reset_update = threading.Event()
        self.disconnecting_lock = threading.Lock()
        self.disconnect_failed = False
        self.daemonize(self._update)
        self.daemonize(self.listen)

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
        if command["command"] == "database":
            self.database.print_database()
        elif command["command"] == "neighbors":
            self.database.print_neighbors()
        elif command["command"] == "connect":
            with self.disconnecting_lock:
                try:
                    self.sock.sendto(self._update_msg(), (command["ip"], int(command["port"])))
                except socket.error:
                    sys.stderr.write("Wrong address given by rpc.\n")
        elif command["command"] == "disconnect":
            self.disconnect_all()
        elif command["command"] == "sync":
            self.reset_update.set()
        else:
            sys.stderr.write(f"Unknown rpc command: {command['command']}\n")
        connection.close()

    def exit(self):
        self.disconnect_all()
        self.rpc_sock.close()
        try:
            os.unlink("./socket_node" + self.id)
        except OSError:
            pass
        self.sock.close()

    def disconnect_all(self):
        with self.disconnecting_lock:
            for i in range(6):
                self.disconnect_failed = False
                neighbors = tuple(self.database.neighbors.keys())
                threads = []
                for neighbor in neighbors:
                    threads.append(self.daemonize(self.send_disconnect, neighbor))
                for thread in threads:
                    thread.join()
                if not self.disconnect_failed:
                    break
                print("Disconnect failed\n")
            self.database.remove_all()

    def send_disconnect(self, addr):
        msg, txid = self._disconnect_msg()
        event = self.add_msg_ack_wait(txid)
        self.sock.sendto(msg, addr)
        if event.wait(2):
            if self.msg_ack_wait[txid][1]:
                sys.stderr.write("Error response: " + self.msg_ack_wait[txid][1] + "\n")
        else:
            print("Disconnect failed")
            self.disconnect_failed = True
        self.rm_msg_ack_wait(txid)

    def _parse_message(self, message_dict, addr):
        try:
            self.logger.debug(f"{message_dict['type']} addr:{addr}")
            if message_dict["type"] == "hello":
                if self.database.register_peer(message_dict):
                    self.reset_update.set()
            elif message_dict["type"] == "update":
                if self.disconnecting_lock.locked():
                    return
                new_neighbors = self.database.update_database(addr[0], addr[1], message_dict["db"])
                for neighbor in new_neighbors:
                    self.sock.sendto(self._update_msg(), neighbor)
            elif message_dict["type"] == "getlist":
                self.daemonize(self._send_list, addr=addr, txid=message_dict["txid"])
            elif message_dict["type"] == "disconnect":
                self.database.remove_neighbor(addr[0], addr[1])
                self.send_ack(addr, message_dict["txid"])
            elif message_dict["type"] == "ack":
                self.ack_arrived(message_dict["txid"])
            elif message_dict["type"] == "error":
                self.error_arrived(message_dict["txid"], message_dict["verbose"])
        except KeyError:
            sys.stderr.write(f"Wrong format of message:{message_dict}\n")
            if "txid" in message_dict:
                self.send_error(addr, message_dict["txid"], "Wrong format\n")
        except ValueError:
            sys.stderr.write(f"Wrong value in message:{message_dict}\n")
            if "txid" in message_dict:
                self.send_error(addr, message_dict["txid"], "Value error\n")
        except socket.error:
            sys.stderr.write(f"Socket error - probably wrong addr in message:{message_dict}\n")
            if "txid" in message_dict:
                self.send_error(addr, message_dict["txid"], "socket error - probably wrong addr\n")

    def _send_list(self, addr, txid):
        if self.database.is_registered(addr[0], addr[1]):
            self.send_ack(addr, txid)
        else:
            self.send_error(addr, txid, "Peer is not registered\n")
            sys.stderr.write(f"Probe on list from unregistered peer: {addr}\n")
            return
        msg, txid = self._list_msg()
        event = self.add_msg_ack_wait(txid)
        self.sock.sendto(msg, addr)
        if event.wait(2):
            if self.msg_ack_wait[txid][1]:
                sys.stderr.write("Error response: " + self.msg_ack_wait[txid][1] + "\n")
        else:
            sys.stderr.write(f"Ack for list txid: {txid} has not arrived\n")
        self.rm_msg_ack_wait(txid)

    def _update(self):
        while True:
            with self.disconnecting_lock:
                neighbors = tuple(self.database.neighbors.keys())
                for neighbor in neighbors:
                    self.sock.sendto(self._update_msg(), neighbor)
            self.reset_update.wait(4)
            self.reset_update.clear()

    def _update_msg(self):
        msg = {"type": "update", "txid": self.get_message_id(), "db": self.database.content()}
        return bytes(Bencoder.encode(msg), encoding="utf8")

    def _list_msg(self):
        txid = self.get_message_id()
        msg = {"type": "list", "txid": txid, "peers": self.database.list()}
        return bytes(Bencoder.encode(msg), encoding="utf8"), txid

    def _disconnect_msg(self):
        txid = self.get_message_id()
        msg = {"type": "disconnect", "txid": txid}
        return bytes(Bencoder.encode(msg), encoding="utf8"), txid


def parse_arguments():
    parser = argparse.ArgumentParser(allow_abbrev=False, add_help=False)
    parser.add_argument('--id', type=str, required=True)
    parser.add_argument('--reg-ipv4', type=str, required=True)
    parser.add_argument('--reg-port', type=int, required=True)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_arguments()
    node = Node(args.id, args.reg_ipv4, args.reg_port)
    try:
        node.listen_rpc()
    except KeyboardInterrupt:
        print("\n\nDisconnecting and terminating node..\n")
    finally:
        node.exit()
