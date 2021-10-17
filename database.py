#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-

import threading


class Database:
    def __init__(self, ip, port):
        self.database = dict()
        self.neighbors = dict()
        self.peers = dict()
        self.key = self._create_key(ip, port)
        self.database[self.key] = dict()
        self.db_lock = threading.Lock()

    def register_peer(self, peer_dict):
        with self.db_lock:
            username = peer_dict["username"]
            is_new_peer = True
            if username in self.peers and self.peers[username]:
                self.peers[username].cancel()
                is_new_peer = False
                if peer_dict["ipv4"] == "0.0.0.0" and peer_dict["port"] == 0:
                    del self.peers[username]
                    del self.database[self.key][username]
                    return True
            self.database[self.key][username] = {
                "username": username,
                "ipv4": peer_dict["ipv4"],
                "port": peer_dict["port"]
            }
            self.peers[username] = self.daemonize_with_timer(30, self.remove_peer, args=[username])
            return is_new_peer

    def update_database(self, node_ip, node_port, db_dict):
        with self.db_lock:
            key = self._create_key(node_ip, node_port)
            if key == self.key:
                return []
            self.database[key] = dict()
            new_neighbors = []
            for neighbor in db_dict.keys():
                if neighbor == self.key:
                    continue
                addr = self._get_addr_from_key(neighbor)
                if addr not in self.neighbors:
                    new_neighbors.append(addr)
            if (node_ip, node_port) in self.neighbors and self.neighbors[(node_ip, node_port)]:
                self.neighbors[(node_ip, node_port)].cancel()
            self.neighbors[(node_ip, node_port)] = self.daemonize_with_timer(
                12, self.remove_neighbor, kwargs=dict(ip=node_ip, port=node_port)
            )
            if key in db_dict:
                for peer in db_dict[key].values():
                    self.database[key][peer["username"]] = peer
            return new_neighbors

    def list(self):
        with self.db_lock:
            ret = dict()
            index = 0
            for node in self.database.values():
                for peer in node.values():
                    ret[str(index)] = peer
                    index += 1
            return ret

    def content(self):
        with self.db_lock:
            ret = dict()
            for key, peer_set in self.database.items():
                ret[key] = dict()
                index = 0
                for peer in peer_set.values():
                    ret[key][str(index)] = peer
                    index += 1
            return ret

    def remove_neighbor(self, ip, port):
        with self.db_lock:
            if (ip, port) in self.neighbors and self.neighbors[(ip, port)]:
                self.neighbors[(ip, port)].cancel()
                del self.neighbors[(ip, port)]
            key = self._create_key(ip, port)
            if key in self.database:
                del self.database[key]

    def print_neighbors(self):
        with self.db_lock:
            print("Neighbors:")
            for n in self.neighbors:
                print("\t" + self._create_key(n[0], n[1]))

    def print_database(self):
        with self.db_lock:
            print("Database:")
            for node, peers in self.database.items():
                print("\t" + node + ":")
                for username, addr in peers.items():
                    print("\t\t" + username + ": " + self._create_key(addr["ipv4"], addr["port"]))

    def remove_peer(self, username):
        with self.db_lock:
            del self.peers[username]
            del self.database[self.key][username]

    def remove_all(self):
        with self.db_lock:
            for key in self.neighbors.keys():
                if self.neighbors[key]:
                    self.neighbors[key].cancel()
                del self.database[self._create_key(key[0], key[1])]
            self.neighbors = dict()

    def is_registered(self, ip, port):
        with self.db_lock:
            for peer in self.database[self.key].values():
                if peer["ipv4"] == ip and peer["port"] == port:
                    return True
            return False

    @staticmethod
    def _create_key(ip, port):
        return ",".join([ip, str(port)])

    @staticmethod
    def _get_addr_from_key(key):
        addr = key.split(",", 2)
        return addr[0], int(addr[1])

    @staticmethod
    def daemonize_with_timer(seconds, target, args=None, kwargs=None):
        t = threading.Timer(seconds, target, args=args, kwargs=kwargs)
        t.daemon = True
        t.start()
        return t
