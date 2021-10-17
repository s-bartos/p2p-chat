#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-

import socket
import threading
import sys
from bencode import Bencoder, BencodeDecodeError
from abc import ABC, abstractmethod


class Base(ABC):
    def __init__(self, identifier, ip, port):
        self.id = identifier
        self.ip = ip
        self.port = port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            self.sock.bind((self.ip, self.port))
        except socket.error:
            sys.stderr.write("Wrong address parameter\n")
            self.sock.close()
            exit(1)
        self._message_id = -1  # start from 0
        self._message_id_lock = threading.Lock()
        self.msg_ack_wait = dict()
        self.rpc_sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.rpc_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.logger = get_logger()

    def listen(self):
        while True:
            message, addr = self.sock.recvfrom(4096)
            try:
                message_dict = Bencoder.decode(str(message, encoding="utf8"))
            except (BencodeDecodeError, StopIteration):
                sys.stderr.write("Failed decoding of incoming message\n")
                continue
            self._parse_message(message_dict, addr)

    @abstractmethod
    def _parse_message(self, message_dict, addr):
        pass

    @abstractmethod
    def exit(self):
        pass

    def get_message_id(self):
        with self._message_id_lock:
            self._message_id += 1
            return self._message_id

    def add_msg_ack_wait(self, txid):
        event = threading.Event()
        self.msg_ack_wait[txid] = [event, None]
        return event

    def rm_msg_ack_wait(self, txid):
        del self.msg_ack_wait[txid]

    def ack_arrived(self, txid):
        if txid in self.msg_ack_wait:
            self.msg_ack_wait[txid][0].set()

    def error_arrived(self, txid, text):
        if txid in self.msg_ack_wait:
            self.msg_ack_wait[txid][1] = text
            self.msg_ack_wait[txid][0].set()
        else:
            sys.stderr.write("Error response: " + text + "\n")

    def send_ack(self, addr, txid):
        self.sock.sendto(self._ack_msg(txid), addr)

    def send_error(self, addr, txid, error):
        self.sock.sendto(self._err_msg(txid, error), addr)

    @staticmethod
    def daemonize(target, *args, **kwargs):
        t = threading.Thread(target=target, args=args, kwargs=kwargs)
        t.daemon = True
        t.start()
        return t

    @staticmethod
    def _ack_msg(txid):
        msg = {"type": "ack", "txid": txid}
        return bytes(Bencoder.encode(msg), encoding="utf8")

    @staticmethod
    def _err_msg(txid, error):
        msg = {"type": "error", "txid": txid, "verbose": error}
        return bytes(Bencoder.encode(msg), encoding="utf8")



def get_logger():
    """
    Python logger configuration
    :param app_name: node or peer
    :param log_level: Set logging level
    :param app_id: ID of node or peer application
    :param disabled: Enable or disable logging
    :return: configured logger object
    """
    import logging
    logger = logging.getLogger("pds")
    formatter = logging.Formatter(
        "%(asctime)s - %(message)s"
    )

    file_handler = logging.FileHandler(f"pds.log")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # if log_level == "DEBUG":
    #     logger.setLevel(logging.DEBUG)
    # elif log_level == "INFO":
    logger.setLevel(logging.DEBUG)
    return logger
