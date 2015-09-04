#!/usr/bin/env python
# encoding: utf-8

import pyroute2,sys

sock = socket.socket(socket.AF_NETLINK, socket.SOCK_RAW)
sock.bind((0,0))
sock.send('bullshit')
print sock.recv(65535)
sys.exit()
