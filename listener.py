#!/usr/bin/env python3

import socket
import pyarrow as pa
import pyarrow.ipc

listen = "127.0.0.1"
port = 56565

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    sock.bind((listen, port))
    sock.listen()
    print(f"Listening on {listen} on port {port}")
    conn, _ = sock.accept()
    with conn:
        conn_file = conn.makefile(mode="b")
        reader = pyarrow.ipc.RecordBatchStreamReader(conn_file)
        table = reader.read_all()
        print(table)
        print(table.to_pandas())
