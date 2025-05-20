#!/usr/bin/env python3

import time
import datetime
import argparse
import signal
import socketserver
import threading
from lib.xAppBase import xAppBase

class MyXapp(xAppBase):
    def __init__(self, config, http_server_port, rmr_port):
        super(MyXapp, self).__init__(config, http_server_port, rmr_port)
        pass

    # Mark the function as xApp start function using xAppBase.start_function decorator.
    # It is required to start the internal msg receive loop.
    @xAppBase.start_function
    def start(self, e2_node_id, ue_id):
        while self.running:
            min_prb_ratio = 1
            max_prb_ratio = 5
            current_time = datetime.datetime.now()
            print("{} Send RIC Control Request to E2 node ID: {} for UE ID: {}, PRB_min_ratio: {}, PRB_max_ratio: {}".format(current_time.strftime("%H:%M:%S"), e2_node_id, ue_id, min_prb_ratio, max_prb_ratio))
            self.e2sm_rc.control_slice_level_prb_quota(e2_node_id, ue_id, min_prb_ratio, max_prb_ratio, ack_request=1)
            time.sleep(10)

            min_prb_ratio = 1
            max_prb_ratio = 40
            current_time = datetime.datetime.now()
            print("{} Send RIC Control Request to E2 node ID: {} for UE ID: {}, PRB_min_ratio: {}, PRB_max_ratio: {}".format(current_time.strftime("%H:%M:%S"), e2_node_id, ue_id, min_prb_ratio, max_prb_ratio))
            self.e2sm_rc.control_slice_level_prb_quota(e2_node_id, ue_id, min_prb_ratio, max_prb_ratio, ack_request=1)
            time.sleep(10)

            min_prb_ratio = 1
            max_prb_ratio = 100
            current_time = datetime.datetime.now()
            print("{} Send RIC Control Request to E2 node ID: {} for UE ID: {}, PRB_min_ratio: {}, PRB_max_ratio: {}".format(current_time.strftime("%H:%M:%S"), e2_node_id, ue_id, min_prb_ratio, max_prb_ratio))
            self.e2sm_rc.control_slice_level_prb_quota(e2_node_id, ue_id, min_prb_ratio, max_prb_ratio, ack_request=1)
            time.sleep(10)

class UEIDRequestHandler(socketserver.BaseRequestHandler):
    def handle(self):
        data = self.request.recv(1024).strip().decode("utf-8")
        print(f"Received request for slice: {data}")
        response = SLICE_DATA.get(data, [])
        self.request.sendall(str(response).encode("utf-8"))

class ThreadedServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    daemon_threads = True  # Ensure threads exit when main program exits
    allow_reuse_address = True

def run_server(host="0.0.0.0", port=5000):
    with ThreadedServer((host, port), UEIDRequestHandler) as server:
        print(f"Server running on {host}:{port}")
        server.serve_forever()

def start_server_in_thread(host="0.0.0.0", port=5000):
    server_thread = threading.Thread(target=run_server, args=(host, port), daemon=True)
    server_thread.start()
    return server_thread


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='My example xApp')
    parser.add_argument("--config", type=str, default='', help="xApp config file path")
    parser.add_argument("--http_server_port", type=int, default=8090, help="HTTP server listen port")
    parser.add_argument("--rmr_port", type=int, default=4560, help="RMR port")
    parser.add_argument("--e2_node_id", type=str, default='gnbd_001_001_00019b_0', help="E2 Node ID")
    parser.add_argument("--ran_func_id", type=int, default=3, help="E2SM RC RAN function ID")
    parser.add_argument("--ue_id", type=int, default=0, help="UE ID")


    args = parser.parse_args()
    config = args.config
    e2_node_id = args.e2_node_id # TODO: get available E2 nodes from SubMgr, now the id has to be given.
    ran_func_id = args.ran_func_id # TODO: get available E2 nodes from SubMgr, now the id has to be given.
    ue_id = args.ue_id

    # Create MyXapp.
    myXapp = MyXapp(config, args.http_server_port, args.rmr_port)
    myXapp.e2sm_rc.set_ran_func_id(ran_func_id)

    # Connect exit signals.
    signal.signal(signal.SIGQUIT, myXapp.signal_handler)
    signal.signal(signal.SIGTERM, myXapp.signal_handler)
    signal.signal(signal.SIGINT, myXapp.signal_handler)

    # Start xApp.
    myXapp.start(e2_node_id, ue_id)
