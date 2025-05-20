#!/usr/bin/env python3
import time
import datetime
import signal
import argparse
import queue
from lib.xAppBase import xAppBase
from utils import request_ueids

now = datetime.datetime.now()
date_time_id = now.strftime("%m-%d-%H-%M-%S")

SLICE_CONFIGS = [
(8, 14, 5, 8),
(8, 14, 5, 11),
(8, 14, 5, 14),
(8, 14, 8, 11),
(8, 14, 8, 14),
(8, 14, 11, 14),
(11, 14, 2, 5),
(11, 14, 2, 8),
(11, 14, 2, 11),
(11, 14, 2, 14),
(11, 14, 5, 8),
(11, 14, 5, 11),
(11, 14, 5, 14),
(11, 14, 8, 11),
(11, 14, 8, 14),
(11, 14, 11, 14)
]


def write_to_file(data):
    with open(f"slice_config.csv", "a") as file: # _{date_time_id}
        file.write(data + "\n")


class MyXapp(xAppBase):
    def __init__(self, config, http_server_port, rmr_port):
        super(MyXapp, self).__init__(config, http_server_port, rmr_port)
        pass

    @xAppBase.start_function
    def start(self, e2_node_id):
        for config in SLICE_CONFIGS:
            slice1_min_prb_ratio, slice1_max_prb_ratio, slice2_min_prb_ratio, slice2_max_prb_ratio = config

            slice1_rntis = request_ueids(1)
            if type(slice1_rntis) is not set:
                print(f"no ue in slice 1")
            else:
                slice1_rntis = set(slice1_rntis)
                for ue_id in slice1_rntis:
                    current_time = datetime.datetime.now()
                    print(f"{current_time.strftime('%H:%M:%S')} Send RIC Control Request to Slice ID: 1, UE ID: {ue_id}, PRB_min_ratio: {slice1_min_prb_ratio}, PRB_max_ratio: {slice1_max_prb_ratio}")
                    self.e2sm_rc.control_slice_level_prb_quota(e2_node_id, int(ue_id), slice1_min_prb_ratio, slice1_max_prb_ratio, ack_request=1, sst=1, sd=1)
                    # try:
                    #     ack_result = self.ack_queue.get(timeout=3)  # Wait for ACK
                    #     print(f"Received ACK: {ack_result}")
                    # except queue.Empty:
                    #     print(f"Request timed out")

            slice2_rntis = request_ueids(2)
            if type(slice2_rntis) is not set:
                print(f"no ue in slice 2")
            else:
                slice2_rntis = set(slice2_rntis)
                for ue_id in slice2_rntis:
                    current_time = datetime.datetime.now()
                    print(f"{current_time.strftime('%H:%M:%S')} Send RIC Control Request to Slice ID: 2, UE ID: {ue_id}, PRB_min_ratio: {slice2_min_prb_ratio}, PRB_max_ratio: {slice2_max_prb_ratio}")
                    self.e2sm_rc.control_slice_level_prb_quota(e2_node_id, ue_id, slice2_min_prb_ratio, slice2_max_prb_ratio, ack_request=1, sst=2, sd=2)
                    # try:
                    #     ack_result = self.ack_queue.get(timeout=3)  # Wait for ACK
                    #     print(f"Received ACK: {ack_result}")
                    # except queue.Empty:
                    #     print(f"Request timed out")

            write_to_file(f"{int(time.time())},{slice1_min_prb_ratio},{slice1_max_prb_ratio},{slice2_min_prb_ratio},{slice2_max_prb_ratio}")
            time.sleep(60)
        
        print("Test completed.")


if __name__ == '__main__':
    with open(f"slice_config.csv", 'w') as file: # _{date_time_id}
        write_to_file("timestamp,slice1_min_prb_ratio,slice1_max_prb_ratio,slice2_min_prb_ratio,slice2_max_prb_ratio")

    parser = argparse.ArgumentParser(description='Slice control xApp')
    parser.add_argument("--config", type=str, default='', help="xApp config file path")
    parser.add_argument("--http_server_port", type=int, default=8090, help="HTTP server listen port")
    parser.add_argument("--rmr_port", type=int, default=4560, help="RMR port")
    parser.add_argument("--e2_node_id", type=str, default='gnbd_001_001_00019b_0', help="E2 Node ID")
    parser.add_argument("--ran_func_id", type=int, default=3, help="E2SM RC RAN function ID")
    args = parser.parse_args()

    config = args.config
    e2_node_id = args.e2_node_id # TODO: get available E2 nodes from SubMgr, now the id has to be given.
    ran_func_id = args.ran_func_id # TODO: get available E2 nodes from SubMgr, now the id has to be given.

    myXapp = MyXapp(config, args.http_server_port, args.rmr_port)

    # Connect exit signals.
    signal.signal(signal.SIGQUIT, myXapp.signal_handler)
    signal.signal(signal.SIGTERM, myXapp.signal_handler)
    signal.signal(signal.SIGINT, myXapp.signal_handler)

    # Start xApp.
    myXapp.start(e2_node_id)
