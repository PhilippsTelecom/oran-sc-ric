#!/usr/bin/env python3
import argparse
import datetime
import signal
from lib.xAppBase import xAppBase
from utils import read_yaml, request_ueids


class MyXapp(xAppBase):
    def __init__(self, config, http_server_port, rmr_port):
        super(MyXapp, self).__init__(config, http_server_port, rmr_port)
        pass

    @xAppBase.start_function
    def start(self, e2_node_id):
        for slice in slice_yaml['slice_config']:
            print("\nslice: ", slice)
            min_prb_ratio = int(slice['prb_min_max'][1])
            max_prb_ratio = int(slice['prb_min_max'][2])
            sst = int(slice['sst_sd'][0])
            sd = int(slice['sst_sd'][1])
            ue_ids = set(request_ueids(sd)) # from kpm
            if ue_ids is None:
                print(f"ueid retreival failed for slice {sd}")
                continue
            print(f"ue_ids in slide {sd}: {ue_ids}")

            if len(ue_ids) > 0:
                for ue_id in ue_ids:
                    current_time = datetime.datetime.now()
                    print(f"{current_time.strftime('%H:%M:%S')} Send RIC Control Request to Slice ID: {sd}, UE ID: {ue_id}, PRB_min_ratio: {min_prb_ratio}, PRB_max_ratio: {max_prb_ratio}")
                    self.e2sm_rc.control_slice_level_prb_quota(e2_node_id, int(ue_id), min_prb_ratio, max_prb_ratio, ack_request=1, sst=sst, sd=sd)
            else:
                print(f"No ueid found for slice {sd}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Slice control xApp')
    parser.add_argument("--slice_yaml", type=str, default='./slice_config.yaml', help="Slice yaml config")
    parser.add_argument("--config", type=str, default='', help="xApp config file path")
    parser.add_argument("--http_server_port", type=int, default=8090, help="HTTP server listen port")
    parser.add_argument("--rmr_port", type=int, default=4560, help="RMR port")
    parser.add_argument("--e2_node_id", type=str, default='gnbd_001_001_00019b_0', help="E2 Node ID")
    parser.add_argument("--ran_func_id", type=int, default=3, help="E2SM RC RAN function ID")

    args = parser.parse_args()
    slice_yaml = read_yaml(args.slice_yaml)
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
