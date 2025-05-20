#!/usr/bin/env python3

import argparse
import signal
import typing
from lib.xAppBase import xAppBase
from prometheus_client import start_http_server, Gauge
import traceback
import time
import threading
import socketserver
from functools import partial


class Lab_xApp(xAppBase):
    def __init__(self, config, http_server_port, rmr_port):
        super(Lab_xApp, self).__init__(config, http_server_port, rmr_port)
        self.setup_prometheus_metrics()
        threading.Thread(target=self.old_labels_removal_thread).start()


    def translate_metric_name(self, metric_name):
        return metric_name.replace(".", "_")

    def setup_prometheus_metrics(self):
        start_http_server(8000)
        self.metrics = {
            "CQI": Gauge("CQI", "Channel Quality Indicator (0-15)", ["ue_id", "nssai"]),
            "RSRP": Gauge("RSRP", "Reference Signal Received Power (dBm)", ["ue_id", "nssai", ]),
            "RSRQ": Gauge("RSRQ", "Reference Signal Received Quality (dB)", ["ue_id", "nssai"]),
            "RRU.PrbAvailDl": Gauge(self.translate_metric_name("RRU.PrbAvailDl"), "PRB Available Downlink", ["ue_id", "nssai"]),
            "RRU.PrbAvailUl": Gauge(self.translate_metric_name("RRU.PrbAvailUl"), "PRB Available Uplink", ["ue_id", "nssai"]),
            "RRU.PrbUsedDl": Gauge(self.translate_metric_name("RRU.PrbUsedDl"), "PRB Used Downlink", ["ue_id", "nssai"]),
            "RRU.PrbUsedUl": Gauge(self.translate_metric_name("RRU.PrbUsedUl"), "PRB Used Uplink", ["ue_id", "nssai"]),
            "RRU.PrbTotDl": Gauge(self.translate_metric_name("RRU.PrbTotDl"), "PRB Total Downlink", ["ue_id", "nssai"]),
            "RRU.PrbTotUl": Gauge(self.translate_metric_name("RRU.PrbTotUl"), "PRB Total Uplink", ["ue_id", "nssai"]),
            "DRB.RlcSduDelayDl": Gauge(self.translate_metric_name("DRB.RlcSduDelayDl"), "RLC SDU Delay Downlink", ["ue_id", "nssai"]),
            "DRB.PacketSuccessRateUlgNBUu": Gauge(self.translate_metric_name("DRB.PacketSuccessRateUlgNBUu"), "Packet Success Rate Uplink gNB Uu", ["ue_id", "nssai"]),
            "DRB.UEThpDl": Gauge(self.translate_metric_name("DRB.UEThpDl"), "Downlink throughput (Mbps)", ["ue_id", "nssai"]),
            "DRB.UEThpUl": Gauge(self.translate_metric_name("DRB.UEThpUl"), "Uplink throughput (Mbps)", ["ue_id", "nssai"]),
            "DRB.RlcPacketDropRateDl": Gauge(self.translate_metric_name("DRB.RlcPacketDropRateDl"), "RLC Packet Drop Rate Downlink", ["ue_id", "nssai"]),
            "DRB.RlcSduTransmittedVolumeDL": Gauge(self.translate_metric_name("DRB.RlcSduTransmittedVolumeDL"), "RLC SDU Transmitted Volume DL", ["ue_id", "nssai"]),
            "DRB.RlcSduTransmittedVolumeUL": Gauge(self.translate_metric_name("DRB.RlcSduTransmittedVolumeUL"), "RLC SDU Transmitted Volume UL", ["ue_id", "nssai"]),
            "DRB.AirIfDelayUl": Gauge(self.translate_metric_name("DRB.AirIfDelayUl"), "Air Interface Delay Uplink", ["ue_id", "nssai"]),
            "DRB.RlcDelayUl": Gauge(self.translate_metric_name("DRB.RlcDelayUl"), "RLC Delay Uplink", ["ue_id", "nssai"]),
            "RACH.PreambleDedCell": Gauge(self.translate_metric_name("RACH.PreambleDedCell"), "Preamble Dedicated Cell", ["ue_id", "nssai"]),
        }

        self.metric_labels = {}
        self.label_metrics = {}
        self.label_timeout = 5

    def update_metrics(self, user_metrics: typing.Dict[typing.AnyStr, typing.Dict[typing.AnyStr, float]]):
        current_metric_labels = []
        for ue_id, ue_metrics in user_metrics.items():
            nssai = ue_metrics.pop("SD")
            if nssai is None:
                print(f"Warning: received None for nssai value of ue_id: {ue_id}  ")
                continue
            rnti = ue_metrics.pop("RNTI")
            print(f"ue_id: {ue_id}, nssai: {nssai}")
            for metric_name, value in ue_metrics.items():
                current_metric_labels.append((ue_id, nssai, rnti, metric_name))
                self.metrics[metric_name].labels(ue_id=ue_id, nssai=nssai).set(value)

        self.update_metric_labels(current_metric_labels)

    def update_metric_labels(self, current_metric_labels):
        now = time.time()
        for metric_label in current_metric_labels:
            self.metric_labels[metric_label] = now

    def old_labels_removal_thread(self):
        while True:
            self.remove_old_labels()
            time.sleep(1)

    def remove_old_labels(self):
        now = time.time()

        def is_metric_label_item_old(metric_label_item):
            _, timestamp = metric_label_item
            return now - timestamp > self.label_timeout

        old_metric_labels = list(filter(is_metric_label_item_old, self.metric_labels.items()))
        for metric_label, timestamp in old_metric_labels:
            label = metric_label[:2]
            metric_name = metric_label[-1]
            print(f"removing label {label} for {metric_label} by timestamp {timestamp}")
            self.metric_labels.pop(metric_label)
            self.metrics[metric_name].remove(*label)


    def my_subscription_callback(self, e2_agent_id, subscription_id, indication_hdr, indication_msg, ue_id):
        # print("\nRIC Indication Received from {} for Subscription ID: {}".format(e2_agent_id, subscription_id))

        indication_hdr = self.e2sm_kpm.extract_hdr_info(indication_hdr)
        meas_data = self.e2sm_kpm.extract_meas_data(indication_msg)

        # print("E2SM_KPM RIC Indication Content:")
        # print("-ColletStartTime: ", indication_hdr['colletStartTime'])
        # print("-Measurements Data:")

        granulPeriod = meas_data.get("granulPeriod", None)
        if granulPeriod is not None:
            # print("-granulPeriod: {}".format(granulPeriod))
            pass

        try:
            all_metrics = {}
            for ue_id, ue_meas_data in meas_data["ueMeasData"].items():
                print("--UE_id: {}".format(ue_id))
                ue_metrics = {}
                granulPeriod = ue_meas_data.get("granulPeriod", None)
                if granulPeriod is not None:
                    pass
                    # print("---granulPeriod: {}".format(granulPeriod))

                for metric_name, value in ue_meas_data["measData"].items():
                    print("---Metric: {}, Value: {}".format(metric_name, value))
                    ue_metrics[metric_name] = value[0]
                
                all_metrics[ue_id] = ue_metrics
            self.update_metrics(all_metrics)
        except Exception as e:
            traceback.print_exc()
            print("Error during metrics extraction: {}".format(e))


    @xAppBase.start_function
    def start(self, e2_node_id, metric_names):
        report_period = 1000
        granul_period = 1000

        matchingUeConds = [
            {
                'testCondInfo': {
                    'testType': ('sNSSAI', 'true'), 
                    'testExpr': 'equal', 
                    'testValue': ('valueInt', 2)
                }
            }
        ]
        
        subscription_callback = lambda agent, sub, hdr, msg: self.my_subscription_callback(agent, sub, hdr, msg, None)
        print("Subscribe to E2 node ID: {}, RAN func: e2sm_kpm, metrics: {}".format(e2_node_id, metric_names))
        self.e2sm_kpm.subscribe_report_service_style_4(e2_node_id, report_period, matchingUeConds, metric_names, granul_period, subscription_callback)


    def get_rntis(self, slice_id):
        rntis = set()
        for (_, nssai, rnti, _)  in self.metric_labels.keys():
            if int(slice_id) == int(nssai):
                rntis.add(rnti)

        return rntis


class UEIDRequestHandler(socketserver.BaseRequestHandler):
    def __init__(self, kpm, *args, **kwargs):
        self.kpm = kpm
        super().__init__(*args, **kwargs)

    def handle(self):
        data = self.request.recv(1024).strip().decode("utf-8")
        # print(f"UEIDRequestHandler: Received request for slice: {data}")
        response = self.kpm.get_rntis(data)
        # print(f"UEIDRequestHandler: Received ue ids: {response}")
        self.request.sendall(str(response).encode("utf-8"))

class ThreadedServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    daemon_threads = True  # Ensure threads exit when main program exits
    allow_reuse_address = True

def run_server(myXapp, host="0.0.0.0", port=5000):
    handler = partial(UEIDRequestHandler, myXapp)
    with ThreadedServer((host, port), handler) as server:
        print(f"Server running on {host}:{port}")
        server.serve_forever()

def start_server_in_thread(myXapp, host="0.0.0.0", port=5000):
    server_thread = threading.Thread(target=run_server, args=(myXapp, host, port), daemon=True)
    server_thread.start()
    return server_thread


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='My example xApp')
    parser.add_argument("--config", type=str, default='', help="xApp config file path")
    parser.add_argument("--http_server_port", type=int, default=8092, help="HTTP server listen port")
    parser.add_argument("--rmr_port", type=int, default=4562, help="RMR port")
    parser.add_argument("--e2_node_id", type=str, default='gnbd_001_001_00019b_0', help="E2 Node ID")
    parser.add_argument("--ran_func_id", type=int, default=2, help="RAN function ID")
    parser.add_argument("--metrics", type=str, default='DRB.UEThpUl,DRB.UEThpDl', help="Metrics name as comma-separated string")

    args = parser.parse_args()
    config = args.config
    e2_node_id = args.e2_node_id # TODO: get available E2 nodes from SubMgr, now the id has to be given.
    ran_func_id = args.ran_func_id # TODO: get available E2 nodes from SubMgr, now the id has to be given.
    metrics = args.metrics.split(",")

    # Create MyXapp.
    myXapp = Lab_xApp(config, args.http_server_port, args.rmr_port)
    myXapp.e2sm_kpm.set_ran_func_id(ran_func_id)

    thread = start_server_in_thread(myXapp)

    # Connect exit signals.
    signal.signal(signal.SIGQUIT, myXapp.signal_handler)
    signal.signal(signal.SIGTERM, myXapp.signal_handler)
    signal.signal(signal.SIGINT, myXapp.signal_handler)

    # Start xApp.
    myXapp.start(e2_node_id, metrics)
    # Note: xApp will unsubscribe all active subscriptions at exit.
    thread.join()