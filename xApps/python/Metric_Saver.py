#!/usr/bin/env python3

import argparse
import signal
from lib.xAppBase import xAppBase
# Added: to create DataBase (training)
import time
import os
import csv


class MyXapp(xAppBase):
    def __init__(self, config, http_server_port, rmr_port):
        super(MyXapp, self).__init__(config, http_server_port, rmr_port)
        # Create Directory: store Data
        if not os.path.exists("Data"):
            os.makedirs("Data")
        # Initializes dictionary
        self.dico={}

    def my_subscription_callback(self, e2_agent_id, subscription_id, indication_hdr, indication_msg, kpm_report_style, ue_id):
        timestamp = time.time()

        if kpm_report_style == 2:
            print("\nRIC Indication Received from {} for Subscription ID: {}, KPM Report Style: {}, UE ID: {}".format(e2_agent_id, subscription_id, kpm_report_style, ue_id))
        else:
            # print("\nRIC Indication Received from {} for Subscription ID: {}, KPM Report Style: {}".format(e2_agent_id, subscription_id, kpm_report_style))
            pass

        indication_hdr = self.e2sm_kpm.extract_hdr_info(indication_hdr)
        meas_data = self.e2sm_kpm.extract_meas_data(indication_msg)

        # print("E2SM_KPM RIC Indication Content:")
        # print("-ColletStartTime: ", indication_hdr['colletStartTime'])
        # print("-Measurements Data:")

        granulPeriod = meas_data.get("granulPeriod", None)
        if granulPeriod is not None:
            # print("-granulPeriod: {}".format(granulPeriod))
            pass

        if kpm_report_style in [1,2]:
            for metric_name, value in meas_data["measData"].items():
                # print("--Metric: {}, Value: {}".format(metric_name, value))
                pass

        else:
            for ue_id, ue_meas_data in meas_data["ueMeasData"].items():
                # Create entry for UE
                if not ue_id in self.dico.keys():
                    self.dico[ue_id]=[]

                granulPeriod = ue_meas_data.get("granulPeriod", None)
                if granulPeriod is not None:
                    # print("---granulPeriod: {}".format(granulPeriod))
                    pass

                # Retrieve data of a specific UE in the current report
                for metric_name, value in ue_meas_data["measData"].items():
                    # print("---Metric: {}, Value: {}".format(metric_name, value))
                    self.dico[ue_id].append((timestamp,metric_name,value))
                


    # Mark the function as xApp start function using xAppBase.start_function decorator.
    # It is required to start the internal msg receive loop.
    @xAppBase.start_function
    def start(self, e2_node_id, kpm_report_style, ue_ids, metric_names):
        report_period = 500 # 1000 initially
        granul_period = 500 # 1000 initially

        # use always the same subscription callback, but bind kpm_report_style parameter
        subscription_callback = lambda agent, sub, hdr, msg: self.my_subscription_callback(agent, sub, hdr, msg, kpm_report_style, None)

        if (kpm_report_style == 1):
            print("Subscribe to E2 node ID: {}, RAN func: e2sm_kpm, Report Style: {}, metrics: {}".format(e2_node_id, kpm_report_style, metric_names))
            self.e2sm_kpm.subscribe_report_service_style_1(e2_node_id, report_period, metric_names, granul_period, subscription_callback)

        elif (kpm_report_style == 2):
            # need to bind also UE_ID to callback as it is not present in the RIC indication in the case of E2SM KPM Report Style 2
            subscription_callback = lambda agent, sub, hdr, msg: self.my_subscription_callback(agent, sub, hdr, msg, kpm_report_style, ue_ids[0])
            
            print("Subscribe to E2 node ID: {}, RAN func: e2sm_kpm, Report Style: {}, UE_id: {}, metrics: {}".format(e2_node_id, kpm_report_style, ue_ids[0], metric_names))
            self.e2sm_kpm.subscribe_report_service_style_2(e2_node_id, report_period, ue_ids[0], metric_names, granul_period, subscription_callback)

        elif (kpm_report_style == 3):
            if (len(metric_names) > 1):
                metric_names = metric_names[0]
                print("INFO: Currently only 1 metric can be requested in E2SM-KPM Report Style 3, selected metric: {}".format(metric_names))
            # TODO: currently only dummy condition that is always satisfied, useful to get IDs of all connected UEs
            # example matching UE condition: ul-rSRP < 1000
            matchingConds = [{'matchingCondChoice': ('testCondInfo', {'testType': ('ul-rSRP', 'true'), 'testExpr': 'lessthan', 'testValue': ('valueInt', 1000)})}]

            print("Subscribe to E2 node ID: {}, RAN func: e2sm_kpm, Report Style: {}, metrics: {}".format(e2_node_id, kpm_report_style, metric_names))
            self.e2sm_kpm.subscribe_report_service_style_3(e2_node_id, report_period, matchingConds, metric_names, granul_period, subscription_callback)

        elif (kpm_report_style == 4):
            # TODO: currently only dummy condition that is always satisfied, useful to get IDs of all connected UEs
            # example matching UE condition: ul-rSRP < 1000
            matchingUeConds = [{'testCondInfo': {'testType': ('ul-rSRP', 'true'), 'testExpr': 'lessthan', 'testValue': ('valueInt', 1000)}}]
            
            print("Subscribe to E2 node ID: {}, RAN func: e2sm_kpm, Report Style: {}, metrics: {}".format(e2_node_id, kpm_report_style, metric_names))
            self.e2sm_kpm.subscribe_report_service_style_4(e2_node_id, report_period, matchingUeConds, metric_names, granul_period, subscription_callback)

        elif (kpm_report_style == 5):
            if (len(ue_ids) < 2):
                dummyUeId = ue_ids[0] + 1
                ue_ids.append(dummyUeId)
                print("INFO: Subscription for E2SM_KPM Report Service Style 5 requires at least two UE IDs -> add dummy UeID: {}".format(dummyUeId))

            print("Subscribe to E2 node ID: {}, RAN func: e2sm_kpm, Report Style: {}, UE_ids: {}, metrics: {}".format(e2_node_id, kpm_report_style, ue_ids, metric_names))
            self.e2sm_kpm.subscribe_report_service_style_5(e2_node_id, report_period, ue_ids, metric_names, granul_period, subscription_callback)

        else:
            print("INFO: Subscription for E2SM_KPM Report Service Style {} is not supported".format(kpm_report_style))
            exit(1)

    # Saves the metrics before unsuscribing (1 file per UE)
    def signal_handler(self, sig, frame):
        # Save Data
        print("In the closing function")
        print("Testing the Keys")
        print(self.dico.keys())
        for ue_id,data in self.dico.items():
            print("Handling UE %s"%ue_id)
            csvfile = open("Data/ue_%s.csv"%ue_id,'w')
            metrics_writer = csv.writer(csvfile, delimiter=',')
            # Format (timestamp, metric, values)
            for metrics in data:
                metrics_writer.writerow([metrics[0],metrics[1],metrics[2][0]]) # Usually the value is in a list `[]`
            csvfile.close()
        # Unsuscribe all
        super().stop()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='My example xApp')
    parser.add_argument("--config", type=str, default='', help="xApp config file path")
    parser.add_argument("--http_server_port", type=int, default=8092, help="HTTP server listen port")
    parser.add_argument("--rmr_port", type=int, default=4562, help="RMR port")
    parser.add_argument("--e2_node_id", type=str, default='gnbd_001_001_00019b_0', help="E2 Node ID") # This is the DU
    parser.add_argument("--ran_func_id", type=int, default=2, help="RAN function ID")
    parser.add_argument("--kpm_report_style", type=int, default=1, help="xApp config file path") # KPM Report Style: 3
    parser.add_argument("--ue_ids", type=str, default='0', help="UE ID")
    parser.add_argument("--metrics", type=str, default='DRB.UEThpUl,DRB.UEThpDl', help="Metrics name as comma-separated string")

    args = parser.parse_args()
    config = args.config
    e2_node_id = args.e2_node_id # TODO: get available E2 nodes from SubMgr, now the id has to be given.
    # CU = gnbd_001_001_00019b_1
    # DU = gnbd_001_001_00019b_0
    ran_func_id = args.ran_func_id # TODO: get available E2 nodes from SubMgr, now the id has to be given.
    ue_ids = list(map(int, args.ue_ids.split(","))) # Note: the UE id has to exist at E2 node!
    kpm_report_style = args.kpm_report_style
    metrics = args.metrics.split(",")
    
    # Xtracted metrics
    metrics = ['CQI','RSRP','RSRQ','RACH.PreambleDedCell',
    'RRU.PrbAvailDl','RRU.PrbAvailUl','RRU.PrbUsedDl','RRU.PrbUsedUl','RRU.PrbTotDl','RRU.PrbTotUl',
    'DRB.UEThpDl','DRB.UEThpUl','DRB.AirIfDelayUl',
    'DRB.RlcPacketDropRateDl','DRB.RlcSduTransmittedVolumeDL','DRB.RlcSduTransmittedVolumeUL','DRB.RlcSduDelayDl','DRB.RlcDelayUl',
    ]

    # Create MyXapp.
    myXapp = MyXapp(config, args.http_server_port, args.rmr_port)
    myXapp.e2sm_kpm.set_ran_func_id(ran_func_id)

    # Connect exit signals.
    signal.signal(signal.SIGQUIT, myXapp.signal_handler)
    signal.signal(signal.SIGTERM, myXapp.signal_handler)
    signal.signal(signal.SIGINT, myXapp.signal_handler)

    # Start xApp.
    myXapp.start(e2_node_id, kpm_report_style, ue_ids, metrics)
    # Note: xApp will unsubscribe all active subscriptions at exit.