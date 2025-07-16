#!/usr/bin/env python3

import argparse
import signal
from lib.xAppBase import xAppBase
# Added: to create DataBase (training)
import time
import os
import csv


# THOSE UES WILL SUBSCRIBE OT THOSE METRICS
UE_IDS=[0,1]
METRICS=['DRB.RlcSduDelayDl','DRB.RlcSduTransmittedVolumeDL','DRB.RlcPacketDropRateDl']
# UE NODES
DU_NODE_ID = "gnbd_001_001_00019b_0"
CU_NODE_ID = "gnb_001_001_00019b"


class MyXapp(xAppBase):
    def __init__(self, config, http_server_port, rmr_port):
        super(MyXapp, self).__init__(config, http_server_port, rmr_port)
        # Create Directory: store Data
        if not os.path.exists("Data"):
            os.makedirs("Data")
        # Initializes dictionary
        self.dico           = {}
        [self.dico.setdefault(i,[]) for i in UE_IDS]
        # Perfs
        self.FirstReport    = 0
        self.LastReport     = 0
        self.Handled        = 0


    def my_subscription_callback(self, e2_agent_id, subscription_id, indication_hdr, indication_msg, kpm_report_style, ue_id):
        timestamp = time.time()

        # Related to perfs
        if self.FirstReport == 0: self.FirstReport=timestamp # Init
        self.LastReport     = timestamp
        self.Handled        = self.Handled + 1 # Increment

        # Retrieves header + data
        indication_hdr = self.e2sm_kpm.extract_hdr_info(indication_hdr)
        meas_data = self.e2sm_kpm.extract_meas_data(indication_msg)

        # METRICS FOR ONE DRB
        if kpm_report_style == 2:
            ue_id = UE_IDS[0]
            for metric_name, value in meas_data["measData"].items():
                self.dico[ue_id].append((timestamp,metric_name,value))

         # METRICS FOR SEVERAL DRBs
        if kpm_report_style == 5:
            for ue_id, ue_meas_data in meas_data["ueMeasData"].items():
                # Retrieve data of a specific UE in the current report
                for metric_name, value in ue_meas_data["measData"].items():
                    self.dico[ue_id].append((timestamp,metric_name,value))


    # Mark the function as xApp start function using xAppBase.start_function decorator.
    # It is required to start the internal msg receive loop.
    @xAppBase.start_function
    def start(self):
        
        report_period = 1000 # 1000 by default
        granul_period = 1000 # 1000 by default

        if(len(UE_IDS)==1):
            Service_Style = 2
            subscription_callback = lambda agent, sub, hdr, msg: self.my_subscription_callback(agent, sub, hdr, msg, Service_Style, None)
            print("Subscribe to E2 node ID: {}, RAN func: e2sm_kpm, Report Style: {}, UE_ids: {}, metrics: {}".format(DU_NODE_ID, Service_Style, UE_IDS, METRICS))
            self.e2sm_kpm.subscribe_report_service_style_2(DU_NODE_ID, report_period, UE_IDS[0], METRICS, granul_period, subscription_callback) # Report for one UE (DRB)
            # use always the same subscription callback, but bind kpm_report_style parameter
        elif len(UE_IDS) > 1:
            Service_Style = 5
            subscription_callback = lambda agent, sub, hdr, msg: self.my_subscription_callback(agent, sub, hdr, msg, Service_Style, None)
            print("Subscribe to E2 node ID: {}, RAN func: e2sm_kpm, Report Style: {}, UE_ids: {}, metrics: {}".format(DU_NODE_ID, Service_Style, UE_IDS, METRICS))
            self.e2sm_kpm.subscribe_report_service_style_5(DU_NODE_ID, report_period, UE_IDS, METRICS, granul_period, subscription_callback) # Report for several UEs (DRBs)


    # Saves the metrics before unsuscribing (1 file per UE)
    def signal_handler(self, sig, frame):
        # Perfs
        print("[!] Handled %d reports in %f "%(self.Handled,self.LastReport-self.FirstReport))
        # Save Data
        print("In the closing function")
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
        
    # CONSTANT VALUES
    http_server_port = 8092
    rmr_port = 4562
    ran_func_id_kpm = 2
    ran_func_id_rc = 3

    # Create MyXapp.
    myXapp = MyXapp('', http_server_port, rmr_port)
    myXapp.e2sm_kpm.set_ran_func_id(ran_func_id_kpm)

    # Connect exit signals.
    signal.signal(signal.SIGQUIT, myXapp.signal_handler)
    signal.signal(signal.SIGTERM, myXapp.signal_handler)
    signal.signal(signal.SIGINT, myXapp.signal_handler)

    # Start xApp.
    myXapp.start()
    # Note: xApp will unsubscribe all active subscriptions at exit.