#!/usr/bin/env python3

from lib.xAppBase import xAppBase
import signal
import time
import pandas as pd
import numpy as np
import math 
import csv
# THREADING
from threading import Thread, Event, current_thread
import queue




# RELATED TO PREDICTION
L4S     = []
NL4S    = [0,1]
AVG_THROUGH     = 'DRB.UEThpDl' # kbps
QUE_SIZE        = 'DRB.RlcStateDL' 
DU_METRICS      = [AVG_THROUGH, QUE_SIZE]
CU_METRICS      = ['DRB.PdcpTxBytes']
# L4S THRESHOLDS (SAME AS IN DU)
MIN_THRESH_DELAYS = 5
MAX_THRESH_DELAYS = 10
# CONNECT TO E2 NODE (wget 10.10.5.13:8080/ric/v1/get_all_e2nodes)
DU_NODE_ID = "gnbd_001_001_00019b_0"
CU_NODE_ID = "gnb_001_001_00019b"  




################################ CONTROL THREAD: SENDS CONTROLS ################################




class Controller():
    
    def __init__(self,dat_input,e2sm_rc,eve_stop: Event):
        self.stop   = eve_stop
        self.input  = dat_input
        self.sender = e2sm_rc


    def Start(self):
        while True:
            if self.stop.is_set(): # RECEIVED A STOP SIGNAL
                print("[!] Stopping controller thread %d ... Bye!"%current_thread().native_id)
                break
            try:
                ue_id, drb_id, mark_prob = self.input.get(timeout=1)
                self.sender.control_drb_qos(CU_NODE_ID, ue_id,drb_id,mark_prob,ack_request=1)
            except Exception as e:
                continue




################################ MAIN THREAD: LISTEN TO REPORTS ################################




class Get_Metrics(xAppBase):
    def __init__(self, config, http_server_port, rmr_port, dat_output, stop_threads):
        super(Get_Metrics, self).__init__(config, http_server_port, rmr_port)        
        # THREAD RELATED
        self.stop_control   = stop_threads
        self.qu_output      = dat_output
        # RELATED TO PERFS
        self.FirstReport    = 0
        self.LastReport     = 0
        self.Handled        = 0
        # TO PRINT
        self.display        = {}
        [self.display.setdefault(i,[]) for i in L4S + NL4S]


    ################################ HANDLES INCOMING REPORT ################################


    # COMPUTE MARKING PROBABILITY 
    def compute_mark_prob(self,queue_delay):
        proba = 0
        if queue_delay > MAX_THRESH_DELAYS: proba = 100
        elif queue_delay > MIN_THRESH_DELAYS: proba = (queue_delay - MIN_THRESH_DELAYS) * 100 / (MAX_THRESH_DELAYS - MIN_THRESH_DELAYS)
        return proba


    # HANDLE REPORT STYLE =2 (ONE UE)
    def report_for_one_ue(self,meas_data,timestamp):
        # RETRIEVE 
        queue_size  = meas_data["measData"][QUE_SIZE][0] # bytes
        avg_through = meas_data["measData"][AVG_THROUGH][0] # kbps
        # COMPUTE 
        if avg_through>0: 
            est_delay   = float(queue_size * 8) / avg_through # bits / kbps => ms
            mark_proba  = self.compute_mark_prob(est_delay)
        else:
            est_delay   = ""
            mark_proba  = 100
        # SEND
        ue_id   = 0
        drb_id  = 1
        self.qu_output.put((ue_id,drb_id,mark_proba))
        
        # SAVE L4S SPECIFIC METRICS (COMPUTED)
        self.display[ue_id].append((timestamp,"DRB.ComputedDelay",[est_delay]))
        self.display[ue_id].append((timestamp,"DRB.MarkProba",[mark_proba]))
        # SAVE GENERAL METRICS
        for metric_name, value in meas_data["measData"].items():
            # print("%s = "%(metric_name),value)
            self.display[ue_id].append((timestamp,metric_name,value))


    # HANDLE REPORT STYLE =5 (SEVERAL UEs)
    def report_for_several_ues(self,meas_data,timestamp):
        drb_id = 1
        
        # HANDLE L4S SIGNALS 
        for ue_id in L4S:
            # RETRIEVE 
            queue_size  = meas_data["ueMeasData"][ue_id]["measData"][QUE_SIZE][0]
            avg_through = meas_data["ueMeasData"][ue_id]["measData"][AVG_THROUGH][0]
            # COMPUTE 
            if avg_through>0: 
                est_delay   = float(queue_size * 8) / avg_through # bits / kbps => ms
                mark_proba = self.compute_mark_prob(est_delay)
            else:
                est_delay   = ""
                mark_proba  = 100
            # SAVE L4S SPECIFIC METRICS (COMPUTED)
            self.display[ue_id].append((timestamp,"DRB.ComputedDelay",[est_delay]))
            self.display[ue_id].append((timestamp,"DRB.MarkProba",[mark_proba]))  
            #print("[!] L4S QUEUE DELAY = %f ms / MARK PROB = %d "%(est_delay,mark_proba))
            # SEND
            drb_id  = 1
            self.qu_output.put((ue_id,drb_id,mark_proba))

        # SAVE GENERAL METRICS
        for ue_id, ue_meas_data in meas_data["ueMeasData"].items():
            for metric_name, value in ue_meas_data["measData"].items():
                # print("%s = "%(metric_name),value)
                self.display[ue_id].append((timestamp,metric_name,value))
            # # ADD QUEUE DELAY VALUE
            # queue_size  = meas_data["ueMeasData"][ue_id]["measData"][QUE_SIZE][0]
            # avg_through = meas_data["ueMeasData"][ue_id]["measData"][AVG_THROUGH][0]
            # if avg_through > 0:
            #     est_delay   = float(queue_size * 8) / avg_through # bits / kbps => ms
            # else:
            #     est_delay   = ""
            # self.display[ue_id].append((timestamp,"DRB.ComputedDelay",est_delay))  
    

    ################################ HANDLES INCOMING REPORT ################################


    # CALLED WHEN RECEIVING A RIC INDICATION MSG 
    def my_subscription_callback(self, e2_agent_id, subscription_id, indication_hdr, indication_msg, kpm_report_style, ue_id):
        # TIMESTAMP
        timestamp = time.time()

        # RELATED TO PERS
        if self.FirstReport == 0: self.FirstReport=timestamp # Init
        self.LastReport     = timestamp
        self.Handled        = self.Handled + 1 # Increment

        # RETRIEVES HEADER + DATA
        indication_hdr = self.e2sm_kpm.extract_hdr_info(indication_hdr)
        timestamp = indication_hdr['colletStartTime']
        meas_data = self.e2sm_kpm.extract_meas_data(indication_msg)

        # METRICS FOR ONE DRB / NO NEED TO AGGREGATE
        if kpm_report_style == 2:
            self.report_for_one_ue(meas_data,timestamp)
            
        # METRICS FOR SEVERAL DRBs
        if kpm_report_style in [3,4,5]:
            self.report_for_several_ues(meas_data,timestamp)


    ################################ SUBSCRIBES TO ################################


    # SUBSCRIBES TO METRICS `metrics` OF NODE `e2_node_id` FOR ALL UES `ue_ids`
    def subscribe_to(self,e2_node_id,ue_ids,metrics):
        # Fixed
        report_period = 50 # 1000 by default
        granul_period = 50 # 1000 by default
        
        if(len(ue_ids)==1):
            Service_Style = 2
            subscription_callback = lambda agent, sub, hdr, msg: self.my_subscription_callback(agent, sub, hdr, msg, Service_Style, None)
            print("Subscribe to E2 node ID: {}, RAN func: e2sm_kpm, Report Style: {}, UE_ids: {}, metrics: {}".format(e2_node_id, Service_Style, ue_ids, metrics))
            self.e2sm_kpm.subscribe_report_service_style_2(e2_node_id, report_period, ue_ids[0], metrics, granul_period, subscription_callback) # Report for one UE (DRB)
            # use always the same subscription callback, but bind kpm_report_style parameter
        elif len(ue_ids) > 1:
            Service_Style = 5
            subscription_callback = lambda agent, sub, hdr, msg: self.my_subscription_callback(agent, sub, hdr, msg, Service_Style, None)
            print("Subscribe to E2 node ID: {}, RAN func: e2sm_kpm, Report Style: {}, UE_ids: {}, metrics: {}".format(e2_node_id, Service_Style, ue_ids, metrics))
            self.e2sm_kpm.subscribe_report_service_style_5(e2_node_id, report_period, ue_ids, metrics, granul_period, subscription_callback) # Report for several UEs (DRBs)


    # Mark the function as xApp start function using xAppBase.start_function decorator.
    # It is required to start the internal msg receive loop.
    @xAppBase.start_function
    def start(self):
        # Subscription for L4S
        self.subscribe_to(DU_NODE_ID,L4S+NL4S,DU_METRICS)
        # Other metric from CU 
        #self.subscribe_to(CU_NODE_ID,L4S+NL4S,CU_METRICS)



    ################################ UNSUSCRIBES ################################
    
    
    # Unsuscribes
    def signal_handler(self, sig, frame):
        # PERFS
        last = self.LastReport-self.FirstReport
        print("[!] Handled %d reports in %f "%(self.Handled,last))
        if last > 0 : print("[!] This represents %f reports per second."%(self.Handled / last))
        
        # STOPS MARKING
        drb_id = 1
        mark_prob = 0
        for ue_id in L4S:
            self.qu_output.put((ue_id,drb_id,mark_prob))
        
        # DISPLAY STATS
        for ue_id,data in self.display.items():
            print("Handling UE %s"%ue_id)
            csvfile = open("Data/ue_%s.csv"%ue_id,'w')
            metrics_writer = csv.writer(csvfile, delimiter=',')
            # Format (timestamp, metric, values)
            for metrics in data:
                metrics_writer.writerow([metrics[0],metrics[1],metrics[2][0]]) # Usually the value is in a list `[]`
            csvfile.close()
        
        # UNSUSCRIBE
        self.stop_control.set()
        super().stop()





################################ LANCHES THREADS ################################


def control_thread(qu_input,sender,stop):
    controller = Controller(qu_input,sender,stop) # Class Instance
    control = Thread(target=controller.Start) # Thread
    control.start()


################################ MAIN ################################


# MAIN FUNCTION 
if __name__ == '__main__':

    # CONSTANT VALUES
    http_server_port = 8092
    rmr_port = 4562
    ran_func_id_kpm = 2
    ran_func_id_rc = 3


    # THREAD COMMUNICATION
    stop_threads = Event()
    queue_control = queue.Queue()
    # MAIN THREAD: READ METRICS
    print("[!] Main Thread: starting reading KPM Metrics")
    myXapp = Get_Metrics('', http_server_port, rmr_port,queue_control,stop_threads)
    myXapp.e2sm_kpm.set_ran_func_id(ran_func_id_kpm) # KPM
    myXapp.e2sm_rc.set_ran_func_id(ran_func_id_rc) # RC
    # LAUNCHES CONTROL THREAD
    print("[!] Launching 'control' thread")
    control_thread(queue_control,myXapp.e2sm_rc,stop_threads)


    # EXIT SIGNALS
    signal.signal(signal.SIGQUIT, myXapp.signal_handler)
    signal.signal(signal.SIGTERM, myXapp.signal_handler)
    signal.signal(signal.SIGINT, myXapp.signal_handler)
    # START XAPP
    myXapp.start()
    # Note: xApp will unsubscribe all active subscriptions at exit.
    # It also stops the threads (Prediction + Control)