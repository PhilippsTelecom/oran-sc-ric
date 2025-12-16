#!/usr/bin/env python3

from lib.xAppBase import xAppBase
import signal
import time
import numpy as np
import argparse

# SubModule
import Controller

# THREADING
from threading import Thread, Event, current_thread
import queue




# CONNECT TO E2 NODE (wget 10.0.2.13:8080/ric/v1/get_all_e2nodes)
CU_NODE_ID          = "gnb_001_001_00019b"      # "gnb_001_001_00019b"
DU_NODE_ID          = "gnbd_001_001_00019b_1"   # "gnbd_001_001_00019b_0"
QUE_DEL             = 'DRB.RlcSduLastDelayDl'
HTTP_SERVER_PORT    = 8092
RMR_PORT            = 4562
RAN_FUNC_ID_KPM     = 2
RAN_FUNC_ID_RC      = 3




################################ MAIN THREAD: LISTEN TO REPORTS ################################




class Get_Metrics(xAppBase):
    def __init__(self, config, ues_index_, kpm_period_, http_server_port, rmr_port, dat_output, stop_threads):
        super(Get_Metrics, self).__init__(config, http_server_port, rmr_port)   
        # DISTINGUISHING UEs
        self.l4s_ues        = ues_index_
        self.kpm_period     = kpm_period_
        # THREAD RELATED
        self.stop_control   = stop_threads
        self.qu_output      = dat_output
        # RELATED TO PERFS
        self.FirstReport    = 0
        self.LastReport     = 0
        self.Handled        = 0
        self.InterArrivals  = []


    ################################ HANDLES INCOMING REPORT ################################


    # HANDLE REPORT STYLE =2 (ONE UE)
    def report_for_one_ue(self,meas_data,time_related:tuple):
        timestamp,start = time_related
        ue_id   = 0
        # COMPUTE PROBA
        queue_delay = meas_data["measData"][QUE_DEL][0]
        # SEND
        drb_id  = 1
        self.qu_output.put((start,CU_NODE_ID,ue_id,drb_id,queue_delay))


    # HANDLE REPORT STYLE =5 (SEVERAL UEs)
    def report_for_several_ues(self,meas_data,time_related:tuple):
        timestamp,start = time_related
        drb_id = 1
        
        # HANDLE L4S SIGNALS 
        for ue_id in self.l4s_ues:
            # COMPUTE
            queue_delay = meas_data["ueMeasData"][ue_id]["measData"][QUE_DEL][0]
            # SEND
            drb_id  = 1
            self.qu_output.put((start,CU_NODE_ID,ue_id,drb_id,queue_delay))
    

    ################################ HANDLES INCOMING REPORT ################################


    # CALLED WHEN RECEIVING A RIC INDICATION MSG 
    def my_subscription_callback(self, e2_agent_id, subscription_id, indication_hdr, indication_msg, kpm_report_style, ue_id):
        # TIMESTAMP
        start = time.time()

        # RELATED TO PERS
        if self.FirstReport == 0: self.FirstReport=start # Init
        # BUNDLING
        else: self.InterArrivals.append(start - self.LastReport)
        self.LastReport     = start
        self.Handled        = self.Handled + 1 # Increment

        # RETRIEVES HEADER + DATA
        indication_hdr = self.e2sm_kpm.extract_hdr_info(indication_hdr)
        timestamp = indication_hdr['colletStartTime']
        meas_data = self.e2sm_kpm.extract_meas_data(indication_msg)

        # METRICS FOR ONE DRB / NO NEED TO AGGREGATE
        if kpm_report_style == 2:
            self.report_for_one_ue(meas_data,(timestamp,start))
            
        # METRICS FOR SEVERAL DRBs
        if kpm_report_style in [3,4,5]:
            self.report_for_several_ues(meas_data,(timestamp,start))


    ################################ SUBSCRIBES TO ################################


    # SUBSCRIBES TO METRICS `metrics` OF NODE `e2_node_id` FOR ALL UES `ue_ids`
    def subscribe_to(self,e2_node_id,ue_ids,metrics):
        # Fixed
        report_period = self.kpm_period # 1000 by default
        granul_period = self.kpm_period # 1000 by default
        
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
        # Subscription
        self.subscribe_to(DU_NODE_ID, self.l4s_ues ,[QUE_DEL])


    ################################ UNSUSCRIBES ################################
    
    
    # Unsuscribes
    def signal_handler(self, sig, frame):
        # PERFS
        last = self.LastReport-self.FirstReport
        print("[!] Handled %d reports in %f "%(self.Handled,last))
        if last > 0 : print("[!] This represents %f reports per second."%(self.Handled / last))
        # BUNDLING 
        print("[!] Related to bundling: mean IAT ~ %f / var IAT ~ %f / min IAT = %f / max IAT = %f "%(np.mean(self.InterArrivals),np.var(self.InterArrivals),np.min(self.InterArrivals),np.max(self.InterArrivals)))
        
        # STOPS MARKING
        drb_id  = 1
        rc_id   = -1
        for ue_id in self.l4s_ues:
            self.qu_output.put((0,CU_NODE_ID,ue_id,drb_id,rc_id))
        
        # UNSUSCRIBE
        self.stop_control.set()
        super().stop()




################################ LANCHES THREADS ################################


def control_thread(thread_related:tuple,l4s_thresh:tuple,sender): 
    controller = Controller.Controller(thread_related,l4s_thresh,sender)   # Class Instance
    control = Thread(target=controller.Start)                               # Thread
    control.start()


################################ MAIN ################################


# MAIN FUNCTION 
if __name__ == '__main__':
    parser  = argparse.ArgumentParser(description='Marking xApp _ PERIODICAL')

    parser.add_argument("--l4s_ue_id", nargs='*',type=int, default=0, help="L4S UE ID")
    parser.add_argument("--l4s_min_thr", nargs='*',type=float, default=0, help="L4S minimum marking threshold")
    parser.add_argument("--l4s_max_thr", nargs='*',type=float, default=0, help="L4S maximum marking threshold")
    parser.add_argument("--kpm_period", nargs='*',type=int, default=10, help="KPM reporting period")
    
    args        = parser.parse_args()
    L4S         = [] if not args.l4s_ue_id else args.l4s_ue_id
    KPM_period  = args.kpm_period
    KPM_min_thr = args.l4s_min_thr
    KPM_max_thr = args.l4s_max_thr


    # THREAD COMMUNICATION
    stop_threads    = Event()
    queue_control   = queue.Queue()
    # MAIN THREAD: READ METRICS
    print("[!] Main Thread: starting reading Metrics")
    myXapp = Get_Metrics('', L4S, KPM_period, HTTP_SERVER_PORT, RMR_PORT, queue_control, stop_threads)
    myXapp.e2sm_kpm.set_ran_func_id(RAN_FUNC_ID_KPM)    # KPM / DU
    myXapp.e2sm_rc.set_ran_func_id(RAN_FUNC_ID_RC)      # RC / CU
    # LAUNCHES CONTROL THREAD
    print("[!] Launching 'control' thread")
    control_thread((queue_control,stop_threads),(KPM_min_thr,KPM_max_thr),myXapp.e2sm_rc)


    # EXIT SIGNALS
    signal.signal(signal.SIGQUIT, myXapp.signal_handler)
    signal.signal(signal.SIGTERM, myXapp.signal_handler)
    signal.signal(signal.SIGINT, myXapp.signal_handler)
    # START XAPP
    myXapp.start()
    # Note: xApp will unsubscribe all active subscriptions at exit.
    # It also stops the threads (Prediction + Control)