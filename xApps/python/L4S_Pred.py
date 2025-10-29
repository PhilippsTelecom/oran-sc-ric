#!/usr/bin/env python3

from lib.xAppBase import xAppBase
import signal
import time
import numpy as np
from itertools import chain
import csv
import os

# SubModules
import Data_Preparation
import Predictor
import Controller

# THREADING
from threading import Thread, Event
import queue


# METRIC CATEGORIES
DRB_METRICS = [
    'DRB.RlcSduLastDelayDl', 'DRB.UEThpDl','DRB.RlcPacketDropRateDl','DRB.RlcSduTransmittedVolumeDL','DRB.RlcStateDL',
    'DRB.RlcDelayUl','DRB.UEThpUl','DRB.RlcSduTransmittedVolumeUL'] # 8 
RRU_METRICS = [
    'RRU.PrbAvailDl','RRU.PrbUsedDl','RRU.PrbTotDl',
    'RRU.PrbAvailUl','RRU.PrbUsedUl','RRU.PrbTotUl'] # 6
MISC_METRICS = [
    'CQI','RSRP','RSRQ','RACH.PreambleDedCell'] # 4
CU_METRIC = ['DRB.PdcpTxBytes'] # 1
ALL_METRICS     = DRB_METRICS + RRU_METRICS + MISC_METRICS 
METRICS_L4S     = ['DRB.RlcSduLastDelayDl','CQI']
METRICS_AGG     = ['DRB.RlcSduLastDelayDl']
WINDOW          = 10 # context used to do prediction 


# CONNECT TO E2 NODE (wget 10.10.5.13:8080/ric/v1/get_all_e2nodes)
DU_NODE_ID = "gnbd_001_001_00019b_1"
CU_NODE_ID = "gnb_001_001_00019b" 


# HOW TO AGGREGATE DATA: AGG ID <-> UEs
CONF = { 1: [0,1,2]
}
L4S     = [0]                                                       # L4S UEs
NL4S    = list(set(chain.from_iterable(CONF.values())) - set(L4S))  # Non-L4S UEs
AGGS    = CONF.keys()




################################ MAIN THREAD: LISTEN TO REPORTS ################################




class Get_Metrics(xAppBase):
    def __init__(self, config,http_server_port, rmr_port, dat_output, stop_threads):
        super(Get_Metrics, self).__init__(config, http_server_port, rmr_port)
        # HANDLING DATA
        self.PreProc        = Data_Preparation.Pre_Process((L4S,METRICS_L4S),(NL4S,METRICS_AGG))    # Data Cleaning
        self.Arrange        = Data_Preparation.Arrange_Data((L4S,METRICS_L4S),(CONF,METRICS_AGG))   # Aggregates UEs
        self.Smooth         = Data_Preparation.Smooth(10,(L4S,METRICS_L4S),(AGGS,METRICS_AGG))      # Data Smoothing
        # SAVING TEMPORAL WINDOW
        self.l4s_wind       = {key: (np.zeros((WINDOW,len(METRICS_L4S))),0,False) for key in L4S}
        self.agg_wind       = {key: (np.zeros((WINDOW,len(METRICS_AGG)*3)),0,False) for key in AGGS}
        # THREAD RELATED
        self.stop_control   = stop_threads
        self.qu_output      = dat_output
        # RELATED TO PERFS
        self.times          = []
        self.nbReports      = 0 
        self.datFrstReport  = 0
        self.datLastReport  = 0
        # RELATED TO GRAPHS
        self.display        = {}
        [self.display.setdefault(i,[]) for i in L4S + NL4S]



    ################################ HANDLES INCOMING REPORT ################################


    # HANDLE REPORT STYLE =2 (ONE UE - WE SUPPOSE IT IS L4S)
    def report_for_one_ue(self,meas_data,time_related:tuple):
        print("[!]\tThere is only one UE, this xApp won't do anything")
        pass
        

    ################################ HANDLES SEVERAL UES ################################


    def save_new_samples(self,x_l4s,x_agg):
        """
            Save new samples into internal windows
            For all L4S DRBs, all L4S metrics
            For all AGG, all AGG metrics * 3 (aggregation functions)
        """
        all_aggs_window = True

        # Handle L4S
        nb_l4s_metrics = len(METRICS_L4S)
        for i,ue_id in enumerate(L4S): 
            ind_strt_report         = i*nb_l4s_metrics
            wind, index, full_wind  = self.l4s_wind[ue_id]
            # Retrieve and save last report
            new_reports             = x_l4s[0,ind_strt_report:ind_strt_report + nb_l4s_metrics] # Metrics of 'ue_id'
            wind[index]             = new_reports
            # Update index and save
            index                   = (index + 1) % WINDOW
            full_wind               = full_wind or index==0
            self.l4s_wind[ue_id]    = (wind, index, full_wind)
        # Handle AGG
        nb_agg_metrics      = len(METRICS_AGG)
        nb_agg_functions    = 3
        for i,agg_id in enumerate(AGGS): 
            ind_strt_report         = i*nb_agg_metrics*nb_agg_functions
            wind, index, full_wind  = self.agg_wind[agg_id]
            # Retrieve and save last report
            new_reports             = x_agg[0,ind_strt_report:ind_strt_report + nb_agg_metrics * nb_agg_functions] # Metrics of 'agg_id'
            wind[index]             = new_reports
            # Update index and save
            index                   = (index + 1) % WINDOW
            full_wind               = full_wind or index==0
            all_aggs_window         = all_aggs_window and full_wind
            self.agg_wind[agg_id]   = (wind, index, full_wind)

        return all_aggs_window


    def retrieve_window_aggs(self):
        """
           Takes all aggregator windows (in the right order) and concatenates them
        """
        nb_aggs             = len(AGGS)
        nb_agg_metrics      = len(METRICS_AGG)
        nb_agg_functions    = 3
        # Pre-allocation: big array to put all agregators
        X_AGG = np.zeros((WINDOW,nb_aggs * nb_agg_metrics * nb_agg_functions))

        for i,agg_id in enumerate(AGGS):
            wind, index, full_wind  = self.agg_wind[agg_id]
            idx = i*nb_agg_metrics*nb_agg_functions
            X_AGG[0:(WINDOW-index), idx:idx+nb_agg_metrics*nb_agg_functions] = wind[index:,:]   # First part (From index to end window)
            X_AGG[(WINDOW-index):, idx:idx+nb_agg_metrics*nb_agg_functions] = wind[0:index,:]   # Second part (From 0 to index)
        
        return X_AGG


    def browse_l4s_drbs(self,agg_metrics,tim_received):
        """
            This method triggers a prediction for each L4S DRB having full window
            'Index' is the place where to insert the next report (contains the oldest report)
        """
        l4s_drb = np.zeros((WINDOW,len(METRICS_L4S))) # Features of one L4S DRB
        for l4s in self.l4s_wind.keys():
            arr, index, full_window = self.l4s_wind[l4s]
            if full_window: # Can make prediction 
                l4s_drb[0:(WINDOW-index),:] = arr[index:,:]
                l4s_drb[(WINDOW-index):,:] = arr[:index]
                # Send prediction to prediction Thread
                drb_id = 1
                self.qu_output.put((tim_received,CU_NODE_ID,l4s,drb_id,l4s_drb.copy(),agg_metrics.copy()))


    def save_metrics_plot(self,meas_data:dict,timestamp):
        for ue_id, ue_meas_data in meas_data["ueMeasData"].items():
            for metric_name, value in ue_meas_data["measData"].items():
                self.display[ue_id].append((timestamp,metric_name,value))


    # HANDLE REPORT STYLE =5 (SEVERAL UEs)
    def report_for_several_ues(self,meas_data,time_related:tuple):
        """
            Handles a KPM report related to several UEs (>1)
        """
        tim_Header,tim_Received = time_related

        # (I) HANDLE 'NANS' AND 'INFS' (PRE-PROCESSING)
        meas_data   = self.PreProc.handle_nans(meas_data)
        meas_data   = self.PreProc.handle_infs(meas_data)
        self.PreProc.update_last_values(meas_data)

        # (II) SAVE DATA TO PLOT
        self.save_metrics_plot(meas_data,tim_Header)

        # (III) ARRANGE VALUES: AGGREGATION + REGROUP L4S FEATS (at time 't')
        X_l4s = self.Arrange.handle_l4s(meas_data)
        X_agg = self.Arrange.handle_aggregators(meas_data)

        # (IV) SMOOTH VALUES
        X_l4s = self.Smooth.moving_average_l4s(X_l4s)
        X_agg = self.Smooth.moving_average_agg(X_agg)

        # (V) INFERENCE (one prediction per L4S DRB)
        all_agg_full_window = self.save_new_samples(X_l4s,X_agg)
        if all_agg_full_window:
            X_AGG = self.retrieve_window_aggs()         # All data for aggregators
            self.browse_l4s_drbs(X_AGG,tim_Received)    # Triggers predictions


    ################################ SUBSCRIBES TO ################################


    # JUST TO KNOW PERFORMANCES
    def performances(self,timestamp):
        # INIT
        if self.nbReports == 0:
            self.datFrstReport = timestamp
        # UPDATE
        self.nbReports = self.nbReports +1
        self.datLastReport  = timestamp


    # CALLED WHEN RECEIVING A RIC INDICATION MSG 
    def my_subscription_callback(self, e2_agent_id, subscription_id, indication_hdr, indication_msg, kpm_report_style, ue_id):
        # Assess perfs
        start = time.time()
        self.performances(start)

        # Retrieves header + data
        meas_data       = self.e2sm_kpm.extract_meas_data(indication_msg)
        indication_hdr  = self.e2sm_kpm.extract_hdr_info(indication_hdr)
        timestamp       = indication_hdr['colletStartTime']
        #print(timestamp)

        # METRICS FOR ONE DRB / NO NEED TO AGGREGATE
        if kpm_report_style == 2:
            print("[!] Only one UE, the xApp won't do anything ")
            self.report_for_one_ue(meas_data,(timestamp,start))
            
        # METRICS FOR SEVERAL DRBs
        if kpm_report_style in [3,4,5]:
            self.report_for_several_ues(meas_data,(timestamp,start))

        # CF PROCESSING DELAY
        end = time.time()
        self.times.append(end - start)


    # SUBSCRIBES TO METRICS `metrics` OF NODE `e2_node_id` FOR ALL UES `ue_ids`
    def subscribe_to(self,e2_node_id,ue_ids,metrics):
        # Fixed
        report_period = 10 # 1000 by default
        granul_period = 10 # 1000 by default
        
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
        # SUSCRIPTION FOR ALL UEs
        ALL_UEs = list(chain.from_iterable(CONF.values()))
        self.subscribe_to(DU_NODE_ID,ALL_UEs,ALL_METRICS)
        #self.subscribe_to(CU_NODE_ID,ALL_UEs,CU_METRIC)
        


    ################################ UNSUSCRIBES ################################
    
    
    # Unsuscribes
    def signal_handler(self, sig, frame):
        length = self.datLastReport - self.datFrstReport
        print("[!]\t[L4S_Pred]: Time between 1st and last reports = %f, total reports = %d, mean reports per second = %f"%(length,self.nbReports,self.nbReports/length))
        
        # SAVE STATS
        for ue_id,data in self.display.items():
            print("Handling UE %s"%ue_id)
            if not os.path.isdir('./Data/'):
                os.mkdir('Data/')
            csvfile = open("Data/ue_%s.csv"%ue_id,'w')
            metrics_writer = csv.writer(csvfile, delimiter=',')
            # Format (timestamp, metric, values)
            for metrics in data:
                metrics_writer.writerow([metrics[0],metrics[1],metrics[2][0]]) # Usually the value is in a list `[]`
            csvfile.close()

        # SHOW PROCESSING TIME
        if len(self.times) > 0:
            print("[!]\t[Main]: Handled %d E2SM-KPM reports"%len(self.times))
            print("[!]\t[Main]: Mean processing time (pre-processing + aggregation) ~= %f"%(sum(self.times)/len(self.times)))

        # Unsuscribe
        self.stop_control.set()
        super().stop()


################################ LANCHES THREADS ################################


def prediction_threads(thread_related:tuple,prediction_related:tuple):
    """
        Launches the classes used to do the prediction (1 thread per class)
        Arguments: 
            - thread_related: queue to communicate + event to stop all threads
            - prediction_related: path to model + scaler (to load) + index metric to predict
    """
    NB_THREADS=1
    
    print("[!] Launching %d threads to predict "%NB_THREADS)
    for i in range(NB_THREADS):
        instance = Predictor.LightGBM(thread_related,prediction_related)    # Class Instance
        thread = Thread(target=instance.Start)                              # Thread
        thread.start()


def control_thread(thread_related:tuple,sender):
    controller = Controller.Controller(thread_related,sender)   # Class Instance
    control = Thread(target=controller.Start)                   # Thread
    control.start()


################################ MAIN ################################


# MAIN FUNCTION 
if __name__ == '__main__':

    # CONSTANT VALUES
    # E2SM
    http_server_port = 8092
    rmr_port = 4562
    ran_func_id_kpm = 2
    ran_func_id_rc = 3
    # PREDICTION
    path_trained_model  ="./Model/lgb-steps-4.model"
    path_trained_scaler ="./Model/scaler-lgb-steps-4.sav"
    kpm_to_pred="DRB.RlcSduLastDelayDl"
    ind_to_pred=METRICS_L4S.index(kpm_to_pred)


    # THREAD COMMUNICATION
    stop_threads = Event()
    queue_predict = queue.Queue()
    queue_control = queue.Queue()
    # MAIN THREAD: READ METRICS
    print("[!] Main Thread: starting reading KPM Metrics")
    myXapp = Get_Metrics('', http_server_port, rmr_port,queue_predict,stop_threads)
    myXapp.e2sm_kpm.set_ran_func_id(ran_func_id_kpm)    # KPM
    myXapp.e2sm_rc.set_ran_func_id(ran_func_id_rc)      # RC
    # LAUNCHES PREDICTION THREADS
    print("[!] Launching 'predicting' threads")
    prediction_threads((queue_predict,queue_control,stop_threads),(path_trained_model,path_trained_scaler,ind_to_pred))
    # LAUNCHES CONTROL THREAD
    print("[!] Launching 'control' thread")
    control_thread((queue_control,stop_threads),myXapp.e2sm_rc)


    # EXIT SIGNALS
    signal.signal(signal.SIGQUIT, myXapp.signal_handler)
    signal.signal(signal.SIGTERM, myXapp.signal_handler)
    signal.signal(signal.SIGINT, myXapp.signal_handler)
    # START XAPP
    myXapp.start()
    # Note: xApp will unsubscribe all active subscriptions at exit.
    # It also stops the threads (Prediction + Control)
