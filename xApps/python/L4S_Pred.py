#!/usr/bin/env python3

from lib.xAppBase import xAppBase
import signal
import time
import pandas as pd
import numpy as np
import math 
# THREADING
from threading import Thread, Event, current_thread
import queue
# LOAD MODELS
import joblib
import tensorflow as tf
import keras # 2.13.1 (Must be same as Predict.py)




# CONSTANT
SAV_SCALER="./Model/min_max"
SAV_MODEL ="./Model/lstm.keras"
# METRIC CATEGORIES
DRB_METRICS     = [
    'DRB.RlcSduDelayDl','DRB.UEThpDl','DRB.RlcPacketDropRateDl','DRB.RlcSduTransmittedVolumeDL',
    'DRB.RlcDelayUl','DRB.UEThpUl','DRB.AirIfDelayUl','DRB.RlcSduTransmittedVolumeUL']
RRU_METRICS     = [
    'RRU.PrbAvailDl','RRU.PrbUsedDl','RRU.PrbTotDl',
    'RRU.PrbAvailUl','RRU.PrbUsedUl','RRU.PrbTotUl']
MISC_METRICS    = [
    'CQI','RSRP','RSRQ','RACH.PreambleDedCell']
ALL_METRICS     = DRB_METRICS + RRU_METRICS + MISC_METRICS 
# PRE-PROCESSING
METRICS_NAN_ZERO= ['CQI','DRB.RlcSduDelayDl','DRB.RlcDelayUl','DRB.AirIfDelayUl'] # METRICS WHERE NAN CAN BE REPLACED BY '0'
METRICS_NAN_ZERO_DRB= list(set(DRB_METRICS) & set(METRICS_NAN_ZERO))
METRICS_NAN_PREV= list(set(ALL_METRICS) - set(METRICS_NAN_ZERO)) # OTHER METRICS
METRICS_NAN_PREV_DRB= list(set(DRB_METRICS) & set(METRICS_NAN_PREV))
# RELATED TO PREDICTION
METRIC_TO_PRED  = 'DRB.RlcSduDelayDl'
COL_INDEX = ALL_METRICS.index(METRIC_TO_PRED)
WINDOW          = 10
# L4S THRESHOLDS
TH_MIN = 2
TH_MAX = 10
# CONNECT TO E2 NODE (wget 10.10.5.13:8080/ric/v1/get_all_e2nodes)
DU_NODE_ID = "gnbd_001_001_00019b_0"
CU_NODE_ID = "gnb_001_001_00019b"  
# HOW TO AGGREGATE DATA: UE ID <-> AGGREGATOR
CONFIG          = {0:1,
    1:1
}
# L4S UEs
L4S = [0]




################################ PREDICTION THREADS ################################




class Predictor():


    def __init__(self,dat_input,dat_output,eve_stop):
        # LOAD SCALER TO RESCALE DELAYS
        self.scaler =  joblib.load(SAV_SCALER)
        self.val_min = self.scaler.data_min_[COL_INDEX]
        self.val_max = self.scaler.data_max_[COL_INDEX]
        # LOAD LSTM MODEL
        self.model = keras.models.load_model(SAV_MODEL)
        # THREADS RELATED
        self.stop = eve_stop
        self.input = dat_input
        self.output = dat_output
        # AGG IDs
        self.aggregators = list(set(CONFIG.values())) 
        # PERFS
        self.times = []


    # PRE-PROCESS DATA: HANDLES NAN, ETC. FOR AGGREGATED AND L4S DFs
    def replace(self,dataFrame,replace_by_zero,replace_by_prev):
        # Replace by 0
        for mu in replace_by_zero: 
            dataFrame[mu] = dataFrame[mu].fillna(0)
        # Replace by prev
        for mu in replace_by_prev: 
            curr_ind=0
            last_val=0
            # BROWSE ALL ITS ROWS
            for row in dataFrame[mu]:
                if not math.isnan(row):# Not a NaN: save value
                    last_val = row
                else:# Have a NaN: retrieve last good value
                    dataFrame[mu].iloc[curr_ind] = last_val
                curr_ind+=1
        # TYPE OF VALUES
        dataFrame = dataFrame.astype(float)
        return dataFrame


    def pre_process(self,l4s_df,agg_df):
        # HANDLE NANs
        # > l4s
        replace_by_zero = METRICS_NAN_ZERO
        replace_by_prev = METRICS_NAN_PREV
        l4s_df = self.replace(l4s_df,replace_by_zero,replace_by_prev)
        # > agg
        replace_by_zero = [ "%s-%d"%(measure,drb) for drb in self.aggregators for measure in METRICS_NAN_ZERO_DRB ] 
        replace_by_prev = [ "%s-%d"%(measure,drb) for drb in self.aggregators for measure in METRICS_NAN_PREV_DRB ]
        agg_df = self.replace(agg_df,replace_by_zero,replace_by_prev)

        # CONCATENATE L4S + MERGED AGG DF 
        X_df = pd.concat([l4s_df,agg_df], axis = 1)
        
        # SCALING (MinMax Scaler)
        X_df = pd.DataFrame(self.scaler.transform(X_df.values),columns=X_df.columns)

        return X_df


    def re_scale(self,delay):
        return delay * (self.val_max - self.val_min) + self.val_min


    def compute_proba(self,delay):
        # TESTS ON DELAY
        mark_prob=0
        if delay > TH_MAX:
            mark_prob = 100
        elif delay > TH_MIN:
            mark_prob = (delay - TH_MIN) / (TH_MAX - TH_MIN) * 100 # Linear Probability
        # RETURN PROBA
        return mark_prob
        

    def Start(self):
        while True:
            if self.stop.is_set(): # RECEIVED A STOP SIGNAL
                print("[!] Stopping predicting thread %d ... Bye!"%current_thread().native_id)
                if len(self.times) > 0:
                    print("[!] Thread made %d classifications"%(len(self.times)))
                    print("[!] Perfs: Mean Inference Time = %f "%(sum(self.times)/len(self.times)))
                    for el in self.times: print(el.numpy())
                break
            try:
                # RETRIEVE VALUES
                ue_id, drb_id, l4s_df, agg_df = self.input.get(timeout=1)
                # PERFORMANCES: INFERENCE TIME
                start = tf.timestamp() 
                # PREPROCESSING
                X_df = self.pre_process(l4s_df,agg_df)
                # PREDICTION
                l4s_delay=self.model.predict(np.array([X_df]))[0][0]
                # GET NORMAL VALUE (CF SCALER)
                l4s_delay = 0
                l4s_delay = self.re_scale(l4s_delay)
                # print("[!] val_max = %f ; val_min = %f "%(self.val_max,self.val_min))
                # print("[!] Predicted Delay = %f "%l4s_delay)
                # COMPUTE PROBA
                mark_prob = self.compute_proba(l4s_delay)
                # SENDS TO CONTROL THREAD
                mark_prob = 0
                self.output.put((ue_id,drb_id,mark_prob))
                # print("[!] We put something in the queue -> queue size = %d"%self.output.qsize())
                
                # PERFORMANCES: INFERENCE TINE
                end = tf.timestamp() # Perfs
                self.times.append(end-start)
            except Exception as e:
                continue




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
        # DATABASES
        self.l4s_drb = {key: (pd.DataFrame(columns=ALL_METRICS),0) for key in L4S } # KEY = UE_ID / VALUE = DATA FRAME
        self.agg_drb = {key: (pd.DataFrame(columns=[ "%s-%d"%(metric,key) for metric in DRB_METRICS]),0) for key in CONFIG.values()} # KEY = AGG_DRB / VALUE = AGGREGATED DATA FRAME
        # THREAD RELATED
        self.stop_control   = stop_threads
        self.qu_output      = dat_output
        # RELATED TO PERFS
        self.nbReports      = 0 
        self.datFrstReport  = 0
        self.datLastReport  = 0

    ################################ AGGREGATION + MERGING ################################


    # AGGREGATE DATA: FROM SEVERAL ROWS TO ONE ROW
    def aggregate(self,dataF):
        # AGGREGATED VALUES
        values = []
        for mu in DRB_METRICS:
            # MEAN FOR EACH UE
            if mu in ['DRB.RlcPacketDropRateDl','DRB.RlcSduDelayDl','DRB.RlcDelayUl']:
                values.append(dataF[mu].mean(axis=0)) # mean for each column
            # SUM FOR EACH UE
            else:
                values.append(dataF[mu].sum(axis=0))
        # DATA FRAME JUST AGG VALUES
        dataF = dataF.drop(dataF.index)
        dataF.loc[0] = values
        return dataF


    # TAKE ALL DATAFRAMES, PUT IN RIGHT ORDER (cf INDEX) AND MERGE INTO A SINGLE ONE
    def Merge_agg_drb(self):
        Ret = pd.DataFrame()
        for aggregDRB in self.agg_drb.keys():
            # RETRIEVE DATA
            time_series = self.agg_drb[aggregDRB][0]
            current_idx = self.agg_drb[aggregDRB][1]
            # EXTRACT DATAFRAME FROM BEGINNING + END => ORDER TIME SERIES
            start = time_series.iloc[current_idx:]
            end = time_series.iloc[0:current_idx]
            res = pd.concat([start,end])
            # ADD ORDERED DF IN MERGED ONE (NEW COLUMNS)
            Ret = pd.concat([Ret,res],axis = 1)
        return Ret
 

    ################################ TEST IF OK TO PREDICT ################################


    # SAYS IF WE HAVE ENOUGH TEMPORAL WINDOWS FOR 'AGG_DRB'
    def isOK_ts_aggDrb(self):
        # RETURN VALUE
        ret = True
        # FEATURES (AGG_DRB)
        drb_id=0
        list_agg_drb = list(self.agg_drb.keys())
        # BROWSE ALL FEATURES
        while ret and (drb_id < len(list_agg_drb)): 
            nbRows = self.agg_drb[list_agg_drb[drb_id]][0].shape[0]
            ret = (nbRows == WINDOW)
            drb_id = drb_id + 1
        return ret


    # SAYS IF WE HAVE ENOUGH TEMPORAL WINDOWS FOR CURRENT 'L4S'
    def isOK_ts_l4s(self,l4s_df):
        return (l4s_df.shape[0] == WINDOW)


    ################################ HANDLES INCOMING REPORT ################################


    # HANDLE REPORT STYLE =2 (ONE UE)
    def report_for_one_ue(self,meas_data):
        # NB METRICS TO DIFFERENTIATE BETWEEN L4S AND OTHER DRBs
        nbm = len(meas_data['measData'].keys())
        # NON L4S DRB
        if( nbm == len(DRB_METRICS) ):
            print("Concurrent, one UE")
            # NO USER ID: IMPOSSIBLE TO KNOW 'AGG_INDEX'
            agg_index = 1
            # RETRIEVE SAVED FEATURES (USED FOR PREDICTION)
            time_series = self.agg_drb[agg_index][0]
            current_idx = self.agg_drb[agg_index][1]
            # SAVE AGGREGATED METRICS IN FEATURES
            time_series.loc[current_idx] = pd.DataFrame(meas_data["measData"]).loc[0]
            current_idx = (current_idx + 1) % WINDOW
            self.agg_drb[agg_index] = (time_series,current_idx)
        # L4S DRB 
        else:
            print("L4S, one UE")
            ue_id = L4S[0]
            # RETRIEVE SAVED FEATURES
            time_series = self.l4s_drb[ue_id][0]
            current_idx =  self.l4s_drb[ue_id][1]
            # SAVE METRICS IN FEATURES
            time_series.loc[current_idx] = pd.DataFrame(meas_data["measData"]).loc[0]
            current_idx = (current_idx + 1) % WINDOW
            self.l4s_drb[ue_id]=(time_series,current_idx)
            # IF WE CAN PREDICT 
            if self.isOK_ts_aggDrb(): # Concurrent traffic
                agg_df = self.Merge_agg_drb()
                if self.isOK_ts_l4s(self.l4s_drb[ue_id][0]): # L4S traffic
                    print("PREDICT - L4S ONE UE")
                    # PREDICTION
                    drb_id = 1
                    self.qu_output.put((ue_id,drb_id,self.l4s_drb[ue_id][0].copy(),agg_df.copy()))


    # HANDLE REPORT STYLE =5 (SEVERAL UEs)
    def report_for_several_ues(self,meas_data):
        # TEST IF FOR L4S OR NOT
        first_ue = list(meas_data["ueMeasData"].keys())[0]
        agg_index = CONFIG[first_ue]
        # REPORTING NON L4S DRB
        if agg_index != 0:
            print("Concurrent, several UEs")
            # RETRIEVE SAVED FEATURES (USED FOR PREDICTION)
            time_series = self.agg_drb[agg_index][0]
            current_idx = self.agg_drb[agg_index][1]
            # RETRIEVE METRICS IN REPORT (AS MANY ROWS AS UEs)
            df = pd.DataFrame()
            for ue_id, ue_meas_data in meas_data["ueMeasData"].items():
                df = pd.concat([df,pd.DataFrame(ue_meas_data["measData"])]) # Several rows
            # AGGREGATE METRICS -> ONE ROW
            df = self.aggregate(df)
            # SAVE AGGREGATED METRICS IN FEATURES
            time_series.loc[current_idx] = df.loc[0]
            current_idx = (current_idx + 1) % WINDOW
            self.agg_drb[agg_index] = (time_series,current_idx)
        # REPORTING L4S DRBS
        else:
            print("L4S, several UEs")
            # SAVE L4S METRICS OF ALL L4S UEs
            for ue_id, ue_meas_data in meas_data["ueMeasData"].items():
                # RETRIEVE SAVED FEATURES
                time_series = self.l4s_drb[ue_id][0]
                current_idx =  self.l4s_drb[ue_id][1]
                # SAVE METRICS IN FEATURES
                time_series.loc[current_idx] = pd.DataFrame(ue_meas_data["measData"]).loc[0]
                current_idx = (current_idx + 1) % WINDOW
                self.l4s_drb[ue_id]=(time_series,current_idx)
            # IF WE CAN PREDICT 
            if self.isOK_ts_aggDrb(): # Concurrent traffic
                agg_df = self.Merge_agg_drb()
                for ue_id in meas_data["ueMeasData"].keys():
                    if self.isOK_ts_l4s(self.l4s_drb[ue_id][0]): # L4S trafic
                        print("PREDICT - MULTI UE")
                        # PREDICTION
                        drb_id = 1
                        self.qu_output.put((ue_id,drb_id,self.l4s_drb[ue_id][0].copy(),agg_df.copy()))


    # JUST TO KNOW PERFORMANCES
    def performances(self):
        timestamp = time.time()
        # INIT
        if self.nbReports == 0:
            self.datFrstReport = timestamp
        # UPDATE
        self.nbReports = self.nbReports +1
        self.datLastReport  = timestamp


    # CALLED WHEN RECEIVING A RIC INDICATION MSG 
    def my_subscription_callback(self, e2_agent_id, subscription_id, indication_hdr, indication_msg, kpm_report_style, ue_id):
        # TEMPORARY
        self.performances()

        # Retrieves header + data
        indication_hdr = self.e2sm_kpm.extract_hdr_info(indication_hdr)
        meas_data = self.e2sm_kpm.extract_meas_data(indication_msg)

        # METRICS FOR ONE DRB / NO NEED TO AGGREGATE
        if kpm_report_style == 2:
            self.report_for_one_ue(meas_data)
            
        # METRICS FOR SEVERAL DRBs
        if kpm_report_style in [3,4,5]:
            self.report_for_several_ues(meas_data)


    ################################ SUBSCRIBES TO ################################


    # SUBSCRIBES TO METRICS `metrics` OF NODE `e2_node_id` FOR ALL UES `ue_ids`
    def subscribe_to(self,e2_node_id,ue_ids,metrics):
        # Fixed
        report_period = 500 # 1000 by default
        granul_period = 500 # 1000 by default
        
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
        # Subscription for L4S (ALL)
        self.subscribe_to(DU_NODE_ID,L4S,ALL_METRICS)

        # Subscription for CONCURRENT (DRB)
        agg_id = 1
        ue_ids = [ k for k in CONFIG.keys() if agg_id == CONFIG[k]]
        self.subscribe_to(DU_NODE_ID,ue_ids,DRB_METRICS)


    ################################ UNSUSCRIBES ################################
    
    
    # Unsuscribes
    def signal_handler(self, sig, frame):
        length = self.datLastReport - self.datFrstReport
        print("[!] Time between 1st and last reports = %f, total reports = %d, mean reports per second = %f"%(length,self.nbReports,self.nbReports/length))
        self.stop_control.set()
        super().stop()




################################ LANCHES THREADS ################################


def prediction_threads(qu_input,qu_output,stop):
    NB_THREADS=2
    print("[!] Launching %d threads to predict "%NB_THREADS)
    for i in range(NB_THREADS):
        instance = Predictor(qu_input,qu_output,stop) # Class Instance
        thread = Thread(target=instance.Start) # Thread
        thread.start()


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
    queue_predict = queue.Queue()
    queue_control = queue.Queue()
    # MAIN THREAD: READ METRICS
    print("[!] Main Thread: starting reading KPM Metrics")
    myXapp = Get_Metrics('', http_server_port, rmr_port,queue_predict,stop_threads)
    myXapp.e2sm_kpm.set_ran_func_id(ran_func_id_kpm) # KPM
    myXapp.e2sm_rc.set_ran_func_id(ran_func_id_rc) # RC
    # LAUNCHES PREDICTION THREADS
    print("[!] Launching 'predicting' threads")
    prediction_threads(queue_predict,queue_control,stop_threads)
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
