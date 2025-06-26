#!/usr/bin/env python3

from lib.xAppBase import xAppBase
import signal
import time
import pandas as pd
# LOAD MODELS
import joblib
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



class Get_Metrics(xAppBase):
    def __init__(self, config, http_server_port, rmr_port):
        super(Get_Metrics, self).__init__(config, http_server_port, rmr_port)
        # Load SCALER to rescale delays
        scaler =  joblib.load(SAV_SCALER)
        self.val_min = scaler.data_min_[COL_INDEX]
        self.val_max = scaler.data_max_[COL_INDEX]
        # Load LSTM model
        self.model = keras.models.load_model(SAV_MODEL)
        # L4S DRBs / KEY = UE_ID
        self.l4s_drb = {key: (pd.DataFrame(columns=ALL_METRICS),0) for key in L4S }
        # Aggregated DRBs / KEY = AGG_DRB
        self.agg_drb = {key: (pd.DataFrame(columns=DRB_METRICS),0) for key in CONFIG.values()}

    ################################ UTILS FOR PREDICTION ################################


    # AGGREGATE DATA: FROM SEVERAL ROWS TO ONE ROW
    def aggregate(self,dataF):
        # AGGREGATED VALUES
        values = []
        for mu in DRB_METRICS:
            # MEAN FOR EACH UE
            if mu in ['DRB.RlcPacketDropRateDl','DRB.RlcSduDelayDl','DRB.RlcDelayUl']:
                values.append(dataF[mu].mean(axis=0))
            # SUM FOR EACH UE
            else:
                values.append(dataF[mu].sum(axis=0))
        # DATA FRAME JUST AGG VALUES
        dataF = dataF.drop(dataF.index)
        dataF.loc[0] = values
        return dataF


    # TAKE ALL NON-L4S DATAFRAMES, PUT IN RIGHT ORDER (cf INDEX) AND MERGE INTO A SINGLE ONE
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


    # PREDICT FOR SPECIFIC L4S DRB
    def predict(self,ue_id,l4s_df,non_l4s_df):
        
        # CONCATENATE L4S + MERGED DF (NON-L4S)
        X_df = pd.concat([l4s_df,non_l4s_df], axis = 1)
        print(type(X_df))
        print(X_df)

        # PREDICT DELAY
        l4s_delay = 0
        # l4s_delay=self.model.predict(X_df)
        # GET NORMAL DELAY
        l4s_delay = l4s_delay * (self.val_max - self.val_min) + self.val_min

        # COMPUTE PROBA
        if l4s_delay < TH_MIN:
            mark_prob = 0
        elif l4s_delay > TH_MAX:
            mark_prob = 100
        else: # Linear Probability
            mark_prob = (l4s_delay - TH_MIN) / (TH_MAX - TH_MIN) * 100
        
        # SEND
        drb_id = 1
        mark_prob = 100
        self.e2sm_rc.control_drb_qos(CU_NODE_ID, ue_id,drb_id,mark_prob,ack_request=1)


    ################################ TEST IF OK TO PREDICT ################################


    # SAYS IF WE HAVE ENOUGH TEMPORAL WINDOWS FOR 'AGG_DRB'
    def isOK_ts_aggDrb(self):
        # RETURN VALUE
        ret = True
        # FEATURES (AGG_DRB)
        index=0
        list_agg_drb = list(self.agg_drb.keys())
        # BROWSE ALL FEATURES
        while ret and (index < len(list_agg_drb)): 
            nbRows = self.agg_drb[list_agg_drb[index]][0].shape[0]
            ret = (nbRows == WINDOW)
            index = index + 1
        return ret


    # SAYS IF WE HAVE ENOUHG TEMPORAL WINDOWS FOR CURRENT 'L4S'
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
            ue_id = 0
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
                    self.predict(ue_id,self.l4s_drb[ue_id][0],agg_df)


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
                df = pd.concat([df,pd.DataFrame(ue_meas_data["measData"])])
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
                        self.predict(ue_id,self.l4s_drb[ue_id][0],agg_df)


    # CALLED WHEN RECEIVING A RIC INDICATION MSG 
    def my_subscription_callback(self, e2_agent_id, subscription_id, indication_hdr, indication_msg, kpm_report_style, ue_id):
        timestamp = time.time()

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
        report_period = 500 # 1000 by default
        granul_period = 500 # 1000 by default

        # Subscription for L4S (ALL)
        self.subscribe_to(DU_NODE_ID,L4S,ALL_METRICS)

        # Subscription for CONCURRENT (DRB)
        agg_id = 1
        ue_ids = [ k for k in CONFIG.keys() if agg_id == CONFIG[k]]
        self.subscribe_to(DU_NODE_ID,ue_ids,DRB_METRICS)


    ################################ UNSUSCRIBES ################################
    
    
    # Unsuscribes
    def signal_handler(self, sig, frame):
        super().stop()




# MAIN FUNCTION 
if __name__ == '__main__':


    # CREATE KPM XAPP
    # CONSTANT VALUES
    http_server_port = 8092
    rmr_port = 4562
    ran_func_id_kpm = 2
    ran_func_id_rc = 3
    # CREATION
    myXapp = Get_Metrics('', http_server_port, rmr_port)
    myXapp.e2sm_kpm.set_ran_func_id(ran_func_id_kpm) # KPM
    myXapp.e2sm_rc.set_ran_func_id(ran_func_id_rc) # RC
    # EXIT SIGNALS
    signal.signal(signal.SIGQUIT, myXapp.signal_handler)
    signal.signal(signal.SIGTERM, myXapp.signal_handler)
    signal.signal(signal.SIGINT, myXapp.signal_handler)
    # START XAPP 
    myXapp.start()
    # Note: xApp will unsubscribe all active subscriptions at exit.