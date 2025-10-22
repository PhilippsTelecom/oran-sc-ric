# LOAD MODELS
import joblib
import lightgbm as lgb
from sklearn.preprocessing import StandardScaler
import numpy as np
from threading import current_thread


# RELATED TO PREDICTION
# SAV_MODEL ="./Model/lgb-steps-4.model"
# METRIC_TO_PRED  = 'DRB.RlcSduLastDelayDl'
# COL_INDEX       = ALL_METRICS.index(METRIC_TO_PRED)


class LightGBM():

    # This class will:
    #   - Scale Data
    #   - Perform Predictions 

    # Input: (ue_id, drb_id, l4s_df, agg_df)
    #     - l4s_df contains all L4S DRB features. First row = time t-(W-1) (oldest) / last row = time t (newest)
    #     - agg_df contains all aggregated traffic


    def __init__(self,thread_related:tuple,prediction_related:tuple):
        # THREADS RELATED
        self.input, self.output, self.stop          = thread_related
        path_model, path_scaler, self.index_pred    = prediction_related
        # LOAD MODEL + SCALER
        print(lgb.__version__)
        self.model = lgb.Booster(model_file=path_model)
        self.scaler = joblib.load(path_scaler)
        print("LOADED")
        # PERFS
        self.times = []


    # Adapts Data for lightgbm model 
    def adapt_data_lightgbm(self,X):
        # Test dimensionality 
        nb_metric_all = len(ALL_METRICS)
        nb_metric_agg = len(METRICS_AGG)
        nb_aggregator = len(CONF.keys())
        #assert(X.shape[1] == (nb_metric_all + nb_metric_agg * 3 * nb_aggregator)), "adapt_data_lightgbm: bad dimension"
        # Browse features
        row = []
        for col in range(X.shape[1]):   
            window = X[:,col]           # There are WINDOW lines             
            row.extend(window)          # Metrics values fron 't-(W-1)' to 't'
            row.append(np.mean(window)) # AVG on this window
            row.append(np.std(window))  # STD on this window
        return np.array(row).reshape(1,-1)


     def adapt_l4s_lightgbm(self,X):
        """
            Receives L4S data from DRB
        """
        pass
        
    
    def adapt_agg_lightgbm(self,X_agg):
        pass


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
                ue_id, drb_id, l4s_arr, agg_arr = self.input.get(timeout=1)
                # PERFORMANCES: INFERENCE TIME
                start = time.time()
                # ADAPT DATA for LightGBM
                overall_vector  = np.zeros(())
                l4s_lightgbm    = self.adapt_l4s_lightgbm(l4s_arr)
                agg_lightgbm    = self.adapt_agg_lightgbm(agg_arr) 

                #l4s_delay=self.model.predict(np.array([X_df]))[0][0]
                
                # print("[!] Predicted Delay = %f "%l4s_delay)
                # COMPUTE PROBA
                # mark_prob = self.compute_proba(l4s_delay)
                # SENDS TO CONTROL THREAD
                mark_prob = 0
                self.output.put((ue_id,drb_id,mark_prob))
                # print("[!] We put something in the queue -> queue size = %d"%self.output.qsize())
                
                # PERFORMANCES: INFERENCE TINE
                end = time.time() # Perfs
                self.times.append(end-start)
            except Exception as e:
                continue