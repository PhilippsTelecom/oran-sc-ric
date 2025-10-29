# LOAD MODELS
import joblib
import lightgbm as lgb
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score, mean_absolute_percentage_error
import numpy as np
import time
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
        # LOAD MODEL + SCALER
        path_model, path_scaler, self.index_pred    = prediction_related
        print(lgb.__version__)
        self.model = lgb.Booster(model_file=path_model)
        self.scaler = joblib.load(path_scaler)
        # PERFS
        self.features   = []
        self.predict    = [] 
        # STUDY PRECISION
        self.groundTruth    = []
        self.predicted      = []


    def adapt_dat_lightgbm(self,Input_Array):
        """
            Received data being L4S or AGG-related
            The output has only one row. It will be given to the ML model. 
                - Input shape: (WINDOW ; nb_metrics * [nb_agg_functions] ) ; 
                - Output shape: (1 ; nb_agg_metrics * [nb_agg_functions] * (WINDOW+2) )
        """
        win_sze             = Input_Array.shape[0]
        nb_features         = Input_Array.shape[1]
        res_vector          = np.zeros(nb_features * (win_sze + 2))

        idx=0
        for feature in range(nb_features):
            window                              = Input_Array[:,feature]                # Value of agg_met over temporal window
            mean                                = np.mean(window)                       # Statistic over time window
            std                                 = np.std(window)
            extended                            = np.concatenate([window,[mean,std]])   # Save statistics
            res_vector[idx: idx+win_sze + 2]    = extended
            idx                                 = idx + win_sze + 2

        return res_vector.reshape(1,-1)


    def sMAPE(self,Y_ref,Y_pred):
        # Some delays equal 0, MAPE is not adapted
        # Use sMAPE instead
        Y_ref = np.array(Y_ref)
        Y_pred = np.array(Y_pred)
        denominator = np.abs(Y_ref) + np.abs(Y_pred)
        diff = np.abs(Y_pred - Y_ref)
        # Avoid division by zero
        nonzero = denominator != 0
        smape = 100 / len(Y_ref) * np.sum(2 * diff[nonzero] / denominator[nonzero])
        return smape


    def compare_results(self,Y_ref,Y_pred):
        # Metrics
        mae     = mean_absolute_error(Y_ref, Y_pred)
        mape    = mean_absolute_percentage_error(Y_ref, Y_pred)
        smape   = self.sMAPE(Y_ref,Y_pred)
        mse     = mean_squared_error(Y_ref, Y_pred)
        r2      = r2_score(Y_ref, Y_pred)
        # Print results
        print("[!]\t[Predictor]: MAE= %f ; MAPE = %f ; sMAPE = %f ; MSE = %f ; r2 = %f "%(mae,mape,smape,mse,r2))


    def Start(self):
        while True:
            if self.stop.is_set(): # RECEIVED A STOP SIGNAL
                print("[!]\t[Predictor]: Stopping predicting thread %d ... Bye!"%current_thread().native_id)
                if len(self.features) > 0:
                    print("[!]\t[Predictor]: Thread made %d classifications"%(len(self.features)))
                    print("[!]\t[Predictor]: Mean Feature delay ~= %f"%(sum(self.features)/len(self.features)))
                    print("[!]\t[Predictor]: Mean Classif delay ~= %f"%(sum(self.predict)/len(self.predict)))
                    T = 4
                    self.compare_results(self.groundTruth[T:],self.predicted[:-T])                
                break
            try:
                # RETRIEVE VALUES
                tim_received, cu_id, ue_id, drb_id, l4s_arr, agg_arr = self.input.get(timeout=1)
                start = time.time() # Perfs
                
                # ADAPT DATA for LightGBM
                l4s_lightgbm    = self.adapt_dat_lightgbm(l4s_arr)
                nb_feats_l4s    = l4s_lightgbm.shape[1]

                agg_lightgbm    = self.adapt_dat_lightgbm(agg_arr)
                nb_feats_agg    = agg_lightgbm.shape[1]

                # Input Vector
                X_lightgbm                  = np.zeros(nb_feats_l4s + nb_feats_agg)
                X_lightgbm[0:nb_feats_l4s]  = l4s_lightgbm
                X_lightgbm[nb_feats_l4s:]   = agg_lightgbm
                # Scaling
                X_lightgbm                  = X_lightgbm.reshape(1,-1)
                X_scaled                    = self.scaler.transform(X_lightgbm)
                end_feats                   = time.time()
                self.features.append(end_feats-start)

                # Prediction
                l4s_delay_pred              = self.model.predict(X_scaled)
                end_classif = time.time()
                self.predicted.append(float(l4s_delay_pred))                # Save prediction 
                self.groundTruth.append(float(l4s_arr[-1,self.index_pred])) # Save ground truth
                self.predict.append(end_classif - end_feats)
                # SENDS TO CONTROL THREAD
                self.output.put((tim_received,cu_id,ue_id,drb_id,float(l4s_delay_pred)))

                # PERFORMANCES: INFERENCE TINE
                
            except Exception as e:
                print(f"Exception occurred (problem): {e}")
                continue