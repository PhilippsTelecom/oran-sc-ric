import math
import numpy as np


METRICS_NAN_ZERO= ['CQI','DRB.RlcSduDelayDl','DRB.RlcSduLastDelayDl', 'DRB.RlcDelayUl','DRB.AirIfDelayUl'] 


class Pre_Process():
    """
        This class handles the NaNs and the INFs
    """

    def __init__(self,l4s_data_: tuple,agg_data_: tuple):
        # UEs and their associated metrics
        self.l4s_ues,self.l4s_met   = l4s_data_
        self.nl4s_ues,self.nl4s_met = agg_data_
        # Rules to handle NAN: l4s
        self.l4s_nan_zero   = list(set(METRICS_NAN_ZERO) & set(self.l4s_met))
        self.l4s_nan_prev   = list(set(self.l4s_met) - set(self.l4s_nan_zero))
        # Rules to handle NAN: nl4s
        self.nl4s_nan_zero   = list(set(METRICS_NAN_ZERO) & set(self.nl4s_met))
        self.nl4s_nan_prev   = list(set(self.nl4s_met) - set(self.nl4s_nan_zero))
        # Last Known Values
        self.last_l4s_drbs  = {key: np.zeros(len(self.l4s_met)) for key in self.l4s_ues }
        self.last_nl4s_drbs = {key: np.zeros(len(self.nl4s_met)) for key in self.nl4s_ues }     


    def get_last_value(self,metric_:tuple,context_:tuple):
        """
            Retrieves the last known value for a given metric
            Returns 0 y default
        """
        # Retrieve Data
        metric, list_metric = metric_
        idf, data           = context_
        metric_index        = list_metric.index(metric)
        # Vector of UE 'idf', at column 'metic_index'
        last_value          = data[idf][metric_index]

        return last_value


    def update_last_values_(self,clean_KPM_report:dict,ues_infos:tuple):
        """
            Used by 'update_last_values'
        """
        list_ues, list_metrics, context = ues_infos

        for ue in list_ues:
            if ue in clean_KPM_report["ueMeasData"].keys(): # UE in KPM report => Update
                ue_array        = context[ue]
                ue_kpm_report   = clean_KPM_report["ueMeasData"][ue]["measData"]
                # For each metric: update saved value
                for mu_index,mu_name in enumerate(list_metrics):
                    ue_array[mu_index] = ue_kpm_report[mu_name][0]
                # Update index
                context[ue] = ue_array


    def update_last_values(self,clean_KPM_report:dict):
        """
            Given a "cleaned" KPM report, i.e. with no NaN or INF
            Updates the last values (saved)
        """
        self.update_last_values_(clean_KPM_report,(self.l4s_ues,self.l4s_met,self.last_l4s_drbs))       # L4S
        self.update_last_values_(clean_KPM_report,(self.nl4s_ues,self.nl4s_met,self.last_nl4s_drbs))    # nL4S 


    def handle_nans(self,KPM_report:dict):
        """
            This function removes all NaN in current KPM_report
                - For some metrics; NaN replaced by 0
                - Else; NaN replaced by last known value
        """
       # Handle L4S
        for l4s in self.l4s_ues:
            for mu in self.l4s_nan_zero:
                value   = KPM_report["ueMeasData"][l4s]["measData"][mu][0]
                KPM_report["ueMeasData"][l4s]["measData"][mu][0] = 0 if (value is None or math.isnan(value)) else value
            for mu in self.l4s_nan_prev:
                value   = KPM_report["ueMeasData"][l4s]["measData"][mu][0]
                KPM_report["ueMeasData"][l4s]["measData"][mu][0] = self.get_last_value((mu,self.l4s_met),(l4s,self.last_l4s_drbs)) if (value is None or math.isnan(value)) else value
        # Handle nl4s
        for nl4s in self.nl4s_ues:
            for mu in self.nl4s_nan_zero:
                value = KPM_report["ueMeasData"][nl4s]["measData"][mu][0]
                KPM_report["ueMeasData"][nl4s]["measData"][mu][0] = 0 if (value is None or math.isnan(value)) else value
            for mu in self.nl4s_nan_prev:
                value           = KPM_report["ueMeasData"][nl4s]["measData"][mu][0]
                KPM_report["ueMeasData"][nl4s]["measData"][mu][0] = self.get_last_value((mu,self.nl4s_met),(nl4s,self.last_nl4s_drbs)) if (value is None or math.isnan(value)) else value
        return KPM_report


    def handle_infs(self,KPM_report:dict):
        """
            This function removes all INF in current KPM_report
            For each metric: replaces by last known value
        """
        # Handle L4S
        for l4s in self.l4s_ues:
            for mu in self.l4s_met:
                value = KPM_report["ueMeasData"][l4s]["measData"][mu][0]
                KPM_report["ueMeasData"][l4s]["measData"][mu][0] = self.get_last_value((mu,self.l4s_met),(l4s,self.last_l4s_drbs)) if (value is None or math.isinf(value)) else value
        # Handle nl4s
        for nl4s in self.nl4s_ues:
            for mu in self.nl4s_met:
                value = KPM_report["ueMeasData"][nl4s]["measData"][mu][0]
                KPM_report["ueMeasData"][nl4s]["measData"][mu][0] = self.get_last_value((mu,self.nl4s_met),(nl4s,self.last_nl4s_drbs)) if (value is None or math.isinf(value)) else value
        return KPM_report




################################################################




class Smooth():
    """
        This class "smooth" the Data 
        We implement the "moving average", but it is possible to use other metrics
    """

    def __init__(self,window_:int,l4s_data_:tuple,agg_data_:tuple):
        self.window = window_
        self.l4s_ues, self.l4s_met = l4s_data_
        self.agg_ids, self.agg_met = agg_data_
        # Keep Window in memory
        self.last_l4s_drbs  = {key: (np.zeros((self.window,len(self.l4s_met))), 0, False) for key in self.l4s_ues }
        self.last_agg_drbs  = {key: (np.zeros((self.window,len(self.agg_met)*3)), 0, False) for key in self.agg_ids } 


    def moving_average_l4s(self,X_l4s):
        """
            Computes the moving average through the last 'window' reports of L4S UEs
            If there is less than 'window' reports: write value directly
        """
        # Handle L4S
        for i,l4s in enumerate(self.l4s_ues):
            values, index, full_window = self.last_l4s_drbs[l4s]
            # Do not compute moving avg
            if not full_window: 
                for j,mu in enumerate(self.l4s_met): # Save current measures
                    values[index,j] = X_l4s[0,i*len(self.l4s_met)+j]
            # Compute moving average
            else:
                for j,mu in enumerate(self.l4s_met):
                    cur_val                         = X_l4s[0,i*len(self.l4s_met)+j]
                    values[index,j]                 = cur_val
                    mov_avg                         = np.average(values[:,j]) # avg of 'mu' over last 'window' values 
                    X_l4s[0,i*len(self.l4s_met)+j]  = mov_avg
            # Update and save changes
            index = (index + 1) % self.window
            full_window = full_window or index==0
            self.last_l4s_drbs[l4s] = (values, index, full_window)

        return X_l4s


    def moving_average_agg(self,X_agg):
        """
            Computes the moving average through the last 'window' reports if AGG IDs
            If there is less than 'window' reports: write value directly
        """
        nb_agg_functions=3

        # Handle AGG
        for i,agg in enumerate(self.agg_ids):
            values, index, full_window = self.last_agg_drbs[agg]
            # Do not compute moving avg
            if not full_window: 
                for j,mu in enumerate(self.agg_met): # Save current measures
                    for k in range(nb_agg_functions):
                        idx = i*len(self.agg_met)*nb_agg_functions + j*nb_agg_functions + k
                        values[index,j*nb_agg_functions+k] = X_agg[0,idx]
            # Compute moving average
            else:
                for j,mu in enumerate(self.agg_met):
                    for k in range(nb_agg_functions):
                        idx                                                     = i*len(self.agg_met)*nb_agg_functions + j*nb_agg_functions + k
                        cur_val                                                 = X_agg[0,idx]
                        values[index,j*nb_agg_functions + k]                    = cur_val
                        mov_avg                                                 = np.average(values[:,j*nb_agg_functions + k])
                        X_agg[0,idx]                                            = mov_avg # avg of 'mu' over last 'window' values
            # Update and save changes
            index = (index + 1) % self.window
            full_window = full_window or index==0
            self.last_agg_drbs[agg] = (values, index, full_window)

        return X_agg

        


################################################################




class Arrange_Data():
    """
        This class arrange Data; it computes 
            - The aggregated vector 'X_agg' (nb_aggs x metrics_agg * 3)
            - The L4S ues 'X_l4s' (nb_l4s * metrics_l4s)
    """
    
    def __init__(self,l4s_related:tuple,nl4s_related:tuple):
        self.l4s_drbs, self.metrics_l4s = l4s_related
        self.config, self.metrics_agg   = nl4s_related


    def handle_aggregators(self,KPM_report:dict):
        """
            Aggregates metrics for each configures aggregator
            The returned value has a shape (1 , NB_AGG * NB_METR_AGG * 3) ; for each aggregator, each aggregated metric there 3 times
        """
        nb_aggreg   = len(self.config.keys())
        agg_data    = np.zeros((1, nb_aggreg * len(self.metrics_agg) * 3))  # Pre-allocation: 3 values per metric * nb_aggregators
        agg_cnt     = 0

        # Browse all aggregators                                          
        for agg_index,list_ues in self.config.items():
            raw_data    = np.zeros((1, len(self.metrics_agg) * len(list_ues)))   # Pre-allocation
            col         = 0
            # Browse UEs of current aggregator
            for ue_id in list_ues:                                      
                ue_meas = KPM_report["ueMeasData"][ue_id]["measData"]
                for metric in self.metrics_agg:
                    raw_data[0,col] = ue_meas[metric][0]
                    col+=1

            # Aggregate their metrics
            base_index  = agg_cnt * len(self.metrics_agg) * 3
            for i,metric in enumerate(self.metrics_agg):
                metric_cols         = raw_data[:, i::len(self.metrics_agg)] # Values of metric accross all UEs
                agg_data[0,base_index + 3*i]     = np.mean(metric_cols)     # avg
                agg_data[0,base_index + 3*i+1]   = np.sum(metric_cols)      # sum
                agg_data[0,base_index + 3*i+2]   = np.max(metric_cols)      # max
            # Save agg KPM metrics
            agg_cnt = agg_cnt + 1

        return agg_data


    def handle_l4s(self,KPM_report:dict):
        """
            Reads the KPM report, and extracts the L4S-related features
            Returns a vector of size: nb_l4s_drbs * nb_l4s_metrics
        """
        nb_l4s      = len(self.l4s_drbs)
        l4s_data    = np.zeros((1, nb_l4s * len(self.metrics_l4s)))  # Pre-allocation

        for i,l4s in enumerate(self.l4s_drbs):
            ue_meas     = KPM_report["ueMeasData"][l4s]["measData"]
            base_index  = i * len(self.metrics_l4s)
            for j,metric in enumerate(self.metrics_l4s):
                l4s_data[0,base_index + j]= ue_meas[metric][0]

        return l4s_data