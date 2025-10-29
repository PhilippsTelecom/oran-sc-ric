from threading import current_thread
import time


ALPHA               = 0.8
MIN_THRESH_DELAYS   =5
MAX_THRESH_DELAYS   =10


class Controller():
    """
        This class retrieves the predictions made by the 'Predictors'
        It computes the marking probability, and sends it to the CU
    """


    def __init__(self,thread_related:tuple,e2sm_rc_):
        self.input, self.stop   = thread_related 
        self.sender             = e2sm_rc_
        self.MarkProba          = 0
        self.times              = []    # Time between E2SM-KPM and E2SM-RC
        self.global_times       = []    # Check if sending is the problem 


    def Start(self):
        """
            Waits for predictions until program end (stop.is_set())
        """
        while True:
                if self.stop.is_set(): # RECEIVED A STOP SIGNAL
                    print("[!]\t[Controller]: Stopping Controller (thread %d) ... Bye!"%current_thread().native_id)
                    if len(self.times) > 0:
                        print("[!]\t[Controller]: Handled %d E2SM-KPM reports"%len(self.times))
                        print("[!]\t[Controller]: Mean E2E delay ~= %f"%(sum(self.global_times)/len(self.global_times)))
                        print("[!]\t[Controller]: Mean processing time ~= %f"%(sum(self.times)/len(self.times)))    
                    break
                try:
                    # RETRIEVE VALUES
                    tim_received, cu_id, ue_id, drb_id, pred_delay  = self.input.get(timeout=1)
                    start = time.time()
                    
                    mark_prob                                       = self.compute_mark_prob(pred_delay)
                    self.sender.control_drb_qos(cu_id, ue_id,drb_id,mark_prob,ack_request=1)
                    
                    end  = time.time()
                    self.global_times.append(end - tim_received)
                    self.times.append(end - start)
                except Exception as e:
                    print(f"Exception occurred (problem Controller): {e}")
                    continue


    def compute_mark_prob(self,queue_delay):
        """
            Computes the marking probability as specified in our Paper
                - proba_tmp is the marking probability according to the last known delay
                - self.MarkProba is a weighted average (alpha = 0.8) gives 80% importance to last measure
        """
        # Proba related to current queue delay
        proba_tmp = 0
        if queue_delay is not None:
            if queue_delay > MAX_THRESH_DELAYS: proba_tmp = 100 # queue_delay == '' or 
            elif queue_delay > MIN_THRESH_DELAYS: proba_tmp = int((queue_delay - MIN_THRESH_DELAYS) * 100 / (MAX_THRESH_DELAYS - MIN_THRESH_DELAYS))
        
        # Weighted average
        self.MarkProba = int(ALPHA * proba_tmp + (1-ALPHA) * self.MarkProba)
        
        return self.MarkProba