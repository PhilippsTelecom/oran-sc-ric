#!/usr/bin/env python3

from lib.xAppBase import xAppBase
import signal
import time
import os


# THIS PROGRAM SENDS A E2SM-RC TO THE CU EVERY 10 MS
# OBJECTIVE: CHECK IF PACKET BUNDLING FOR E2SM-RC ON E2 INTERFACE


# CONNECT TO E2 NODE (wget 10.0.2.13:8080/ric/v1/get_all_e2nodes)
CU_NODE_ID = "gnb_001_001_00019b" # "gnb_001_001_00019b"  
PERIOD = 0.010 # Toutes les 10 ms




################################ MAIN THREAD: LISTEN TO REPORTS ################################




class Send_Controls(xAppBase):
    def __init__(self, config, http_server_port, rmr_port):
        super(Send_Controls, self).__init__(config, http_server_port, rmr_port)
        self.keepSending = True 
        self.nbSent = 0 
        self.begin = time.time()

    def send_controls(self):
        # HOW TO MARK
        ue_id=0
        drb_id=1
        mark_prob=20

        last = time.time() # LAST RC
        current = time.time() # CURRENT TIME

        while self.keepSending:
            # SEND CONTROL
            if current - last > PERIOD : 
                self.e2sm_rc.control_drb_qos(CU_NODE_ID, ue_id,drb_id,mark_prob,ack_request=1)
                last = current
                self.nbSent = self.nbSent + 1
            current = time.time()

    # Mark the function as xApp start function using xAppBase.start_function decorator.
    # It is required to start the internal msg receive loop.
    @xAppBase.start_function
    def start(self):
        self.send_controls()
    
    
    # Unsuscribes
    def signal_handler(self, sig, frame):
        
        # STOP SENDING
        self.keepSending = False
        
        # STOPS MARKING
        ue_id=0
        drb_id=1
        mark_prob=0
        self.e2sm_rc.control_drb_qos(CU_NODE_ID, ue_id,drb_id,mark_prob,ack_request=1)

        # STATS
        last = time.time() - self.begin
        print("[!] Time Elapsed = %f / Sent %d reports => %f reports per second "%(last,self.nbSent,self.nbSent/last))
        
        super().stop()




################################ MAIN ################################


# MAIN FUNCTION 
if __name__ == '__main__':
    
    # CONSTANT VALUES
    http_server_port    = 8092
    rmr_port            = 4562
    ran_func_id_kpm     = 2
    ran_func_id_rc      = 3


    # MAIN THREAD: READ METRICS
    myXapp = Send_Controls('', http_server_port, rmr_port)
    myXapp.e2sm_rc.set_ran_func_id(ran_func_id_rc)      # RC


    # EXIT SIGNALS
    signal.signal(signal.SIGQUIT, myXapp.signal_handler)
    signal.signal(signal.SIGTERM, myXapp.signal_handler)
    signal.signal(signal.SIGINT, myXapp.signal_handler)
    # START XAPP
    myXapp.start()
    # Note: xApp will unsubscribe all active subscriptions at exit.
    # It also stops the threads (Prediction + Control)