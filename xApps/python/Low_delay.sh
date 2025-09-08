#!/bin/bash


# This script modifies the library ricxappframe
# The file 'rmr.py' is a wrapper; it calls C functions
# The C function 'rmr_set_low_latency' does exist, but is not defined in the wrapper
# We are modifying the wrapper to add this function


# CONSTANT
PATH="/opt/ric-plt-xapp-frame-py/ricxappframe/"
FILE="rmr/rmr.py"


# FUNCTION LOW LATENCY
l1="\n_rmr_set_low_latency=_wrap_rmr_function('rmr_set_low_latency', None, [c_void_p]) \n"
l2="def rmr_set_low_latency(vctx: c_void_p) -> None: \n"
l3="\treturn _rmr_set_low_latency(vctx) \n"


# Modify
echo -e $l1$l2$l3 >> $PATH$FILE