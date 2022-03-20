#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Oct 25 11:03:06 2020

@author: dieterchvatal
"""
import pandas as pd
import numpy as np
from scipy.stats.distributions import chi2

#from dmyplant.dValidation import Validation


def lipson_equality(p, t1, t2, beta) -> float:
    nl = p * ((t1/t2) ** beta)
    pl = nl/p
    return (nl, pl)


def demonstrated_reliability_sr(val, start, end, beta=1.21, CL=0.9, T=30000, ft=pd.DataFrame, size=10):
    # failures array
    f_arr = np.zeros(size)
    # time points array
    t_arr = np.linspace(start, end, size)

    # populate the number of failures vs time array
    # based on failures Dataframe information
    if not(ft.empty):
        for row in ft.values:
            fl = np.where(t_arr > row[0].timestamp(), row[1], 0)
            f_arr += fl

    # initialize demonstrated Reliability vs. time array
    dr_arr = []

    # exeecute the algorithm
    m = np.array([e.Cylinders for e in val.engines])
    for i, t in enumerate(t_arr):
        tt = np.array([e.oph(t) for e in val.engines])
        tt_max = max(tt)
        if tt_max > 0.0:  # avoid division by zero
            # sum all part's per lipson equality to max hours at time t
            n_lip = sum(m*(tt/tt_max) ** beta)
            # use Lipson equality again to calc n@T hours
            n_lip_T = n_lip * ((tt_max/T) ** beta)
            # calc demonstrated Reliability per Chi.square dist (see A.Kleyner Paper)
            dr = np.exp(-chi2.ppf(CL, 2*(f_arr[i]+1))/(2*n_lip_T)) * 100.0
            # store in list for numpy vector
            dr_arr.append(dr)
        else:
            dr_arr.append(0.0)
    return (t_arr, np.array(dr_arr), f_arr)
