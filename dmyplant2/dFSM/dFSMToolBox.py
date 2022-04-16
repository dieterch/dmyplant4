import pandas as pd
import numpy as np
from .dFSMData import load_data


def loadramp_edge_detect(fsm, startversuch, debug=False, periodfactor=3, helplinefactor=0.8):
    # 1.4.2022 Aufgrund von Bautzen, der sehr langsam startet
    # periodfactor = 3, helplinefactor = 0.8
    # der Start
    # Start: 201 xmax: 2021-07-19 09:20:31, ymax:   3387, duration: 528.7, ramprate: 0.19 %/s
    # von: 19.07.2021 09:07:44 bis: 19.07.2021 09:34:47
    # ist zu kurz, => Änderung des Algorithmus auf Last max statt Last letzter Punkt
    if 'loadramp' in startversuch['startstoptiming']:
        s = startversuch['startstoptiming']['loadramp'][-1]['start'].timestamp()
        e = startversuch['startstoptiming']['loadramp'][-1]['end'].timestamp()
        e2 = s + periodfactor * (e-s)
        pdef = ['Power_PowerAct','Hyd_PressOil','Hyd_PressOilDif','Hyd_TempOil','TecJet_Lambda1','TecJet_GasDiffPress','Exhaust_TempCylMin','Exhaust_TempCylMax']
        data = load_data(fsm, cycletime=1, tts_from=s, tts_to=e2, silent=True, p_data=pdef, p_forceReload=False, p_suffix='loadramp', debug=debug)
        if not data.empty:
            data = data[(data['time'] >= int(s * 1000)) & (data['time'] <= int(e2 * 1000))]
            #s,e,e2, data.iloc[0]['time'], data.iloc[-1]['time'],
            x0 = data.iloc[0]['datetime']
            y0 = 0.0
            x1 = data.iloc[-1]['datetime']
            #y1 = data.iloc[-1]['Power_PowerAct'] * helplinefactor
            y1 = max(data['Power_PowerAct']) * helplinefactor
            data['helpline'] = data['Power_PowerAct'] + (x0 - data['datetime'])* (y1-y0)/(x1-x0) + y0
            
            point = data['helpline'].idxmax()
            if point == point: # test for not NaN
                edge = data.loc[point]
                xmax = edge['datetime']
                ymax = data.at[edge.name,'Power_PowerAct']
            else:
                return pd.DataFrame([]), startversuch['endtime'], 0.0, 0.0, 0.0
        else:
            return pd.DataFrame([]), startversuch['endtime'], 0.0, 0.0, 0.0
    else:
        return pd.DataFrame([]), startversuch['endtime'], 0.0, 0.0, 0.0
    duration = xmax.timestamp()-s
    ramprate = ymax / duration
    if duration < 5: # konstante Last ?
        return pd.DataFrame([]), startversuch['endtime'], 0.0, 0.0, 0.0
    return data, xmax, ymax, duration, ramprate 

def xwhere(data,key,level):
    return data.iloc[data['datetime'][1:][np.array(data[key][1:]-level) * np.array(data[key][:-1]-level) < 0].index]

def xwhere2(data,key,level):
    pts = data['datetime'][1:][np.array(data[key][1:]-level) * np.array(data[key][:-1]-level) < 0]
    newlist =[x for p in list(pts.index) for x in [p-1,p]]
    return data.iloc[newlist]

def savitzky_golay(y, window_size, order, deriv=0, rate=1):
    r"""Smooth (and optionally differentiate) data with a Savitzky-Golay filter.
    The Savitzky-Golay filter removes high frequency noise from data.
    It has the advantage of preserving the original shape and
    features of the signal better than other types of filtering
    approaches, such as moving averages techniques.
    Parameters
    ----------
    y : array_like, shape (N,)
        the values of the time history of the signal.
    window_size : int
        the length of the window. Must be an odd integer number.
    order : int
        the order of the polynomial used in the filtering.
        Must be less then `window_size` - 1.
    deriv: int
        the order of the derivative to compute (default = 0 means only smoothing)
    Returns
    -------
    ys : ndarray, shape (N)
        the smoothed signal (or it's n-th derivative).
    Notes
    -----
    The Savitzky-Golay is a type of low-pass filter, particularly
    suited for smoothing noisy data. The main idea behind this
    approach is to make for each point a least-square fit with a
    polynomial of high order over a odd-sized window centered at
    the point.
    Examples
    --------
    t = np.linspace(-4, 4, 500)
    y = np.exp( -t**2 ) + np.random.normal(0, 0.05, t.shape)
    ysg = savitzky_golay(y, window_size=31, order=4)
    import matplotlib.pyplot as plt
    plt.plot(t, y, label='Noisy signal')
    plt.plot(t, np.exp(-t**2), 'k', lw=1.5, label='Original signal')
    plt.plot(t, ysg, 'r', label='Filtered signal')
    plt.legend()
    plt.show()
    References
    ----------
    .. [1] A. Savitzky, M. J. E. Golay, Smoothing and Differentiation of
       Data by Simplified Least Squares Procedures. Analytical
       Chemistry, 1964, 36 (8), pp 1627-1639.
    .. [2] Numerical Recipes 3rd Edition: The Art of Scientific Computing
       W.H. Press, S.A. Teukolsky, W.T. Vetterling, B.P. Flannery
       Cambridge University Press ISBN-13: 9780521880688
    """
    import numpy as np
    from math import factorial
    
    try:
        window_size = np.abs(np.int(window_size))
        order = np.abs(np.int(order))
    except ValueError as msg:
        raise ValueError("window_size and order have to be of type int")
    if window_size % 2 != 1 or window_size < 1:
        raise TypeError("window_size size must be a positive odd number")
    if window_size < order + 2:
        raise TypeError("window_size is too small for the polynomials order")
    order_range = range(order+1)
    half_window = (window_size -1) // 2
    # precompute coefficients
    b = np.mat([[k**i for i in order_range] for k in range(-half_window, half_window+1)])
    m = np.linalg.pinv(b).A[deriv] * rate**deriv * factorial(deriv)
    # pad the signal at the extremes with
    # values taken from the signal itself
    firstvals = y[0] - np.abs( y[1:half_window+1][::-1] - y[0] )
    lastvals = y[-1] + np.abs(y[-half_window-1:-1][::-1] - y[-1])
    y = np.concatenate((firstvals, y, lastvals))
    return np.convolve( m[::-1], y, mode='valid')