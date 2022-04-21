import pandas as pd
import numpy as np
from .dFSMData import load_data

class Start_Data_Collector:
    def __init__(self):
        self._vset = []
        self.start = None
        self.end = None
        self._data = pd.DataFrame([])

    @property
    def data(self):
        return self._data

    def phase_timing(self, startversuch, phases):
        ok = all([k in startversuch['startstoptiming'] for k in phases])
        if ok:
            self.start = startversuch['startstoptiming'][phases[0]][0]['start'].timestamp()
            self.end = startversuch['startstoptiming'][phases[-1]][0]['end'].timestamp()
        return ok

    def cut_data(self, startversuch, data, phases):
        if self.phase_timing(startversuch, phases):
            return data[(data.time > int(self.start * 1000)) & (data.time < int(self.end * 1000))].reset_index(drop=True)
        else:
            return pd.DataFrame([])

    def check_from(self, tfrom):
        if tfrom is None:
            return self.start
        else: 
            return self.start if self.start < tfrom else tfrom

    def check_to(self, tto):
        if tto is None:
            return self.end
        else:
            return self.end if self.end > tto else tto

    def left_upper_edge(self, v, data, factor, xmax, ymax):
        self._data = data[((data['time'] >= int( self.start * 1000)) & (data['time'] <= int(self.end * 1000)))]
        x0 = self._data.iloc[0]['datetime']
        y0 = 0
        x1 = self._data.iloc[-1]['datetime']
        y1 = max(self._data[v]) * factor
        self._data['helpline'] = self._data[v]  + (x0 - self._data['datetime'])* (y1-y0)/(x1-x0) + y0
        point = self._data['helpline'].idxmax()
        if point == point: # test for not NaN
            edge = self._data.loc[point]
            xmax = edge['datetime']
            ymax = self._data.at[edge.name,v]
        return xmax, ymax

    def collect(self, startversuch, result, data):
        pass

    def register(self, startversuch, vset, tfrom, tto):
        return vset, tfrom, tto

class Target_load_Collector(Start_Data_Collector):
    def __init__(self, ratedload, period_factor=3, helplinefactor=0.8):
        super().__init__()
        self._vset += ['Power_PowerAct']
        self.ratedload = ratedload
        self.period_factor=period_factor
        self.helplinefactor=helplinefactor

    def collect(self, startversuch ,results, data):
        xmax = startversuch['endtime'] # set xmax to the latest possible time to avoid duration to be 0.
        ymax = 0.0
        if 'loadramp' in startversuch['startstoptiming']:
            if not data.empty:
                xmax, ymax = self.left_upper_edge('Power_PowerAct', data, self.helplinefactor, xmax, ymax)
            duration = xmax.timestamp() - self.start
            ramprate = ymax / duration
            if  duration < 5: # constant load ?
                xmax = startversuch['endtime']
                ymax = 0.0

            sno = startversuch['no']
            results['starts'][sno]['startstoptiming']['loadramp'][0]['end'] = xmax
            if 'targetoperation' in results['starts'][sno]['startstoptiming']:
                results['starts'][sno]['startstoptiming']['targetoperation'][0]['start'] = xmax
            results['starts'][sno]['targetload'] = ymax
            results['starts'][sno]['ramprate'] = ramprate / self.ratedload * 100.0
            return results

    def register(self,startversuch,vset=[],tfrom=None,tto=None):
        vset += self._vset
        vset = list(set(vset)) # unique list ...
        if self.phase_timing(startversuch,['loadramp']):
            tfrom = self.check_from(tfrom)
            self.end = self.start + self.period_factor * (self.end-self.start)
            tto = self.check_to(tto)
        return vset, tfrom, tto

class Exhaust_temp_Collector(Start_Data_Collector):
    def __init__(self):
        super().__init__()
        self._vset += ['Power_PowerAct','Exhaust_TempCylMin','Exhaust_TempCylMax']

    def collect(self, startversuch, results, data):
        tdata = self.cut_data(startversuch, data, ['loadramp'])
        sno = startversuch['no']
        res = {'tmax':np.nan, 'spread_at_tmax':np.nan, 'power_at_tmax': np.nan }
        if not tdata.empty:
            point = tdata['Exhaust_TempCylMax'].idxmax()
            if point == point: # test for not NaN
                datapoint = tdata.loc[point]
                tmax = tdata.at[datapoint.name,'Exhaust_TempCylMax']
                tmin = tdata.at[datapoint.name,'Exhaust_TempCylMin']
                tspread = tmax - tmin
                tpow  = tdata.at[datapoint.name,'Power_PowerAct']
                res = {
                        'tmax':tmax,
                        'spread_at_tmax': tspread,  
                        'power_at_tmax': tpow
                    }
        results['starts'][sno].update(res) 
        return results 

    def register(self,startversuch,vset=[],tfrom=None,tto=None):
        vset += self._vset
        vset = list(set(vset)) # unique list ...
        if self.phase_timing(startversuch,['loadramp']):
            tfrom = self.check_from(tfrom)
            tto = self.check_to(tto)
        return vset, tfrom, tto

class Tecjet_Collector(Start_Data_Collector):
    def __init__(self):
        super().__init__()
        self._vset += ['TecJet_Lambda1','TecJet_GasTemp1','TecJet_GasPress1','TecJet_GasDiffPress']

    def collect(self, startversuch, results, data):
        tjdata = self.cut_data(startversuch, data, ['loadramp'])
        sno = startversuch['no']
        res = {'dpmin':np.nan, 'p_at_dpmin':np.nan, 't_at_dpmin': np.nan }
        if not tjdata.empty:
            point = tjdata['TecJet_GasDiffPress'].idxmin()
            if point == point: # test for not NaN
                datapoint = tjdata.loc[point]
                dpmin = tjdata.at[datapoint.name,'TecJet_GasDiffPress']
                p_at_dpmin = tjdata.at[datapoint.name,'TecJet_GasPress1']
                t_at_dpmin = tjdata.at[datapoint.name,'TecJet_GasTemp1']
                res = {
                        'dpmin': dpmin,  
                        'p_at_dpmin': p_at_dpmin,  
                        't_at_dpmin': t_at_dpmin
                    }
        results['starts'][sno].update(res)
        return results  

    def register(self,startversuch,vset=[],tfrom=None,tto=None):
        vset += self._vset
        vset = list(set(vset)) # unique list ...
        if self.phase_timing(startversuch,['loadramp']):
            tfrom = self.check_from(tfrom)
            tto = self.check_to(tto)
        return vset, tfrom, tto


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