import sys
import pandas as pd
import numpy as np
from .dFSMData import load_data


####################################################
### generic Data Collector base class 
####################################################
class Start_Data_Collector:
    def __init__(self, phases):
        self._vset = []
        self._phases = phases
        self.start = None
        self.end = None
        self._data = pd.DataFrame([])

    def phase_timing(self, startversuch, phases):
        ok = all([k in startversuch['startstoptiming'] for k in phases])
        if ok:
            self.start = startversuch['startstoptiming'][phases[0]][0]['start'].timestamp()
            self.end = startversuch['startstoptiming'][phases[-1]][0]['end'].timestamp()
        else:
            self.start = None
            self.end = None
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

    def left_upper_edge(self, dataItem, data, factor, xmax, ymax):
        self._data = data[((data['time'] >= int( self.start * 1000)) & (data['time'] <= int(self.end * 1000)))]
        x0 = self._data.iloc[0]['datetime']
        y0 = 0
        x1 = self._data.iloc[-1]['datetime']
        y1 = max(self._data[dataItem]) * factor
        self._data['helpline'] = self._data[dataItem]  + (x0 - self._data['datetime'])* (y1-y0)/(x1-x0) + y0
        point = self._data['helpline'].idxmax()
        if point == point: # test for not NaN
            edge = self._data.loc[point]
            xmax = edge['datetime']
            ymax = self._data.at[edge.name,dataItem]
        return xmax, ymax

    def collect(self, startversuch, result, data):
        pass

    def register(self,startversuch,vset=[],tfrom=None,tto=None):
        vset += self._vset
        vset = list(set(vset)) # make list unique in case other collectors request the same dataItems...
        if self.phase_timing(startversuch,self._phases):
            tfrom = self.check_from(tfrom)
            tto = self.check_to(tto)
        return vset, tfrom, tto

####################################################
### calculate loadramp, Targetload, Maxoad, ramprate  
####################################################
class Target_load_Collector(Start_Data_Collector):
    def __init__(self, results, engine, period_factor=3, helplinefactor=0.8):
        self.phases = ['loadramp']
        super().__init__(self.phases)
        self._e = engine
        self.ratedload = self._e['Power_PowerNominal']
        self._vset += ['Power_PowerAct']
        self.period_factor=period_factor
        self.helplinefactor=helplinefactor

    def collect(self, startversuch ,results, data):
        xmax = startversuch['endtime'] # set xmax to the latest possible time to avoid duration to be 0.
        ymax = 0.0
        if 'loadramp' in startversuch['startstoptiming']:
            if not data.empty:
                if data['Power_PowerAct'].min() > 0.05 * self.ratedload:   # removes false detected cases with constant load
                    results['run2_failed'].append(startversuch)
                    return results
                try:
                    maxload = data['Power_PowerAct'].max()
                    xmax, ymax = self.left_upper_edge('Power_PowerAct', data, self.helplinefactor, xmax, ymax)
                except Exception as err:
                    # Berechnung sAbbrechen
                    results['run2_failed'].append(startversuch)
                    return results
            duration = xmax.timestamp() - self.start
            if duration > 900:              #5 * startversuch['loadramp']:         # avoid cases, where no meaningful targetload reached can be found in the data
                    results['run2_failed'].append(startversuch)
                    return results                
            ramprate = ymax / duration
            if  duration < 5: # why this code , forgot the purpose?
                xmax = startversuch['endtime']
                ymax = 0.0

            sno = startversuch['no']
            results['starts'][sno]['startstoptiming']['loadramp'][0]['end'] = xmax
            if 'targetoperation' in results['starts'][sno]['startstoptiming']:
                results['starts'][sno]['startstoptiming']['targetoperation'][0]['start'] = xmax
            results['starts'][sno]['targetload'] = ymax
            results['starts'][sno]['ramprate'] = ramprate / self.ratedload * 100.0
            results['starts'][sno]['maxload'] = maxload
        return results

    def register(self,startversuch,vset=[],tfrom=None,tto=None):
        vset, tfrom, tto = super().register(startversuch,vset,tfrom,tto)
        if (self.start is not None) and (self.end is not None):
            self.end = self.start + self.period_factor * (self.end-self.start)
            tto = self.check_to(tto)
        return vset, tfrom, tto

####################################
### collect Exhaust Temperature Data
####################################
class Exhaust_temp_Collector(Start_Data_Collector):
    def __init__(self, results, engine):
        self.name = 'exhaust'

        # where ?
        self.phases = ['loadramp']
        super().__init__(self.phases)

        # what ?
        self._e = engine
        self.tcyl = self._e.dataItemsCyl('Exhaust_TempCyl*')
        self._vset += ['Power_PowerAct','Exhaust_TempCylMin','Exhaust_TempCylMax'] + self.tcyl

        # results to collect:
        #self._content = ['ExhTempCylMax','ExhSpread_at_Max','Power_at_ExhTempCylMax','ExhSpreadMax','Power_at_ExhSpreadMax']
        self._content = ['ExTCylMax',
                        'ExTCylMaxNo',
                        'ExTCylMin@Max',
                        'ExTCylMin@MaxNo',
                        'ExSpread@Max',
                        'PWR@ExTCylMax',
                        'ExSpreadMax',
                        'ExTCylMax@SpreadMax',
                        'ExTCylMax@SpreadMaxNo',
                        'ExTCylMin@SpreadMax',
                        'ExTCylMin@SpreadMaxNo',
                        'PWR@ExSpreadMax']
        results['run2_content'][self.name] = ['no'] + self._content

    def min_max_cyl_positions(self, tdata, pos):
        pass

    def collect(self, startversuch, results, data):
        tdata = self.cut_data(startversuch, data, self._phases)
        res = { k:np.nan for k in self._content } # initialize results
        if not tdata.empty:

            point = tdata['Exhaust_TempCylMax'].idxmax()
            if point == point: # test for not NaN
                datapoint = tdata.loc[point]
                te = list(datapoint[self.tcyl])
                tmax = max(te); tmax_pos = te.index(tmax)
                tmin = min(te); tmin_pos = te.index(tmin)
                #tmax_org = tdata.at[datapoint.name,'Exhaust_TempCylMax']
                #tmin_org = tdata.at[datapoint.name,'Exhaust_TempCylMin']
                tspread = tmax - tmin
                tpow  = tdata.at[datapoint.name,'Power_PowerAct']
                res.update({'ExTCylMax':tmax,
                            #'ExhTempCylMaxOrg':tmax_org,
                            'ExTCylMaxNo':tmax_pos + 1,
                            'ExTCylMin@Max':tmin,
                            #'ExhTempCylMin_at_Max_org':tmin_org,
                            'ExTCylMin@MaxNo':tmin_pos +1 ,
                            'ExSpread@Max':tspread,  
                            'PWR@ExTCylMax':tpow })

            #ExhaustTempSpreadMax            
            tdata['spread'] = tdata['Exhaust_TempCylMax'] - tdata['Exhaust_TempCylMin']
            point = tdata['spread'].idxmax()
            if point == point:
                sdatapoint = tdata.loc[point]
                ste = list(sdatapoint[self.tcyl])
                stmax = max(ste); stmax_pos = ste.index(stmax)
                stmin = min(ste); stmin_pos = ste.index(stmin)
                spreadmax = tdata.at[sdatapoint.name,'spread']
                spreadpow = tdata.at[sdatapoint.name,'Power_PowerAct']
                res.update({'ExSpreadMax':spreadmax,
                            'ExTCylMax@SpreadMax':stmax,
                            'ExTCylMax@SpreadMaxNo':stmax_pos + 1,
                            'ExTCylMin@SpreadMax':stmin,
                            'ExTCylMin@SpreadMaxNo':stmin_pos + 1,
                            'PWR@ExSpreadMax':spreadpow })

            # # ExhaustTempMax            
            # point = tdata['Exhaust_TempCylMax'].idxmax()
            # if point == point: # test for not NaN
            #     datapoint = tdata.loc[point]
            #     tmax = tdata.at[datapoint.name,'Exhaust_TempCylMax']
            #     tmin = tdata.at[datapoint.name,'Exhaust_TempCylMin']
            #     tspread = tmax - tmin
            #     tpow  = tdata.at[datapoint.name,'Power_PowerAct']
            #     res.update({'ExhTempCylMax':tmax,
            #                 'ExhSpread_at_Max': tspread,  
            #                 'Power_at_ExhTempCylMax': tpow })

            # # ExhaustTempSpreadMax            
            # tdata['spread'] = tdata['Exhaust_TempCylMax'] - tdata['Exhaust_TempCylMin']
            # point = tdata['spread'].idxmax()
            # if point == point:
            #     sdatapoint = tdata.loc[point] 
            #     spreadmax = tdata.at[sdatapoint.name,'spread']
            #     spreadpow = tdata.at[sdatapoint.name,'Power_PowerAct']
            #     res.update({'ExhSpreadMax':spreadmax,
            #                 'Power_at_ExhSpreadMax':spreadpow })

        sno = startversuch['no']
        results['starts'][sno].update(res) 
        return results 

####################################
### collect Tecjet Data
####################################
class Tecjet_Collector(Start_Data_Collector):
    def __init__(self, results, engine):
        self.phases = ['loadramp']
        super().__init__(self.phases)
        self._e = engine
        self._vset += ['TecJet_Lambda1','TecJet_GasTemp1','TecJet_GasPress1','TecJet_GasDiffPress','TecJet_ValvePos1']
        self._content = ['TJ_GasDiffPressMin','TJ_GasPress1_at_Min','TJ_GasTemp1_at_Min','TJ_Pos_at_Min']
        # define table results:
        results['run2_content']['tecjet'] = ['no'] + self._content

    def collect(self, startversuch, results, data):
        tjdata = self.cut_data(startversuch, data, self._phases)
        res = { k:np.nan for k in self._content } # initialize results
        if not tjdata.empty:
            point = tjdata['TecJet_GasDiffPress'].idxmin()
            if point == point: # test for not NaN
                datapoint = tjdata.loc[point]
                dpmin = tjdata.at[datapoint.name,'TecJet_GasDiffPress']
                p_at_dpmin = tjdata.at[datapoint.name,'TecJet_GasPress1']
                t_at_dpmin = tjdata.at[datapoint.name,'TecJet_GasTemp1']
                pos_at_dpmin = tjdata.at[datapoint.name,'TecJet_ValvePos1']
                res = {
                        'TJ_GasDiffPressMin': dpmin,  
                        'TJ_GasPress1_at_Min': p_at_dpmin,  
                        'TJ_GasTemp1_at_Min': t_at_dpmin,
                        'TJ_Pos_at_Min': pos_at_dpmin
                    }
        sno = startversuch['no']
        results['starts'][sno].update(res)
        return results  

####################################
### collect Synchronization Data
####################################
class Sync_Current_Collector(Start_Data_Collector):
    def __init__(self, results, engine):
        self.phases = ['idle','synchronize']
        super().__init__(self.phases)
        self._e = engine
        self._speed_nominal = self._e.Speed_nominal
        self._vset += ['Various_Values_SpeedAct','TecJet_Lambda1', 'Hyd_TempOil', 'Hyd_TempCoolWat']
        self._content = ['rpm_dmax','rpm_dmin','rpm_spread', 'Lambda_rpm_max', 'TempOil_rpm_max', 'TempCoolWat_rpm_max']
        # define table results:
        results['run2_content']['synchronisation'] = ['no'] + self._content


    def collect(self, startversuch, results, data):
        sydata = self.cut_data(startversuch, data, self._phases) # ['idle','synchronize']
        res = { k:np.nan for k in self._content } # initialize results
        if not sydata.empty:
            # lookup highest speed in phases
            point = sydata['Various_Values_SpeedAct'].idxmax()
            if point == point: # test for not NaN
                datapoint = sydata.loc[point]
                xmax = datapoint['datetime']
                res['rpm_dmax'] = sydata.at[datapoint.name,'Various_Values_SpeedAct'] # - self._speed_nominal
                res['Lambda_rpm_max'] = sydata.at[datapoint.name,'TecJet_Lambda1']
                res['TempOil_rpm_max'] = sydata.at[datapoint.name,'Hyd_TempOil']
                res['TempCoolWat_rpm_max'] = sydata.at[datapoint.name,'Hyd_TempCoolWat']
                # filter data from point of highest speed to end of phase
                tsleft = int(xmax.timestamp() * 1e3)
                sydata2 = sydata[sydata.time > tsleft].reset_index(drop=True)
                if not sydata2.empty:
                    # and lookup lowest speed
                    point2 = sydata2['Various_Values_SpeedAct'].idxmin()
                    if point2 == point2:
                        datapoint2 = sydata2.loc[point2]
                        # calcultae speed spread during synchronization
                        res['rpm_dmin'] = sydata2.at[datapoint2.name,'Various_Values_SpeedAct'] # - self._speed_nominal
                        res['rpm_spread'] = res['rpm_dmax'] - res['rpm_dmin']
        sno = startversuch['no']
        results['starts'][sno].update(res)
        return results  


# --------------------- helper functions ---------------------------#

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

def msg_smalltxt(msg):
    return f"{msg['severity']} {pd.to_datetime(int(msg['timestamp'])*1e6).strftime('%d.%m.%Y %H:%M:%S')}  {msg['name']} {msg['message']}"

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

def get_size(obj, seen=None):
    """Recursively finds size of objects"""
    size = sys.getsizeof(obj)
    if seen is None:
        seen = set()
    obj_id = id(obj)
    if obj_id in seen:
        return 0
    # Important mark as seen *before* entering recursion to gracefully handle
    # self-referential objects
    seen.add(obj_id)
    if isinstance(obj, dict):
        size += sum([get_size(v, seen) for v in obj.values()])
        size += sum([get_size(k, seen) for k in obj.keys()])
    elif hasattr(obj, '__dict__'):
        size += get_size(obj.__dict__, seen)
    elif hasattr(obj, '__iter__') and not isinstance(obj, (str, bytes, bytearray)):
        size += sum([get_size(i, seen) for i in obj])
    return size