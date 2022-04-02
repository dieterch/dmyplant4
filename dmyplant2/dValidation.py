from datetime import datetime
from functools import reduce
from numpy.lib.arraypad import _pad_dispatcher
from numpy.testing._private.utils import build_err_msg
import pandas as pd
import numpy as np
import sys
import logging
from dmyplant2.dEngine import Engine
from dmyplant2.dMyplant import MyPlant, load_pkl, save_pkl
from pprint import pprint as pp
from scipy.stats.distributions import chi2

import arrow
from pprint import pprint as pp, pformat as pf
from tqdm.auto import tqdm
from IPython.display import HTML, display

# class HandleID():
#     df = None
#     def __init__(self, filename=None, datdict=None):
#         if filename:
#             self._load_csv(filename)
#         elif datdict:
#             self._load_dict(datdict)
#         else:
#             raise ValueError("no Request data defined")        

#     def _load_csv(self, filename):
#         self.df = pd.read_csv(filename, sep=';', encoding='utf-8')

#     def _load_dict(self, dat):
#         self.df = pd.DataFrame(
#             [[k]+v for k, v in dat.items()], columns=['ID', 'myPlantName', 'unit'])

#     def _unit_name(self, name):
#         try:
#             ret = list(self.df[self.df['myPlantName'] == name]['unit'])[0]
#         except:
#             raise ValueError(f"HandleID: ItemId Name '{name}' not found")
#         return ret

#     def _unit_id(self, id):
#         try:
#             ret = list(self.df[self.df['ID'] == id]['unit'])[0]
#         except:
#             raise ValueError(f"HandleID: ItemId number '{id}' not found")        
#         return ret

#     def datdict(self):
#         return {rec['ID']: [rec['myPlantName'], rec['unit']] for rec in self.df.to_dict('records')}

#     def unit(self, id=None, name=None):
#         if id:
#             return self._unit_id(id)
#         elif name:
#             return self._unit_name(name)
#         else:
#             raise ValueError("no valid Parameters provided (id or name")


class Validation:

    _dash = None
    _val = None
    _engines = []

    @classmethod
    def from_dval(cls, mp, dval, lengine=Engine, eval_date=None, cui_log=False):

        return cls(mp,dval, lengine, eval_date, cui_log)

    def __init__(self, mp, dval, lengine=Engine, eval_date=None, cui_log=False):
        """ Myplant Validation object
            collects and provides the engines list.
            compiles a dashboard as pandas DataFrame
            dval ... Pandas DataFrame with the Validation Definition,
                     defined in Excel sheet 'validation'
        """
        self._mp = mp
        self._val = dval

        self._now_ts = datetime.now().timestamp()
        self._eval_ts = self._now_ts if not eval_date else eval_date
        self._valstart_ts = dval['val start'].min()

        engines = self._val.to_dict('records')
        # create and initialise all Engine Instances
        self._engines = []
        if not cui_log:
            pbar = tqdm(total=len(engines), ncols=80, mininterval=1, unit=' engines', desc="VAL Engines")

        for i, eng in enumerate(engines):
            try:
                e = lengine.from_eng(mp, eng)
            except:
                print("Engine Instances cannot not be created.")
                sys.exit(1)
            self._engines.append(e)
            log = f"{i:02d} {e}"
            logging.info(log)
            if cui_log:
                print(log)
            else:
                pbar.update()

        if not cui_log:
            pbar.close()

    @ classmethod
    def load_def_csv(cls, filename):
        """load CSV Validation definition file
        """ 
        return MyPlant.load_def_csv(filename)

    @ classmethod
    def load_def_excel(cls, filename, sheetname, mp=None):
        """load Excel Validation definition file
        """
        return MyPlant.load_def_excel(filename, sheetname, mp)

    @ classmethod
    def load_failures_csv(cls, filename):
        """load CSV Failure Observation file 

        example content:
        date;failures;serialNumber;comment
        28.12.2020;1;1319151;München V008 M1 Z8 - Reiber, mit Boroskop am 28.12.2020 festgestellt, Cold Scuff, Motor lief 431 Stunden nach BSI
        ....

        Args:
            filename ([string]): [Filename of Failure Observation file]

        Returns:
            [pd.dataFrame]: [Failure Observations as dataFrame]
        """
        fl = pd.read_csv(filename, sep=';', encoding='utf-8')
        fl['date'] = pd.to_datetime(fl['date'], format='%d.%m.%Y')
        return fl

    @ property
    def now_ts(self):
        """the current date as EPOCH timestamp"""
        return self._now_ts

    @ property
    def eval_ts(self):
        """the current date as EPOCH timestamp"""
        return self._eval_ts

    @ property
    def valstart_ts(self):
        """Validation Start as EPOCH timestamp"""
        return self._valstart_ts.timestamp()

    # @ property
    # def valstart(self):
    #     return self._valstart_ts

    @ property
    def dashboard(self):
        """ Validation Dasboard as Pandas DataFrame """
        ldash = [e.dash for e in self._engines]
        return pd.DataFrame(ldash)

    @ property
    def properties_keys(self):
        """
        Properties: Collect all Keys from all Validation engines
        in a list - remove double entries
        """
        keys = []
        for e in self._engines:
            keys += e.properties.keys()     # add keys of each engine
            keys = list(set(keys))          # remove all double entries
        keys = sorted(keys, key=str.lower)
        dd = []
        for k in keys:                      # for all keys in all Val Engines
            for e in self._engines:         # iterate through al engines
                if k in e.properties.keys():
                    d = e.properties.get(k, None)  # get property dict
                    if d['value']:                 # if value exists
                        dd.append([d['name'], d['id']])  # store name, id pair
                        break
        return pd.DataFrame(dd, columns=['name', 'id'])

    @ property
    def dataItems_keys(self):
        """
        DataItems: Collect all Keys from all Validation engines
        in a list - remove double entries
        """
        keys = []
        for e in self._engines:
            keys += e.dataItems.keys()     # add keys of each engine
            keys = list(set(keys))          # remove all double entries
        keys = sorted(keys, key=str.lower)
        dd = []
        for k in keys:                      # for all keys in all Val Engines
            for e in self._engines:         # iterate through al engines
                if k in e.dataItems.keys():
                    d = e.dataItems.get(k, None)  # get dataItem dict
                    if d.get('name', None):                 # if value exists
                        dd.append([
                            d.get('name', None),
                            d.get('unit', None),
                            d.get('id', None)
                        ])
                        break
        return pd.DataFrame(dd, columns=['name', 'unit', 'id'])

    @ property
    def properties(self):
        """
        Properties: Asset Data properties of all Engines
        as Pandas DataFrame
        """
        # Collect all Keys in a big list and remove double counts
        keys = []
        for e in self._engines:
            keys += e.properties.keys()  # add keys of each engine
            keys = list(set(keys))  # remove all double entries
        keys = sorted(keys, key=str.lower)
        try:
            keys.remove('IB ItemNumber Engine')
            keys.insert(0, 'IB ItemNumber Engine')
        except ValueError:
            raise
        # Collect all values in a Pandas DateFrame
        loc = [[e.get_property(k)
                for k in keys] + [e.id, e.Name] for e in self._engines]
        return pd.DataFrame(loc, columns=keys + ['AssetID', 'Name'])

    @ property
    def dataItems(self):
        """
        dataItems: Asset Data dataItems of all Engines
        as Pandas DataFrame
        """
        # Collect all Keys in a big list and remove double counts
        keys = []
        for e in self._engines:
            keys += e.dataItems.keys()
            keys = list(set(keys))
        keys = sorted(keys, key=str.lower)
        loc = [[e.get_dataItem(k)
                for k in keys] + [e.Name] for e in self._engines]
        return pd.DataFrame(loc, columns=keys + ['Name'])

    @ property
    def validation_definition(self):
        """
        Validation Definition Information as pandas DataFrame
        """
        return self._val

    @ property
    def engines(self):
        """
        list of Validation Engine Objects
        """
        return self._engines

    def eng_name(self, name):
        """
        Return the Engines containing Name Validation
        """
        try:
            return [e for e in self._engines if name in e.Name]
        except:
            raise ValueError(f'Engine {name} not found in Validation Engines')


    def eng_serialNumber(self, serialNumber):
        """
        Return the Engines containing Name Validation
        """
        try:
            return [e for e in self._engines if str(serialNumber) == str(e.serialNumber)][0]
        except:
            raise ValueError(
                f'Engine SN {serialNumber} not found in Validation Engines')


    def quick_report(self):

        from tabulate import tabulate

        # Read Values defined in tdef from Myplant into a pd.dataframe
        tdef = {161: 'Count_OpHour', 102: 'Power_PowerAct', 1258: 'OperationalCondition', 19074: 'Various_Bits_CollAlarm'}
        
        ntable = [[e] + [e['id']] + [e.get_dataItem(v) for v in tdef.values()] for e in self.engines]
        dft = pd.DataFrame(ntable, columns=['Name','id'] + list(tdef.values()))

        #pp(dft)

        d = self.dashboard

        print(f"{dft.OperationalCondition.count():2.0f} Engines / {d.P.sum()} PU's in Validation Fleet.\n")

        print(f"{dft[((dft.OperationalCondition == 'Running') | (dft.Power_PowerAct > 0))].OperationalCondition.count():2.0f} Validation Engines UP and Running")
        print(f"{dft[((dft.OperationalCondition != 'Running') & (dft.Power_PowerAct == 0))].OperationalCondition.count():2.0f} Validation Engines not Running:")
        print(f"{dft[dft.Power_PowerAct.isnull()].OperationalCondition.count():2.0f} Validation Engine(s) with unknown Running Condition:\n")


        print(f"{max(d['oph parts']):7.0f} fleet leader oph")
        #print(f"{np.quantile(d['oph parts'],q=0.75):7.0f} 75% quantile oph")
        #print(f"{np.median(d['oph parts']):7.0f} median oph")
        print(f"{np.quantile(d['oph parts'],q=0.5):7.0f} 50% quantile / median oph")
        #print(f"{np.quantile(d['oph parts'],q=0.25):7.0f} 25% quantile oph")

        print(f"{np.average(d['oph parts']):7.0f} average oph")
        print(f"{np.average(d['oph parts'].sort_values(ascending=False)[:10]):7.0f} average of top ten oph")
        print(f"{np.average(d['oph parts'].sort_values(ascending=True)[:10]):7.0f} average of last ten oph\n")

        print(f"{sum(d['oph parts']):7.0f} cumulated oph\n")

        print("\nEngines without contact:")

        display(HTML(dft[((dft.OperationalCondition == 'No Contact') | (dft.OperationalCondition == 'Never Connected'))].to_html(escape=False)))
        #print(tabulate(dft[((dft.OperationalCondition == 'No Contact') | (dft.OperationalCondition == 'Never Connected'))], headers=dft.columns),"\n")

        print("\nEngines not running:")
        display(HTML(dft[((dft.OperationalCondition != 'Running') & (dft.Power_PowerAct == 0))].to_html(escape=False)))
        #print(tabulate(dft[((dft.OperationalCondition != 'Running') & (dft.Power_PowerAct == 0))], headers=dft.columns),"\n")

        print("\nEngines with Alarm FLag != 0 or Tripped condition:")
        display(HTML(dft[(dft.Various_Bits_CollAlarm == 1) | (dft.OperationalCondition == 'Tripped')].to_html(escape=False)))
        #print(tabulate(dft[(dft.Various_Bits_CollAlarm == 1) | (dft.OperationalCondition == 'Tripped')], headers=dft.columns),"\n")

        dtripped = dft[(dft.Various_Bits_CollAlarm == 1) | (dft.OperationalCondition == 'Tripped')]
        # for eng in dtripped.values:
        #     le = eng[0] 
        #     print(le)
        #     dtrips = le.batch_hist_alarms(p_severities=[800], p_offset=0, p_limit=5)
        #     dtrips['datetime'] = pd.to_datetime(dtrips['timestamp'] * 1000000.0).dt.strftime("%m-%d-%Y %H:%m")
        #     print(tabulate(dtrips[['datetime', 'message', 'name','severity']]))
        #     print()
        
        return dtripped


if __name__ == "__main__":
    
    import pandas as pd
    import numpy as np
    import os
    import dmyplant2

    test = [
        "n;Validation Engine;serialNumber;val start;oph@start;starts@start;Asset ID",
        "0;POLYNT - 2 (1145166-T241) --> Sept;1145166;12.10.2020;31291;378;103791",
        "1;REGENSBURG;1175579;14.09.2020;30402;1351;106622",
        "2;ROCHE PENZBERG KWKK;1184199;27.04.2020;25208;749;108532"
        ]
    with open('temp.csv', 'w') as f:
        for line in test:
            f.write(line + '\n')

    dval = dmyplant2.Validation.load_def_csv("temp.csv")
    print(dval)

    print()
    vfn = os.getcwd() + '/data/validations.pkl'
    if os.path.exists(vfn):
        validations = load_pkl(vfn)
        print(pf(validations))
    print()


    dmyplant2.cred()
    mp = dmyplant2.MyPlant(7200)
    vl = dmyplant2.Validation.from_dval(mp,dval, cui_log=False) 
    d = vl.dashboard
    print(d.T)   
    os.remove('temp.csv')