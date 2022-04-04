from datetime import datetime, timedelta
import math
from multiprocessing.sharedctypes import Value
from pprint import pprint as pp
import pandas as pd
import numpy as np
from dmyplant2 import _validationsfile
from dmyplant2.dMyplant import epoch_ts, mp_ts, save_json, load_json, save_pkl, load_pkl
from dmyplant2.dPlot import datastr_to_dict
import sys
import os
import pickle
import logging
import json
import arrow
import warnings
warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)

class Engine:
    """
    Class to encapsulate Engine properties & methods
    for easy MyPlant Access
    """
    
    _info = {}
    #_validationsfile = '/data/validations.pkl'

    @classmethod
    def lookup_Installed_Fleet(cls, mp, sn):
        """load data from Installed Fleet file,
        note this date is not automatically updated.
        to update, call mp._fetch_installed_base(self):

        Args:
            mp : Myplant Instance
            sn : serialNumber

        Returns:
            _type_: _description_
        """
        f = mp.get_installed_fleet()
        df = f[f['serialNumber'] == str(sn)]
        return df.to_dict(orient='records')[0]

    @classmethod
    def _list_cached_validations(cls):
        vfn = os.getcwd() + _validationsfile
        if os.path.exists(vfn):
            validations = load_pkl(vfn)
            return pd.DataFrame(validations)

    @classmethod
    def _get_cached_validations(cls, sn):
        vfn = os.getcwd()  + _validationsfile
        validations = {}
        if os.path.exists(vfn):
            validations = load_pkl(vfn)
        return validations

    @classmethod
    def _save_cached_validations(cls, validations):
        vfn = os.getcwd() + _validationsfile
        save_pkl(vfn, validations)

    @classmethod
    def from_fleet(cls, mp, edf, n=0, name=None, valstart=None, oph_start=None, start_start=None, 
        Old_Parts_first_replaced_OPH=None, Old_Parts_replaced_before_upgrade=None):
        id = int(edf['id'])
        sn = str(edf['serialNumber'])
        validations = cls._get_cached_validations(sn)
        if str(sn) in validations:
            valrec = validations[str(sn)]
            valstart = pd.to_datetime(valrec['val start'],infer_datetime_format=True)
            oph_start = valrec['oph@start']
            start_start = valrec['starts@start']
            name = valrec['Validation Engine']
        else:
            if pd.isnull(valstart): # take Commissioning date if no valstart date is given.
                valstart = pd.to_datetime(edf['Commissioning Date'],infer_datetime_format=True)
                if pd.isnull(valstart):
                    valstart = pd.to_datetime(edf['IB Unit Commissioning Date'],infer_datetime_format=True)
                    if pd.isnull(valstart):
                        raise ValueError(f"SN {sn} => no valid commissioning data found.")
            valstart = pd.to_datetime(valstart,infer_datetime_format=True)
            ts = int(valstart.timestamp()*1e3)
            if not oph_start:
                oph_help = mp.historical_dataItem(id, 161, ts)
                oph_start = oph_help if oph_help else 0
            if not start_start:
                start_help = mp.historical_dataItem(id, 179, ts)
                start_start = start_help if start_help else 0
            if not name:
                name = edf['IB Site Name'] + ' ' + edf['Engine ID']            

            valrec = {
                    'Asset ID': id,
                    'Validation Engine': name,
                    'n': 999, # ????
                    'oph@start': oph_start,
                    'serialNumber': int(sn),
                    'starts@start': start_start,
                    'val start': valstart,
                    'source':'from_MyPlant'
                }

            validations[sn] = valrec
            cls._save_cached_validations(validations)

        return cls(
            mp,
            int(id), 
            int(sn), 
            n, 
            name, 
            valstart.date().strftime('%Y-%m-%d'), 
            oph_start, start_start, 
            Old_Parts_first_replaced_OPH, Old_Parts_replaced_before_upgrade)

    @classmethod
    def from_sn(cls, mp, sn, n=0, name=None, valstart=None, oph_start=None, start_start=None, 
        Old_Parts_first_replaced_OPH=None, Old_Parts_replaced_before_upgrade=None):
        edf = cls.lookup_Installed_Fleet(mp, sn)
        return cls.from_fleet(
            mp, 
            edf, 
            n, 
            name, 
            valstart, 
            oph_start, start_start, 
            Old_Parts_first_replaced_OPH, 
            Old_Parts_replaced_before_upgrade)

    @classmethod
    def from_eng(cls, mp, eng):
        # speichere ValidierungsInfo in einer lokalen Database
        # zur Nutzung durch die from_sn und from_fleet constructors

        vfn = os.getcwd() + '/data/validations.pkl'
        validations = {}

        if not ('Asset ID' in eng):
            edf = cls.lookup_Installed_Fleet(mp, eng['serialNumber'])
            eng['Asset ID'] = edf['id']

        if os.path.exists(vfn):
            validations = load_pkl(vfn)
        if validations and ((not eng['serialNumber'] in validations) or (validations[eng['serialNumber']]['source'] != 'from_eng')):
            eng['source'] = 'from_eng'
            validations[eng['serialNumber']] = eng
            save_pkl(vfn, validations)

        return cls(
            mp, 
            eng['Asset ID'],
            eng['serialNumber'], 
            eng['n'],
            eng['Validation Engine'],
            eng['val start'],
            eng['oph@start'],
            eng['starts@start'] if 'starts@start' in eng else 0,
            eng['Old Parts first replaced OPH'] if 'Old Parts first replaced OPH' in eng else None,
            eng['Old Parts replaced before upgrade'] if 'Old Parts replaced before upgrade' in eng else None)

    def __init__(self, mp, id=None, sn=None, n=None, name=None, valstart = None, oph_start=None, start_start=None, 
        Old_Parts_first_replaced_OPH=None, Old_Parts_replaced_before_upgrade=None):
        """Engine Constructor

        Args:
            mp (dmyplant2.Myplant): Myplant class instance
            eng (dict): Validation engine input data

        Doctest:
        >>> e = dmyplant2.Engine.from_sn(mp, '1320072')
        >>> e.serialNumber
        '1320072'
        """

        # if not all([sn!= None,name!= None,valstart!= None,oph_start!= None,start_start!=None]):
        #     raise ValueError('Engine Constructor - missing parameters')

        # if (id==None or sn == None or name== None or valstart== None or oph_start== None or start_start==None):
        #     raise ValueError('Engine Constructor - missing parameters')

        # take engine Myplant Serial Number from Validation Definition
        self._mp = mp
        self._id = str(id)
        self._sn = str(sn)
        self._name = name
        self._data_base = os.getcwd() + f'/data/{str(self._sn)}'
        #self._data_base = os.getcwd() + f'/data/{str(self._sn)}'
        if not os.path.exists(self._data_base):
            os.makedirs(self._data_base)        
        self._picklefile = self._fname + '.pkl'    # load persitant data
        self._infofile = self._fname + '.json'
        self._last_fetch_date = None


        # load info json & lastfetchdate
        try:
            with open(self._infofile) as f:
                #self._info = json.load(f)
                _info = json.load(f)
                #if 'last_fetch_date' in self._info:
                if 'last_fetch_date' in _info:
                    self._last_fetch_date = _info['last_fetch_date']
                    #self._last_fetch_date = self._info['last_fetch_date']
                #self._info = {**self._info, **self._eng}
                #self._info['val start'] = arrow.get(
                #    self._eng['val start']).timestamp()
        except FileNotFoundError:
            pass

        # except: # gefährlicher, uspezifischer Code ?!
        #     self._info = {**self._info, **self._eng}
        try:
            cachexpired = self._cache_expired()['bool']
            checkpickle = self._check_for_pickling_error()
            if cachexpired or not checkpickle:
                
                local_asset = self._mp._asset_data(self._sn)
                #local_asset = self._mp._asset_data_graphQL(self._id)

                #logging.debug(f"{temp.eng['Validation Engine']}, Engine Data fetched from Myplant")
                logging.debug(f"{name}, Engine Data fetched from Myplant")
                #local_asset['validation'] = temp.eng
                local_val = Engine.lookup_Installed_Fleet(self._mp, self._sn)
                local_val.update({  
                    'val start': pd.to_datetime(valstart,infer_datetime_format=True),
                    'oph@start': int(oph_start),
                    'starts@start': int(start_start)
                })
                local_asset['validation'] = local_val
                self.assetdata = self._restructure(local_asset)

                # add patch.json values
                fpatch = os.getcwd() + '/patch.json'
                if os.path.exists(fpatch):
                    with open(os.getcwd() + "/patch.json", "r", encoding='utf-8-sig') as file:
                        patch = json.load(file)
                        if self._sn in patch:
                            for k,v in patch[self._sn].items():
                                if k in self.assetdata:
                                    self.assetdata[k] = {**self.assetdata[k], **v}
                                else:
                                    self.assetdata[k] = v

                self._last_fetch_date = epoch_ts(datetime.now().timestamp())
            else:
                self.__dict__ = self.ldata
                # with open(self._picklefile, 'rb') as handle:
                #     self.__dict__ = pickle.load(handle)
        except FileNotFoundError:
            logging.debug(
                f"{self._picklefile} not found, fetch Data from MyPlant Server")
        else:
            logging.debug(
                f"{__name__}: in cache mode, load data from {self._sn}.pkl")
        finally:
            logging.debug(
                f"Initialize Engine Object, SerialNumber: {self._sn}")
            #for k,v in self.lookup_Installed_Fleet(self._mp,self._sn).items():
            #    self[k] = v
            #self._engine_data(temp.eng)
            self._engine_data()
            self._set_oph_parameter()
            self._save()

    @property
    def _fname(self):
        return self._data_base + '/' + self._sn

    def _check_for_pickling_error(self):
        if os.path.exists(self._picklefile):
            try:
                with open(self._picklefile, 'rb') as handle:
                    self.ldata = pickle.load(handle)
                    return True
            #except pickle.PicklingError as err:
            except Exception:
                self.ldata = None
                return False
        else:
            return False

    def __str__(self):
        return f"{self['serialNumber']} {self['Name']}"
        #return f"{self['serialNumber']} {self['Engine ID']} {self['Name'][:20] + (self['Name'][20:] and ' ..'):23s}"

    # lookup name in all available myplant datastructures & the valdation definition dict
    def _get_xxx(self, name):
        # search key in myplant asset structure
        _keys = ['nokey', 'dataItems', 'properties', 'validation']
        for _k in _keys:
            _res = self.get_data(_k, name)
            if _res:
                return _res # found => return value & exit function
        return _res

    def __setitem__(self, key, value):
        self.assetdata[key] = value

    def __getitem__(self, key):
        if isinstance(key, list):
            return [self._get_xxx(k) for k in key]
        else:
            return self._get_xxx(key)

    def __getattr__(self,name):
        return self[name]

    def _get_keyItem_xxx(self, name):
        # search key in myplant asset structure
        _keys = ['dataItems', 'properties']
        for _k in _keys:
            _res = self.get_keyItem_data(_k, name)
            if _res:
                return _res # found => return value & exit function
        return None # not found, return None.

    def get_keyItem(self,key):
        return self._get_keyItem_xxx(key)

    def get_keyId(self,key):
        try:
            return self._get_keyItem_xxx(key)['id']
        except KeyError:
            raise ValueError(f'no "id" for "{key}" found.')

    def get_dataItems(self, dat=['Count_OpHour']):
        ret = {}
        for item in dat:
            res = self.get_keyItem(item)
            ret.update({ res.get('id',None) : [res.get('name',None),res.get('unit', '')] })
        return ret

    def assess_dataItems(self, testset, p_ts):
        result = []
        for item in testset:
            try:
                testdata = self._mp.hist_data(
                    self.id,
                    itemIds= self.get_dataItems([item]), 
                    p_from=p_ts, 
                    p_to=p_ts.shift(seconds=1), 
                    timeCycle=1,
                    silent=True)
                result.append({
                    item:True, 
                    'value': testdata[item].iloc[0] if not testdata.empty else -9999,
                    })
            except ValueError as err:
                result.append({item:False})
        return result

    def dataItemsCyl(self, name):
        return [name.replace('*',f"{i+1:02d}") for i in range(self.Cylinders)]

    @property
    def time_since_last_server_contact(self):
        """get time since last Server contact

        Returns:
            float: time since last Server contact
        """
        now = datetime.now().timestamp()
        if self._last_fetch_date != None:
            delta = now - self._last_fetch_date
        else: 
            delta = 0.0
        return delta

    def _cache_expired(self):
        delta = self.time_since_last_server_contact
        return {'delta': delta, 'bool': delta > self._mp.caching}


    def _restructure(self, local_asset):
        # restructure downloaded data for easier data lookup
        # beautiful effective python: dict comprehension :-)
        local_asset['properties'] = {
            p['name']: p for p in local_asset['properties']}
        local_asset['dataItems'] = {
            d['name']: d for d in local_asset['dataItems']}
        local_asset['validation'] = { 
            k: {'name': k, 'value' : v} for k,v in local_asset['validation'].items()}
        return local_asset

    def _set_oph_parameter(self):
        # for the oph(ts) function
        # this function uses the exect date to calculate
        # the interpolation line
        llastDataFlowDate = epoch_ts(self.assetdata['status'].get('lastDataFlowDate',None))
        
        if ( llastDataFlowDate - self.valstart_ts ) == 0:
            self._k = 0
        else:
            self._k = float(self['oph_parts'] /
                            (llastDataFlowDate - self.valstart_ts))
        # for the oph2(ts) function
        # this function uses the myplant reported hours and the
        # request time to calculate the inperpolation
        # for low validation oph this gives more consistent results
        if (arrow.now().timestamp() - self.valstart_ts) == 0:
            self._k = 0
        else:
            self._k2 = float(self['oph_parts'] /
                            (arrow.now().timestamp() - self.valstart_ts))

    def oph(self, ts):
        """Interpolated Operating hours

        Args:
            ts (log int): timestamp

        Returns:
            float: Operating time rel. to Validation start

        doctest:
        >>> e = dmyplant2.Engine.from_sn(mp, '1320072', name='BMW LANDSHUT 4.10',valstart='07.02.2020', oph_start = 6316)
        >>> 3000.0 <= e.oph2(arrow.get("2021-03-01 00:00").timestamp())
        True
        """
        y = self._k * (ts - self.valstart_ts)
        y = y if y > 0.0 else 0.0
        return y

    def oph2(self, ts):
        """Interpolated Operating hours, method 2

        Args:
            ts (log int): timestamp

        Returns:
            float: Operating time rel. to Validation start

        doctest:
        >>> e = dmyplant2.Engine.from_sn(mp, '1320072', name='BMW LANDSHUT 4.10',valstart='07.02.2020', oph_start = 6316)
        >>> 3000.0 <= e.oph2(arrow.get("2021-03-01 00:00").timestamp())
        True
        """
        y = self._k2 * (ts - self.valstart_ts)
        y = y if y > 0.0 else 0.0
        return y

    #def _engine_data(self, eng):
    def _engine_data(self):

        #self._valstart_ts = epoch_ts(arrow.get(self['val start']).timestamp())
        #self._lastDataFlowDate = epoch_ts(self['status'].get(
        #            'lastDataFlowDate', None))
        if self['Engine Type']:
            self['P'] = int(str(self['Engine Type'])[-2:] if str(self['Engine Type'])[-2:].isdigit() else 0)
            #self._P = int(str(self['Engine Type'])[-2:] if str(self['Engine Type'])[-2:].isdigit() else 0)
        else:
            raise Exception(f'Key "Engine Type" missing in asset of SN {self._sn}\nconsider a patch in patch.json')

        # for compatibility
        self['oph_start'] = self['oph@start']
        self['starts_start'] = self['starts@start']
        self['val_start'] = self['val start']
        #self.serialNumber = self['serialNumber']
        #self.id = self['id']

        self['Name'] = self._name 
        self['Name'] = self['Name'] if self['Name'] else self['IB Project Name']
        self._name = self['Name']
        self['Validation Engine'] = self._name
        self['oph_parts'] = self['Count_OpHour'] - self['oph@start']
        #self.oph@start = self['oph@start']
        # for compatibility

    def _save(self):
        try:
            _info = { 'last_fetch_date' : self._last_fetch_date }
            #self._info['Validation Engine'] = self['IB Project Name']
            #self._info['val start'] = arrow.get(
            #    self['val start']).format('YYYY-MM-DD')
            with open(self._infofile, 'w') as f:
                json.dump(_info, f)
                #json.dump(self._info, f)
        except FileNotFoundError:
            errortext = f'Cound not write to File {self._infofile}.'
            logging.error(errortext)
            raise

        try:
            with open(self._picklefile, 'wb') as handle:
                pickle.dump(self.__dict__, handle, protocol=4)
        except FileNotFoundError:
            errortext = f'Cound not write to File {self._picklefile}.'
            logging.error(errortext)
            raise

    def get_data(self, key, item):
        """
        Get Item Value by Key, Item Name pair
        valid Myplant Keys are
        'nokey' data Item in Asset Date base structure
        'properties' data Item is in 'properties' list
        'dataItems' data Item is in 'dataItems' list

        e.g.: oph = e.get_data('dataItems','Count_OpHour')
        
        doctest:
        >>> e = dmyplant2.Engine.from_sn(mp, '1320072')
        >>> e.get_data('nokey','id')
        117617
        >>> e.get_data('nokey','nothing') == None
        True
        >>> e.get_data('dataItems','Power_PowerNominal')
        4500.0
        >>> e.get_data('dataItems','nothing') == None
        True
        >>> e.get_data('properties','Engine ID')
        'M4'
        >>> e.get_data('properties','nothing') == None
        True
        """
        return self.assetdata.get(item, None) if key == 'nokey' else self.assetdata[key].setdefault(item, {'value': None})['value']

    def get_keyItem_data(self, key, item):
        return self.assetdata.get(item, None) if key == 'nokey' else self.assetdata[key].setdefault(item, {'value': None})

    def get_property(self, item):
        """
        Get properties Item Value by Item Name

        e.g.: vers = e.get_property("Engine Version")

        doctest:
        >>> e = dmyplant2.Engine.from_sn(mp, '1320072')
        >>> e.get_property('Engine ID')
        'M4'
        >>> e.get_property('nothing') == None
        True
        """
        try:
            return self.get_data('properties', item)
        except:
            raise            

    def get_dataItem(self, item):
        """
        Get  dataItems Item Value by Item Name

        e.g.: vers = e.get_dataItem("Monic_VoltCyl01")

        doctest:
        >>> e = dmyplant2.Engine.from_sn(mp, '1320072')
        >>> e.get_dataItem('Power_PowerNominal')
        4500.0
        >>> e.get_dataItem('nothing') == None
        True
        """
        try:
            return self.get_data('dataItems', item)
        except:
            raise

    def assess_dataItems(self, testset, p_ts):
        result = []
        for item in testset:
            try:
                testdata = self._mp.hist_data(
                    self.id,
                    itemIds= self.get_dataItems([item]), 
                    p_from=p_ts, 
                    p_to=p_ts.shift(seconds=1), 
                    timeCycle=1,
                    silent=True)
                result.append({
                    item:True, 
                    'value': testdata[item].iloc[0] if not testdata.empty else -9999,
                    })
            except ValueError as err:
                result.append({item:False})
        return result

    def historical_dataItem(self, itemId, timestamp):
        """
        Get historical dataItem
        dataItemId  int64   Id of the DataItem to query.
        timestamp   int64   Optional,  timestamp in the DataItem history to query for.

        doctest:
        >>> e = dmyplant2.Engine.from_sn(mp, '1320072')
        >>> e.historical_dataItem(161, arrow.get("2021-03-01 00:00").timestamp())
        12575.0
        """
        try:
            res = self._mp.historical_dataItem(
                self['id'], itemId, mp_ts(timestamp)).get('value', None)
        except Exception as e:
            print(e)
            res = None
        return res

    def batch_hist_dataItem(self, itemId, p_from, p_to, timeCycle=3600):
        """
        Get np.array of a single dataItem history
        dataItemId  int64   Id of the DataItem to query.
        p_from      int64   timestamp start timestamp.
        p_to        int64   timestamp stop timestamp.
        timeCycle   int64   interval in seconds.
        """
        try:
            res = self._mp.history_dataItem(
                self['id'], itemId, mp_ts(p_from), mp_ts(p_to), timeCycle)
            df = pd.DataFrame(res)
            df.columns = ['timestamp', str(itemId)]
            return df
        except:
            pass

    def hist_data(self, itemIds={161: ['CountOph', 'h']}, p_limit=None, p_from=None, p_to=None, timeCycle=86400,
                  assetType='J-Engine', includeMinMax='false', forceDownSampling='false', slot=0, 
                  forceReload=False, debug=False, userfunc=None, silent=False):
        """
        Get pandas dataFrame of dataItems history, either limit or From & to are required
        ItemIds             dict   e.g. {161: ['CountOph','h']}, dict of dataItems to query.
        p_limit             number of datapoints back from "now".
        p_from              string from iso date or timestamp,
        p_to                string stop iso date or timestamp.
        timeCycle           int64  interval in seconds.
        assetType           string default 'J-Engine'
        includeMinMax       string 'false'
        forceDownSampling   string 'false'
        slot                int     dataset differentiator, defaults to 0
        forceReload         bool    force reload of data from Myplant, defaults to False
        """

        def collect_info():
            # File Metadata
            info = self._info
            info['p_from'] = p_from
            info['p_to'] = p_to
            info['Timezone'] = 'Europe/Vienna'
            info['timeCycle'] = timeCycle
            info['Exported_By'] = self._mp.username
            info['Export_Date'] = arrow.now().to(
                'Europe/Vienna').format('DD.MM.YYYY - HH:mm')
            info['dataItems'] = itemIds
            return pd.DataFrame.from_dict(info)

        def check_and_loadfile(p_from, fn, forceReload):
            ldf = pd.DataFrame([])
            last_p_to = p_from
            if forceReload:
                if os.path.exists(fn):
                    os.remove(fn)
            if os.path.exists(fn):
                try:
                    dinfo = pd.read_hdf(fn, "info").to_dict()
                    # wenn die daten im file den angeforderten daten entsprechen ...
                    if set(itemIds) == set(dinfo['dataItems']):
                        ffrom = list(dinfo['p_from'].values())[0]
                        if ffrom.to('Europe/Vienna') <= p_from.to('Europe/Vienna'):
                            ldf = pd.read_hdf(fn, "data")
                            os.remove(fn)
                            # Check last lp_to in the file and update the file ....
                            last_p_to = arrow.get(
                                list(ldf['time'][-2:-1])[0]).to('Europe/Vienna')
                            # list(ldf['time'][-2:-1])[0] + timeCycle)
                            # new starting point ...
                            if debug:
                                print(f"\nitemIds: {set(itemIds)}, Shape={ldf.shape}, from: {p_from.format('DD.MM.YYYY - HH:mm')}, to:   {last_p_to.format('DD.MM.YYYY - HH:mm')}, loaded from {fn}")
                except:
                    pass
            return ldf, last_p_to

        try:
            # make sure itemids have the format { int: [str,str], int: [str,str], ...}
            itemIds = { int(k):v for (k,v) in itemIds.items() }
            
            df = pd.DataFrame([])
            fn = self._fname + fr"_{timeCycle}_{int(slot):02d}.hdf"
            df, np_from = check_and_loadfile(p_from, fn, forceReload)

            np_to = arrow.get(p_to).shift(seconds=-timeCycle)
            if np_from.to('Europe/Vienna') < np_to.to('Europe/Vienna'):
                ndf = self._mp.hist_data(
                    self['id'], itemIds, np_from, p_to, timeCycle, silent=silent)

                # 2022-02-19 pandas Deprecation warning: use pd.concat instead of append.
                #df = df.append(ndf)
                df = pd.concat([df,ndf])

                if debug:
                    print(f"\nitemIds: {set(itemIds)}, Shape={ndf.shape}, from: {np_from.format('DD.MM.YYYY - HH:mm')}, to:   {p_to.format('DD.MM.YYYY - HH:mm')}, added to {fn}")

            df.reset_index(drop=True, inplace=True)

            dinfo = collect_info()
            dinfo.to_hdf(fn, "info", complevel=6)
            df.to_hdf(fn, "data", complevel=6)
            if userfunc:
                print("Calling user defined function...")
                df = userfunc(df)

            return df
        except:
            raise ValueError("Engine hist_data Error")

###########################################
#improved hist_data ? Dieter, 8.3.2022

    def hist_data2(self, itemIds={161: ['CountOph', 'h']}, p_limit=None, p_from=None, p_to=None, timeCycle=86400,
                  assetType='J-Engine', includeMinMax='false', forceDownSampling='false', slot=0, 
                  forceReload=False, debug=False, userfunc=None, silent=False, suffix=''):
        """
        Get pandas dataFrame of dataItems history, either limit or From & to are required
        ItemIds             dict   e.g. {161: ['CountOph','h']}, dict of dataItems to query.
        p_limit             number of datapoints back from "now".
        p_from              string from iso date or timestamp,
        p_to                string stop iso date or timestamp.
        timeCycle           int64  interval in seconds.
        assetType           string default 'J-Engine'
        includeMinMax       string 'false'
        forceDownSampling   string 'false'
        slot                int     dataset differentiator, defaults to 0
        forceReload         bool    force reload of data from Myplant, defaults to False
        """

        def collect_info():
            # File Metadata
            info = self._info
            info['p_from'] = p_from
            info['p_to'] = p_to
            info['Timezone'] = 'Europe/Vienna'
            info['timeCycle'] = timeCycle
            info['Exported_By'] = self._mp.username
            info['Export_Date'] = arrow.now().to(
                'Europe/Vienna').format('DD.MM.YYYY - HH:mm')
            info['dataItems'] = itemIds
            return pd.DataFrame.from_dict(info)

        def check_and_loadfile(p_from, fn, itemIds, forceReload):
            ldf = pd.DataFrame([])
            last_p_to = p_from
            if forceReload:
                if os.path.exists(fn):
                    os.remove(fn)
            if os.path.exists(fn):
                try:
                    dinfo = pd.read_hdf(fn, "info").to_dict()
                    # alt: wenn die daten im file den angeforderten daten entsprechen ...
                    #if set(itemIds) == set(dinfo['dataItems']):
                    
                    # neu: wenn die angeforderten Daten im File vorhanden sind, itemIds auf dInfo setzen und die im File vorhandenen Daten ergänzen...
                    if all([k in dinfo['dataItems'].keys() for k in itemIds ]):
                        itemIds = dinfo['dataItems']
                    ###########################################################    
                        ffrom = list(dinfo['p_from'].values())[0]
                        if ffrom.to('Europe/Vienna') <= p_from.to('Europe/Vienna'):
                            ldf = pd.read_hdf(fn, "data")
                            os.remove(fn)
                            # Check last lp_to in the file and update the file ....
                            last_p_to = arrow.get(
                                list(ldf['time'][-2:-1])[0]).to('Europe/Vienna')
                            # list(ldf['time'][-2:-1])[0] + timeCycle)
                            # new starting point ...
                            if debug:
                                print(f"\nitemIds: {set(itemIds)}, Shape={ldf.shape}, from: {p_from.format('DD.MM.YYYY - HH:mm:ss')}, to:   {last_p_to.format('DD.MM.YYYY - HH:mm:ss')}, loaded from {fn}")
                                print(ldf.head(5))
                                print('...')
                                print(ldf.tail(5))
                except:
                    pass
            return ldf, last_p_to, itemIds

        try:
            # make sure itemids have the format { int: [str,str], int: [str,str], ...}
            itemIds = { int(k):v for (k,v) in itemIds.items() }
            
            df = pd.DataFrame([])
            fn = self._fname + fr"_{timeCycle}_{int(slot):02d}{suffix}.hdf"
            df, np_from, itemIds = check_and_loadfile(p_from, fn, itemIds, forceReload)

            np_to = arrow.get(p_to).shift(seconds=-timeCycle)
            if np_from.to('Europe/Vienna') < np_to.to('Europe/Vienna'):
                ndf = self._mp.hist_data(
                    self['id'], itemIds, np_from, p_to, timeCycle, silent=silent)

                # 2022-02-19 pandas Deprecation warning: use pd.concat instead of append.
                #df = df.append(ndf)
                df = pd.concat([df,ndf])

                if debug:
                    print(f"\nitemIds: {set(itemIds)}, Shape={ndf.shape}, from: {np_from.format('DD.MM.YYYY - HH:mm:ss')}, to:   {p_to.format('DD.MM.YYYY - HH:mm:ss')}, added to {fn}")
                    print(ldf.head(5))
                    print('...')
                    print(ldf.tail(5))

            df.reset_index(drop=True, inplace=True)

            dinfo = collect_info()
            dinfo.to_hdf(fn, "info", complevel=6)
            df.to_hdf(fn, "data", complevel=6)
            if userfunc:
                print("Calling user defined function...")
                df = userfunc(df)

            return df
        except:
            raise ValueError("Engine hist_data2 Error")

###########################################

    def fetch_dataItems(self, ts, items):
        itemIds = self.get_dataItems(items)
        tdj = ','.join([str(s) for s in itemIds])
        url=fr"/asset/{self['id']}/history/batchdata?assetType=J-Engine&from={ts}&to={ts}&dataItemIds={tdj}&timeCycle=30"
        data =  self._mp.fetchdata(url)
        dtime = {'timestamp' : [value[0] for value in [data['data'][0]]]}
        ddata = {itemIds[skey][0]:[value[1][j][0] for value in [data['data'][0]]] for j,skey in enumerate(data['columns'][1])}
        return pd.DataFrame({**dtime,**ddata})
        # for python 9.x or higher 
        return pd.DataFrame(dtime|ddata)

    def _batch_hist_dataItems(self, itemIds={161: ['CountOph', 'h']}, p_limit=None, p_from=None, p_to=None, timeCycle=3600,
                              assetType='J-Engine', includeMinMax='false', forceDownSampling='false'):
        """
        Get pandas dataFrame of dataItems history, either limit or From & to are required
        DEPRECATED- please use hist_data2 instead.

        ItemIds             dict   e.g. {161: ['CountOph','h']}, dict of dataItems to query.
        limit               int64, number of points to download
        p_from              string from iso date or timestamp,
        p_to                string stop iso date or timestamp.
        timeCycle           int64  interval in seconds.
        assetType           string default 'J-Engine'
        includeMinMax       string 'false'
        forceDownSampling   string 'false'
        """
        try:
            tt = r""
            if p_limit:
                tt = r"&limit=" + str(p_limit)
            else:
                if p_from and p_to:
                    tt = r'&from=' + str(int(arrow.get(p_from).timestamp()) * 1000) + \
                        r'&to=' + str(int(arrow.get(p_to).timestamp()) * 1000)
                else:
                    raise Exception(
                        r"batch_hist_dataItems, invalid Parameters")

            tdef = itemIds
            tdj = ','.join([str(s) for s in tdef.keys()])

            ttimecycle = timeCycle
            tassetType = assetType
            tincludeMinMax = includeMinMax
            tforceDownSampling = forceDownSampling

            url = r'/asset/' + str(self['id']) + \
                r'/history/batchdata' + \
                r'?assetType=' + str(tassetType) + \
                tt + \
                r'&dataItemIds=' + str(tdj) + \
                r'&timeCycle=' + str(ttimecycle) + \
                r'&includeMinMax=' + str(tincludeMinMax) + \
                r'&forceDownSampling=' + str(tforceDownSampling)

            # fetch data from myplant ....
            data = self._mp.fetchdata(url)

            # restructure data to dict
            ds = dict()
            ds['labels'] = ['time'] + [tdef[x][0] for x in data['columns'][1]]
            ds['data'] = [[r[0]] + [rr[0] for rr in r[1]]
                          for r in data['data']]

            # import to Pandas DataFrame
            df = pd.DataFrame(ds['data'], columns=ds['labels'])
            # Addtional Datetime column calculated from timestamp
            df['datetime'] = pd.to_datetime(df['time'] * 1000000)
            return df
        except:
            raise Exception("Error in call to _batch_hist_dataItems")

    # def _Validation_period_LOC_prelim(self):
    #     """ Work in progress on a better LOC Function
            
    #         - synchronize other data etc.

    #     """
    #     def _localfunc(dloc):
    #         dat0 = {
    #             161: ['Count_OpHour', 'h'], 
    #             102: ['Power_PowerAct', 'kW'],
    #             228: ['Hyd_OilCount_Trend_OilVolume','ml'],
    #             107: ['Various_Values_SpeedAct','rpm'],
    #             69: ['Hyd_PressCoolWat','bar'],
    #             16546: ['Hyd_PressOilDif','bar']
    #         }

    #         l_from = arrow.get(dloc.datetime.iloc[-1])
    #         _cyclic = self.hist_data(
    #             itemIds= dat0, 
    #             p_from = l_from,
    #             p_to=arrow.now('Europe/Vienna'),
    #             timeCycle=60,
    #             slot=11
    #         )

    #         ts_list = list(dloc['time'])
    #         loc_list = list(dloc['OilConsumption'])

    #         # Add Values from _cyclic to dloc
    #         # add Count_OpHour
    #         #value_list = [_cyclic['Count_OpHour'].iloc[_cyclic['time'].values.searchsorted(a)] - self.oph@start for a in ts_list]
    #         #dloc['oph_parts'] = value_list
            
    #         # add Count_OpHour
    #         #value_list = [_cyclic['Power_PowerAct'].iloc[_cyclic['time'].values.searchsorted(a)] for a in ts_list]
    #         #dloc['Power_PowerAct'] = value_list

    #         # add Count_OpHour
    #         #value_list = [_cyclic['Hyd_OilCount_Trend_OilVolume'].iloc[_cyclic['time'].values.searchsorted(a)] for a in ts_list]
    #         #dloc['Hyd_OilCount_Trend_OilVolume'] = value_list


    #         # Add Values from dloc to _cyclic
    #         #_cyclic['OilConsumption'] = np.nan
    #         #for i, ts in enumerate(ts_list):
    #         #    _cyclic['OilConsumption'].iloc[_cyclic['time'].values.searchsorted(ts)] = loc_list[i]
    #             #print(f"LOC {loc_list[i]} at position {ts} inserted.")

    #         return dloc, _cyclic

    #     dloc = self.Validation_period_LOC()
    #     dloc=_localfunc(dloc)
    #     return dloc

    def Validation_period_LOC(self):
        """Oilconsumption vs. Validation period

        Raises:
            Exception: [description]
            Exception: [description]

        Returns:
            pd.DataFrame:

            columns
            227: ['OilConsumption', 'g/kWh'],
            237: ['DeltaOpH', 'h'],
            228: ['OilVolume', 'ml'],
            225: ['ActiveEnergy', 'MWh'],
            226: ['AvgPower', 'kW']
        """
        # Lube Oil Consumption data
        locdef = {
            227: ['OilConsumption', 'g/kWh'],
            # 237: ['DeltaOpH', 'h'],
            # 228: ['OilVolume', 'ml'],
            # 225: ['ActiveEnergy', 'MWh'],
            226: ['AvgPower', 'kW'],
        }

        limit = 4000

        try:
            dloc = self._batch_hist_dataItems(
                itemIds=locdef, p_limit=limit, timeCycle=1)
            #dloc = add_column(dloc, 161)
            cnt = dloc['OilConsumption'].count()
            DebugStr = f"Data Start {arrow.get(dloc.datetime.iloc[-1]).format('DD.MM.YYYY')}\nVal  Start {arrow.get(self.val_start).format('DD.MM.YYYY')}"
            DebugStr = "LOC, all available data received,\n" + DebugStr if (cnt != limit) else f"limit={int(limit)},\n" + DebugStr
            print(DebugStr)
        except:
            raise Exception("Loop Error in Validation_period_LOC")

        # skip values before validation start
        dloc = dloc[dloc.datetime > pd.to_datetime(self.val_start)]
        
        # Filter outliers by < 3 * stdev - remove refilling, engine work etc..
        dloc = dloc[np.abs(dloc.OilConsumption-dloc.OilConsumption.mean())
                    <= (3*dloc.OilConsumption.std())]

        # Calculate Rolling Mean values for Power and LOC
        dloc['LOC'] = dloc.OilConsumption.rolling(10).mean()
        dloc['Pow'] = dloc.AvgPower.rolling(10).mean()
        return dloc

    def timestamp_LOC(self,starttime, endtime, windowsize=50, return_OPH=False):  #starttime, endtime, 
        """Oilconsumption vs. Validation period

        Args:
            starttime: arrow object in right timezone
            endtime: arrow object in right timezone
            windowsize (optional): Engine instance to get number of cylinders from
            return_OPH (optional): Option to directly return the engine OPH in the dataframe at the LOC-data points

        Returns:
            pd.DataFrame:

        """
        #Lube Oil Consumption data
        locdef = ['Operating hours engine', 'Oil counter active energy', 'Oil counter power average', 'Oil counter oil consumption', 'Oil counter oil volume', 'Oil counter operational hours delta']
        
        ans1=datastr_to_dict(locdef)
        locdef=ans1[0]
        try:
#            dloc = self.hist_data(
            dloc = self.hist_data2(
                itemIds=locdef, p_from=starttime,
                p_to=endtime, timeCycle=3600, slot=1)
            dloc.rename(columns = ans1[1], inplace = True)

            dloc.drop(['time'], axis=1, inplace=True)
            dloc = dloc.set_index('datetime')
            dloc=dloc.drop_duplicates(['Oil counter active energy', 'Oil counter power average', 'Oil counter oil consumption', 'Oil counter oil volume', 'Oil counter operational hours delta'])


            dloc.drop(dloc[((dloc['Oil counter oil volume']*10)%1!=0)].index, inplace=True)
            dloc.drop(dloc[(dloc['Oil counter power average']%1!=0)].index, inplace=True)
            dloc.drop(dloc[(dloc['Oil counter operational hours delta']%1!=0)].index, inplace=True)

            dloc.drop(dloc[(dloc['Oil counter oil consumption']>5)].index, inplace=True) #Filter very large LOC, e.g. when refilling over the oil counter. Value according to Edward Rogers and Dieter Chvatal
            dloc.drop(dloc[(dloc['Oil counter oil consumption']<0.005)].index, inplace=True) #Filter very small LOC, according to Dieter Chavatal

            hoursum = 0
            volumesum = 0
            energysum = 0

            LOC_ws = []
            LOC_raw = []
            hours_filtered=[]
            OPH_engine=[]

            for i in range(len(dloc)):
                hoursum = hoursum + dloc.iloc[i, dloc.columns.get_loc('Oil counter operational hours delta')]
                volumesum = volumesum + dloc.iloc[i, dloc.columns.get_loc('Oil counter oil volume')]
                energysum = energysum + dloc.iloc[i, dloc.columns.get_loc('Oil counter active energy')]

                if hoursum >= windowsize:
                    LOC_ws.append(volumesum * 0.886 / energysum) #only make 3 decimal points
                    hoursum = 0
                    volumesum = 0
                    energysum = 0
                else:
                    LOC_ws.append(np.nan)

                LOC_raw.append (dloc.iloc[i, dloc.columns.get_loc('Oil counter oil consumption')])
                OPH_engine.append(dloc.iloc[i, dloc.columns.get_loc('Operating hours engine')])
                hours_filtered.append(dloc.index[i])

            
            if return_OPH:
                dfres = pd.DataFrame(data={'datetime': hours_filtered, 'OPH_engine': OPH_engine, 'LOC_average': LOC_ws, 'LOC_raw': LOC_raw})
            else:
                dfres = pd.DataFrame(data={'datetime': hours_filtered, 'LOC_average': LOC_ws, 'LOC_raw': LOC_raw})

            dfres=dfres.set_index('datetime')
        except:
                raise Exception("Loop Error in Validation_period_LOC")
        return dfres

    def get_OilStatus(self):
        oilStatus = list()
        reps = self.get_OilReports_Overview()
        #####
        #print()
        #pp(reps[:1])
        ####
        for rep in reps:
            if len(reps) > 0:
                loilStatus = rep
                loilStatus['date'] = arrow.get(rep['dateTaken']).format('DD.MM.YYYY')
                loilStatus['unitHours_at_sample'] = loilStatus['unitHours'] # move unithours to historic unithours
                loilStatus['unitHours'] = self['Count_OpHour'] # actual unithours

                loilStatus['oilHours_at_sample'] = loilStatus['oilHours'] # move oilhours to oil sample hours
                try:
                    loilStatus['oilHours'] = loilStatus['oilHours_at_sample'] + loilStatus['unitHours'] - loilStatus['unitHours_at_sample'] # recalc actual oil age (?) What if oil is excanged ?
                except TypeError as err:
                    print(f"Name: .... {str(err)}")
                    loilStatus['oilHours'] = loilStatus['oilHours_at_sample']
                oilStatus.append(loilStatus)
        return oilStatus

    @ property
    def get_OilGrade(self):
        reps = self.get_OilReports_Overview()
        if len(reps) > 0:
            oilGrade = reps[0]['oilGrade']
        else:
            oilGrade = 'Information not available'
        return oilGrade

    def get_OilReports_Overview(self):
        url = r'/asset/' + str(self['id']) + r'/report/Oil'
        try:
            try:
                res = load_json('data/oilreports_raw.json') # for development, avoid frequent downloads
                print("###### in dengine.getOilReports_Overview => sample Data for Development provided")
            except FileNotFoundError:
                res = self._mp.fetchdata(url)
                save_json('data/oilreports_raw.json', res)
                print("###### in dengine.getOilReports_Overview => sample Data saved for Development")
            # Fetch all Oil samples
            nrl = []
            for rep in res:
                rec = dict()
                rec['datetime'] = None
                rec['engineSerialNumber'] = rep['sampleMetadata'].get('engineSerialNumber', None)
                rec['jNumber'] = rep['sampleMetadata'].get('jNumber', None)
                rec['oilGrade'] = rep['sampleMetadata'].get('oilGrade', None)  
                rec['oilHours'] = rep['sampleMetadata'].get('oilHours', None)  
                rec['unitHours'] = rep['sampleMetadata'].get('unitHours', None)  
                rec['oilCondition'] = rep['sampleMetadata'].get('oilCondition', None)  
                rec['dateTaken'] =  rep['sampleMetadata'].get('dateTaken', None)
                rec['sampleId'] =  rep.get('sampleId', None)
                rec['provider'] =  rep.get('provider', None)
                nrl.append(rec)
        except:
            print(f"No Oil Report available for {self['Name']}")
            raise
        #save_json('oilreports.json', nrl)
        return nrl

    def get_OilReports(self):
        try:
            nrl = self.get_OilReports_Overview()
            for rec in nrl:
                try:
                    # fetch analysis details
                    rec.update(self.get_OilLabReport(rec['provider'], rec['sampleId']))
                except Exception as err:
                    print("###### Error:", str(err))
                    raise
            # move into a pd.dataframe
            dfrep = pd.DataFrame(nrl)
            dfrep['datetime'] = pd.to_datetime(dfrep['dateTaken'] * 1000000.0) #.dt.strftime("%d-%m-%Y")
            del dfrep['dateTaken']
            # and return oil data with earliest sample in first row.
        except:
            print(f"No Oil Reports available for {self['Name']}")
            return pd.DataFrame([])
        #return dfrep
        return dfrep.iloc[::-1]

    def get_OilLabReport(self, provider, sampleId):

        def get_corr(s, key, default):
            erg = s.get(key, default)
            if type(erg) == str:
                if erg in ['', '-', None]:
                    return None
                #if key[-6:] == "-alert":
                #    return erg
                if erg[0] == '<':
                    erg = erg[1:]
                try:
                    erg = float(erg)
                except ValueError as err:
                    pass #kein float ... einfach als String weitergeben.
                    #print(f"Returning Value {erg} as String")
            return erg 

        url = r'/report/sample/Oil/' + sampleId + \
            r'?provider=' + provider
        rec = dict()
        try:
            try:
                sample = load_json(self._fname+'/'+sampleId+'.json')
                print('.', end='')
                #print(f"###### in dengine.get_OilLabReport => sample Data for Development provided: {sampleId}")
            except FileNotFoundError:
                raise ValueError('I am in Development mode - do not dare to fetch data from myplant :-)')
                #sample = self._mp.fetchdata(url)
                print(f"###### in dengine.get_OilLabReport => sample Data saved for Development: {sampleId}")
                save_json(self._fname+'/'+sampleId+'.json', sample)
            rec['probe.aluminium'] = get_corr(sample,'probe.aluminium', None)  #'2',
            rec['probe.aluminium-alert'] = get_corr(sample,'probe.aluminium-alert', None) # 'G',
            rec['probe.barium'] = get_corr(sample,'probe.barium', None) # '<0.0001',
            rec['probe.barium-alert'] = get_corr(sample,'probe.barium-alert', None) #  'G',
            rec['probe.boron'] = get_corr(sample,'probe.boron', None) #  '4',
            rec['probe.boron-alert'] = get_corr(sample,'probe.boron-alert', None) #  'G',
            rec['probe.calcium'] = get_corr(sample,'probe.calcium', None) #  '0.2898',
            rec['probe.calcium-alert'] = get_corr(sample,'probe.calcium-alert', None) #  'G',
            rec['probe.chlorine'] = get_corr(sample,'probe.chlorine', None) #  '',
            rec['probe.chlorine-alert'] = get_corr(sample,'probe.chlorine-alert', None) #  'U',
            rec['probe.chromium'] = get_corr(sample,'probe.chromium', None) #  '<1',
            rec['probe.chromium-alert'] = get_corr(sample,'probe.chromium-alert', None) #  'G',
            rec['probe.copper'] = get_corr(sample,'probe.copper', None) #  '<1',
            rec['probe.copper-alert'] = get_corr(sample,'probe.copper-alert', None) #  'G',
            rec['probe.glycol'] = get_corr(sample,'probe.glycol', None) #  'NEG',
            rec['probe.glycol-alert'] = get_corr(sample,'probe.glycol-alert', None) #  'G',
            rec['probe.insolubles'] = get_corr(sample,'probe.insolubles', None) #  '0.01',
            rec['probe.insolubles-alert'] = get_corr(sample,'probe.insolubles-alert', None) #  'G',
            rec['probe.iron'] = get_corr(sample,'probe.iron', None) #  '11',
            rec['probe.iron-alert'] = get_corr(sample,'probe.iron-alert', None) #  'G',
            rec['probe.lead'] = get_corr(sample,'probe.lead', None) #  '4',
            rec['probe.lead-alert'] = get_corr(sample,'probe.lead-alert', None) #  'G',
            rec['probe.magnesium'] = get_corr(sample,'probe.magnesium', None) #  '0.0009',
            rec['probe.magnesium-alert'] = get_corr(sample,'probe.magnesium-alert', None) #  'G',
            rec['probe.molybdenum'] = get_corr(sample,'probe.molybdenum', None) #  '1',
            rec['probe.molybdenum-alert'] = get_corr(sample,'probe.molybdenum-alert', None) #  'G',
            rec['probe.nickel'] = get_corr(sample,'probe.nickel', None) #  '<1',
            rec['probe.nickel-alert'] = get_corr(sample,'probe.nickel-alert', None) #  'G',
            rec['probe.nitration'] = get_corr(sample,'probe.nitration', None) #  '4',
            rec['probe.nitration-alert'] = get_corr(sample,'probe.nitration-alert', None) #  'G',
            rec['probe.oxidation'] = get_corr(sample,'probe.oxidation', None) #  '18',
            rec['probe.oxidation-alert'] = get_corr(sample,'probe.oxidation-alert', None) #  'G',
            rec['probe.ph-oil'] = get_corr(sample,'probe.ph-oil', None) #  '5.42',
            rec['probe.ph-oil-alert'] = get_corr(sample,'probe.ph-oil-alert', None) #  'G',
            rec['probe.phosphorus'] = get_corr(sample,'probe.phosphorus', None) #  '0.0303',
            rec['probe.phosphorus-alert'] = get_corr(sample,'probe.phosphorus-alert', None) #  'G',
            rec['probe.potassium'] = get_corr(sample,'probe.potassium', None) #  '2',
            rec['probe.potassium-alert'] = get_corr(sample,'probe.potassium-alert', None) #  'G',
            rec['probe.silicon'] = get_corr(sample,'probe.silicon', None) #  '3',
            rec['probe.silicon-alert'] = get_corr(sample,'probe.silicon-alert', None) #  'G',
            rec['probe.sodium'] = get_corr(sample,'probe.sodium', None) #  '8',
            rec['probe.sodium-alert'] = get_corr(sample,'probe.sodium-alert', None) #  'G',
            rec['probe.sulphur'] = get_corr(sample,'probe.sulphur', None) #  '',
            rec['probe.sulphur-alert'] = get_corr(sample,'probe.sulphur-alert', None) #  'U',
            rec['probe.tan'] = get_corr(sample,'probe.tan', None) #  '2.40',
            rec['probe.tan-alert'] = get_corr(sample,'probe.tan-alert', None) #  'G',
            rec['probe.tbn'] = get_corr(sample,'probe.tbn', None) #  '4.3',
            rec['probe.tbn-alert'] = get_corr(sample,'probe.tbn-alert', None) #  'G',
            rec['probe.tin'] = get_corr(sample,'probe.tin', None) #  '<1',
            rec['probe.tin-alert'] = get_corr(sample,'probe.tin-alert', None) #  'G',
            rec['probe.viscosity-100c'] = get_corr(sample,'probe.viscosity-100c', None) #  '14.4',
            rec['probe.viscosity-100c-alert'] = get_corr(sample,'probe.viscosity-100c-alert', None) #  'G',
            rec['probe.viscosity-40c'] = get_corr(sample,'probe.viscosity-40c', None) #  '135',
            rec['probe.viscosity-40c-alert'] = get_corr(sample,'probe.viscosity-40c-alert', None) #  'G',
            rec['probe.water'] = get_corr(sample,'probe.water', None) #  '<0.05',
            rec['probe.water-alert'] = get_corr(sample,'probe.water-alert', None) #  'G',
            rec['probe.zinc'] = get_corr(sample,'probe.zinc', None) #  '0.0393',
            rec['probe.zinc-alert'] = get_corr(sample,'probe.zinc-alert', None) #  'G',
        except: 
            raise Exception("Failed to fetch Oil sample Data")
        return rec

    def get_messages2(self, p_from=None, p_to=None):
        """load messages ready for the Finite State Mchine Analysis

        Args:
            p_from (date, understandable by arrow, optional): first message date. Defaults to very first message.
            p_to (date, understandable by arrow, optional): last message date. Defaults to Now.

        Raises:
            ValueError: Inform the User/Programmer if 500000 or more messages are available. 
            Inthis case the constant value needs to be increased in the code of this function. 

        Returns:
            pd.DataFrame: Diane Messages.
        """
        # messages consist of the following severities
        sev = [600,650,700,800]
        p_from_ts = int(arrow.get(p_from).timestamp() * 1e3)
        p_to_ts = int(arrow.get(p_to).timestamp() * 1e3)
        messages = self.batch_hist_alarms(p_severities=sev, p_from = p_from_ts, p_to = p_to_ts)
        messages = messages.iloc[::-1]
        return messages.reset_index()


    def get_messages(self, p_from=None, p_to=None):
        """load messages ready for the Finite State Mchine Analysis

        Args:
            p_from (date, understandable by arrow, optional): first message date. Defaults to very first message.
            p_to (date, understandable by arrow, optional): last message date. Defaults to Now.

        Raises:
            ValueError: Inform the User/Programmer if 500000 or more messages are available. 
            Inthis case the constant value needs to be increased in the code of this function. 

        Returns:
            pd.DataFrame: Diane Messages.
        """
        # messages consist of the following severities
        sev = [600,650,700,800]
        # download all available data at the first request and store
        # to the engine specific pickle file.
        # check if there is a pickle file, messages are stored as dataframes
        pfn = self._fname +"_messages.pkl"
        if os.path.exists(pfn):
            messages = pd.read_pickle(pfn)    
            if messages.empty:      # avoid errors with an empty messages dataframe ---
                os.remove(pfn)

        if not os.path.exists(pfn):
            # or download the available date and store it otherwise.
            messages = self.batch_hist_alarms(p_severities=sev, p_limit=500000)
            if messages.shape[0] >= 500000:
                raise ValueError('more than 500000 messages, please change the code in dEngine !!!')
            messages = messages.iloc[::-1] # turn the messages around, so that the oldest message is the first one. 
            messages.to_pickle(pfn)

        last_ts = int(messages.iloc[-1]['timestamp'])
        if p_to != None:
            p_to_ts = int(arrow.get(p_to).timestamp() * 1e3)
        else:
            p_to_ts = int(arrow.now().timestamp() * 1e3)
        if p_to_ts > last_ts: # not all messages are in the pkl file ...
            new_messages = self.batch_hist_alarms(p_from = last_ts, p_to = p_to_ts)
            if not new_messages.empty:
                new_messages = new_messages[::-1] # turn around the response
                #messages.append(new_messages) # and append to existing messages.
                messages = pd.concat([messages[:-1],new_messages])
                # do not store the changes for performance reasons.
        else:
            messages = messages[messages['timestamp'] <= p_to_ts]
        
        if p_from != None:
            p_from_ts = int(arrow.get(p_from).timestamp() * 1e3)
            messages = messages[messages['timestamp'] >= p_from_ts]

        return messages.reset_index()

    # https://api.myplant.io/api-docs/swagger-ui/index.html?url=https://api.myplant.io/v2/api-docs#/history/historicAlarmsRoute
    def batch_hist_alarms(self, p_severities=[500, 600, 650, 700, 800], p_offset=0, p_limit=None, p_from=None, p_to=None):
        """
        Get pandas dataFrame of Events history, either limit or From & to are required
        p_severities        list
                                500,600,650 ... operational messages
                                700         ... warnings
                                800         ... alarms
        p_offset            int64, number of messages to skip
        p_limit             int64, number of messages to download
        p_from              string timestamp in milliseconds.
        p_to                string timestamp in milliseconds.
        """

        tt = r""
        if p_limit:
            tt = r"&offset=" + str(p_offset) + \
                r"&limit=" + str(p_limit)
        else:
            if p_from and p_to:
                tt = r'&from=' + str(int(arrow.get(p_from).timestamp()) * 1000) + \
                    r'&to=' + str(int(arrow.get(p_to).timestamp()) * 1000)
            else:
                raise Exception(
                    r"batch_hist_alarms, invalid Parameters")

        tsvj = ','.join([str(s) for s in p_severities])

        url = r'/asset/' + str(self['id']) + \
            r'/history/alarms' + \
            r'?severities=' + str(tsvj) + tt

        # fetch messages from myplant ....
        messages = self._mp.fetchdata(url)

        # import to Pandas DataFrame
        dm = pd.DataFrame(messages)
        return dm

    # @ property
    # def oph_parts(self):
    #     """
    #     Oph since Validation Start
    #     """
    #     return int(self['Count_OpHour'] - self['oph@start'])
    
    # @ property
    # def starts_parts(self):
    #     """
    #     Starts since Validation Start
    #     """
    #     return int(self['Count_Starts'] - self['starts@start'])

    @ property
    def properties(self):
        """
        properties dict
        e.g.: prop = e.properties
        """
        return self.assetdata['properties']

    @ property
    def dataItems(self):
        """
        dataItems dict
        e.g.: dataItems = e.dataItems
        """
        return self.assetdata['dataItems']

    @ property
    def valstart_ts(self):
        """
        Individual Validation Start Date
        as EPOCH timestamp
        e.g.: vs = e.valstart_ts
        """
        return epoch_ts(arrow.get(self['val start']).timestamp())


    def myplant_workbench_link(self, ts, interval, dset):
        link_url = 'https://myplant.io'
        items = ','.join([str(v[0]) for v in dset.values()])
        return fr'<a href="{link_url}/#/fleet/workbench?q={ts - interval * 1000}-{ts + interval * 1000}|{id}-{items}">link to Myplant Workbench</a>' 

    ############################
    #Calculated & exposed values
    ############################

    @ staticmethod
    def _bore(platform):
        """
        return bore for platform in [mm]
        """
        lbore = {
            '9': 310.0,
            '6': 190.0,
            '4': 145.0,
            '3': 135.0
        }
        return lbore[platform]

    @ property
    def bore(self):
        """
        bore in [mm]
        """
        lkey = self['Engine Series']
        return self._bore(lkey)

    @ staticmethod
    def _stroke(platform):
        """
        return stroke for platform in [mm]
        """
        lstroke = {
            '9': 350.0,
            '6': 220.0,
            '4': 185.0,
            '3': 170.0
        }
        return lstroke[platform]

    @ property
    def stroke(self):
        """
        stroke in [mm]
        """
        lkey = self['Engine Series']
        return self._stroke(lkey)

    @ classmethod
    def _cylvol(cls, platform):
        """
        Swept Volume for platform per Cylinder in [l]
        """
        lbore = cls._bore(platform)
        lstroke = cls._stroke(platform)
        return (lbore / 100.0) * (lbore / 100.0) * np.pi / 4.0 * (lstroke / 100.0)

    @ classmethod
    def _mechpower(cls, platform, cylanz, bmep, speed):
        """
        mechanical power in [kW]
        platform ... '3','4','6','9'
        cylanz ... int
        bmep ... bar
        speed ... int
        """
        return np.around(cls._cylvol(platform) * cylanz * bmep * speed / 1200.0, decimals=0)

    @ property
    def cylvol(self):
        """
        Swept Volume per Cylinder in [l]
        """
        lkey = self['Engine Series'] or '6'
        return self._cylvol(lkey)

    @ property
    def engvol(self):
        """
        Swept Volume per Engine in [l]
        """
        lkey = self['Engine Series'] or '6' # lkey defaults to type 6
        return self._cylvol(lkey) * self.Cylinders

    @ property
    def Cylinders(self):
        """
        Number of Cylinders
        """
        return int(str(self['Engine Type'][-2:]))

    # @ property
    # def P_nominal(self):
    #     """
    #     Nominal electrical Power in [kW]
    #     """
    #     try:
    #         return np.around(float(self['Power_PowerNominal']), decimals=0)
    #     except:
    #         return 0.0

    @ property
    def cos_phi(self):
        """
        cos phi ... current Power Factor[-]
        """
        return self['halio_power_fact_cos_phi']

    @ property
    def Generator_Efficiency(self):
        # gmodel = self.get_property('Generator Model')
        # cosphi = self.get_dataItem('halio')
        el_eff = {
            '624': 0.981,
            '620': 0.98,
            '616': 0.976,
            '612': 0.986,
            '312': 0.965,
            '316': 0.925,
            '320': 0.975,
            '412': 0.973,
            '416': 0.974,
            '420': 0.973,
            '920': 0.985
        }
        lkey = self['Engine Type']
        return el_eff[lkey] or 0.95

    @ property
    def Pmech_nominal(self):
        """
        Nominal, Calculated mechanical Power in [kW]
        """
        return np.around(self['Power_PowerNominal'] / self.Generator_Efficiency, decimals=1)
        #return np.around(self.P_nominal / self.Generator_Efficiency, decimals=1)

    @ property
    def Speed_nominal(self):
        """
        Nominal Speed in [rp/m]
        """
        speed = {
            '3': 1500.0,
            '4': 1500.0,
            '5': 1500.0,
            '6': 1500.0,
            '9': 1000.0
        }
        lkey = self['Engine Series']
        return speed[lkey] or 1500.0
        # return self.get_dataItem('Para_Speed_Nominal')

    @ property
    def BMEP(self):
        return np.around(1200.0 * self.Pmech_nominal / (self.engvol * self.Speed_nominal), decimals=1)

    def _calc_BMEP(self, p_Pel, p_Speed):
        warnings.simplefilter('ignore')
        try:
            _bmep = 1200.0 * (p_Pel / self.Generator_Efficiency) / (self.engvol * p_Speed)
        except ZeroDivisionError:
            _bmep = 0.0
        return _bmep

    @ property
    def dash(self):
        _dash = dict()
        _dash['Name'] = self['Validation Engine']
        _dash['Engine ID'] = self['Engine ID']
        _dash['Design Number'] = self['Design Number']
        _dash['Engine Type'] = self['Engine Type']
        _dash['Engine Version'] = self['Engine Version']
        _dash['P'] = self.Cylinders
        _dash['P_nom'] = self['Power_PowerNominal']
        _dash['BMEP'] = self.BMEP
        _dash['serialNumber'] = self['serialNumber']
        _dash['id'] = self['id']
        _dash['Count_OpHour'] = self['Count_OpHour']
        _dash['val start'] = self['val start']
        _dash['oph@start'] = self['oph@start']
        _dash['oph parts'] = self['oph_parts']
        _dash['LOC'] = self['RMD_ListBuffMAvgOilConsume_OilConsumption']
        return _dash


if __name__ == "__main__":

    import dmyplant2
    #import pandas
    dmyplant2.cred()
    mp = dmyplant2.MyPlant(0)

    import doctest
    print('Doctest started.')
    doctest.testmod()
    print('Doctest completed.')