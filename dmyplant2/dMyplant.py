from sqlite3 import Timestamp
import arrow
import json
import base64
import requests
import logging
import os
import sys
from datetime import datetime, timedelta
from tqdm.auto import tqdm
import time
import pickle
import pandas as pd
import numpy as np
from pprint import pprint as pp
import logging

try:
    import httplib # type: ignore comment;
except:
    import http.client as httplib

maxdatapoints = 100000  # Datapoints per request, limited by Myplant

def save_json(fil, d):
    with open(fil, 'w') as f:
        json.dump(d, f)

def load_json(fil):
    with open(fil, "r", encoding='utf-8-sig') as f:
        return json.load(f)


def save_pkl(fil, d):
    with open(fil, 'wb') as f:
        pickle.dump(d, f, protocol=4)

def load_pkl(fil):
    with open(fil, 'rb') as f:
        return pickle.load(f)


def epoch_ts(ts) -> float:
    try:
        if ts >= 10000000000.0:
            return float(ts/1000.0)
        else:
            return float(ts)
    except:
        return 0.0

def mp_ts(ts) -> int:
    try:    
        if ts >= 10000000000.0:
            return int(ts)
        else:
            return int(ts * 1000.0)
    except:
        return 0

class MyPlantException(Exception):
    pass

burl = 'https://api.myplant.io'
errortext = {
    200: 'successful operation',
    400: 'Request is missing required HTTP header \'x-seshat-token\'',
    401: 'The supplied authentication is invalid',
    403: 'No permission to access this resource',
    404: 'No data was found',
    500: 'Internal Server Error',
    504: 'Gateway Timeout'
}


def have_internet():
    conn = httplib.HTTPConnection('api.myplant.io', timeout=5)
    try:
        conn.request("HEAD", "/")
        conn.close()
        return True
    except:
        conn.close()
        return False

class MyPlantClientException(Exception):
    pass

class MyPlant:

    #Class Variables
    _name = ''
    _password = ''
    _session = None
    _caching = 0

    _dfn = 'data/dataitems.pkl'
    _dataitems = pd.DataFrame([])

    def __init__(self, caching=0):
        """MyPlant Constructor"""
        if not have_internet():
            raise Exception("Error, Check Internet Connection!")

        self._data_basedir = os.getcwd() + f'/data'

        self._caching = caching
        # load and manage credentials from hidden file
        try:
            with open("./data/.credentials", "r", encoding='utf-8-sig') as file:
                cred = json.load(file)
            self._name = cred['name']
            self._password = cred['password']
        except FileNotFoundError:
            raise

        self._appuser_token = None
        self._token = None
        #self.login()
        
        #if not os.path.isfile('data/dataitems.csv'):
        if not os.path.isfile(self._dfn):
            self.create_request_csv()

        # store dataitems in class variable at start
        self.load_dataitems()        

    def del_Credentials(self):
            os.remove("./data/.credentials")

    @ classmethod
    def load_dataitems(cls):
        try:
            with open(cls._dfn, 'rb') as handle:
                cls._dataitems = pickle.load(handle)
        except FileNotFoundError:
            cls._dataitems = pd.DataFrame([])

    @ classmethod
    def get_dataitems(cls):
        if cls._dataitems.empty:
            cls.load_dataitems()
        return cls._dataitems

    # @classmethod
    # def get_itemIDs(cls,dat=['Count_OpHour']):
    #     ret = {}
    #     for item in dat:
    #         res = cls.lookup_dataitems(lookup=item).to_dict('records')[0]
    #         ret.update({ res.get('id',None) : [res.get('name',None),res.get('unit', '')] })
    #     return ret
    
    @classmethod
    def lookup_dataitems(cls,lookup, exclude=''):
        di = cls.get_dataitems()
        if exclude != '':
            return di.loc[(di.name.str.contains(lookup, case=False) & ~(di.name.str.contains(exclude, case=False)))].reset_index(drop='index')
        else:
            return di.loc[di.name.str.contains(lookup, case=False)].reset_index(drop='index')

    @ classmethod
    def load_dataitems_csv(cls, filename):
        """load CSV dataitems definition file

        example content:
        ID;myPlantName;unit
        52;Exhaust_TempCylAvg;C (high)
        103;Various_Values_PosTurboBypass;%
        19079;Exhaust_TCSpeedA;1000/min
        ....

        Args:
            filename (string): CSV dataitems definition file

        Returns:
            dict: CSV dataitems dict
        """
        data_req = pd.read_csv(filename,
                               sep=';', encoding='utf-8')
        dat = {a[0]: [a[1], a[2]] for a in data_req.values}
        return dat

    def deBase64(self, text):
        return base64.b64decode(text).decode('utf-8')

    def gdi(self, ds, sub_key, data_item_name):
        """Unpack value from Myplant Json datastructure based on key & DataItemName"""
        if sub_key == 'nokey':
            return ds.get(data_item_name, None)
        else:
            local = {x['value']
                     for x in ds[sub_key] if x['name'] == data_item_name}
            return local.pop() if len(local) != 0 else None

    @ classmethod
    def load_def_csv(cls, filename):
        """load CSV Validation definition file 

        example content:
        n;Validation Engine;serialNumber;val start;oph@start;starts@start;Asset ID;Old PU first replaced OPH;Old PUs replaced before upgrade
        0;POLYNT - 2 (1145166-T241) --> Sept;1145166;12.10.2020;31291;378;103791;;
        ....

        Args:
            filename ([string]): [Filename of definition file]

        Returns:
            [pd.dataFrame]: [Validation definition as dataFrame]
        """
        dv = pd.read_csv(filename, sep=';', encoding='utf-8')
        dv['val start'] = pd.to_datetime(dv['val start'], format='%d.%m.%Y')
        return dv

    @ classmethod
    def load_def_excel(self, filename, sheetname, mp=None):
        """load CSV Validation definition file 
        oph@start and starts@start can be automatically calculated based on val start if no information provided
        (Data is taken from the end of the startday from the validation)

        example content:
        n;Validation Engine;serialNumber;val start;oph@start;starts@start;Asset ID;Old PU first replaced OPH;Old PUs replaced before upgrade
        0;POLYNT - 2 (1145166-T241) --> Sept;1145166;12.10.2020;31291;378;103791;;
        ....
        
        Args:
            filename ([string]): [Filename of definition file] must include .xslx at the end
            sheetname ([string]): Relevant sheetname in file
            mp (myPlant Objekt): Optional myplant object to enable auto filling of missing values

        Returns:
            [pd.dataFrame]: [Validation definition as dataFrame]
        """
        dval=pd.read_excel(filename, sheet_name=sheetname, usecols=['Validation Engine', 'serialNumber', 'val start', 'oph@start', 'starts@start'])
        dval.dropna(subset=['Validation Engine', 'serialNumber', 'val start'], inplace=True)
        dval['n']=dval.index #add column 'n for handling in further methods
        dval['serialNumber'] = dval['serialNumber'].astype(int).astype(str)
        if mp!=None:
            for i in range(len(dval)):
                if np.isnan(dval['oph@start'].iloc[i]) or np.isnan(dval['starts@start'].iloc[i]): #check for missing values
                    asset = mp._asset_data(dval['serialNumber'].iloc[i]) #get assetId from Serial Number
                    assetId=asset['properties'][0]['assetId']
                    itemIds={161: ['CountOph', 'h'], 179: ['Starts', '']}
                    p_from=arrow.get(dval['val start'].iloc[i]).to('Europe/Vienna')
                    p_to=p_from.shift(days=1)
                    add_data=mp.hist_data(assetId, itemIds, p_from, p_to, timeCycle=3600)
                    if add_data.empty:
                        raise ValueError('Error! No setup data available for engine '+dval['Validation Engine'].iloc[i]+' for specified val start. Please change the val start date or insert the oph@start and starts@start manually in the excel file and run the program again.')
                    if np.isnan(dval['oph@start'].iloc[i]): dval['oph@start'].iloc[i]=add_data['CountOph'].iloc[-1]
                    if np.isnan(dval['starts@start'].iloc[i]): dval['starts@start'].iloc[i]=add_data['Starts'].iloc[-1]
            
        return dval

    @property
    def caching(self):
        """the current cache time"""
        return self._caching

    @property
    def username(self):
        return self.deBase64(self._name)

    # def login(self):
    #     """Login to MyPlant"""
    #     if self._session is None:
    #         logging.debug(f"SSO {self.deBase64(self._name)} MyPlant login")
    #         self._session = requests.session()
    #         headers = {'Content-Type': 'application/json', }
    #         body = {
    #             "username": self.deBase64(self._name),
    #             "password": self.deBase64(self._password)
    #         }
    #         loop = 1
    #         try:
    #             while loop < 3:
    #                 response = self._session.post(burl + "/auth",
    #                                               data=json.dumps(body), headers=headers)
    #                 if response.status_code == 200:
    #                     logging.debug(f'login {self.deBase64(self._name)} successful.')
    #                     #self.r = response.json()
    #                     self._token = response.json()['token']
    #                     self._appuser_token = self._token
    #                     break
    #                 else:
    #                     logging.error(
    #                         f'login failed with response code {response.status_code}')
    #                 loop += 1
    #                 logging.error(f'Myplant login attempt #{loop}')
    #                 time.sleep(1)
    #             if loop >= 3:
    #                 logging.error(f'Login {self.deBase64(self._name)} failed')
    #                 raise Exception(
    #                     f'Login {self.deBase64(self._name)} failed')
                    
    #         except:
    #             self.del_Credentials()
    #             raise Exception("Login Failed, invalid Credentials ?")

# # CHATGPT verbesserungen :
# #################################################################
#     def login(self):
#         """Login to MyPlant"""
#         if self._session is None:
#             logging.debug(f"SSO {self.deBase64(self._name)} MyPlant login")
#             self._session = requests.session()
#             headers = {'Content-Type': 'application/json', }
#             body = {
#                 "username": self.deBase64(self._name),
#                 "password": self.deBase64(self._password)
#             }

#             for i in range(3):
#                 response = self._session.post(burl + "/auth", data=json.dumps(body), headers=headers)

#                 if response.status_code == 200:
#                     logging.debug(f'login {self.deBase64(self._name)} successful.')
#                     self._token = response.json()['token']
#                     self._appuser_token = self._token
#                     break
#                 else:
#                     logging.error(f'Myplant login attempt #{i + 1} failed with response code {response.status_code}')
#                     time.sleep(1)
#             else:
#                 logging.error(f'Login {self.deBase64(self._name)} failed after 3 attempts.')
#                 self.del_Credentials()
#                 raise Exception(f"Login Failed, invalid Credentials ?")

# # with totp MFA - code is failing to authorize currently  :
# #################################################################
    def login(self):
        """Login to MyPlant"""

        if self._session is None:
            logging.debug(f"SSO {self.deBase64(self._name)} MyPlant login")
            self._session = requests.session()
            headers = {'Content-Type': 'application/json', }
            body = {
                "username": self.deBase64(self._name),
                "password": self.deBase64(self._password)
            }

            for i in range(3):
                response = self._session.post(burl + "/auth", data=json.dumps(body), headers=headers)

                # Generate a TOTP code using the secret key from your Authenticator app
                print("Please enter your authenticator code: ")
                totp_secret = input()
                body_mfa = {"username": body['username'], "challenge": response.json()['challenge'], "otp": totp_secret}
                response = self._session.post('https://api.myplant.io/auth/mfa/totp/confirmation', data=json.dumps(body_mfa), headers=headers)

                if response.status_code == 200:
                    logging.debug(f'login {self.deBase64(self._name)} successful.')
                    self._token = response.json()['token']
                    self._appuser_token = self._token
                    break
                else:
                    logging.error(f'Myplant login attempt #{i + 1} failed with response code {response.status_code}')
                    time.sleep(1)
            else:
                logging.error(f'Login {self.deBase64(self._name)} failed after 3 attempts.')
                self.del_Credentials()
                raise Exception(f"Login Failed, invalid Credentials ?")


    def logout(self):
        """Logout from Myplant and release self._session"""
        if self._session != None:
            self._session.close()
            self._session = None

    # def fetchdata(self, url):
    #     """login and return data based on url"""
    #     self.login()
    #     logging.debug(f'url: {url}')
    #     response = self._session.get(burl + url)

    #     if response.status_code == 200:
    #         logging.debug(f'fetchdata: download successful')
    #         res = response.json()
    #         return res
    #     else:
    #         logging.error(
    #             f"Code: {url}, {response.status_code}, {errortext.get(response.status_code,'no HTTP Error text available.')}")


############# CHATGPT variante
    def fetchdata(self, url):
        """login and return data based on url"""
        #self.login()
        logging.debug(f'url: {url}')

        retries = 0
        while retries < 3:
            try:
                headers = {'x-seshat-token': self.app_token}
                response = self._session.get(burl + url, headers=headers, timeout=30)
                response.raise_for_status()

                logging.debug(f'fetchdata: download successful')
                res = response.json()
                return res
            except requests.exceptions.RequestException as e:
                logging.error(f'Request failed: {e}')
                #if isinstance(e, requests.exceptions.Timeout):
                #    self.login() # login again if a timeout occurred
                retries += 1
                time.sleep(5)
        logging.error(f'Failed to fetch data from {url} after 3 attempts')
        return None
#############


    def _asset_data(self, serialNumber):
        """
        Returns an Asset based on its id with all details
        including properties and DataItems.

        Parameters:
        Name	    type    Description
        sn          int     IB ItemNumber Engine
        ----------------------------------------------
        url: /asset?assetType=J-Engine&serialNumber=sn
        """
        return self.fetchdata(url=r"/asset?assetType=J-Engine&serialNumber=" + str(serialNumber))

    def application_user_login(self):
        # luser = 'JQHTKP1T496PG'
        # lpassword = 'ae0874a64b659fea0af47e1f5c72f2dc'
        # url = burl + '/oauth/token'
        # auth = (luser, lpassword)
        # data = {'grant_type': 'client_credentials'}
        # r = requests.post(url, auth=auth, data=data, verify=True, proxies=None)
        # self._appuser_token = r.json()['access_token']
        self.login()

    @property
    def app_token(self):
        if not self._appuser_token:
            self.application_user_login()
        return self._appuser_token

    def _request(self, method, endpoint, params=None, json_data=None):
        headers = {'x-seshat-token': self.app_token}
        request_method = {
            'get': requests.get,
            'post': requests.post,
        }
        r = request_method[method](
            burl + '/' + endpoint,
            headers=headers,
            params=params,
            json=json_data,
            verify=True,
            proxies=None
        )
        if r.status_code != 200:
            print(f'{r.status_code}: {r.text}')
            raise MyPlantClientException(r.text)
        return r

    def _asset_data_graphQL(self, assetId=None):
        properties = [
            "Engine Series",
            "Engine Type",
            "Engine Version",
            "Customer Engine Number",
            "Engine ID",
            "Design Number",
            "Gas Type",
            "Commissioning Date",
            "Contract.Service Contract Type",
        ]
        dataItems = [
            "OperationalCondition",
            "Count_OpHour",
            "Count_Start",
            "Power_PowerNominal",
            "Para_Speed_Nominal",
            "rP_Ramp_Set",
            "RMD_ListBuffMAvgOilConsume_OilConsumption",
        ]
        r = self._request_asset_graphql(assetId=assetId, properties=properties, dataItems=dataItems) 
        return r['data']['asset']

    def _request_asset_graphql(self, assetId=None, properties=[], dataItems=[]):
        """
        Returns specific Asset Data
        Parameters:

        Name	    type    Description
        assetId     int     AssetId Engine
        """
        graphQL = """
        {
            asset( id: %s ) {
                id
                serialNumber
                modelId
                model
                site {
                    id
                    name
                    country
                }
                customer {
                    id
                    name
                }
                status {
                    lastContactDate
                    lastDataFlowDate
                }
                properties(
                    names: [
                        %s
                    ]
                ) {
                    id
                    name
                    value
                }
                dataItems(
                    query: [
                        %s
                    ]
                ) {
                    id
                    name
                    value
                    unit
                    timestamp
                }
            }
        }
        """ % (
            assetId,
            ','.join([f'"{i}"' for i in properties]),
            ','.join([f'"{i}"' for i in dataItems]),            
        )
        r = self._request('post', endpoint='graphql', json_data={'query': graphQL})
        return r.json()

    def historical_dataItem(self, id, itemId, timestamp):
        """
        url: /asset/{assetId}/dataitem/{dataItemId}
        Parameters:
        Name	    type    Description
        assetId     int64   Id of the Asset to query the DateItem for.
        dataItemId  int64   Id of the DataItem to query.
        timestamp   int64   Optional,  timestamp in the DataItem history to query for.
        highres     Boolean Whether to use high res data. Much slower but gives the raw data.
        """
        res = self.fetchdata(url=fr"/asset/{id}/dataitem/{itemId}?timestamp={timestamp}")
        return res['value']

    # def history_dataItem(self, id, itemId, p_from, p_to, timeCycle=3600):
    #     """
    #     url: /asset/{assetId}/dataitem/{dataItemId}
    #     Parameters:
    #     Name	    type    Description
    #     assetId     int64   Id of the Asset to query the DateItem for.
    #     dataItemId  int64   Id of the DataItem to query.
    #     p_from      int64   timestamp start timestamp.
    #     p_to        int64   timestamp stop timestamp.
    #     timeCycle   int64   interval in seconds.
    #     """
    #     return self.fetchdata(url=fr"/asset/{id}/history/data?from={p_from}&to={p_to}&assetType=J-Engine&dataItemId={itemId}&timeCycle={timeCycle}&includeMinMax=false&forceDownSampling=false")

    def _history_batchdata(self, id, itemIds, lp_from, lp_to, timeCycle=3600):
        # make sure itemids have the format { int: [str,str], int: [str,str], ...}
        itemIds = { int(k):v for (k,v) in itemIds.items() if k}
        # comma separated string of DataItemID's
        IDS = ','.join([str(s) for s in itemIds.keys()])
        ldata = self.fetchdata(
            url=fr"/asset/{id}/history/batchdata?from={lp_from}&to={lp_to}&timeCycle={timeCycle}&assetType=J-Engine&includeMinMax=false&forceDownSampling=false&dataItemIds={IDS}")
        # restructure data to dict
        if ldata:
            ds = dict()
            ds['labels'] = ['time'] + [itemIds[x][0] for x in ldata['columns'][1]]
            ds['data'] = [[r[0]] + [rr[0] for rr in r[1]] for r in ldata['data']]
            # import data to Pandas DataFrame and return result
            df = pd.DataFrame(ds['data'], columns=ds['labels'])
            return df
        else:
            return pd.DataFrame([])

    def hist_data(self, id, itemIds, p_from, p_to, timeCycle=3600, silent=False):
        """
        url: /asset/{assetId}/dataitem/{dataItemId}
        Parameters:
        Name	    type            Description
        assetId     int64           Id of the Asset to query the DateItem for.
        itemIds     dict            DataItem Id's, Names & Units
        p_from      int64           timestamp start timestamp in ms.
        p_to        int64           timestamp stop timestamp in ms.
        timeCycle   int64           interval in seconds.
        """

        # initialize a data collector
        df = pd.DataFrame([])

        # calculate how many full rows per request within the myplant limit are possible
        rows_per_request = maxdatapoints // len(itemIds)
        try:
            rows_total = int(p_to.timestamp() - p_from.timestamp()) // timeCycle
        except ValueError as err:
            print(err)
            print('Please check arrow version! Make sure you have version 1.0.3 or higher installed!')
            print('Update arrow by writing in command prompt: pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org arrow==1.0.3')    
        if not silent:
            pbar = tqdm(total=rows_total, ncols=80, mininterval=1, unit=' datarows', desc="Load Data")

        # initialize loop
        lp_from = int(p_from.timestamp()) * 1000  # Start at lp_from
        lp_to = min((lp_from + rows_per_request * timeCycle * 1000),
                    int(p_to.timestamp()) * 1000)

        while lp_from < int(p_to.timestamp()) * 1000:
            # for now assume same itemID's are always included ... need to be included in a check
            ldf = self._history_batchdata(
                id, itemIds, lp_from, lp_to, timeCycle)

            # and append each chunk to the return df
            #df = df.append(ldf) # 2022-02-19: deprecation warning => use concat in future!
            df = pd.concat([df, ldf])
            
            if not silent:
                pbar.update(rows_per_request)
            # calculate next cycle
            lp_from = lp_to + timeCycle * 1000
            lp_to = min((lp_to + rows_per_request *
                        timeCycle * 1000), int(p_to.timestamp()) * 1000)
        
        if not silent:
            pbar.close()
        
        if not df.empty:
            # Addtional Datetime column calculated from timestamp
            df['datetime'] = pd.to_datetime(df['time'] * 1000000)
        return df

    def stitch_df(self, **dataframes):
        """Stitch Dataframes together
        1.) Check if dataframes share the same ItemId's
        2.) Remove overlapping areas, keep the higher frequent part
        3.) return stitched Dataframe 

        Args:
            **dataframes

        Returns:
            pd.DataFrame: Stitched Dataframe
        """        
        return pd.DataFrame([])

    def create_request_csv(self):
        """Create Request_csv with id, name, unit, myPlantName and save in /data"""
        
        print(f"downnload available dataItems & properties from Myplant,store locally.")
        model=self.fetchdata('/model/J-Engine')
        dataitems=self.fetchdata('/system/localization?groups=data-items&groupResult=true')

        model=pd.json_normalize(model, record_path =['dataItems'])

        dataitems_df=pd.DataFrame(columns=['lan','dataitem', 'lan_item'])

        for lan in dataitems:
            output=pd.DataFrame(dataitems[lan]['groups'][0]['values'].items(), columns=['dataitem','myPlantName'])
            output['lan']=lan
            #dataitems_df=dataitems_df.append(output, ignore_index=True)
            dataitems_df = pd.concat([dataitems_df,output], ignore_index=True)
        dataitems_df.head()

        def remove_jen (row): #with best practice could probably be shortened
            return row.split('_',1)[1]
        dataitems_df['dataitem']=dataitems_df.dataitem.apply(remove_jen)
        model=model.merge(dataitems_df[dataitems_df.lan=='en'], how='inner', left_on='name', right_on='dataitem')
        model=model.loc[:,['id', 'name', 'unit', 'myPlantName']]
        #model.to_csv('data/dataitems.csv', sep=';', index=False)
        model.to_pickle(self._dfn)

    def _reshape_asset(self, rec):
        ret = dict()
        for key, value in rec.items():
            if type(value) == list:
                for lrec in value:
                    ret[lrec['name']] = lrec.get('value',None)
            else:
                ret[key] = value
        return ret

    def fetch_available_data(self):
        url = "/model/J-Engine"
        res = self.fetchdata(url)
        return res

    def fetch_installed_base(self,fields, properties, dataItems, limit = None):
        url = "/asset/" + \
            "?fields=" + ','.join(fields) + \
            "&properties=" + ','.join(properties) + \
            "&dataItems="  + ','.join(dataItems) + \
            "&assetTypes=J-Engine"
        if limit:
            url = url + f"&limit={limit}"
        res = self.fetchdata(url)
        return pd.DataFrame.from_records([self._reshape_asset(a) for a in res['data']])

    def _fetch_installed_base(self):
        #print("Download Installed Fleet DataBase, story locally")
        fields = ['serialNumber']
        properties =  [
            'Design Number','Engine Type','Engine Version','Engine Series','Engine ID',
            'Control System Type',
            'Country','IB Site Name','Commissioning Date','IB Unit Commissioning Date','Contract.Warranty Start Date', 'Contract.Warranty End Date','IB Status',
            'IB NOX', 'IB Frequency', 'IB Item Description Engine','Product Program'
            ]

        dataItems = ['OperationalCondition','Module_Vers_HalIO','starts_oph_ratio','startup_counter',
        'shutdown_counter','Count_OpHour','Power_PowerNominal','Para_Speed_Nominal'
        ]
        fleet = self.fetch_installed_base(fields, properties, dataItems, limit = None)
        fleet.to_pickle(self._data_basedir + '/Installed_base.pkl')
        return fleet

    def reload_installed_fleet(self):
        self._fetch_installed_base()

    def get_installed_fleet(self):
        if os.path.exists(self._data_basedir + '/Installed_base.pkl'):
            fleet = pd.read_pickle(self._data_basedir + '/Installed_base.pkl')
        else:
            fleet= self._fetch_installed_base()
        return fleet

    def search_installed_fleet_by_contains_name(self, name):
        def sfun(x):
            return all([ (str(name).upper() in f"{str(x['IB Site Name'])} {str(x['serialNumber'])} {str(x['Design Number'])} {str(x['Engine Type'])} {str(x['Engine Version'])}".upper()),  (x['OperationalCondition'] != 'Decommissioned') ])
        return self.search_installed_fleet(sfun)

    def search_installed_fleet(self, sfun):
        """Search the installed base by a complex search Function:

        e.g.
        def sfun(x):
            return (
                ("BMW" in str(x['IB Site Name'])) and 
                ("6" == x['Engine Series']) 
                ... )

        Args:
            sfun (function(x)): function that returns True if found.

        Returns:
            df: a pandas dataFrame containing all 
        """
        fleet = self.get_installed_fleet()
        return fleet[fleet.apply(lambda x: sfun(x), axis=1)].reset_index()

    def def_from_installed_fleet(self, res):
        val_dict = {
            'n': [],
            'Validation Engine': [],
            'serialNumber': [],
            'val start': [],
            'oph@start': [],
            'starts@start': [],
            'Asset ID': [],
        }
        for i, r in tqdm(res.iterrows(), total=res.shape[0], ncols=120, mininterval=1, unit=' engines', desc="Loading Myplant Data"):
            val_dict['n'].append(i)
            val_dict['Validation Engine'].append(r['IB Site Name'] + ' ' + r['Engine ID'])
            val_dict['serialNumber'].append(np.int64(r['serialNumber']))
            val_dict['val start'].append(np.datetime64(r['Commissioning Date']))

            # id = int(r['id'])
            # ts = int(pd.to_datetime(r['Commissioning Date']).timestamp()*1e3)
            # oph = self.historical_dataItem(id, 161, ts).get('value', None) or 0
            # starts = self.historical_dataItem(id, 179, ts).get('value', None) or 0
            oph = -1
            starts = -1
            
            val_dict['oph@start'].append(np.int64(oph))
            val_dict['starts@start'].append(np.int64(starts))
            val_dict['Asset ID'].append(np.float64(r['id']))

        # val_dict = {
        #     'n': [i for i, r in res.iterrows()],
        #     'Validation Engine': [r['IB Site Name'] + ' ' + r['Engine ID'] for i, r in res.iterrows()],
        #     'serialNumber': [np.int64(r['serialNumber']) for i, r in res.iterrows()],
        #     'val start': [np.datetime64(r['Commissioning Date']) for i, r in res.iterrows()],
        #     'oph@start': [np.int64(0)] * res.shape[0],
        #     'starts@start': [np.int64(0)] * res.shape[0],
        #     'Asset ID': [np.float64(r['id']) for i, r in res.iterrows()],
        # }
        return pd.DataFrame(val_dict)

if __name__ == "__main__":
    
    import dmyplant2
    from pprint import pprint as pp
    import time
    t0 = time.time()
    dmyplant2.cred()
    mp = dmyplant2.MyPlant(0)
    mp.login()
    #dmyplant2.Engine._list_cached_validations()
    t1 = time.time()
    e = dmyplant2.Engine.from_sn(mp, '1486144')
    t2 = time.time()
    pp(e.dash)
    print(f"Login: {(t1-t0):3.2f}sec\nEngine: {(t2-t1):3.2}sec")
