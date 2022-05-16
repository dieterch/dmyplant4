import copy
import gc
from datetime import datetime
import traceback
import logging
import os
import sys
import time
import pickle
import warnings
from pprint import pprint as pp, pformat as pf
import arrow
import dmyplant2
import numpy as np
import pandas as pd
from tqdm import tqdm

from dmyplant2.dEngine import Engine
from .dFSMToolBox import Target_load_Collector, Exhaust_temp_Collector, Tecjet_Collector, Sync_Current_Collector, Oil_Start_Collector, load_data, msg_smalltxt

warnings.simplefilter(action='ignore', category=FutureWarning)

#Various_Bits_CollAlarm
###########################################################################################
## nStateVector Data Class
## hold state switch relevant data and timings
###########################################################################################
class nStateVector:
    statechange = False
    laststate = ''
    laststate_start = None
    currentstate = ''
    currentstate_start = None
    attrs = ['statechange','laststate','laststate_start','currentstate_start']

    def __str__(self):
        return  f"{'*' if self.statechange else '':2}|"+ \
                f"LST {self.laststate_start.strftime('%d.%m %H:%M:%S')} " + \
                f"LS  {self.laststate:18}| " + \
                f"CSS {self.currentstate_start.strftime('%d.%m %H:%M:%S')} " + \
                f"CS  {self.currentstate:18}| "
                
    def __repr__(self):
        return  f"{'*' if self.statechange else '':2}|"+ \
                f"LST {self.laststate_start.strftime('%d.%m %H:%M:%S')} " + \
                f"LS  {self.laststate:18}| " + \
                f"CSS {self.currentstate_start.strftime('%d.%m %H:%M:%S')} " + \
                f"CS  {self.currentstate:18}| "

    def log(self):
        d = dict()
        for a in self.attrs:
            d['a'] = self.getattr(self,a)
        return d

###########################################################################################
## State class
## States und Transferfunktionen, Sammeln von Statebezogenen Daten ... 
###########################################################################################
class State:
    """class state and its inherits
    check if a transferfunction triggers a statechange based
    on the transferfunction dict.
    """
    def __init__(self, statename, transferfun_list):
        self._statename = statename
        self._transferfunctions = transferfun_list
        self._trigger = False
    
    def checkmsg(self,msg):
        for transfun in self._transferfunctions: # screen triggers
            self._trigger = msg['name'] == transfun['trigger'][:4]
            if self._trigger:
                return transfun['new-state']
        return self._statename

    def trigger_on_vector(self, vector, msg):
        vector.currentstate = self.checkmsg(msg)
        vector.statechange = self._trigger #???
        if self._trigger:
            vector.laststate = self._statename
            vector.laststate_start = vector.currentstate_start
            vector.currentstate_start = pd.to_datetime(msg['timestamp'] * 1e6)
        return vector

class LoadrampStateV2(State):
    """Inherits State and provides a calulated end of the loadramp
    the Constructor takes 2 additional arguments comared to the base Class:

    operator(FSMOperator class): points to the acting FSMOperator Instance
    e (dmyplant2.dEngine.Engine): points to the currently analyzed Engine Instance
    """
    def __init__(self, statename, transferfun_list, operator, e):
        self._e = e
        self._operator = operator
        self._full_load_timestamp = None
        self._loadramp = self._e['rP_Ramp_Set'] or 0.625 # %/sec
        self._default_ramp_duration = (100.0 - e.sync_load) / self._loadramp
        super().__init__(statename, transferfun_list)
        if self._operator.act_run in self._operator.logrun:
            logging.debug(f"{self._operator.act_run} in init - loadramp: {self._loadramp:5.1f}, duration {self._default_ramp_duration:5.1f}, timestamp {str(self._full_load_timestamp):15}")

    def trigger_on_vector(self, vector, msg):
        vector = super().trigger_on_vector(vector, msg)
        if self._operator.act_run in self._operator.logrun:
            logging.debug(f"{self._operator.act_run} SNO{self._operator.nsvec['startno']:5d}, {'trigger_on_vector':21}, full_load_timestamp {str(self._full_load_timestamp):15},{vector},{msg_smalltxt(msg)}")

        if self._full_load_timestamp is not None:
            if msg['timestamp'] > int(self._full_load_timestamp + 2 * self._default_ramp_duration * 1e3): # Emergency, message times got most likely confused.
                if self._operator.act_run == 1:
                    new_msg = copy.deepcopy(msg)
                    new_msg['name'] = '9047'
                    new_msg['message'] = 'Target load reached (emergency)'
                    new_msg['timestamp'] = msg['timestamp']
                    new_msg['severity'] = 600
                    if self._operator.act_run in self._operator.logrun:
                        logging.debug(f"{self._operator.act_run} SNO{self._operator.nsvec['startno']:5d}, {'> emergency +':21}, full_load_timestamp {str(self._full_load_timestamp):15},{vector},{msg_smalltxt(new_msg)}")
                    vector.statechange = True
                    return vector #emergency exit

        # calculate the end of ramp time in the first call, triggered if full_load_timestamp is None.
        if self._full_load_timestamp == None:
            self._full_load_timestamp = int((vector.currentstate_start.timestamp() + self._default_ramp_duration) * 1e3)
            #self._operator.inject_message({'name':'9047', 'message':'Target load reached (calculated)','timestamp':self._full_load_timestamp,'severity':600})
            if self._operator.act_run == 0: # bei run 0 eine neue message erzeugen.
                new_msg = copy.deepcopy(msg)
                new_msg['name'] = '9047'
                new_msg['message'] = 'Target load reached (calculated)'
                new_msg['timestamp'] = self._full_load_timestamp
                new_msg['severity'] = 600
                self._operator.inject_message(new_msg)
                if self._operator.act_run in self._operator.logrun:
                    logging.debug(f"{self._operator.act_run} SNO{self._operator.nsvec['startno']:5d}, {'> inject +':21}, full_load_timestamp {str(self._full_load_timestamp):15},{vector},{msg_smalltxt(new_msg)}")

        # use the message target load reached to make the trigger more accurate. (This message isnt available on all engines.)
        if msg['name'] == '9047':
            if self._operator.act_run == 0:
                self._full_load_timestamp = msg['timestamp']
                self._operator.replace_message(msg)
                if self._operator.act_run in self._operator.logrun:
                    logging.debug(f"{self._operator.act_run} SNO{self._operator.nsvec['startno']:5d}, {'> replace +':21}, full_load_timestamp {str(self._full_load_timestamp):15},{vector},{self._operator.msg_smalltxt(msg)}")

        if vector.statechange:
            if self._operator.act_run in self._operator.logrun:
                logging.debug(f"{self._operator.act_run} SNO{self._operator.nsvec['startno']:5d}, {'> normal statechange':21}, full_load_timestamp {str(self._full_load_timestamp):15},{vector},{self._operator.msg_smalltxt(msg)}")
            self._full_load_timestamp = None # reset the trigger!

        return vector

###########################################################################################
## abstract Base FSM class
###########################################################################################
class FSM:
    """base class for FSM Definitions
    """
    def __init__(self):
        self.name = 'generic'
        self._initial_state = None
        self.svec = None
        self._states = None

    @property
    def initial_state(self):
        return self._initial_state

    @property
    def states(self):
        return self._states

    def initialize_statevector(self, first_message):
        v = nStateVector()
        v.statechange = True
        v.laststate = 'init'
        v.laststate_start = first_message
        v.currentstate = self.initial_state
        v.currentstate_start = first_message
        
        self.svec = v
        return self.svec        

    def call_trigger_states(self, nsvec):
        nsvec[self.name] = self.states[nsvec[self.name].currentstate].trigger_on_vector(nsvec[self.name], nsvec['msg'])
        if nsvec[self.name].statechange:
            if self.name in nsvec['logstates']: # hardcoded, log  only states for startstop 
                    logging.debug(f"- SNO{nsvec['startno']:5d}, in FSM {self.name}, changed state from {nsvec[self.name].laststate} to {nsvec[self.name].currentstate}")
        return nsvec

    def dot(self, fn):
        """Create a FSM Diagram of specified states in *.dot Format
        fn : Filename
        """
        with open(fn, 'w') as f:
            f.write("digraph G {\n")
            f.write('    graph [rankdir=TB labelfontcolor=red fontname="monospace" nodesep=1 size="20,33"]\n')
            f.write('    node [fontname="monospace" fontsize=10  shape="circle"]\n')
            f.write('    edge [fontname="monospace" color="grey" fontsize=10]\n')
            for s in self._states:
                f.write(f'    {s.replace("-","")} [label="{s}"]\n')
                for t in self._states[s]._transferfunctions:
                    f.write(f'    {s.replace("-","")} -> {t["new-state"].replace("-","")} [label="{t["trigger"]}"]\n')
            f.write("}\n")

    def collect_data(self, nsvec, results):
        pass

###########################################################################################
## Oil Pump FSM
###########################################################################################
class OilPumpFSM(FSM):
    def __init__(self):
        self.name = 'oilpump'
        self.vec_name = 'oil_pump'
        self._initial_state = 'undefined'
        self._states = {
                'undefined': State('undefined',
                    [{ 'trigger':'1259 Demand oil pump on', 'new-state': 'ON'},      
                     { 'trigger':'1260 Demand oil pump off', 'new-state': 'OFF'}]),
                'ON': State('ON',
                    [{ 'trigger':'1260 Demand oil pump off', 'new-state': 'OFF'}]),
                'OFF': State('OFF',
                    [{ 'trigger':'1259 Demand oil pump on', 'new-state': 'ON'}]),
        }
    def collect_data(self, nsvec, results):
        if nsvec[self.name].statechange:
            nsvec[self.vec_name] = nsvec[self.name].currentstate
            results['oilpumptiming'].append({
                'state':nsvec[self.name].currentstate,
                'time':nsvec[self.name].currentstate_start})

###########################################################################################
## Service Selector FSM
###########################################################################################
class ServiceSelectorFSM(FSM):
    def __init__(self):
        self.name = 'serviceselector'
        self.vec_name = 'service_selector'
        self._initial_state = 'undefined'
        self._states = {
                'undefined': State('undefined',[
                    { 'trigger':'1225 Service selector switch Off', 'new-state': 'OFF'},         
                    { 'trigger':'1226 Service selector switch Manual', 'new-state': 'MAN'},         
                    { 'trigger':'1227 Service selector switch Automatic', 'new-state': 'AUTO'},         
                    ]),
                'OFF': State('OFF',[
                    { 'trigger':'1226 Service selector switch Manual', 'new-state': 'MAN'},         
                    { 'trigger':'1227 Service selector switch Automatic', 'new-state': 'AUTO'},         
                    ]),
                'MAN': State('MAN',[
                    { 'trigger':'1225 Service selector switch Off', 'new-state': 'OFF'},         
                    { 'trigger':'1227 Service selector switch Automatic', 'new-state': 'AUTO'},         
                    ]),
                'AUTO': State('AUTO',[
                    { 'trigger':'1225 Service selector switch Off', 'new-state': 'OFF'},         
                    { 'trigger':'1226 Service selector switch Manual', 'new-state': 'MAN'},         
                    ]),
        }
    def collect_data(self, nsvec, results):
        # do in case of a statchange:
        if nsvec[self.name].statechange:
            nsvec[self.vec_name] = nsvec[self.name].currentstate
            results['serviceselectortiming'].append({
                'state':nsvec[self.name].currentstate,
                'time':nsvec[self.name].currentstate_start})

###########################################################################################
## Start Stop FSM
###########################################################################################
class startstopFSM(FSM):
    # useful abbrevations
    run2filter_content = ['no','success','mode','oilfilling','degasing','starter','speedup','idle','synchronize','loadramp','cumstarttime','targetload','ramprate','maxload','targetoperation','rampdown','coolrun','runout','A', 'W']
    vertical_lines_times = ['oilfilling','degasing','starter','speedup','idle','synchronize','loadramp','targetoperation','rampdown','coolrun','runout']
    start_timing_states =  ['oilfilling','degasing','starter','speedup','idle','synchronize','loadramp']
    
    def __init__(self, operator, e):
        self.name = 'startstop'
        self._operator = operator
        self._e = e
        self._successtime = 300
        self._initial_state = 'standstill'
        self._states = {
                'standstill': State('standstill',[
                    { 'trigger':'1231 Request module on', 'new-state': 'oilfilling'},            
                    ]),
                'oilfilling': State('oilfilling',[
                    #{ 'trigger':'1249 Starter on', 'new-state': 'starter'},
                    { 'trigger':'1262 Demand oil pump (DC) off', 'new-state': 'degasing'},
                    { 'trigger':'1225 Service selector switch Off', 'new-state': 'standstill'},
                    { 'trigger':'1232 Request module off', 'new-state': 'standstill'}
                    ]),
                'degasing': State('degasing',[
                    { 'trigger':'1249 Starter on', 'new-state': 'starter'},
                    { 'trigger':'1225 Service selector switch Off', 'new-state': 'standstill'},
                    { 'trigger':'1232 Request module off', 'new-state': 'standstill'}
                    ]),
                'starter': State('starter',[
                    { 'trigger':'3225 Ignition on', 'new-state':'speedup'},
                    { 'trigger':'1232 Request module off', 'new-state':'standstill'}
                    ]),
                'speedup': State('speedup',[
                    { 'trigger':'2124 Idle', 'new-state':'idle'},
                    { 'trigger':'2139 Request Synchronization', 'new-state':'synchronize'}, 
                    { 'trigger':'3226 Ignition off', 'new-state':'standstill'}
                    ]),             
                'idle': State('idle',[
                    { 'trigger':'1232 Request module off', 'new-state':'coolrun'},
                    { 'trigger':'2139 Request Synchronization', 'new-state':'synchronize'},
                    { 'trigger':'3226 Ignition off', 'new-state':'standstill'}
                    ]),
                'synchronize': State('synchronize',[
                    { 'trigger':'1232 Request module off', 'new-state':'coolrun'},
                    { 'trigger':'1235 Generator CB closed', 'new-state':'loadramp'},                
                    { 'trigger':'3226 Ignition off', 'new-state':'standstill'}
                    ]),             
                'loadramp': LoadrampStateV2('loadramp',[
                    { 'trigger':'3226 Ignition off', 'new-state':'standstill'},
                    { 'trigger':'1232 Request module off', 'new-state':'rampdown'}, # lead to an error at Bautzen ???
                    #{ 'trigger':'1225 Service selector switch Off', 'new-state': 'standstill'}, # Egg Gera 1.3.2019 => did not lead to engine stop
                    #{ 'trigger':'Calculated statechange', 'new-state':'targetoperation'}, #enable with run1 & LoadrampState
                    { 'trigger':'9047 Target load reached', 'new-state':'targetoperation'},#enable with run1, enable with run1V2 & LoadrampStateV2
                    ], operator, e),             
                'targetoperation': State('targetoperation',[
                    { 'trigger':'1232 Request module off', 'new-state':'rampdown'},
                    #{ 'trigger':'1239 Group alarm - shut down', 'new-state':'rampdown'},
                    #{ 'trigger':'1225 Service selector switch Off', 'new-state': 'standstill'},
                    { 'trigger':'1236 Generator CB opened', 'new-state':'idle'},
                    ]),
                'rampdown': State('rampdown',[
                    { 'trigger':'1236 Generator CB opened', 'new-state':'coolrun'},
                    { 'trigger':'3226 Ignition off', 'new-state':'standstill'},
                    { 'trigger':'1231 Request module on', 'new-state':'targetoperation'},
                    ]),
                'coolrun': State('coolrun',[
                    { 'trigger':'1234 Operation off', 'new-state':'runout'},
                    { 'trigger':'1231 Request module on', 'new-state':'idle'},
                    { 'trigger':'3226 Ignition off', 'new-state':'standstill'}
                    ]),
                'runout': State('runout',[
                    { 'trigger':'3226 Ignition off', 'new-state':'standstill'},
                    { 'trigger':'1231 Request module on', 'new-state': 'oilfilling'},            
                ])
            }

    def set_successtime(self, successtime):
        self._successtime = successtime

    def _harvest_timings(self, sv, phases, results):
        """calculates state phase durations from state switch Timestamps
        is called at Run_1 and after changed to the Timestamps in Run_xx.
        calculates the cumulated starttime 

        Args:
            sv (dict): dict with data for a start attempt
            phases (list): List of state names to collect.
        """
        durations = { ph:pd.Timedelta(sv['startstoptiming'][ph][-1]['end'] - sv['startstoptiming'][ph][-1]['start']).total_seconds() for ph in phases}
        durations['cumstarttime'] = sum([v for k,v in durations.items() if k in self.start_timing_states])
        results['starts'][sv['no']].update(durations)

    def check_success(self, start):
        # check if a start is successful:
        A_in_startphase = len([a for a in start['alarms'] if a['state'] in self.start_timing_states])
        if 'targetoperation' in start:
            if (start['targetoperation'] > self._successtime) and (A_in_startphase == 0):
                start['success'] = 'success'
                return
        # check an alarm occured 
        if A_in_startphase > 0:
            start['success'] = 'failed'
        else: # no alarm, but too short engine operation => assume to be intentionally stopped early.
            start['success'] = 'undefined'
        return

    def collect_data(self, nsvec, results):
        # do with every message
        key = 'starts' if nsvec['in_operation'] == 'on' else 'stops'
        if nsvec['msg']['severity'] == 800:
            results[key][-1]['alarms'].append({
                'state':nsvec[self.name].currentstate, 
                'msg': nsvec['msg']
                })
        if nsvec['msg']['severity'] == 700:
            results[key][-1]['warnings'].append({
                'state':nsvec[self.name].currentstate, 
                'msg': nsvec['msg']
                })

        # do in case of a statechange
        if nsvec[self.name].statechange:
            # start a 'on' cycle
            if nsvec[self.name].currentstate == 'oilfilling':
                results['stops'][-1]['endtime'] = nsvec[self.name].currentstate_start
                results['stops'][-1]['A'] = len(results['stops'][-1]['alarms'])
                results['stops'][-1]['W'] = len(results['stops'][-1]['warnings'])
                # apends a new record to the Starts list.
                results['starts'].append({
                    'run2':False,
                    'run4':False,
                    'no':results['starts_counter'],
                    'success': False,
                    'mode':nsvec['service_selector'],
                    'starttime': nsvec[self.name].currentstate_start,
                    'endtime': pd.Timestamp(0),
                    'cumstarttime': pd.Timedelta(0),
                    #'startpreparation':np.nan,
                    'oilfilling':np.nan,
                    'degasing':np.nan,
                    'starter':np.nan,
                    'speedup':np.nan,
                    'idle':np.nan,
                    'synchronize':np.nan,
                    'loadramp':np.nan,
                    'targetoperation':np.nan,
                    'rampdown':np.nan,
                    'coolrun':np.nan,
                    'runout':np.nan,
                    'startstoptiming': {},
                    'alarms': [],
                    'warnings': [],
                    'targetload': np.nan,
                    'ramprate': np.nan,
                    'maxload': np.nan
                })
                if self._operator.act_run in self._operator.logrun:
                    logging.debug(f"{self._operator.act_run} SNO{self._operator.nsvec['startno']:5d}, Start initialized in startstopFSM collect_data")
                nsvec['startno'] = results['starts_counter']
                results['starts_counter'] += 1 # index for next start
                nsvec['in_operation'] = 'on'

            # do while 'on' in all states
            elif nsvec['in_operation'] == 'on': 
                #results['starts'][-1]['mode'] = nsvec['service_selector']
                rec = {'start':nsvec[self.name].laststate_start, 'end':nsvec[self.name].currentstate_start}
                if not nsvec[self.name].laststate in results['starts'][-1]['startstoptiming']:
                    results['starts'][-1]['startstoptiming'][nsvec[self.name].laststate]=[rec]
                else:
                    results['starts'][-1]['startstoptiming'][nsvec[self.name].laststate].append(rec)

            if nsvec[self.name].currentstate == 'standstill':
                
                # end an 'on' cycle, harvest collected data
                if nsvec['in_operation'] == 'on':
                    # start finished
                    results['starts'][-1]['endtime'] = nsvec[self.name].currentstate_start
                    # calc phase durations
                    sv = results['starts'][-1]
                    phases = list(sv['startstoptiming'].keys())

                    # handle a special case, switching back and forth between 'targetoperation' and 'rampdown'  
                    if 'targetoperation' in phases:
                        tlr = sv['startstoptiming']['targetoperation']
                        tlr = [{'start':tlr[0]['start'], 'end':tlr[-1]['end']}] # just use the first start and the last end time. (mulitple rampdown cycles)
                        sv['startstoptiming']['targetoperation_org'] = sv['startstoptiming']['targetoperation'] # back up original data
                        sv['startstoptiming']['targetoperation'] = tlr # and replace with modified data

                    self._harvest_timings(sv, phases, results)

                    # count alarms an warnings
                    results['starts'][-1]['A'] = len(results['starts'][-1]['alarms'])
                    results['starts'][-1]['W'] = len(results['starts'][-1]['warnings'])

                    self.check_success(results['starts'][-1])
                    # ######################################################################################################
                    # assess if a start is successful:
                    # ... if it reaches 'targetoperation'
                    # if 'targetoperation' in results['starts'][-1]:
                    #     # other criterias may apply.
                    #     # ... and it stayed longer than 'successtime'
                    #     results['starts'][-1]['success'] = (results['starts'][-1]['targetoperation'] > self._successtime)
                    ########################################################################################################

                # change to 'off' mode
                nsvec['in_operation'] = 'off'
                results['stops_counter'] += 1 # index for next start
                results['stops'].append({
                    'run2':False,
                    'no': results['stops_counter'],
                    'mode': nsvec['service_selector'],
                    'starttime': nsvec[self.name].laststate_start,
                    'endtime': pd.Timestamp(0),
                    'alarms':[],
                    'warnings':[]
                })

class FSMOperator:
    def __init__(self, e, p_from = None, p_to=None, skip_days=None, frompickle='NOTIMPLEMENTED'):
        self._e = e
        self._p_from = p_from
        self._p_to = p_to
        self.load_messages(e, p_from, p_to, skip_days)
        self.message_queue = []
        self.extra_messages = []
        self.runs_completed = []
        self.logrun = [1,2] # possibility to limit logging to certain runs.
        self.logstates = ['startstop']
        self.act_run = 0

        #register statehandlers
        #TODO: wite a registering interface so that all implementation steps
        # are handled in a kind of one stop shop.  
        self.startstopHandler = startstopFSM(self, self._e)
        self.serviceSelectorHandler = ServiceSelectorFSM()
        self.oilpumpHandler = OilPumpFSM()

        #initialize common StateVector dict
        self.nsvec = {
            self.startstopHandler.name: self.startstopHandler.initialize_statevector(self.first_message),
            self.serviceSelectorHandler.name: self.serviceSelectorHandler.initialize_statevector(self.first_message),
            self.oilpumpHandler.name: self.oilpumpHandler.initialize_statevector(self.first_message),
            'logstates' : self.logstates,
            'in_operation': 'off',
            'service_selector':  self.serviceSelectorHandler.initial_state,
            'oil_pump': self.oilpumpHandler._initial_state,
            'msg': 'none',
            'startno': 0
        }
        self.init_results()

        self.pfn = self._e._fname + '_statemachine.pkl'
        self.hdfn = self._e._fname + '_statemachine.hdf'
        self.tempfn = self._e._fname + '_temp.feather'

    def init_results(self):
        self.results = {
            'sn' : str(self._e._sn),
            'save_date': None,
            'first_message': self.first_message,
            'last_message': self.last_message,
            'starts': [],
            'starts_counter':0,
            'stops': [
            {
                'run2':False,
                'no': 0,
                'mode': self.nsvec['service_selector'],
                'starttime': self.first_message,
                'endtime': pd.Timestamp(0),
                'alarms':[],
                'warnings':[]                
            }],
            'run2_content': {
                'startstop': startstopFSM.run2filter_content
            },
            'serviceselectortiming':[],
            'oilpumptiming':[],
            'stops_counter':0,
            'run2_failed':[],
            'runlog': [],
            'runlogdetail': []
        } 

    def save_docu(self):
        self.startstopHandler.dot('startstopFSM.dot')
        self.serviceSelectorHandler.dot('ServiceSelectorFSM.dot')
        self.oilpumpHandler.dot('ServiceSelectorFSM.dot')

    @property
    def starts(self):
        return pd.DataFrame(self.results['starts'])

    @property
    def stops(self):
        return pd.DataFrame(self.results['stops'])

    def restore(self):
        if os.path.exists(self.pfn):
            with open(self.pfn, 'rb') as handle:
                self.results = pickle.load(handle)

    def store(self):
        self.unstore()
        self.results['save_date'] = pd.to_datetime('today').normalize()
        with open(self.pfn, 'wb') as handle:
            pickle.dump(self.results, handle, protocol=5)

    def unstore(self):
        if self.exists:
            os.remove(self.pfn)

    def save_results(self, filename):
        if len(self.starts) > 0:
            self.unstore()
            self.results['info'] = dict(
                save_date = pd.to_datetime('today').normalize(),
                p_from = self._p_from,
                p_to = self._p_to,
                run2 = all(self.starts['run2']),
                starts = len(self.starts)
            )
            self.results['info'].update(self._e.description)
            with open(filename, 'wb') as handle:
                pickle.dump(self.results, handle, protocol=5)
                logging.info(f"Results successfully saved to {filename}")
        else:
            print('no results to save.')        

    @classmethod
    def load_results(cls, mp, filename):
        if os.path.exists(filename):
            with open(filename, 'rb') as handle:
                results = pickle.load(handle)
                #pp(results['info'])
                e = Engine.from_sn(mp, results['info']['serialNumber'])
                lfsm = cls(e, p_from=results['info']['p_from'], p_to=results['info']['p_to'])
                lfsm.results = results
                return lfsm
        else:
            raise FileNotFoundError(filename)

    @property        
    def exists(self):
        return os.path.exists(self.pfn) 

    ## message handling
    def load_messages(self,e, p_from=None, p_to=None, skip_days=None, untilnow=False):
        #self._messages = e.get_messages(p_from, p_to) #using stored messages.
        self._messages = e.get_messages2(p_from, p_to, untilnow) 
        pfrom_ts = int(pd.to_datetime(p_from, infer_datetime_format=True).timestamp() * 1000) if p_from else 0
        pto_ts = int(pd.to_datetime(p_to, infer_datetime_format=True).timestamp() * 1000) if p_to else int(pd.Timestamp.now().timestamp() * 1000)
        self._messages = self._messages[(self._messages.timestamp > pfrom_ts) & (self._messages.timestamp < pto_ts)]
        self.first_message = pd.to_datetime(self._messages.iloc[0]['timestamp']*1e6)
        self.last_message = pd.to_datetime(self._messages.iloc[-1]['timestamp']*1e6)
        self._period = pd.Timedelta(self.last_message - self.first_message).round('S')
        if skip_days and not p_from:
            self.first_message = pd.Timestamp(arrow.get(self.first_message).shift(days=skip_days).timestamp()*1e9)
            self._messages = self._messages[self._messages['timestamp'] > int(arrow.get(self.first_message).shift(days=skip_days).timestamp()*1e3)]
        self.count_messages = self._messages.shape[0]

    def inject_message(self, msg):
        """Puts a message into the extra_messages list.
        The State Machine Operator Class then injects this message
        into the main message stream at the right timestamp
        Args:
            msg (dict or pandas Row): both type need to include the message fields timestamp, name, message
        """
        self.extra_messages.append(msg)

    def replace_message(self, msg):
        """Replaces a messgae with a certain name, if 
        a messge of the same name is already in the extra_messages list
        ans the operator has not consumed it yet.
        purpose: some later XT4 Versions have "9047 target load reached" messages,
        some earlier do not. if the message is available, the calculated message is replaced
        by the diane message and startstoptiming.

        Args:
            msg (dict or pandas Row): both type need to include the message fields timestamp, name, message
        """
        msg['message'] += ' (replaced)' #rename message in message_queue, msg is a reference to the original msg.
        local_extra_messages = []
        for m in self.extra_messages:
            if (m['name'] == '9047' and m['message'] == 'Target load reached (calculated)'):
                pass #remove message from extra_messages list
            else:
                local_extra_messages.append(m) # keep all other messages in place
        self.extra_messages = local_extra_messages

    def msgtxt(self, msg, idx=0):
        return f"{idx:>06} {msg['severity']} {msg['timestamp']} {pd.to_datetime(int(msg['timestamp'])*1e6).strftime('%d.%m.%Y %H:%M:%S')}  {msg['name']} {msg['message']}"

    def msg_smalltxt(self, msg):
        return f"{msg['severity']} {pd.to_datetime(int(msg['timestamp'])*1e6).strftime('%d.%m.%Y %H:%M:%S')}  {msg['name']} {msg['message']}"

    def save_messages(self, fn):
        with open(fn, 'w') as f:
            for index, msg in self._messages.iterrows():
                f.write(self.msgtxt(msg, index)+'\n')
                #f.write(f"{index:>06} {msg['severity']} {msg['timestamp']} {pd.to_datetime(int(msg['timestamp'])*1e6).strftime('%d.%m.%Y %H:%M:%S')}  {msg['name']} {msg['message']}\n")
                if 'associatedValues' in msg:
                    if msg['associatedValues'] == msg['associatedValues']:  # if not NaN ...
                        f.write(f"{pf(msg['associatedValues'])}\n\n")

    def save_runlog(self, fn):
        if len(self._runlog):
            with open(fn, 'w') as f:
                for line in self._runlog:
                    f.write(line + '\n')

    def save_detailrunlog(self, fn):
        if len(self.results['runlogdetail']):
            with open(fn, 'w') as f:
                for vec in self.results['runlogdetail']:
                    f.write(vec.__str__() + '\n')

    def runlogdetail(self, startversuch, statechanges_only = False):
        def makestr(x):
            return  f"{'*' if x['statechange'] else '':2}|"+ \
                    f"{x['startno']:04}| " + \
                    f"LST {x['laststate_start']} " + \
                    f"LS  {x['laststate']:18}| " + \
                    f"CSS {x['currentstate_start']} " + \
                    f"CS  {x['currentstate']:18}| " + \
                    f"{x['in_operation']:4}| " + \
                    f"{x['service_selector']:6}| " + \
                    f"{x['severity']} {pd.to_datetime(int(x['timestamp'])*1e6).strftime('%d.%m.%Y %H:%M:%S')} {x['name']} {x['message']}"
        ts_start = startversuch['starttime'].timestamp() * 1e3
        ts_end = startversuch['endtime'].timestamp() * 1e3
        if statechanges_only:
            log = [x for x in self.results['runlogdetail'] if x['statechange']]
        else:
            log = [x for x in self.results['runlogdetail']]
        log = [makestr(x) for x in log if ((x['timestamp'] >= ts_start) and (x['timestamp'] <= ts_end))]
        return log
    
    def detaillog(self, fsm_name, vec):
        d = dict(
            statechange = vec[fsm_name].statechange,
            laststate = vec[fsm_name].laststate,
            laststate_start = vec[fsm_name].laststate_start.strftime('%d.%m %H:%M:%S'),
            currentstate = vec[fsm_name].currentstate,
            currentstate_start = vec[fsm_name].currentstate_start.strftime('%d.%m %H:%M:%S')
        )
        for k in vec.keys():
            if isinstance(vec[k],str):
                d.update({k:vec[k]})
        d['severity'] = vec['msg']['severity']
        d['timestamp'] = vec['msg']['timestamp']
        d['name'] = vec['msg']['name']
        d['message'] = vec['msg']['message']
        d['startno'] = vec['startno'] - 1
        return d

####################################
### Finite State Machine |     Run 0
####################################
    def debug_msg(self, titel, msgque, max=100000, debug=False):
        """Helper Function for debugging run cycles

        Args:
            titel (str): titlestring
            msgque (list): List with messages
            max (int, optional): limit to max messages to display. Defaults to 100000.
            debug (bool, optional): switch debugging on/off. Defaults to False.
        """
        if debug:
            print(titel, len(msgque), ' items')
            for msg in msgque[:max]:
                print(f"{msg['timestamp']} {pd.to_datetime(msg['timestamp'] * 1000000)} {msg['name']} {msg['severity']} {msg['message']}")
            print()

    def merge_extra_messages(self, messages_queue, extra_messages):
        """Helper Function 
        Extra messages are generated by special state trigger functions,
        which e.g. estimate missing messages like '9047 target load reached'
        this message is not available on all diane versions, but needed to
        operate the statemachine. merge is injecting the extra messages
        exactly at the right timestamp into the message_que.

        Args:
            messages_queue (list): the message queue to merge
            extra_messages (list): list of extra messages collected in run0 (currently only the 9047 messages, if not provided by diane itself)

        Returns:
            tuple: (messages_queue, extra_messages)
        """
        emc = extra_messages.copy()
        max_timestamp = max([ts['timestamp'] for ts in messages_queue]) # check latest timestamp in cycle
        for m in emc:
            if m['timestamp'] < max_timestamp: # only insert messages if they are not later than latess timestamp in cycle!
                messages_queue.append(m)
                extra_messages.remove(m)
        messages_queue.sort(key=lambda x: x['timestamp'], reverse=False)
        return messages_queue, extra_messages


    def run0(self, enforce=False, silent=False ,debug=False):
        """Statemachine Operator Run0 - collect e.g. calculated future events in self.extra_messages 
        Args:
            enforce (bool, optional): if True runs statemachine even if results are already available. Defaults to False.
            silent (bool, optional): do not show progress bar if True. Defaults to False.
        """        

        if len(self.results['starts']) == 0 or enforce:
            self.init_results()            
        #self.message_queue = [m for i,m in self._messages.iterrows()] 
        self.message_queue = []
        self.act_run = 0
        fsm0_starts_counter = 0

        vecstore = copy.deepcopy(self.nsvec) # store statevector
        if not silent:
            pbar = tqdm(total=len(self._messages), ncols=80, mininterval=1, unit=' messages', desc="FSM0", file=sys.stdout)
           #pbar = tqdm(total=len(self.message_queue), ncols=80, mininterval=1, unit=' messages', desc="FSM0", file=sys.stdout)
        #for msg in self.message_queue:
        for i, msg in self._messages.iterrows():
            self.message_queue.append(msg)
            self.nsvec['msg'] = msg
            self.nsvec = self.startstopHandler.call_trigger_states(self.nsvec)
            self.nsvec = self.serviceSelectorHandler.call_trigger_states(self.nsvec)
            self.nsvec = self.oilpumpHandler.call_trigger_states(self.nsvec)

            if self.nsvec[self.startstopHandler.name].statechange:
                if self.nsvec[self.startstopHandler.name].currentstate == 'oilfilling':
                    if self.act_run in self.logrun:
                        logging.debug(f"0 SNO{fsm0_starts_counter:5d}  v-----------------------------v")
                    self.nsvec['startno'] = fsm0_starts_counter
                    fsm0_starts_counter += 1
                    if self.act_run in self.logrun:
                        logging.debug(f"{self.act_run} SNO{self.nsvec['startno']:5d}, new Start logged")
            if not silent:
                pbar.update()
        self.nsvec = vecstore # restore statevector

        # merge original and extra messages, sortbmessaged,  make sure timing is monoton upwards
        self.debug_msg('extra messages before merge:',self.extra_messages, debug=debug)
        self.message_queue, self.extra_messages = self.merge_extra_messages(self.message_queue, self.extra_messages)
        self.debug_msg('extra messages after merge:',self.extra_messages, debug=debug)
        if not silent:
            pbar.close() 

        if os.path.exists(self.tempfn):
            os.remove(self.tempfn)
        pd.DataFrame(self.message_queue).reset_index().to_feather(self.tempfn)
        del(self._messages)
        del(self.message_queue)
        gc.collect()                    
        self.runs_completed.append(self.act_run)
        logging.debug('0 Run completed')


####################################
### Finite State Machine |     Run 1
####################################

    def run1_call_triggers(self, _nsvec):
        _nsvec = self.startstopHandler.call_trigger_states(_nsvec)
        _nsvec = self.serviceSelectorHandler.call_trigger_states(_nsvec)
        _nsvec = self.oilpumpHandler.call_trigger_states(_nsvec)        
        return _nsvec

    def run1_collect_data(self, _nsvec, _results):
        self.startstopHandler.collect_data(_nsvec, _results)
        self.serviceSelectorHandler.collect_data(_nsvec, _results)
        self.oilpumpHandler.collect_data(_nsvec, _results)

    def run1(self, silent=False ,debug= False, successtime=300):
        """Statemachine Operator Run 1 - using also extra messages injected in run0 

        Args:
            silent (bool, optional): do not show progress bar if True. Defaults to False.
            successtime (int, optional): How long an operation cycle needs to stay in state targetoperation to be assessed successful. Defaults to 300.
        """        
        self.act_run = 1
        self.startstopHandler.set_successtime(successtime)
        self._messages = pd.read_feather(self.tempfn)

        # if not silent:
        #     pbar = tqdm(total=len(self.message_queue), ncols=80, mininterval=1, unit=' messages', desc="FSM1", file=sys.stdout)
        if not silent:
            pbar = tqdm(total=len(self._messages), ncols=80, mininterval=1, unit=' messages', desc="FSM1", file=sys.stdout)

        #for msg in self.message_queue:
        for i, msg in self._messages.iterrows():
            # inject new message into StatesVector
            self.nsvec['msg'] = msg
            self.nsvec = self.run1_call_triggers(self.nsvec)
            
            # log Statesvector details
            # TODO: store in a file, in a huge result struchture
            # TODO: retrieve by seeking this file when needed.
            #self.results['runlogdetail'].append(copy.deepcopy(self.nsvec))
            self.results['runlogdetail'].append(self.detaillog(self.startstopHandler.name,self.nsvec))

            # collect & harvest data:
            self.run1_collect_data(self.nsvec, self.results)

            if not silent:
                pbar.update()
        if not silent:
            pbar.close()

        del(self._messages)
        gc.collect()
        self.runs_completed.append(self.act_run)
        logging.debug('1 Run completed')


####################################
### Finite State Machine |     Run 2
####################################

    def run2_collectors_setup(self):
        #ratedload = self._e['Power_PowerNominal']
        self.target_load_collector = Target_load_Collector(self.results, self._e, period_factor=3, helplinefactor=0.8)
        self.exhaust_temp_collector = Exhaust_temp_Collector(self.results, self._e)
        self.tecjet_collector = Tecjet_Collector(self.results, self._e)
        self.sync_current_collector = Sync_Current_Collector(self.results, self._e)
        self.oil_start_collector = Oil_Start_Collector(self.results, self._e)
        

    def run2_collectors_register(self, startversuch):
        vset, tfrom, tto = self.target_load_collector.register(startversuch, vset=[], tfrom=None, tto=None) #vset,tfrom,tto will be populated by the Collectors
        vset, tfrom, tto = self.exhaust_temp_collector.register(startversuch, vset, tfrom, tto)
        vset, tfrom, tto = self.tecjet_collector.register(startversuch, vset, tfrom, tto)
        vset, tfrom, tto = self.sync_current_collector.register(startversuch, vset, tfrom, tto)
        vset, tfrom, tto = self.oil_start_collector.register(startversuch, vset, tfrom, tto)
        return vset, tfrom, tto 

    def run2_collectors_collect(self, startversuch, results, data):
        results = self.target_load_collector.collect(startversuch, results, data)
        results = self.exhaust_temp_collector.collect(startversuch, results, data)
        results = self.tecjet_collector.collect(startversuch, results, data)
        results = self.sync_current_collector.collect(startversuch, results, data)
        results = self.oil_start_collector.collect(startversuch, results, data)
        return results

    def run2(self, silent=False, debug=False, p_refresh=False):
        """Statemachine Operator Run 2 - uses timings collected in previos runs to download 'Power_PowerAct'
        in 1 sec. Intervals around loadramp phase. Use the curve to collect additional and more accurate data
        on loadramps.

        silent (Boolean): whether a progress bar is visible or not. 
        """
        self.act_run = 2
        self.run2_collectors_setup()
        self.results['run2_failed'] = []

        if not silent:
            pbar = tqdm(total=len(self.results['starts']), ncols=80, mininterval=2, unit=' starts', desc="FSM2", file=sys.stdout)

        for i, startversuch in enumerate(self.results['starts']):
            sno = startversuch['no']
            if not self.results['starts'][sno]['run2']:
                self.results['starts'][sno]['run2'] = True
                try:
                    # collect dataItems & phases, align an load data in one request to myplant per Start. 
                    vset, tfrom, tto = self.run2_collectors_register(startversuch)
                    t0 = time.time()
                    data = load_data(self, cycletime=1, tts_from=tfrom, tts_to=tto, silent=True, p_data=vset, p_forceReload=p_refresh, p_suffix='_run2', debug=False)
                    t1 = time.time()
                    #self.results['starts'][sno]['datasize'] = len(data)
                    #self.results['starts'][sno]['loadingtime'] = t1-t0
                    if self.act_run in self.logrun:
                        logging.debug(f"2 SNO{sno:5d} start: {startversuch['starttime'].round('S')} to: {startversuch['endtime'].round('S')} load_data: {(t1-t0):0.1f} sec. v-----------------------------v")
                    if ((tfrom is not None) and (tto is not None)):
                        logging.debug(f"2 SNO{sno:5d} tfrom: {pd.to_datetime(tfrom * 1e9).strftime('%d.%m.%Y %H:%M:%S')} tto: {pd.to_datetime(tto * 1e9).strftime('%d.%m.%Y %H:%M:%S')} tto-tfrom: {(tto-tfrom):.1f} lenght of data: {len(data)} empty? {data.empty}")
                    else:
                        logging.debug(f"2 SNO{sno:5d} tfrom:{tfrom} tto: {tto} tto-tfrom: {'None'} lenght of data: {len(data)} empty? {data.empty}")

                    if not data.empty:
                        if self.act_run in self.logrun:
                            logging.debug(f"2 SNO{sno:5d} Power_PowerAct: Min:{data['Power_PowerAct'].min()} Max:{data['Power_PowerAct'].max()}")
                            #Statistics:\n{data[['Various_Values_SpeedAct','Power_PowerAct']].describe()}")
                            #print(data[['Various_Values_SpeedAct','Power_PowerAct']].min())
                            if 'loadramp' in self.results['starts'][sno]['startstoptiming']:
                                    logging.debug(f"2 SNO{sno:5d} before run2 collectors, S {self.results['starts'][sno]['startstoptiming']['loadramp'][-1]['start'].strftime('%d.%m.%Y %H:%M:%S')} E {self.results['starts'][sno]['startstoptiming']['loadramp'][-1]['end'].strftime('%d.%m.%Y %H:%M:%S')}")
                            else:
                                logging.debug(f"2 SNO{sno:5d} before run2 collectors, {pf(list(self.results['starts'][sno]['startstoptiming'].keys()))}")
                        self.results = self.run2_collectors_collect(startversuch, self.results, data)
                        phases = list(self.results['starts'][sno]['startstoptiming'].keys())
                        self.startstopHandler._harvest_timings(self.results['starts'][sno], phases, self.results)
                        if self.act_run in self.logrun:
                            if 'loadramp' in self.results['starts'][sno]['startstoptiming']:
                                logging.debug(f"2 SNO{sno:5d} after  run2 collectors, S {self.results['starts'][sno]['startstoptiming']['loadramp'][-1]['start'].strftime('%d.%m.%Y %H:%M:%S')} E {self.results['starts'][sno]['startstoptiming']['loadramp'][-1]['end'].strftime('%d.%m.%Y %H:%M:%S')}")
                            else:
                                logging.debug(f"2 SNO{sno:5d} after  run2 collectors, {pf(list(self.results['starts'][sno]['startstoptiming'].keys()))}")

                except Exception as err:
                    err_str = f"\nDuring Run2 {startversuch['no']} from {startversuch['starttime'].round('S')} to {startversuch['endtime'].round('S')}, this Error occured: {err}"
                    logging.error(traceback.format_exc())

            if not silent:
                pbar.update()
                        
        if not silent:
            pbar.close()
        self.runs_completed.append(self.act_run)
        logging.debug('2 Run completed')

####################################
### Finite State Machine |     Run 3
### analyse targetoperation
####################################


####################################
### Finite State Machine |     Run 4
### analyse engine stop
####################################

    def run4_collectors_setup(self):
        pass
        
    def run4_collectors_register(self, startversuch):
        vset = []; tfrom=None; tto=None
        return vset, tfrom, tto 

    def run4_collectors_collect(self, startversuch, results, data):
        return results

    def run4(self, silent=False, debug=False, p_refresh=False):
        """Statemachine Operator Run 4 - uses timings collected in previous runs to analyse data
        silent (Boolean): whether a progress bar is visible or not. 
        """
        self.act_run = 4
        self.run4_collectors_setup()
        self.results['run4_failed'] = []

        if not silent:
            pbar = tqdm(total=len(self.results['starts']), ncols=80, mininterval=2, unit=' starts', desc="FSM4", file=sys.stdout)

        for i, startversuch in enumerate(self.results['starts']):
            sno = startversuch['no']
            if not self.results['starts'][sno]['run4']:
                self.results['starts'][sno]['run4'] = True
                try:
                    # collect dataItems & phases, align an load data in one request to myplant per Start. 
                    vset, tfrom, tto = self.run4_collectors_register(startversuch)
                    t0 = time.time()
                    data = load_data(self, cycletime=1, tts_from=tfrom, tts_to=tto, silent=True, p_data=vset, p_forceReload=p_refresh, p_suffix='_run4', debug=False)
                    t1 = time.time()
                    #self.results['starts'][sno]['datasize'] = len(data)
                    #self.results['starts'][sno]['loadingtime'] = t1-t0
                    if self.act_run in self.logrun:
                        logging.debug(f"4 SNO{sno:5d} start: {startversuch['starttime'].round('S')} to: {startversuch['endtime'].round('S')} load_data: {(t1-t0):0.1f} sec. v-----------------------------v")
                    if ((tfrom is not None) and (tto is not None)):
                        logging.debug(f"4 SNO{sno:5d} tfrom: {pd.to_datetime(tfrom * 1e9).strftime('%d.%m.%Y %H:%M:%S')} tto: {pd.to_datetime(tto * 1e9).strftime('%d.%m.%Y %H:%M:%S')} tto-tfrom: {(tto-tfrom):.1f} lenght of data: {len(data)} empty? {data.empty}")
                    else:
                        logging.debug(f"4 SNO{sno:5d} tfrom:{tfrom} tto: {tto} tto-tfrom: {'None'} lenght of data: {len(data)} empty? {data.empty}")

                    if not data.empty:
                        if self.act_run in self.logrun:
                            logging.debug(f"4 SNO{sno:5d} Power_PowerAct: Min:{data['Power_PowerAct'].min()} Max:{data['Power_PowerAct'].max()}")
                            #Statistics:\n{data[['Various_Values_SpeedAct','Power_PowerAct']].describe()}")
                            #print(data[['Various_Values_SpeedAct','Power_PowerAct']].min())
                            if 'loadramp' in self.results['starts'][sno]['startstoptiming']:
                                    logging.debug(f"4 SNO{sno:5d} before run4 collectors, S {self.results['starts'][sno]['startstoptiming']['loadramp'][-1]['start'].strftime('%d.%m.%Y %H:%M:%S')} E {self.results['starts'][sno]['startstoptiming']['loadramp'][-1]['end'].strftime('%d.%m.%Y %H:%M:%S')}")
                            else:
                                logging.debug(f"4 SNO{sno:5d} before run4 collectors, {pf(list(self.results['starts'][sno]['startstoptiming'].keys()))}")
                        self.results = self.run4_collectors_collect(startversuch, self.results, data)
                        #phases = list(self.results['starts'][sno]['startstoptiming'].keys())
                        #self.startstopHandler._harvest_timings(self.results['starts'][sno], phases, self.results)
                        #if self.act_run in self.logrun:
                        #    if 'loadramp' in self.results['starts'][sno]['startstoptiming']:
                        #        logging.debug(f"2 SNO{sno:5d} after  run2 collectors, S {self.results['starts'][sno]['startstoptiming']['loadramp'][-1]['start'].strftime('%d.%m.%Y %H:%M:%S')} E {self.results['starts'][sno]['startstoptiming']['loadramp'][-1]['end'].strftime('%d.%m.%Y %H:%M:%S')}")
                        #    else:
                        #        logging.debug(f"2 SNO{sno:5d} after  run2 collectors, {pf(list(self.results['starts'][sno]['startstoptiming'].keys()))}")

                except Exception as err:
                    err_str = f"\nDuring Run4 {startversuch['no']} from {startversuch['starttime'].round('S')} to {startversuch['endtime'].round('S')}, this Error occured: {err}"
                    logging.error(traceback.format_exc())

            if not silent:
                pbar.update()
                        
        if not silent:
            pbar.close()
        self.runs_completed.append(self.act_run)
        logging.debug('4 Run completed')
