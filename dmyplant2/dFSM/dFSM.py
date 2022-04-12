import copy
from datetime import datetime
import logging
import os
import sys
import pickle
import warnings
from pprint import pprint as pp, pformat as pf
import arrow
import dmyplant2
import numpy as np
import pandas as pd
from tqdm import tqdm

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
        self._default_ramp_duration = 100.0 / self._loadramp
        super().__init__(statename, transferfun_list)

    def trigger_on_vector(self, vector, msg):
        vector = super().trigger_on_vector(vector, msg)
        # calculate the end of ramp time if it isnt defined.
        if self._full_load_timestamp == None:
            self._full_load_timestamp = int((vector.currentstate_start.timestamp() + self._default_ramp_duration) * 1e3)
            self._operator.inject_message({'name':'9047', 'message':'Target load reached (calculated)','timestamp':self._full_load_timestamp,'severity':600})

        # use the message target load reached to make the trigger more accurate. (This message isnt available on all engines.)
        if msg['name'] == '9047':
            self._full_load_timestamp = msg['timestamp']
            self._operator.replace_message(msg)

        if vector.statechange:
            self._full_load_timestamp = None

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
            nsvec['oil_pump'] = nsvec[self.name].currentstate
            results['oilpumptiming'].append({
                'state':nsvec[self.name].currentstate,
                'time':nsvec[self.name].currentstate_start})

###########################################################################################
## Service Selector FSM
###########################################################################################
class ServiceSelectorFSM(FSM):
    def __init__(self):
        self.name = 'serviceselector'
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
            nsvec['service_selector'] = nsvec[self.name].currentstate
            results['serviceselectortiming'].append({
                'state':nsvec[self.name].currentstate,
                'time':nsvec[self.name].currentstate_start})

###########################################################################################
## Start Stop FSM
###########################################################################################
class startstopFSM(FSM):
    def __init__(self, operator, e):
        self.name = 'startstop'
        self._operator = operator
        self._e = e
        self._successtime = 300
        self._initial_state = 'standstill'
        self.start_timing_states =  ['startpreparation','starter','speedup','idle','synchronize','loadramp']
        self._states = {
                'standstill': State('standstill',[
                    { 'trigger':'1231 Request module on', 'new-state': 'startpreparation'},            
                    ]),
                'startpreparation': State('startpreparation',[
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
                    { 'trigger':'2139 Request Synchronization', 'new-state':'synchronize'},
                    { 'trigger':'3226 Ignition off', 'new-state':'standstill'}
                    ]),
                'synchronize': State('synchronize',[
                    { 'trigger':'1235 Generator CB closed', 'new-state':'loadramp'},                
                    { 'trigger':'3226 Ignition off', 'new-state':'standstill'}
                    ]),             
                'loadramp': LoadrampStateV2('loadramp',[
                    { 'trigger':'3226 Ignition off', 'new-state':'standstill'},
                    #{ 'trigger':'1232 Request module off', 'new-state':'rampdown'}, # lead to an error at Bautzen ???
                    #{ 'trigger':'Calculated statechange', 'new-state':'targetoperation'}, #enable with run1 & LoadrampState
                    { 'trigger':'9047 Target load reached', 'new-state':'targetoperation'},#enable with run1, enable with run1V2 & LoadrampStateV2
                    ], operator, e),             
                'targetoperation': State('targetoperation',[
                    { 'trigger':'1232 Request module off', 'new-state':'rampdown'},
                    #{ 'trigger':'1239 Group alarm - shut down', 'new-state':'rampdown'},
                    { 'trigger':'1236 Generator CB opened', 'new-state':'idle'},
                    ]),
                'rampdown': State('rampdown',[
                    { 'trigger':'1236 Generator CB opened', 'new-state':'coolrun'},
                    { 'trigger':'3226 Ignition off', 'new-state':'standstill'},
                    { 'trigger':'1231 Request module on', 'new-state':'targetoperation'},
                    ]),
                'coolrun': State('coolrun',[
                    { 'trigger':'1234 Operation off', 'new-state':'runout'},
                    { 'trigger':'3226 Ignition off', 'new-state':'standstill'}
                    ]),
                'runout': State('runout',[
                    { 'trigger':'3226 Ignition off', 'new-state':'standstill'},
                    { 'trigger':'1231 Request module on', 'new-state': 'startpreparation'},            
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
            if nsvec[self.name].currentstate == 'startpreparation':
                results['stops'][-1]['endtime'] = nsvec[self.name].currentstate_start
                results['stops'][-1]['count_alarms'] = len(results['stops'][-1]['alarms'])
                results['stops'][-1]['count_warnings'] = len(results['stops'][-1]['warnings'])
                # apends a new record to the Starts list.
                results['starts'].append({
                    'run2':False,
                    'no':results['starts_counter'],
                    'success': False,
                    'mode':nsvec['service_selector'],
                    'starttime': nsvec[self.name].currentstate_start,
                    'endtime': pd.Timestamp(0),
                    'cumstarttime': pd.Timedelta(0),
                    'startpreparation':np.nan,
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
                    'ramprate': np.nan
                })
                results['starts_counter'] += 1 # index for next start
                nsvec['startno'] = results['starts_counter']
                nsvec['in_operation'] = 'on'

            # do while 'on' in all states
            elif nsvec['in_operation'] == 'on': 
                results['starts'][-1]['mode'] = nsvec['service_selector']
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

                    # assess if a start is successful:
                    # ... if it reaches 'targetoperation'
                    if 'targetoperation' in results['starts'][-1]:
                        # other criterias may apply.
                        # ... and it stayed longer than 'successtime'
                        results['starts'][-1]['success'] = (results['starts'][-1]['targetoperation'] > self._successtime)

                    # count alarms an warnings
                    results['starts'][-1]['count_alarms'] = len(results['starts'][-1]['alarms'])
                    results['starts'][-1]['count_warnings'] = len(results['starts'][-1]['warnings'])

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

            _logline= {
                'laststate': nsvec[self.name].laststate,
                'laststate_start': nsvec[self.name].laststate_start,
                'msg': nsvec['msg']['name'] + ' ' + nsvec['msg']['message'],
                'currenstate': nsvec[self.name].currentstate,
                'currentstate_start': nsvec[self.name].currentstate_start,
                'starts': len(results['starts']),
                'Successful_starts': len([s for s in results['starts'] if s['success']]),
                'operation': nsvec['in_operation'],
                'mode': nsvec['service_selector'],
            }
            results['runlog'].append(_logline)

####################################
#TODO: move filetrfsm into the statedefinitions
class filterFSM:
    run2filter_content = ['no','success','mode','startpreparation','starter','speedup','idle','synchronize','loadramp','cumstarttime','targetload','ramprate','targetoperation','rampdown','coolrun','runout','count_alarms', 'count_warnings']
    vertical_lines_times = ['startpreparation','starter','speedup','idle','synchronize','loadramp','targetoperation','rampdown','coolrun','runout']

class FSMOperator:
    def __init__(self, e, p_from = None, p_to=None, skip_days=None, frompickle='NOTIMPLEMENTED'):
        self._e = e
        self.load_messages(e, p_from, p_to, skip_days)
        self.message_queue = []
        self.extra_messages = []

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
            'in_operation': 'off',
            'service_selector':  self.serviceSelectorHandler.initial_state,
            'oil_pump': self.oilpumpHandler._initial_state,
            'msg': 'none',
            'startno': 0
        }
        self.init_results()

        self.pfn = self._e._fname + '_statemachine.pkl'
        self.hdfn = self._e._fname + '_statemachine.hdf'

    def init_results(self):
        self.results = {
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
            'serviceselectortiming':[],
            'oilpumptiming':[],
            'stops_counter':0,
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
        # if os.path.exists(self.hdfn):
        #     dlogdetail = pd.read_hdf(self.hdfn,"runlogdetail")
        #     self.results['runlogdetail'] = list(dlogdetail[0])

    def store(self):
        self.unstore()
        # runlogdetail = self.results['runlogdetail']
        # del self.results['runlogdetail']
        # dlogdetail = pd.DataFrame(runlogdetail)
        # dlogdetail.to_hdf(self.hdfn, 'runlogdetail', complevel=6)
        with open(self.pfn, 'wb') as handle:
            pickle.dump(self.results, handle, protocol=5)
        #self.results['runlogdetail'] = runlogdetail

    def unstore(self):
        if os.path.exists(self.pfn):
            os.remove(self.pfn)
        # if os.path.exists(self.hdfn):
        #     os.remove(self.hdfn)

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
            return  f"{'*' if x[self.startstopHandler.name].statechange else '':2}|"+ \
                    f"{x['startno']:04}| " + \
                    f"LST {x[self.startstopHandler.name].laststate_start.strftime('%d.%m %H:%M:%S')} " + \
                    f"LS  {x[self.startstopHandler.name].laststate:18}| " + \
                    f"CSS {x[self.startstopHandler.name].currentstate_start.strftime('%d.%m %H:%M:%S')} " + \
                    f"CS  {x[self.startstopHandler.name].currentstate:18}| " + \
                    f"{x['in_operation']:4}| " + \
                    f"{x['service_selector']:6}| " + \
                    f"{x['msg']['severity']} {pd.to_datetime(int(x['msg']['timestamp'])*1e6).strftime('%d.%m.%Y %H:%M:%S')} {x['msg']['name']} {x['msg']['message']}"
        ts_start = startversuch['starttime'].timestamp() * 1e3
        ts_end = startversuch['endtime'].timestamp() * 1e3
        if statechanges_only:
            #log = [x for x in self.results['runlogdetail'] if x.statechange]
            log = [x for x in self.results['runlogdetail'] if x[self.startstopHandler.name].statechange]
        else:
            log = [x for x in self.results['runlogdetail']]
        log = [makestr(x) for x in log if ((x['msg']['timestamp'] >= ts_start) and (x['msg']['timestamp'] <= ts_end))]
        return log

#################################################################################################################
### die Finite State Operating Runs:

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
        max_timestamp = max([ts['timestamp'] for ts in messages_queue])
        for m in emc:
            if m['timestamp'] < max_timestamp:
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
        self.message_queue = [m for i,m in self._messages.iterrows()]

        vecstore = copy.deepcopy(self.nsvec) # store statevector
        if not silent:
            pbar = tqdm(total=len(self.message_queue), ncols=80, mininterval=1, unit=' messages', desc="FSM0", file=sys.stdout)
        for msg in self.message_queue:
            self.nsvec['msg'] = msg
            self.nsvec = self.startstopHandler.call_trigger_states(self.nsvec)
            self.nsvec = self.serviceSelectorHandler.call_trigger_states(self.nsvec)
            self.nsvec = self.oilpumpHandler.call_trigger_states(self.nsvec)
            if not silent:
                pbar.update()
        self.nsvec = vecstore # restore statevector

        # merge original and extra messages, sortbmessaged,  make sure timing is monoton upwards
        self.debug_msg('extra messages before merge:',self.extra_messages, debug=debug)
        self.message_queue, self.extra_messages = self.merge_extra_messages(self.message_queue, self.extra_messages)
        self.debug_msg('extra messages after merge:',self.extra_messages, debug=debug)
        if not silent:
            pbar.close()        

    def run1(self, silent=False ,debug= False, successtime=300):
        """Statemachine Operator Run 1 - using also extra messages injected in run0 

        Args:
            silent (bool, optional): do not show progress bar if True. Defaults to False.
            successtime (int, optional): How long an operation cycle needs to stay in state targetoperation to be assessed successful. Defaults to 300.
        """        
        self.startstopHandler.set_successtime(successtime)

        if not silent:
            pbar = tqdm(total=len(self.message_queue), ncols=80, mininterval=1, unit=' messages', desc="FSM1", file=sys.stdout)

        for msg in self.message_queue:
            # inject new message into StatesVector
            self.nsvec['msg'] = msg
            self.nsvec = self.startstopHandler.call_trigger_states(self.nsvec)
            self.nsvec = self.serviceSelectorHandler.call_trigger_states(self.nsvec)
            self.nsvec = self.oilpumpHandler.call_trigger_states(self.nsvec)
            
            # log Statesvector details
            self.results['runlogdetail'].append(copy.deepcopy(self.nsvec))

            # collect & harvest data:
            self.startstopHandler.collect_data(self.nsvec, self.results)
            self.serviceSelectorHandler.collect_data(self.nsvec, self.results)
            self.oilpumpHandler.collect_data(self.nsvec, self.results)

            if not silent:
                pbar.update()
        if not silent:
            pbar.close()        

    def run2(self, silent=False, debug=False):
        """Statemachine Operator Run 2 - uses timings collected in previos runs to download 'Power_PowerAct'
        in 1 sec. Intervals around loadramp phase. Use the curve to collect additional and more accurate data
        on loadramps.

        silent (Boolean): whether a progress bar is visible or not. 
        """
        ratedload = self._e['Power_PowerNominal']
        if not silent:
            pbar = tqdm(total=len(self.results['starts']), ncols=80, mininterval=2, unit=' starts', desc="FSM2", file=sys.stdout)

        for i, startversuch in enumerate(self.results['starts']):
            sno = startversuch['no']
            #if startversuch['run2'] == False:
            if not self.results['starts'][sno]['run2']:
                self.results['starts'][sno]['run2'] = True
                #startversuch['run2'] = True
                try:
                    data, xmax, ymax, duration, ramprate = dmyplant2.loadramp_edge_detect(self, startversuch, debug=debug, periodfactor=3, helplinefactor=0.8)
                    if not data.empty:
                        # update timings accordingly
                        self.results['starts'][sno]['startstoptiming']['loadramp'][0]['end'] = xmax
                        if 'targetoperation' in self.results['starts'][sno]['startstoptiming']:
                            self.results['starts'][sno]['startstoptiming']['targetoperation'][0]['start'] = xmax
                        self.results['starts'][sno]['targetload'] = ymax
                        self.results['starts'][sno]['ramprate'] = ramprate / ratedload * 100.0
                        phases = list(self.results['starts'][sno]['startstoptiming'].keys())
                        self.startstopHandler._harvest_timings(self.results['starts'][sno], phases, self.results)
                    #print(f"Start: {startversuch['no']:3d} xmax: {xmax}, ymax: {ymax:6.0f}, duration: {duration:5.1f}, ramprate: {ramprate / ratedload * 100.0:4.2f} %/s")
                except Exception as err:
                    print(err)
            if not silent:
                pbar.update()
                        
        if not silent:
            pbar.close() 
