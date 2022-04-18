import pandas as pd
import numpy as np
from collections import namedtuple
from IPython.display import HTML, display
from .dFSM import filterFSM

## Resultate aus einem FSM Lauf ermitteln.
def disp_result(startversuch):
    summary = pd.DataFrame(startversuch[startstopFSM.run2filter_content]).T
    #summary = pd.DataFrame.from_dict({k:v for k,v in dict(startversuch[['index'] + fsm.filters['run2filter_times']]).items() if v == v}, orient='index').T.round(2)
    #summary = pd.DataFrame(startversuch[fsm.filters['run2filter_times']], dtype=np.float64).fillna(0).round(2).T
    display(HTML(summary.to_html(escape=False, index=False)))
    #display(HTML('<h3>'+ summary.to_html(escape=False, index=False) + '</h3>'))

def disp_alarms(startversuch):
    ald = []; alt = []
    for al in startversuch['alarms']:
            ald.append({
                    'state':al['state'],'severity':al['msg']['severity'],'Number':al['msg']['name'],
                    'date':pd.to_datetime(int(al['msg']['timestamp'])*1e6).strftime('%d.%m.%Y %H:%M:%S'),
                    'message':al['msg']['message']
            })
            alt.append(pd.to_datetime(int(al['msg']['timestamp'])*1e6))
    aldf = pd.DataFrame(ald)
    if not aldf.empty:
        display(HTML(aldf.to_html(escape=False, index=False)))
        #display(HTML('<h3>'+ aldf.to_html(escape=False, index=False) + '</h3>'))
    return alt

def disp_warnings(startversuch):
    wad = []; wat = []
    for wd in startversuch['warnings']:
            wad.append({
                    'state':wd['state'],'severity':wd['msg']['severity'],'Number':wd['msg']['name'],
                    'date':pd.to_datetime(int(wd['msg']['timestamp'])*1e6).strftime('%d.%m.%Y %H:%M:%S'),
                    'message':wd['msg']['message']
            })
            wat.append(pd.to_datetime(int(wd['msg']['timestamp'])*1e6))
    wdf = pd.DataFrame(wad)
    if not wdf.empty:
        display(HTML(wdf.to_html(escape=False, index=False)))
        #display(HTML('<h3>'+ wdf.to_html(escape=False, index=False) + '</h3>'))
    return wat 


def _pareto(mm):
    unique_res = set([msg['name'] for msg in mm])
    res = [{ 'anz': len([msg for msg in mm if msg['name'] == m]),
                'severity': mm[0]['severity'],
                'number':m,
                'msg':f"{str([msg['message'] for msg in mm if msg['name'] == m][0]):>}"
            } for m in unique_res]
    return sorted(res, key=lambda x:x['anz'], reverse=True)        

def _states_pareto(fsm, severity, states = []):
    rmessages = []
    if type(states) == str:
        states = [states]
    for state in states:
        rmessages += [msg for msg in fsm.states[state]._messages if msg['severity'] == severity]
    return _pareto(rmessages)

def alarms_pareto(fsm, states):
    return pd.DataFrame(_states_pareto(fsm, 800, states))

def warnings_pareto(fsm, states):
    return pd.DataFrame(_states_pareto(fsm, 700, states))

def summary(fsm):
    display(HTML(
        f"""
        <h2>{str(fsm._e)}</h2>
        <br>
        <table>
            <thead>
                <tr>
                    <td></td>
                    <td>From</td>
                    <td>To</td>
                    <td>Days</td>
                </tr>
            </thead>
            <tr>
                <td>Interval</td>
                <td>{fsm.first_message:%d.%m.%Y}</td>
                <td>{fsm.last_message:%d.%m.%Y}</td>
                <td>{fsm.period.days:5}</td>
        </td>
            </tr>
        </table>
        """))
    nsummary = []
    res = fsm.result
    for mode in ['undefined','OFF','MAN', 'AUTO']:
        lstarts = res[res['mode'] == mode].shape[0]
        successful_starts = res[((res.success) & (res['mode'] == mode))].shape[0]
        nsummary.append([lstarts, successful_starts,(successful_starts / lstarts) * 100.0 if lstarts != 0 else 0.0])
    nsummary.append([res.shape[0],res[res.success].shape[0],(res[res.success].shape[0] / res.shape[0]) * 100.0])
    display(HTML(pd.DataFrame(nsummary, index=['undefined','OFF','MAN', 'AUTO','ALL'],columns=['Starts','successful','%'], dtype=np.int64).to_html(escape=False)))

def summary_out(fsm):
    fsum = f"""
        <table>
            <thead>
                <tr>
                    <td></td>
                    <td>From</td>
                    <td>To</td>
                    <td>Days</td>
                </tr>
            </thead>
            <tr>
                <td>{fsm._e['Engine ID']}</td>
                <td>{fsm.first_message:%d.%m.%Y}</td>
                <td>{fsm.last_message:%d.%m.%Y}</td>
                <td>{fsm.period.days:5}</td>
        </td>
            </tr>
        </table>
        <br>
        """
    nsummary = []
    res = fsm.result
    for mode in ['undefined','OFF','MAN', 'AUTO']:
        lstarts = res[res['mode'] == mode].shape[0]
        successful_starts = res[((res.success) & (res['mode'] == mode))].shape[0]
        nsummary.append([lstarts, successful_starts,(successful_starts / lstarts) * 100.0 if lstarts != 0 else 0.0])
    nsummary.append([res.shape[0],res[res.success].shape[0],(res[res.success].shape[0] / res.shape[0]) * 100.0])
    display(HTML(fsum + pd.DataFrame(nsummary, index=['undefined','OFF','MAN', 'AUTO','ALL'],columns=['Starts','successful','%'], dtype=np.int64).to_html(escape=False)))

