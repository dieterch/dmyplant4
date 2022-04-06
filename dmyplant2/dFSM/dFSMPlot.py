import pandas as pd
import numpy as np
import arrow

import bokeh
from bokeh.models import ColumnDataSource, Label, Text, Span, HoverTool #, Range1d#, LabelSet
from bokeh.plotting import figure, output_file, show

from IPython.display import HTML, display
from dmyplant2 import dbokeh_chart, bokeh_show, add_dbokeh_vlines, add_dbokeh_hlines 
from .dFSM import filterFSM
from .dFSMResults import disp_alarms, disp_warnings #, detect_edge_right, detect_edge_left, loadramp_edge_detect
import warnings
warnings.simplefilter(action='ignore', category=UserWarning)

def FSM_splot(fsm,startversuch, data, dset, figsize=(16,10)):
    von_dt=pd.to_datetime(startversuch['starttime']); von=int(von_dt.timestamp())
    bis_dt=pd.to_datetime(startversuch['endtime']); bis=int(bis_dt.timestamp())

    ftitle = f"{fsm._e} ----- Start {startversuch['no']} {startversuch['mode']} | {'SUCCESS' if startversuch['success'] else 'FAILED'} | {startversuch['starttime'].round('S')}"
    print(f"von: {von_dt.strftime('%d.%m.%Y %H:%M:%S')} bis: {bis_dt.strftime('%d.%m.%Y %H:%M:%S')}")

    fig = dbokeh_chart(data, dset, title=ftitle, grid=False, figsize=figsize, style='line', line_width=0)
    return fig

def FSM_add_Notations(fig,fsm,startversuch):

    # annotations: 1 vertical line for every state change ... descriptive text.
    #sv_lines = [v for v in startversuch[filterFSM.vertical_lines_times] if v==v]
    lines = {
        k:{
            'time':startversuch['timing'][k][-1]['start'],
            'duration':startversuch[k]
        } 
        for k in filterFSM.vertical_lines_times 
        if k in startversuch['timing'] and (startversuch['timing'][k][-1]['start'] == startversuch['timing'][k][-1]['start'])}
    lines['End'] = {
        'time':startversuch['endtime'],
        'duration': 0.0
        }
    for k in lines.keys():
        fig.add_layout(Span(location=lines[k]['time'],dimension='height', line_color='red', line_dash='solid', line_alpha=0.4))
        fig.add_layout(Label(x=lines[k]['time'], y=2, x_units='data',y_units='screen',angle=np.pi / 2,
                            text_font_size='8pt', text_color ='darkslategray',text_alpha=0.8,render_mode='css',
                            text=f"{lines[k]['time'].strftime('%Y-%m-%d %H:%M:%S')} | {k.upper()} | {lines[k]['duration']:0.2f}"))

    # Nominal Power as horizontal line
    fig.add_layout(Span(location=fsm._e['Power_PowerNominal'],dimension='width',x_range_name='default', y_range_name='0',line_color='green', line_dash='solid', line_alpha=0.4))

    # max load if available
    if 'maxload' in startversuch:
        if startversuch['maxload'] == startversuch['maxload']:
            fig.add_layout(Span(location=startversuch['maxload'],dimension='width',x_range_name='default', y_range_name='0',line_color='red', line_dash='solid', line_alpha=0.4)) 

    # Nominal Speed as horizontal Line
    fig.add_layout(Span(location=fsm._e.Speed_nominal,dimension='width',x_range_name='default', y_range_name='1',line_color='blue', line_dash='solid', line_alpha=0.4)) 

    # visualize calcualted loadramp
    if 'loadramp' in startversuch['timing']:
        x0 = startversuch['timing']['loadramp'][-1]['start']; y0 = 0.0
        default_ramp_rate = fsm._e['rP_Ramp_Set'] if fsm._e['rP_Ramp_Set'] != None else 0.625
        default_ramp_duration = 100.0 / default_ramp_rate
        x1 = x0 + pd.Timedelta(default_ramp_duration, unit='sec'); y1 = fsm._e['Power_PowerNominal']
        ramp = fig.line(x=[x0,x1],y=[y0,y1], y_range_name='0', line_color='green', line_dash='solid', line_alpha=0.4, line_width=1)                            
    return fig

def FSM_add_Alarms(fig,fsm,startversuch):
    al_lines = disp_alarms(startversuch)
    add_dbokeh_vlines(al_lines,fig,line_color='red', line_dash='dashed', line_alpha=0.4, line_width=2)
    return fig

def FSM_add_Warnings(fig,fsm,startversuch):
    w_lines = disp_warnings(startversuch)
    add_dbokeh_vlines(w_lines,fig,line_color='#d5ac13', line_dash='dashed', line_alpha=0.4, line_width=2)
    return fig

# def FSMPlot_Start(fsm,startversuch, data, vset, dset, figsize=(16,10)):
#     von_dt=pd.to_datetime(startversuch['starttime']); von=int(von_dt.timestamp())
#     bis_dt=pd.to_datetime(startversuch['endtime']); bis=int(bis_dt.timestamp())

#     ftitle = f"{fsm._e} ----- Start {startversuch['no']} {startversuch['mode']} | {'SUCCESS' if startversuch['success'] else 'FAILED'} | {startversuch['starttime'].round('S')}"
#     print(f"von: {von_dt.strftime('%d.%m.%Y %H:%M:%S')} bis: {bis_dt.strftime('%d.%m.%Y %H:%M:%S')}")

#     al_lines = disp_alarms(startversuch)
#     w_lines = disp_warnings(startversuch)
    
#     fig = dbokeh_chart(data, dset, title=ftitle, grid=False, figsize=figsize, style='line', line_width=0)

#     add_dbokeh_vlines(al_lines,fig,line_color='red', line_dash='dashed', line_alpha=0.4, line_width=2)
#     add_dbokeh_vlines(w_lines,fig,line_color='#d5ac13', line_dash='dashed', line_alpha=0.4, line_width=2)
#     add_dbokeh_vlines(states_lines(startversuch),fig,line_color='red', line_dash='solid', line_alpha=0.4)
                            
#     fig.add_layout(Span(location=fsm._e['Power_PowerNominal'],dimension='width',x_range_name='default', y_range_name='0',line_color='red', line_dash='solid', line_alpha=0.4)) 
#     if 'maxload' in startversuch:
#         if startversuch['maxload'] == startversuch['maxload']:
#             fig.add_layout(Span(location=startversuch['maxload'],dimension='width',x_range_name='default', y_range_name='0',line_color='red', line_dash='solid', line_alpha=0.4)) 
#     fig.add_layout(Span(location=fsm._e.Speed_nominal,dimension='width',x_range_name='default', y_range_name='1',line_color='blue', line_dash='solid', line_alpha=0.4)) 

#     return fig

## plotting
def states_lines(startversuch):
    """pd.Timestamp positions of detected state Changes
    including start-event

    Args:
        startversuch (fsm._starts[x]): the selected starts record.

    Returns:
        list: list of pd.Timestamps, ready to be passed into add_bokeh_vlinees
    """
    sv_lines = [v for v in startversuch[filterFSM.vertical_lines_times] if v==v]
    start = startversuch['starttime']; lines=list(np.cumsum(sv_lines)); 
    return [start + pd.Timedelta(value=v,unit='sec') for v in [0] + lines]