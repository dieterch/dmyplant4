figures = {
    'starts_overview': [
    {'col':['cumstarttime'],'ylim':(-400,900), 'color':'darkblue'},
    {'col':['synchronize'],'ylim':(-20,300)},
    {'col':['startpreparation'],'ylim':(-600,900)},
    {'col':['hochlauf'],'ylim':(-100,200), 'color':'orange'},
    {'col':['loadramp'],'ylim':(-150,900), 'color':'red'},
    {'col':['ramprate'],'ylim':(-3,3)},
    {'col':['no'],'ylim':(-100,10000),'color':'black'},
    {'col':['targetload'],'ylim':(500,5000) }
    ],
    'actors' : [
    {'col':['Power_PowerAct'], 'ylim':(0,5000), 'color':'red', 'unit':'kW'},
    {'col':['Various_Values_SpeedAct'],'ylim': [0, 2500], 'color':'blue', 'unit':'rpm'},
    {'col':['Ignition_ITPAvg'],'ylim': [0, 30], 'color':'magenta', 'unit':'°KW'},
    {'col':['TecJet_Lambda1'],'ylim': [0, 3], 'color':'orange', 'unit':'-'},
    {'col':['Various_Values_PosThrottle','Various_Values_PosTurboBypass'],'ylim': [-10, 110], 'color':['hotpink','brown'], 'unit':'%'},
    #{'col':['Hyd_PressCrankCase'],'ylim': [-100, 100], 'color':'orange', 'unit':'mbar'},
    #{'col':['Hyd_PressOilDif'],'ylim': [0, 2], 'color':'black', 'unit': 'bar'},
    #{'col':['Hyd_PressOil'],'ylim': [0, 10], 'color':'brown', 'unit': 'bar'},
    #{'col':['Hyd_TempOil','Hyd_TempCoolWat','Hyd_TempWatRetCoolOut'],'ylim': [0, 110], 'color':['#2171b5','orangered','hotpink'], 'unit':'°C'},
    ],
    'lubrication' : [
    {'col':['Power_PowerAct'], 'ylim':(0,5000), 'color':'red', 'unit':'kW'},
    {'col':['Various_Values_SpeedAct'],'ylim': [0, 2500], 'color':'blue', 'unit':'rpm'},
    #{'col':['Hyd_PressCrankCase'],'ylim': [-100, 100], 'color':'orange', 'unit':'mbar'},
    {'col':['Hyd_PressOilDif'],'ylim': [0, 2], 'color':'black', 'unit': 'bar'},
    {'col':['Hyd_PressOil'],'ylim': [0, 10], 'color':'brown', 'unit': 'bar'},
    {'col':['Hyd_TempOil','Hyd_TempCoolWat','Hyd_TempWatRetCoolOut'],'ylim': [0, 110], 'color':['#2171b5','orangered','hotpink'], 'unit':'°C'},
    ],
}
