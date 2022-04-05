from enum import Enum
################################
# translation from classlib.vb #
# to Python                    #
# 4.4.2022                     #
# Dieter Chvatal               #
################################

class MSG:
    self.MsgNo #int
    self.MsgText: str #String
    self.MsgDate #Date ?
    self.MsgType #Char
    self.MsgResponibility #Char
    self.Status #String ' nur Temporär erforderlich!!!

class PrimaryMSG:
    self.MSGCombination #String
    self.MSGNo #Integers

class EngineAction:
    self.Action_Actual #As Engine_Action
    self.Action_From #As Date
    self.Action_To #As Date
    self.Action_Date #As Date
    #self.Action_Trip #As MSG
    self.DemandSelectorSwitch #As DemandSelectorSwitch_States
    self.ServiceSelectorSwitch #As ServiceSelectorSwitch_States
    #self.AV_MAN_Activated_Status #As Available_States
    #self.Trip_List #As List(Of MSG)
    #self.Reset_List #As List(Of MSG)
    self.Primary_Trip #As MSG

    def __init__(triplist,msg, resetlist):
        self.Trip_List = triplist #New List(Of MSG)
        self.Action_Trip = msg #MSG
        self.Reset_List = resetlist #New List(Of MSG)
        self.AV_MAN_Activated_Status = Available_States.Undefined

class ActionDB:
    def __init__():
        self.AV_MAN_Activated_Status = Available_States.Undefined
        self.Trigger_Date = None #Nothing
        self.Trigger_MSGNo = None #Nothing
        self.Trigger_Text = None #Nothing
        self.Trigger_Responsibility = None #Nothing
        self.Trigger_Count = 0

    self.Action_Actual #As Engine_Action
    self.Action_From #As Date
    self.Action_To #As Date
    #self.Trigger_Date #As DateTime
    #self.Trigger_MSGNo #As Integer
    #self.Trigger_Text #As String
    #self.Trigger_Responsibility As String
    #self.Trigger_Count #As Int16
    self.DemandSelectorSwitch #As DemandSelectorSwitch_States
    self.ServiceSelectorSwitch #As ServiceSelectorSwitch_States
    self.AV_MAN_Activated_Status #As Available_States
    self.CalcDate #As DateTime

class ServiceSelectorSwitch_States(Enum):
    Undefined = 0
    OFF = 2
    MAN = 4
    AUTO = 6

class DemandSelectorSwitch_States(Enum):
    Undefined = 0
    OFF = 2
    EIN = 4
    REMOTE = 6

class Available_States(Enum):
    Undefined = 0
    Not_Available = 2
    Available = 4
    ForcedOutage = 6
    Troubleshooting = 8
    Maintenance = 10
    Deactivated = 12

class Delay_Check(Enum):
    NoCheck = 0
    AlarmInNextSecond = 1
    Betrieb_NetzOrInsel = 2
    NetzStörung = 3
    RemoteReset = 4

#Region "New fleed values RAM meldungen"
class MSG_Trigger(Enum):
    BWS_AUS = 1225
    BWS_Hand = 1226
    BWS_Auto = 1227

    AWS_Remote = 2985
    AWS_AUS = 2983
    AWS_Ein = 2984

    Bereit_Automatic_Ein = 1229
    Bereit_Automatic_Aus = 1230
    Starter_Ein = 1249
    Anforderung_Ein = 1231
    Anforderung_Aus = 1232
    GS_Ein = 1235
    GS_Aus = 1236
    Quittierung = 1256
    Hourly_Message = 9007
    Synchronisieranforderung = 2139
    ColdStartCPU_1254 = 1254
    WarmStartCPU_1253 = 1253
    OPC_Server_start_9004 = 9004
    Sicherungsfall_Modulinterfaceschrank_1140 = 1140
    IGN_Aus = 3226
    Netzparallelbetrieb = 2122
    Inselbetrieb = 2123
    Idle = 2124
    Netzstoerung = 1175
    Power_reduction_In_isolated_operation = 3506

    Remote_Reset = 3252

    TargetLoadReached = 3691

    Troubleshooting_ON = 9029
    Troubleshooting_OFF = 9030
    Troubleshooting_Automatic_OFF = 9035

    Maintenace_ON = 9031
    Maintenace_OFF = 9032
    Maintenace_Automatic_OFF = 9036

    Deactivate_ON = 9033
    Deactivate_OFF = 9034
    Deactivate_Automatic_OFF = 9037

class Powerreduction(Enum):
    Power_reduction_cylinder_pressure_sensor_disturbance = 1296
    Power_reduction_cylinder_pressure_sensor_failure = 1297
    Power_reduction_knock = 2125
    Power_reduction_by_charge_temperature = 2126
    Power_reduction_misfire = 2127
    Power_reduction_analog = 2128
    Power_reduction_plant_management_system = 2129
    Power_reduction_underfrequency = 2130
    Power_reduction_exhaust = 2132
    Power_reduction_charge_temperature_before_lowpressure_turbocharger = 2143
    Power_reduction_by_turbocharger_speed = 2144
    Power_reduction_by_PBC = 2146
    Power_reduction_LEANOX_deviation = 2147
    Power_reduction_Reserve_1 = 2148
    Power_reduction_Reserve_2 = 2149
    Power_reduction_Reserve_3 = 2150
    Power_reduction_Safetypower_PBC = 2151
    Power_reduction_under_voltage = 2152
    Power_reduction_over_voltage = 2153
    Power_reduction_over_frequency = 2154
    Power_reduction_winding_temperature = 2155
    Reactive_power_reduction_winding_temperature = 2156
    Reactive_power_reduction_polar_wheel_angle = 2157
    Power_reduction_jacket_water_temperature = 2158
    Power_reduction_engine_oil_temperature = 2159
    Power_reduction_heating_water_Return_temperature = 2160
    Power_reduction_pressure_pre_chamber_gas_train = 2161
    Power_reduction_by_transformer_temeprature = 2162
    Power_reduction_by_utility_Operator = 2163
    Power_reduction_by_generator_switch_cabinet_temperature = 2167
    Power_reduction_by_generator_air_inlet_temperatur = 2168
    Power_reduction_by__timing_point_optimizing = 2169
    Power_reduction_by_hierarchic_controller = 2170
    Power_reduction_cylinder_pressure_sensor_disturbanceW = 2698
    Power_reduction_cylinder_pressure_sensor_failureW = 2699
    Power_reduction_by_charge_temperature_before_turbocharger = 2803
    Fast_power_reduction_due_To_auto_ignition = 2846
    Power_reduction_crank_Case_pressure = 2875
    RKS_G2_active_due_To_power_reduction = 2913
    Power_reduction_by_gas_pressure_minimum = 2936
    Power_reduction_CH4_content = 3229
    Power_reduction_gas_pressure = 3230
    Power_reduction_suction_pressure = 3231
    Power_reduction_In_isolated_operation = 3506
    Power_reduction_exhaust_temperature_cylinder_high = 3589