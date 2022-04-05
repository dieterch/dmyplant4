#Imports System.Text
#Imports System.ComponentModel
from .VBClassLib import (
    MSG, PrimaryMSG, 
    EngineAction, ActionDB, 
    ServiceSelectorSwitch_States, DemandSelectorSwitch_States, 
    Available_States, Delay_Check,
    MSG_Trigger)

class ErrorInClass(Exception):
    pass

class Calc_Finished(Exception):
    pass

class ClassMainState:
    #Private Statlist As List(Of ActionDB)
    self.__PrimaryMSG = None #Nothing
    # ?? Private WithEvents BGWCalcState As BackgroundWorker
    #__AMM_List #As List(Of MSG)

    def __init__(list_of_message, list_of_primary_messages):
        self.__AMM_List = list_of_message #New List(Of MSG)
        self.__PrimaryMSGList = list_of_primary_messages # New List(Of PrimaryMSG)
        # self.BGWCalcState = New BackgroundWorker With {.WorkerReportsProgress = True}
        self.__Silence = True
        self.__Busy = False
        self.__GapMaxLength = 5400  # in Sekunden, 5400 = 1,5h
        self.__TargetloadreachTime = 300
        self.__Alarm_Delay_Time = 1000

        self.__E_Action = None
        self.__EngineMSG_List = None
        self.__Status_List = None


    def Finalize():
        MyBase.Finalize()

    # ?? Public Event ErrorInClass(ByVal Message As String)
    # ?? Public Event PrimaryMessageNotfound(ByVal Message As String)
    # ?? Public Event Calc_Finisched(ByVal Statlist As List(Of ActionDB))
    # ?? <CategoryAttribute("Input"), Description("Engine Message List. As soon as the List is written, calculation will be started")>
    
    @property
    def EngineMSG_List(self):
        return self.__EngineMSG_List

    @EngineMSG_List.setter
    def EngineMSG_List(self, value):    
            try:
                self.__Busy = True
                #AMM_List.Clear()
                self.__AMM_List = value
                self._Status_List = [] #New List(Of ActionDB)
                if (self.__AMM_List is None) or (len(self.__AMM_List) == 0):
                    raise ErrorInClass("No Messages in EngineMSG_List")
                    self.__Busy = False
                    raise Calc_Finished(self.__Status_List)
                elif (self.PrimaryMSGList is None) or (len(self.__PrimaryMSGList) == 0):
                    raise ErrorInClass("PrimaryMSGList is empty")
                    self.__Busy = False
                    raise Calc_Finished(self.__Status_List)
                else:
                    self.__E_Action = EngineAction()
                    # ?? do this in a Thread ?? BGWCalcState.RunWorkerAsync()
            except Exception as err:
                raise ErrorInClass("Error 33 " + str(err))
                # ?? still dont know how to deal with that ?? BGWCalcState.ReportProgress(-1, "Error 33 " & ex.Message)

    #<CategoryAttribute("Input"), Description("Primary Trip List")>
    @property
    def PrimaryMSGList(self):
        return self.__PrimaryMSGList #As List(Of PrimaryMSG)

    #<CategoryAttribute("Input"), Description("waiting time in ms if an additional Alarm is happen. Default 1500ms")>
    @property
    def Alarm_Delay_Time(self):
        return self.__Alarm_Delay_Time  # As Integer
    
    #<CategoryAttribute("Input"), Description("If Silence is TRUE (DEFAULT), no message is generated if primary Message is not found")>
    @property
    def Silence(self):
        return self.__Silence  #As Boolean

    #<CategoryAttribute("Input"), Description("Max time difference in seconds between two MSG. Default is 5400 sec. (1,5h)")>
    @property
    def GapMaxLength(self):
        return self.__GapMaxLength # As Integer

    #<CategoryAttribute("Input"), Description("After this time ramp up (Island or Paralell) will be end automatic. If the Message 'Tagred Load Reached' is earlier ramp up is finished with message. Default is 300 sec. (5 min.)")>
    @property
    def TargetloadreachTime(self):
        return self.__TargetloadreachTime #As Integer

    #<CategoryAttribute("Output"), ReadOnlyAttribute(True), Description("TRUE if calculation is running")>
    @property
    def Busy(self):
        return self.__Busy # As Boolean

    # 4.4.2022 bis hierher nach python übersetzt.
#     <CategoryAttribute("Output"), ReadOnlyAttribute(True), Description("Engine Status List")>
#     Public ReadOnly Property Status_List As List(Of ActionDB)
#     '    Get
#     '        Return Statlist
#     '    End Get
#     'End Property

# #End Region

# #Region "Private Functions"

#     Private Function Store_Action(ByVal NewAction As Engine_Action, ByVal row As Integer, ByVal Delayckeck As Delay_Ckeck) As Boolean
#         Try
#             Select Case Delayckeck

#                 Case Delay_Ckeck.NetzStörung
#                     Dim NetzStörungFound As Boolean = False
#                     Dim NetzstörungMSG As New MSG
#                     Dim StartMSG As MSG = AMM_List(row)
#                     Dim _LastMSG_Date As DateTime = AMM_List(row).MsgDate
#                     Dim OldAction As Engine_Action = E_Action.Action_Actual
#                     Try
#                         For i = row + 1 To AMM_List.Count - 1
#                             Dim ActualMSG_Date As DateTime = AMM_List(i).MsgDate
#                             If (ActualMSG_Date - _LastMSG_Date).TotalMilliseconds < _Alarm_Delay_Time Then
#                                 If AMM_List(i).MsgType = "A" Then
#                                     E_Action.Trip_List.Add(New MSG With {
#                                     .MsgNo = AMM_List(i).MsgNo,
#                                     .MsgDate = AMM_List(i).MsgDate,
#                                     .MsgText = AMM_List(i).MsgText,
#                                     .MsgType = AMM_List(i).MsgType,
#                                     .MsgResponibility = "M"
#                                 })
#                                 ElseIf AMM_List(i).MsgNo = MSG_Trigger.Netzstoerung Then
#                                     NetzStörungFound = True
#                                     NetzstörungMSG = AMM_List(i)
#                                 End If
#                             Else
#                                 Exit For
#                             End If
#                         Next

#                         If NetzStörungFound AndAlso E_Action.Trip_List.Count > 0 Then
#                             'A_Action.Action_To = Message_Time
#                             E_Action.Action_To = E_Action.Trip_List(0).MsgDate 'AMM_List(row).MsgDate
#                             Store_Action_Line(Engine_Action.Mains_Failure)
#                             E_Action.Action_To = E_Action.Trip_List(0).MsgDate
#                             Store_Action_Line(Engine_Action.Forced_Outage)
#                         ElseIf NetzStörungFound Then ' AndAlso (E_Action.Trip_List(0).MsgDate - NetzstörungMSG.MsgDate).TotalMilliseconds < _Alarm_Delay_Time Then
#                             E_Action.Action_To = AMM_List(row).MsgDate
#                             Store_Action_Line(Engine_Action.Mains_Failure)
#                         ElseIf E_Action.Trip_List.Count > 0 Then
#                             E_Action.Action_To = E_Action.Trip_List(0).MsgDate 'AMM_List(row).MsgDate
#                             Store_Action_Line(Engine_Action.Forced_Outage)
#                         Else
#                             If NewAction <> OldAction And (E_Action.Action_To - E_Action.Action_From).Milliseconds > 0 Then
#                                 If E_Action.Action_From < New DateTime(1990, 1, 1) Then
#                                     E_Action.Action_From = AMM_List(row).MsgDate
#                                 End If
#                                 Store_Action_Line(NewAction)
#                             End If
#                         End If
#                     Catch ex As Exception
#                         BGWCalcState.ReportProgress(-1, "Store_Action " & ex.Message + " in row " + row.ToString + " Date: " + AMM_List(row).MsgDate.ToString)
#                     End Try

#                 Case Delay_Ckeck.RemoteReset
#                     Dim _LastMSG_Date As DateTime = AMM_List(row).MsgDate
#                     For i = row + 1 To AMM_List.Count - 1
#                         Dim ActualMSG_Date As DateTime = AMM_List(i).MsgDate
#                         If (ActualMSG_Date - _LastMSG_Date).TotalMilliseconds < 500 Then
#                             If AMM_List(i).MsgNo = MSG_Trigger.Remote_Reset Then
#                                 E_Action.Reset_List.Add(New MSG With {
#                                 .MsgNo = AMM_List(i).MsgNo,
#                                 .MsgDate = AMM_List(i).MsgDate,
#                                 .MsgText = AMM_List(i).MsgText,
#                                 .MsgType = AMM_List(i).MsgType,
#                                 .MsgResponibility = "M"
#                             })
#                             End If
#                         Else
#                             Exit For
#                         End If
#                     Next
#                 Case Delay_Ckeck.Betrieb_NetzOrInsel
#                     E_Action.Action_To = AMM_List(row).MsgDate
#                     Dim Found As Boolean = False
#                     For i = _Status_List.Count - 1 To 0 Step -1
#                         Select Case _Status_List(i).Action_Actual
#                             Case Engine_Action.Mains_Parallel_Operation
#                                 Store_Action_Line(Engine_Action.Mains_Parallel_Operation)
#                                 Found = True
#                                 Exit For
#                             Case Engine_Action.RampUp_Mains_Parallel_Operation
#                                 Store_Action_Line(Engine_Action.RampUp_Mains_Parallel_Operation)
#                                 Found = True
#                                 Exit For
#                             Case Engine_Action.Island_Operation
#                                 Store_Action_Line(Engine_Action.Island_Operation)
#                                 Found = True
#                                 Exit For
#                             Case Engine_Action.RampUp_Island_Operation
#                                 Store_Action_Line(Engine_Action.RampUp_Island_Operation)
#                                 Found = True
#                                 Exit For
#                         End Select
#                     Next
#                     If Not Found Then
#                         Store_Action_Line(NewAction)
#                     End If
#                 Case Delay_Ckeck.NoCheck
#                     If E_Action.Action_From < New DateTime(1990, 1, 1) Then
#                         E_Action.Action_From = AMM_List(row).MsgDate
#                     End If
#                     Store_Action_Line(NewAction)
#             End Select

#             Return True
#         Catch ex As Exception
#             BGWCalcState.ReportProgress(-1, "Store_Action " & ex.Message + " in row " + row.ToString + " Date: " + AMM_List(row).MsgDate.ToString)
#             Return False
#         End Try
#     End Function

#     Private Sub Store_Action_Line(ByVal NewAction As Engine_Action)
#         Try
#             If E_Action.Action_To < New DateTime(1990, 1, 1) Then
#                 E_Action.Action_To = E_Action.Action_To
#             End If
#             If NewAction = Engine_Action.Troubleshooting Then
#                 E_Action = E_Action
#             End If
#             If E_Action.Action_Actual = Engine_Action.Forced_Outage OrElse (E_Action.Action_Trip Is Nothing AndAlso E_Action.Action_Actual = Engine_Action.Troubleshooting) Then
#                 If E_Action.Trip_List.Count > 0 Then
#                     E_Action.Action_Trip = Select_Primary_MSG(E_Action.Trip_List)
#                     For i = _Status_List.Count - 1 To 0 Step -1
#                         If _Status_List(i).Action_Actual = Engine_Action.Forced_Outage OrElse _Status_List(i).Action_Actual = Engine_Action.Troubleshooting Then
#                             _Status_List(i).Trigger_Date = E_Action.Action_Trip.MsgDate
#                             _Status_List(i).Trigger_MSGNo = E_Action.Action_Trip.MsgNo
#                             _Status_List(i).Trigger_Responsibility = E_Action.Action_Trip.MsgResponibility
#                             _Status_List(i).Trigger_Text = E_Action.Action_Trip.MsgText
#                         Else
#                             Exit For
#                         End If
#                     Next
#                 End If
#                 E_Action.Trip_List.Clear()
#             ElseIf E_Action.Action_Actual = Engine_Action.Troubleshooting Then
#                 E_Action.Trip_List.Clear()
#                 E_Action.Reset_List.Clear()
#             Else
#                 If E_Action.Reset_List.Count > 0 Then
#                     E_Action.Action_Trip = E_Action.Reset_List(E_Action.Reset_List.Count - 1)
#                     E_Action.Reset_List.Clear()
#                 End If
#             End If

#             Dim ST As New ActionDB With {
#                         .Action_Actual = E_Action.Action_Actual,
#                         .Action_From = E_Action.Action_From,
#                         .Action_To = E_Action.Action_To,
#                         .ServiceSelectorSwitch = E_Action.ServiceSelectorSwitch,
#                         .DemandSelectorSwitch = E_Action.DemandSelectorSwitch,
#                         .AV_MAN_Activated_Status = E_Action.AV_MAN_Activated_Status,
#                         .CalcDate = DateTime.Now()
#                     }
#             If E_Action.Action_Trip IsNot Nothing Then
#                 ST.Trigger_Date = E_Action.Action_Trip.MsgDate
#                 ST.Trigger_MSGNo = E_Action.Action_Trip.MsgNo
#                 ST.Trigger_Text = E_Action.Action_Trip.MsgText
#                 ST.Trigger_Responsibility = E_Action.Action_Trip.MsgResponibility
#                 If E_Action.Action_Actual = Engine_Action.Troubleshooting Then
#                     ST.Trigger_Count = 0
#                 Else
#                     ST.Trigger_Count = 1
#                 End If
#             Else
#                 ST.Trigger_Date = E_Action.Action_Date
#             End If
#             _Status_List.Add(ST)
#             E_Action.Action_From = E_Action.Action_To
#             E_Action.Action_Actual = NewAction
#             E_Action.Action_To = Nothing
#             E_Action.Action_Date = E_Action.Action_From

#             If E_Action.Action_Actual <> Engine_Action.Troubleshooting AndAlso E_Action.Action_Actual <> Engine_Action.Forced_Outage Then
#                 E_Action.Action_Trip = Nothing
#             End If
#         Catch ex As Exception
#             BGWCalcState.ReportProgress(-1, "Store_Action_Line " & ex.Message)
#         End Try
#     End Sub
#     Private Sub Store_WS_DS()
#         Try
#             Dim ST As New ActionDB With {
#                                     .Action_Actual = E_Action.Action_Actual,
#                                     .Action_From = E_Action.Action_From,
#                                     .Action_To = E_Action.Action_To,
#                                     .ServiceSelectorSwitch = E_Action.ServiceSelectorSwitch,
#                                     .DemandSelectorSwitch = E_Action.DemandSelectorSwitch,
#                                     .AV_MAN_Activated_Status = E_Action.AV_MAN_Activated_Status,
#                                     .Trigger_Date = E_Action.Action_Date,
#                                     .CalcDate = DateTime.Now()
#                                 }
#             If Not E_Action.Action_Trip Is Nothing Then
#                 ST.Trigger_Date = E_Action.Action_Trip.MsgDate
#                 ST.Trigger_MSGNo = E_Action.Action_Trip.MsgNo
#                 ST.Trigger_Text = E_Action.Action_Trip.MsgText
#                 ST.Trigger_Responsibility = E_Action.Action_Trip.MsgResponibility
#                 ST.Trigger_Count = 0
#             End If
#             _Status_List.Add(ST)
#             E_Action.Action_From = E_Action.Action_To
#         Catch ex As Exception
#             BGWCalcState.ReportProgress(-1, "Store_WS_DS " & ex.Message)
#         End Try

#     End Sub
#     Private Sub Store_BWS(ByVal NewBWS As ServiceSelectorSwitch_States, ByVal StatusZeit As DateTime)
#         Try
#             If E_Action.ServiceSelectorSwitch <> NewBWS Then
#                 E_Action.Action_To = StatusZeit
#                 Store_WS_DS()
#                 E_Action.ServiceSelectorSwitch = NewBWS
#             End If
#         Catch ex As Exception
#             BGWCalcState.ReportProgress(-1, "Store_BWS " & ex.Message + " at: " + StatusZeit.ToString)
#         End Try
#     End Sub
#     Private Function Store_AWS(ByVal NewAWS As DemandSelectorSwitch_States, ByVal StatusZeit As DateTime) As Boolean
#         Try
#             If E_Action.DemandSelectorSwitch <> NewAWS Then
#                 E_Action.Action_To = StatusZeit
#                 Store_WS_DS()
#                 E_Action.DemandSelectorSwitch = NewAWS
#             End If
#             Return True
#         Catch ex As Exception
#             BGWCalcState.ReportProgress(-1, "Store_AWS " & ex.Message + " at: " + StatusZeit.ToString)
#             Return False
#         End Try

#     End Function
#     Private Function Store_AV_MAN_Activated_Status(ByVal NewAVSS As Available_States, ByVal StatusZeit As DateTime, ByVal row As Integer, ByVal Ret As Boolean) As Boolean
#         Dim i As Integer
#         Try
#             If E_Action.AV_MAN_Activated_Status <> NewAVSS Then
#                 E_Action.Action_To = StatusZeit
#                 Store_WS_DS()
#                 If Ret Then
#                     For i = _Status_List.Count - 1 To 0 Step -1
#                         If _Status_List(i).AV_MAN_Activated_Status <> NewAVSS AndAlso _Status_List(i).Action_Actual <> Engine_Action.Forced_Outage AndAlso _Status_List(i).Action_Actual <> Engine_Action.Troubleshooting Then
#                             _Status_List(i).AV_MAN_Activated_Status = NewAVSS
#                         Else
#                             Exit For
#                         End If
#                     Next
#                 End If
#                 E_Action.AV_MAN_Activated_Status = NewAVSS
#             End If
#             Return True
#         Catch ex As Exception
#             BGWCalcState.ReportProgress(-1, "Store_AVSS " & ex.Message + " in row " + row.ToString + " Date: " + AMM_List(row).MsgDate.ToString)
#             Return False
#         End Try
#     End Function
#     Public Sub Close()
#         AMM_List = Nothing
#         PrimaryMSGList = Nothing

#         BGWCalcState.CancelAsync()
#         BGWCalcState.Dispose()
#         BGWCalcState = Nothing
#         Finalize()
#     End Sub
#     Private Sub BGWCalcState_DoWork(sender As Object, e As DoWorkEventArgs) Handles BGWCalcState.DoWork
#         CalcState()
#     End Sub
#     Private Sub BGWCalcState_RunWorkerCompleted(sender As Object, e As RunWorkerCompletedEventArgs) Handles BGWCalcState.RunWorkerCompleted
#         Try
#             If Not e.Error Is Nothing Then
#                 BGWCalcState.ReportProgress(-1, "Backgroundworker Fehler " & e.Error.Message)
#             ElseIf _Status_List Is Nothing Then
#                 BGWCalcState.ReportProgress(-1, "keine Ahnung warum das so ist")
#             Else
#                 RaiseEvent Calc_Finisched(_Status_List)
#             End If
#         Catch ex As Exception
#             RaiseEvent ErrorInClass("Error im Backgroundworker: " + ex.Message)
#             'BGWCalcState.ReportProgress(-1, "Error im Backgroundworker: " + ex.Message)
#         Finally
#             AMM_List.Clear()
#             _Bussy = False
#         End Try
#     End Sub

# #Region "Actionauswertungen"
#     Private Sub CalcState()
#         Dim RowCount As Integer = 0
#         Dim LastMsg As MSG

#         LastMsg = AMM_List(0)
#         E_Action.Action_From = LastMsg.MsgDate
#         E_Action.Action_Actual = Engine_Action.Undefinded
#         E_Action.Action_Trip = Nothing

#         Dim MSGNO As Integer
#         Dim MSGDATE As DateTime
#         Dim MSGTEXT As String
#         Dim MSGType As String
#         For Each row As MSG In AMM_List
#             Try
#                 row.Status = E_Action.Action_Actual.ToString
#                 MSGNO = row.MsgNo
#                 MSGDATE = row.MsgDate
#                 MSGTEXT = row.MsgText
#                 MSGType = row.MsgType

#                 If ((MSGDATE - LastMsg.MsgDate).TotalSeconds > GapMaxLength) AndAlso (E_Action.Action_Actual <> Engine_Action.Data_GAP) Then
#                     E_Action.Action_To = LastMsg.MsgDate
#                     Store_Action(Engine_Action.Data_GAP, RowCount, Delay_Ckeck.NoCheck)
#                     LastMsg = row
#                 End If

#                 If MSGType = "A" Then
#                     E_Action.Trip_List.Add(New MSG With {
#                             .MsgNo = MSGNO,
#                             .MsgDate = MSGDATE,
#                             .MsgText = MSGTEXT,
#                             .MsgType = MSGType,
#                             .MsgResponibility = "M"
#                         })
#                 ElseIf MSGNO = MSG_Trigger.WarmStartCPU_1253 OrElse MSGNO = MSG_Trigger.ColdStartCPU_1254 Then ' Or A_MSG = OPC_Server_starte_9004 Then
#                     E_Action.Trip_List.Add(New MSG With {
#                             .MsgNo = MSGNO,
#                             .MsgDate = MSGDATE,
#                             .MsgText = MSGTEXT,
#                             .MsgType = "A",
#                             .MsgResponibility = "R"
#                         })
#                 ElseIf E_Action.Action_Actual = Engine_Action.Forced_Outage AndAlso MSGNO = MSG_Trigger.Remote_Reset Then
#                     E_Action.Reset_List.Add(New MSG With {
#                             .MsgNo = MSGNO,
#                             .MsgDate = MSGDATE,
#                             .MsgText = MSGTEXT,
#                             .MsgType = MSGType, '"B",
#                             .MsgResponibility = "R"
#                         })

#                 Else
#                     Select Case E_Action.Action_Actual
#                         Case Engine_Action.Undefinded
#                             Check_Action_GAP(E_Action, MSGNO, MSGDATE, RowCount)
#                         Case Engine_Action.Data_GAP
#                             Check_Action_GAP(E_Action, MSGNO, MSGDATE, RowCount)
#                         Case Engine_Action.Start_Preparation
#                             Check_Action_Blockstart(E_Action, MSGNO, MSGDATE, RowCount)
#                         Case Engine_Action.Start
#                             Check_Action_Start(E_Action, MSGNO, MSGDATE, RowCount)
#                         Case Engine_Action.Idle
#                             Check_Action_Idle(E_Action, MSGNO, MSGDATE, RowCount)
#                         Case Engine_Action.Synchronisation
#                             Check_Action_Synch(E_Action, MSGNO, MSGDATE, RowCount)
#                         Case Engine_Action.RampUp_Mains_Parallel_Operation
#                             Check_Action_RampupNetzparallel_Betrieb(E_Action, MSGNO, MSGDATE, RowCount)
#                         Case Engine_Action.Mains_Parallel_Operation
#                             Check_Action_Netzparallel_Betrieb(E_Action, MSGNO, MSGDATE, RowCount)
#                         Case Engine_Action.RampUp_Island_Operation
#                             Check_Action_RampupInselbetrieb_Betrieb(E_Action, MSGNO, MSGDATE, RowCount)
#                         Case Engine_Action.Island_Operation
#                             Check_Action_Inselbetrieb_Betrieb(E_Action, MSGNO, MSGDATE, RowCount)
#                         Case Engine_Action.Load_Rampdown
#                             Check_Action_Rampdown(E_Action, MSGNO, MSGDATE, RowCount)
#                         Case Engine_Action.Engine_Cooldown
#                             Check_Action_Cooldown(E_Action, MSGNO, MSGDATE, RowCount)
#                         Case Engine_Action.Forced_Outage
#                             Check_Action_Forcedoutage(E_Action, MSGNO, MSGDATE, RowCount)
#                         Case Engine_Action.Troubleshooting
#                             Check_Action_Troubleshooting(E_Action, MSGNO, MSGDATE, RowCount)
#                         Case Engine_Action.Ready
#                             Check_Action_Ready(E_Action, MSGNO, MSGDATE, RowCount)
#                         Case Engine_Action.Not_Ready
#                             Check_Action_Not_Ready(E_Action, MSGNO, MSGDATE, RowCount)
#                         Case Engine_Action.Operation 'Wird nie erreicht, da im Store_Action auf den richtigen Action umgeschaltet wird!!
#                             Check_Action_Betrieb(E_Action, MSGNO, MSGDATE, RowCount)
#                         Case Engine_Action.Mains_Failure
#                             Check_Action_NetzStörung(E_Action, MSGNO, MSGDATE, RowCount)
#                         Case Else
#                             'MsgBox("das gibt es nicht", MsgBoxStyle.OkOnly)
#                     End Select
#                     'Auswertung Meldungsnummern
#                     Select Case MSGNO
#                         'Auswertung BWS
#                         Case MSG_Trigger.BWS_AUS
#                             Store_BWS(ServiceSelectorSwitch_States.OFF, MSGDATE)
#                         Case MSG_Trigger.BWS_Hand
#                             Store_BWS(ServiceSelectorSwitch_States.MAN, MSGDATE)
#                         Case MSG_Trigger.BWS_Auto
#                             Store_BWS(ServiceSelectorSwitch_States.AUTO, MSGDATE)
#                         'Auswertung Anforderungswahlschalter
#                         Case MSG_Trigger.AWS_AUS
#                             Store_AWS(DemandSelectorSwitch_States.OFF, MSGDATE)
#                         Case MSG_Trigger.AWS_Ein
#                             Store_AWS(DemandSelectorSwitch_States.EIN, MSGDATE)
#                         Case MSG_Trigger.AWS_Remote
#                             Store_AWS(DemandSelectorSwitch_States.REMOTE, MSGDATE)
#                         'Auswertung RAM Trigger
#                         Case MSG_Trigger.Troubleshooting_ON
#                             Store_AV_MAN_Activated_Status(Available_States.Troubleshooting, MSGDATE, RowCount, False)
#                         Case MSG_Trigger.Troubleshooting_OFF
#                             Store_AV_MAN_Activated_Status(Available_States.Undefined, MSGDATE, RowCount, False)
#                         Case MSG_Trigger.Troubleshooting_Automatic_OFF
#                             Store_AV_MAN_Activated_Status(Available_States.Undefined, MSGDATE, RowCount, True)
#                         Case MSG_Trigger.Maintenace_ON
#                             Store_AV_MAN_Activated_Status(Available_States.Maintenance, MSGDATE, RowCount, False)
#                         Case MSG_Trigger.Maintenace_OFF
#                             Store_AV_MAN_Activated_Status(Available_States.Undefined, MSGDATE, RowCount, False)
#                         Case MSG_Trigger.Maintenace_Automatic_OFF
#                             Store_AV_MAN_Activated_Status(Available_States.Undefined, MSGDATE, RowCount, True)
#                         Case MSG_Trigger.Deactivate_ON
#                             Store_AV_MAN_Activated_Status(Available_States.Deactivated, MSGDATE, RowCount, False)
#                         Case MSG_Trigger.Deactivate_OFF
#                             Store_AV_MAN_Activated_Status(Available_States.Undefined, MSGDATE, RowCount, False)
#                         Case MSG_Trigger.Deactivate_Automatic_OFF
#                             Store_AV_MAN_Activated_Status(Available_States.Undefined, MSGDATE, RowCount, True)
#                     End Select
#                 End If
#                 LastMsg = row
#             Catch ex As Exception
#                 BGWCalcState.ReportProgress(-1, "Error CalcState " & ex.Message + " in row " + row.ToString + " Date: " + AMM_List(RowCount).MsgDate.ToString)
#             End Try
#             RowCount += 1
#         Next
#         E_Action.Action_To = LastMsg.MsgDate
#         Store_Action(E_Action.Action_Actual, RowCount - 1, Delay_Ckeck.NoCheck)
#     End Sub
#     Private Sub Check_Action_Blockstart(ByRef A_Action As EngineAction, ByRef MSGNR As Integer, ByRef Message_Time As Date, ByRef row As Integer)
#         Try
#             If A_Action.Trip_List.Count > 0 Then
#                 A_Action.Action_To = A_Action.Trip_List(0).MsgDate
#                 Store_Action(Engine_Action.Forced_Outage, row, Delay_Ckeck.NetzStörung)
#             Else
#                 If (Message_Time - A_Action.Action_From).TotalMinutes > 25 Then
#                     A_Action.Action_To = Message_Time
#                     Store_Action(Engine_Action.Data_GAP, row, Delay_Ckeck.NoCheck)
#                 Else
#                     Select Case MSGNR
#                         Case MSG_Trigger.Starter_Ein
#                             A_Action.Action_To = Message_Time
#                             Store_Action(Engine_Action.Start, row, Delay_Ckeck.NoCheck)
#                         Case MSG_Trigger.Idle
#                             A_Action.Action_To = Message_Time
#                             Store_Action(Engine_Action.Idle, row, Delay_Ckeck.NoCheck)
#                         Case MSG_Trigger.Synchronisieranforderung
#                             A_Action.Action_To = Message_Time
#                             Store_Action(Engine_Action.Synchronisation, row, Delay_Ckeck.NoCheck)
#                         Case MSG_Trigger.Anforderung_Aus
#                             A_Action.Action_To = Message_Time
#                             Store_Action(Engine_Action.Ready, row, Delay_Ckeck.NetzStörung)
#                         Case MSG_Trigger.Bereit_Automatic_Aus
#                             'A_Action.Action_To = Message_Time
#                             Store_Action(Engine_Action.Start_Preparation, row, Delay_Ckeck.NetzStörung)
#                         Case MSG_Trigger.Netzstoerung
#                             A_Action.Action_To = Message_Time
#                             Store_Action(Engine_Action.Mains_Failure, row, Delay_Ckeck.NoCheck)
#                         Case MSG_Trigger.Netzparallelbetrieb
#                             A_Action.Action_To = Message_Time
#                             Store_Action(Engine_Action.RampUp_Mains_Parallel_Operation, row, Delay_Ckeck.NoCheck)
#                         Case MSG_Trigger.Inselbetrieb
#                             A_Action.Action_To = Message_Time
#                             Store_Action(Engine_Action.RampUp_Island_Operation, row, Delay_Ckeck.NoCheck)
#                     End Select
#                 End If
#             End If
#         Catch ex As Exception
#             BGWCalcState.ReportProgress(-1, "Error Check_Action_Blockstart " & ex.Message + " in row " + row.ToString + " Date: " + AMM_List(row).MsgDate.ToString)
#         End Try
#     End Sub
#     Private Sub Check_Action_Netzparallel_Betrieb(ByRef A_Action As EngineAction, ByRef MSGNR As Integer, ByRef Message_Time As Date, ByRef row As Integer)
#         Try
#             If A_Action.Trip_List.Count > 0 Then
#                 A_Action.Action_To = A_Action.Trip_List(0).MsgDate
#                 Store_Action(Engine_Action.Forced_Outage, row, Delay_Ckeck.NetzStörung)
#             Else
#                 Select Case MSGNR
#                     Case MSG_Trigger.GS_Aus
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.Engine_Cooldown, row, Delay_Ckeck.NetzStörung)
#                     Case MSG_Trigger.Bereit_Automatic_Aus
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.Mains_Parallel_Operation, row, Delay_Ckeck.NetzStörung)
#                     Case MSG_Trigger.Netzstoerung
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.Mains_Failure, row, Delay_Ckeck.NoCheck)
#                     Case MSG_Trigger.Anforderung_Aus
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.Load_Rampdown, row, Delay_Ckeck.NetzStörung)
#                     Case MSG_Trigger.Inselbetrieb
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.Island_Operation, row, Delay_Ckeck.NetzStörung)
#                     Case MSG_Trigger.IGN_Aus
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.Ready, row, Delay_Ckeck.NetzStörung)
#                 End Select
#             End If
#         Catch ex As Exception
#             BGWCalcState.ReportProgress(-1, "Check_Action_Netzparallel_Betrieb " & ex.Message + " in row " + row.ToString + " Date: " + AMM_List(row).MsgDate.ToString)
#         End Try
#     End Sub
#     Private Sub Check_Action_Inselbetrieb_Betrieb(ByRef A_Action As EngineAction, ByRef MSGNR As Integer, ByRef Message_Time As Date, ByRef row As Integer)
#         Try
#             If A_Action.Trip_List.Count > 0 Then
#                 A_Action.Action_To = A_Action.Trip_List(0).MsgDate
#                 Store_Action(Engine_Action.Forced_Outage, row, Delay_Ckeck.NetzStörung)
#             Else
#                 Select Case MSGNR
#                     Case MSG_Trigger.GS_Aus
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.Engine_Cooldown, row, Delay_Ckeck.NetzStörung)
#                     Case MSG_Trigger.Bereit_Automatic_Aus
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.Island_Operation, row, Delay_Ckeck.NetzStörung)
#                     Case MSG_Trigger.Netzstoerung
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.Mains_Failure, row, Delay_Ckeck.NoCheck)
#                     Case MSG_Trigger.Anforderung_Aus
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.Load_Rampdown, row, Delay_Ckeck.NetzStörung)
#                     Case MSG_Trigger.Netzparallelbetrieb
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.Mains_Parallel_Operation, row, Delay_Ckeck.NoCheck)
#                     Case MSG_Trigger.IGN_Aus
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.Ready, row, Delay_Ckeck.NetzStörung)
#                 End Select
#             End If
#         Catch ex As Exception
#             BGWCalcState.ReportProgress(-1, "Error Check_Action_Betrieb " & ex.Message + " in row " + row.ToString + " Date: " + AMM_List(row).MsgDate.ToString)
#         End Try
#     End Sub
#     Private Sub Check_Action_RampupNetzparallel_Betrieb(ByRef A_Action As EngineAction, ByRef MSGNR As Integer, ByRef Message_Time As Date, ByRef row As Integer)
#         Try
#             If A_Action.Trip_List.Count > 0 Then
#                 A_Action.Action_To = A_Action.Trip_List(0).MsgDate
#                 Store_Action(Engine_Action.Forced_Outage, row, Delay_Ckeck.NetzStörung)
#             Else
#                 If (Message_Time - A_Action.Action_From).TotalSeconds > _TaretloadreachTime Then
#                     A_Action.Action_To = A_Action.Action_From.AddSeconds(_TaretloadreachTime)
#                     Store_Action(Engine_Action.Mains_Parallel_Operation, row, Delay_Ckeck.NoCheck)
#                     Check_Action_Netzparallel_Betrieb(A_Action, MSGNR, Message_Time, row)
#                 Else
#                     Select Case MSGNR
#                         Case MSG_Trigger.GS_Aus
#                             A_Action.Action_To = Message_Time
#                             Store_Action(Engine_Action.Engine_Cooldown, row, Delay_Ckeck.NetzStörung)
#                         Case MSG_Trigger.Bereit_Automatic_Aus
#                             A_Action.Action_To = Message_Time
#                             Store_Action(Engine_Action.RampUp_Mains_Parallel_Operation, row, Delay_Ckeck.NetzStörung)
#                         Case MSG_Trigger.Netzstoerung
#                             A_Action.Action_To = Message_Time
#                             Store_Action(Engine_Action.Mains_Failure, row, Delay_Ckeck.NoCheck)
#                         Case MSG_Trigger.Anforderung_Aus
#                             A_Action.Action_To = Message_Time
#                             Store_Action(Engine_Action.Load_Rampdown, row, Delay_Ckeck.NetzStörung)
#                         Case MSG_Trigger.Inselbetrieb
#                             A_Action.Action_To = Message_Time
#                             Store_Action(Engine_Action.Island_Operation, row, Delay_Ckeck.NoCheck)
#                         Case MSG_Trigger.TargetLoadReached
#                             A_Action.Action_To = Message_Time
#                             Store_Action(Engine_Action.Mains_Parallel_Operation, row, Delay_Ckeck.NoCheck)
#                         Case MSG_Trigger.IGN_Aus
#                             A_Action.Action_To = Message_Time
#                             Store_Action(Engine_Action.Ready, row, Delay_Ckeck.NetzStörung)
#                     End Select
#                 End If
#             End If
#         Catch ex As Exception
#             BGWCalcState.ReportProgress(-1, "Error Check_Action_RampupNetzparallelbetrieb " & ex.Message + " in row " + row.ToString + " Date: " + AMM_List(row).MsgDate.ToString)
#         End Try
#     End Sub
#     Private Sub Check_Action_RampupInselbetrieb_Betrieb(ByRef A_Action As EngineAction, ByRef MSGNR As Integer, ByRef Message_Time As Date, ByRef row As Integer)
#         Try
#             If A_Action.Trip_List.Count > 0 Then
#                 A_Action.Action_To = A_Action.Trip_List(0).MsgDate
#                 Store_Action(Engine_Action.Forced_Outage, row, Delay_Ckeck.NetzStörung)
#             Else
#                 If (Message_Time - A_Action.Action_From).TotalSeconds > _TaretloadreachTime Then
#                     A_Action.Action_To = A_Action.Action_From.AddSeconds(_TaretloadreachTime)
#                     Store_Action(Engine_Action.Island_Operation, row, Delay_Ckeck.NoCheck)
#                     Check_Action_Inselbetrieb_Betrieb(A_Action, MSGNR, Message_Time, row)
#                 Else
#                     Select Case MSGNR
#                         Case MSG_Trigger.GS_Aus
#                             A_Action.Action_To = Message_Time
#                             Store_Action(Engine_Action.Engine_Cooldown, row, Delay_Ckeck.NetzStörung)
#                         Case MSG_Trigger.Bereit_Automatic_Aus
#                             A_Action.Action_To = Message_Time
#                             Store_Action(Engine_Action.RampUp_Island_Operation, row, Delay_Ckeck.NetzStörung)
#                         Case MSG_Trigger.Netzstoerung
#                             A_Action.Action_To = Message_Time
#                             Store_Action(Engine_Action.Mains_Failure, row, Delay_Ckeck.NoCheck)
#                         Case MSG_Trigger.Anforderung_Aus
#                             A_Action.Action_To = Message_Time
#                             Store_Action(Engine_Action.Load_Rampdown, row, Delay_Ckeck.NetzStörung)
#                         Case MSG_Trigger.Netzparallelbetrieb
#                             A_Action.Action_To = Message_Time
#                             Store_Action(Engine_Action.Mains_Parallel_Operation, row, Delay_Ckeck.NoCheck)
#                         Case MSG_Trigger.TargetLoadReached
#                             A_Action.Action_To = Message_Time
#                             'MsgBox("der Schei sollte nie eintreten!!")
#                             Store_Action(Engine_Action.Mains_Parallel_Operation, row, Delay_Ckeck.NoCheck)
#                         Case MSG_Trigger.IGN_Aus
#                             A_Action.Action_To = Message_Time
#                             Store_Action(Engine_Action.Ready, row, Delay_Ckeck.NetzStörung)
#                     End Select
#                 End If
#             End If
#         Catch ex As Exception
#             BGWCalcState.ReportProgress(-1, "Error Check_Action_RampupInselbetrieb " & ex.Message + " in row " + row.ToString + " Date: " + AMM_List(row).MsgDate.ToString)
#         End Try
#     End Sub
#     Private Sub Check_Action_Betrieb(ByRef A_Action As EngineAction, ByRef MSGNR As Integer, ByRef Message_Time As Date, ByRef row As Integer)
#         Try
#             If A_Action.Trip_List.Count > 0 Then
#                 A_Action.Action_To = A_Action.Trip_List(0).MsgDate
#                 Store_Action(Engine_Action.Forced_Outage, row, Delay_Ckeck.NetzStörung)
#             Else
#                 Select Case MSGNR
#                     Case MSG_Trigger.GS_Aus
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.Engine_Cooldown, row, Delay_Ckeck.NetzStörung)
#                     Case MSG_Trigger.Bereit_Automatic_Aus
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.Operation, row, Delay_Ckeck.NetzStörung)
#                     Case MSG_Trigger.Netzstoerung
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.Mains_Failure, row, Delay_Ckeck.NoCheck)
#                     Case MSG_Trigger.Anforderung_Aus
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.Load_Rampdown, row, Delay_Ckeck.NetzStörung)
#                     Case MSG_Trigger.Netzparallelbetrieb
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.RampUp_Mains_Parallel_Operation, row, Delay_Ckeck.NoCheck)
#                     Case MSG_Trigger.Inselbetrieb
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.RampUp_Island_Operation, row, Delay_Ckeck.NoCheck)
#                     Case MSG_Trigger.IGN_Aus
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.Ready, row, Delay_Ckeck.NetzStörung)

#                 End Select
#             End If
#         Catch ex As Exception
#             BGWCalcState.ReportProgress(-1, "Error Check_Action_Betrieb " & ex.Message + " in row " + row.ToString + " Date: " + AMM_List(row).MsgDate.ToString)
#         End Try
#     End Sub
#     Private Sub Check_Action_Rampdown(ByRef A_Action As EngineAction, ByRef MSGNR As Integer, ByRef Message_Time As Date, ByRef row As Integer)
#         Try
#             If A_Action.Trip_List.Count > 0 Then
#                 A_Action.Action_To = A_Action.Trip_List(0).MsgDate
#                 Store_Action(Engine_Action.Forced_Outage, row, Delay_Ckeck.NetzStörung)
#             Else
#                 Select Case MSGNR
#                     Case MSG_Trigger.GS_Aus
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.Engine_Cooldown, row, Delay_Ckeck.NetzStörung)
#                     Case MSG_Trigger.Bereit_Automatic_Aus
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.Load_Rampdown, row, Delay_Ckeck.NetzStörung)
#                     Case MSG_Trigger.Netzstoerung
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.Mains_Failure, row, Delay_Ckeck.NoCheck)
#                     Case MSG_Trigger.Anforderung_Ein
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.Operation, row, Delay_Ckeck.Betrieb_NetzOrInsel)
#                     Case MSG_Trigger.Bereit_Automatic_Ein
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.Operation, row, Delay_Ckeck.Betrieb_NetzOrInsel)
#                     Case MSG_Trigger.Netzparallelbetrieb
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.RampUp_Mains_Parallel_Operation, row, Delay_Ckeck.NoCheck)
#                     Case MSG_Trigger.Inselbetrieb
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.RampUp_Island_Operation, row, Delay_Ckeck.NoCheck)
#                     Case MSG_Trigger.IGN_Aus
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.Ready, row, Delay_Ckeck.NetzStörung)
#                 End Select
#             End If
#         Catch ex As Exception
#             BGWCalcState.ReportProgress(-1, "Error Check_Action_Rampdown " & ex.Message + " in row " + row.ToString + " Date: " + AMM_List(row).MsgDate.ToString)
#         End Try
#     End Sub
#     Private Sub Check_Action_Cooldown(ByRef A_Action As EngineAction, ByRef MSGNR As Integer, ByRef Message_Time As Date, ByRef row As Integer)
#         Try
#             If A_Action.Trip_List.Count > 0 Then
#                 A_Action.Action_To = A_Action.Trip_List(0).MsgDate
#                 Store_Action(Engine_Action.Forced_Outage, row, Delay_Ckeck.NetzStörung)
#             Else
#                 Select Case MSGNR
#                     Case MSG_Trigger.IGN_Aus
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.Ready, row, Delay_Ckeck.NetzStörung)
#                     Case MSG_Trigger.Anforderung_Ein
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.Start_Preparation, row, Delay_Ckeck.NoCheck)
#                     Case MSG_Trigger.Synchronisieranforderung
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.Synchronisation, row, Delay_Ckeck.NoCheck)
#                     Case MSG_Trigger.Bereit_Automatic_Aus
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.Engine_Cooldown, row, Delay_Ckeck.NetzStörung)
#                     Case MSG_Trigger.Netzstoerung
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.Mains_Failure, row, Delay_Ckeck.NoCheck)
#                     Case MSG_Trigger.Bereit_Automatic_Ein
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.Ready, row, Delay_Ckeck.NoCheck)
#                     Case MSG_Trigger.Netzparallelbetrieb
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.RampUp_Mains_Parallel_Operation, row, Delay_Ckeck.NoCheck)
#                     Case MSG_Trigger.Inselbetrieb
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.RampUp_Island_Operation, row, Delay_Ckeck.NoCheck)
#                 End Select
#             End If
#         Catch ex As Exception
#             BGWCalcState.ReportProgress(-1, "Error Check_Action_Cooldown " & ex.Message + " in row " + row.ToString + " Date: " + AMM_List(row).MsgDate.ToString)
#         End Try
#     End Sub
#     Private Sub Check_Action_Start(ByRef A_Action As EngineAction, ByRef MSGNR As Integer, ByRef Message_Time As Date, ByRef row As Integer)
#         Try
#             If A_Action.Trip_List.Count > 0 Then
#                 A_Action.Action_To = A_Action.Trip_List(0).MsgDate
#                 Store_Action(Engine_Action.Forced_Outage, row, Delay_Ckeck.NetzStörung)
#             Else
#                 Select Case MSGNR
#                     Case MSG_Trigger.Idle
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.Idle, row, Delay_Ckeck.NoCheck)
#                     Case MSG_Trigger.Synchronisieranforderung
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.Synchronisation, row, Delay_Ckeck.NoCheck)
#                     Case MSG_Trigger.Anforderung_Aus
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.Ready, row, Delay_Ckeck.NetzStörung)
#                     Case MSG_Trigger.Bereit_Automatic_Aus
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.Start, row, Delay_Ckeck.NetzStörung)
#                     Case MSG_Trigger.Netzstoerung
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.Mains_Failure, row, Delay_Ckeck.NoCheck)
#                     Case MSG_Trigger.Netzparallelbetrieb
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.RampUp_Mains_Parallel_Operation, row, Delay_Ckeck.NoCheck)
#                     Case MSG_Trigger.Inselbetrieb
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.RampUp_Island_Operation, row, Delay_Ckeck.NoCheck)
#                 End Select
#             End If
#         Catch ex As Exception
#             BGWCalcState.ReportProgress(-1, "Error Check_Action_Start " & ex.Message + " in row " + row.ToString + " Date: " + AMM_List(row).MsgDate.ToString)
#         End Try

#     End Sub
#     Private Sub Check_Action_Idle(ByRef A_Action As EngineAction, ByRef MSGNR As Integer, ByRef Message_Time As Date, ByRef row As Integer)
#         Try
#             If A_Action.Trip_List.Count > 0 Then
#                 A_Action.Action_To = A_Action.Trip_List(0).MsgDate
#                 Store_Action(Engine_Action.Forced_Outage, row, Delay_Ckeck.NetzStörung)
#             Else
#                 Select Case MSGNR
#                     Case MSG_Trigger.Synchronisieranforderung
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.Synchronisation, row, Delay_Ckeck.NoCheck)
#                     Case MSG_Trigger.Anforderung_Aus
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.Engine_Cooldown, row, Delay_Ckeck.NetzStörung)
#                     Case MSG_Trigger.IGN_Aus
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.Ready, row, Delay_Ckeck.NetzStörung)
#                     Case MSG_Trigger.Bereit_Automatic_Aus
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.Idle, row, Delay_Ckeck.NetzStörung)
#                     Case MSG_Trigger.Netzstoerung
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.Mains_Failure, row, Delay_Ckeck.NoCheck)
#                     Case MSG_Trigger.Netzparallelbetrieb
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.RampUp_Mains_Parallel_Operation, row, Delay_Ckeck.NoCheck)
#                     Case MSG_Trigger.Inselbetrieb
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.RampUp_Island_Operation, row, Delay_Ckeck.NoCheck)
#                 End Select
#             End If
#         Catch ex As Exception
#             BGWCalcState.ReportProgress(-1, "Error Check_Action_Idle " & ex.Message + " in row " + row.ToString + " Date: " + AMM_List(row).MsgDate.ToString)
#         End Try
#     End Sub
#     Private Sub Check_Action_Synch(ByRef A_Action As EngineAction, ByRef MSGNR As Integer, ByRef Message_Time As Date, ByRef row As Integer)
#         Try
#             If A_Action.Trip_List.Count > 0 Then
#                 A_Action.Action_To = A_Action.Trip_List(0).MsgDate
#                 Store_Action(Engine_Action.Forced_Outage, row, Delay_Ckeck.NoCheck)
#             Else
#                 Select Case MSGNR
#                     Case MSG_Trigger.Netzparallelbetrieb
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.RampUp_Mains_Parallel_Operation, row, Delay_Ckeck.NoCheck)
#                     Case MSG_Trigger.Inselbetrieb
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.RampUp_Island_Operation, row, Delay_Ckeck.NoCheck)
#                     Case MSG_Trigger.Anforderung_Aus
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.Engine_Cooldown, row, Delay_Ckeck.NetzStörung)
#                     Case MSG_Trigger.IGN_Aus
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.Ready, row, Delay_Ckeck.NetzStörung)
#                     Case MSG_Trigger.Bereit_Automatic_Aus
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.Synchronisation, row, Delay_Ckeck.NetzStörung)
#                     Case MSG_Trigger.Netzstoerung
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.Mains_Failure, row, Delay_Ckeck.NoCheck)
#                 End Select
#             End If
#         Catch ex As Exception
#             BGWCalcState.ReportProgress(-1, "Error Check_Action_Synch " & ex.Message + " in row " + row.ToString + " Date: " + AMM_List(row).MsgDate.ToString)
#         End Try
#     End Sub
#     Private Sub Check_Action_Ready(ByRef A_Action As EngineAction, ByRef MSGNR As Integer, ByRef Message_Time As Date, ByRef row As Integer)
#         Try
#             If A_Action.Trip_List.Count > 0 Then
#                 A_Action.Action_To = A_Action.Trip_List(0).MsgDate
#                 Store_Action(Engine_Action.Forced_Outage, row, Delay_Ckeck.NoCheck)
#             Else
#                 Select Case MSGNR
#                     Case MSG_Trigger.Anforderung_Ein
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.Start_Preparation, row, Delay_Ckeck.NoCheck)
#                     Case MSG_Trigger.Starter_Ein
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.Start, row, Delay_Ckeck.NoCheck)
#                     Case MSG_Trigger.Idle
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.Idle, row, Delay_Ckeck.NoCheck)
#                     Case MSG_Trigger.Synchronisieranforderung
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.Synchronisation, row, Delay_Ckeck.NoCheck)
#                     'Case MSG_Trigger.Anforderung_Aus
#                     '    A_Action.Action_To = Message_Time
#                     '    Store_Action(Engine_Action.Ready, row, Delay_Ckeck.AlarmInNextSecond)
#                     Case MSG_Trigger.Bereit_Automatic_Aus
#                         If Not CheckBWSNotAuto(row) Then
#                             A_Action.Action_To = Message_Time
#                             Store_Action(Engine_Action.Not_Ready, row, Delay_Ckeck.NetzStörung)
#                         End If
#                     Case MSG_Trigger.Netzstoerung
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.Mains_Failure, row, Delay_Ckeck.NoCheck)
#                     Case MSG_Trigger.Netzparallelbetrieb
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.RampUp_Mains_Parallel_Operation, row, Delay_Ckeck.NoCheck)
#                     Case MSG_Trigger.Inselbetrieb
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.RampUp_Island_Operation, row, Delay_Ckeck.NetzStörung)

#                 End Select
#             End If
#         Catch ex As Exception
#             BGWCalcState.ReportProgress(-1, "Error Check_Action_Ready " & ex.Message + " in row " + row.ToString + " Date: " + AMM_List(row).MsgDate.ToString)
#         End Try
#     End Sub
#     Private Sub Check_Action_Not_Ready(ByRef A_Action As EngineAction, ByRef MSGNR As Integer, ByRef Message_Time As Date, ByRef row As Integer)
#         Try
#             If A_Action.Trip_List.Count > 0 Then
#                 A_Action.Action_To = A_Action.Trip_List(0).MsgDate
#                 Store_Action(Engine_Action.Forced_Outage, row, Delay_Ckeck.NetzStörung)
#             Else
#                 Select Case MSGNR
#                     Case MSG_Trigger.Anforderung_Ein
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.Start_Preparation, row, Delay_Ckeck.NoCheck)
#                     Case MSG_Trigger.Synchronisieranforderung
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.Synchronisation, row, Delay_Ckeck.NoCheck)
#                     Case MSG_Trigger.Bereit_Automatic_Ein
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.Ready, row, Delay_Ckeck.NoCheck)
#                     Case MSG_Trigger.Netzparallelbetrieb
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.RampUp_Mains_Parallel_Operation, row, Delay_Ckeck.NoCheck)
#                     Case MSG_Trigger.Inselbetrieb
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.RampUp_Island_Operation, row, Delay_Ckeck.NoCheck)
#                     Case MSG_Trigger.Starter_Ein
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.Start, row, Delay_Ckeck.NoCheck)
#                     Case MSG_Trigger.GS_Aus
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.Engine_Cooldown, row, Delay_Ckeck.NetzStörung)
#                     Case MSG_Trigger.Netzstoerung
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.Mains_Failure, row, Delay_Ckeck.NoCheck)
#                     Case MSG_Trigger.GS_Ein
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.Operation, row, Delay_Ckeck.Betrieb_NetzOrInsel)
#                     Case MSG_Trigger.Idle
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.Idle, row, Delay_Ckeck.NoCheck)
#                     Case MSG_Trigger.IGN_Aus
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.Ready, row, Delay_Ckeck.NetzStörung)
#                 End Select
#             End If
#         Catch ex As Exception
#             BGWCalcState.ReportProgress(-1, "Error Check_Action_Not_Ready " & ex.Message + " in row " + row.ToString + " Date: " + AMM_List(row).MsgDate.ToString)
#         End Try
#     End Sub
#     Private Sub Check_Action_GAP(ByRef A_Action As EngineAction, ByRef MSGNR As Integer, ByRef Message_Time As Date, ByRef row As Integer)
#         Try
#             If A_Action.Trip_List.Count > 0 Then
#                 A_Action.Action_To = A_Action.Trip_List(0).MsgDate
#                 Store_Action(Engine_Action.Forced_Outage, row, Delay_Ckeck.NetzStörung)
#             Else
#                 Select Case MSGNR
#                     Case MSG_Trigger.Anforderung_Ein
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.Start_Preparation, row, Delay_Ckeck.NoCheck)
#                     Case MSG_Trigger.Anforderung_Aus
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.Load_Rampdown, row, Delay_Ckeck.NetzStörung)
#                     Case MSG_Trigger.Synchronisieranforderung
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.Synchronisation, row, Delay_Ckeck.NoCheck)
#                     Case MSG_Trigger.Bereit_Automatic_Aus
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.Not_Ready, row, Delay_Ckeck.NetzStörung)
#                     Case MSG_Trigger.Bereit_Automatic_Ein
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.Ready, row, Delay_Ckeck.NoCheck)
#                     Case MSG_Trigger.Netzparallelbetrieb
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.RampUp_Mains_Parallel_Operation, row, Delay_Ckeck.NoCheck)
#                     Case MSG_Trigger.Inselbetrieb
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.RampUp_Island_Operation, row, Delay_Ckeck.NoCheck)
#                     Case MSG_Trigger.Starter_Ein
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.Start, row, Delay_Ckeck.NoCheck)
#                     Case MSG_Trigger.GS_Aus
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.Engine_Cooldown, row, Delay_Ckeck.NetzStörung)
#                     Case MSG_Trigger.GS_Ein
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.Operation, row, Delay_Ckeck.Betrieb_NetzOrInsel)
#                     Case MSG_Trigger.Idle
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.Idle, row, Delay_Ckeck.NoCheck)
#                     Case MSG_Trigger.IGN_Aus
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.Ready, row, Delay_Ckeck.NetzStörung)
#                     Case MSG_Trigger.Netzstoerung
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.Mains_Failure, row, Delay_Ckeck.NoCheck)
#                     Case MSG_Trigger.Power_reduction_In_isolated_operation
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.Island_Operation, row, Delay_Ckeck.NoCheck)


#                     Case MSG_Trigger.BWS_Hand, MSG_Trigger.BWS_AUS
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.Troubleshooting, row, Delay_Ckeck.NoCheck)

#                     Case Else
#                         For Each tstEnum As Powerreduction In GetType(Powerreduction).GetEnumValues
#                             If tstEnum = MSGNR Then
#                                 A_Action.Action_To = Message_Time
#                                 Store_Action(Engine_Action.Operation, row, Delay_Ckeck.Betrieb_NetzOrInsel)
#                                 Exit Select
#                             End If
#                         Next

#                         If row > 0 AndAlso ((Message_Time - AMM_List(row - 1).MsgDate).TotalSeconds < GapMaxLength) And E_Action.Action_Actual <> Engine_Action.Undefinded Then
#                             E_Action.Action_To = Message_Time
#                             Store_Action(Engine_Action.Undefinded, row, Delay_Ckeck.NoCheck)
#                         End If
#                 End Select
#             End If
#         Catch ex As Exception
#             BGWCalcState.ReportProgress(-1, "Error Check_Action_Gap " & ex.Message + " in row " + row.ToString + " Date: " + AMM_List(row).MsgDate.ToString)
#         End Try
#     End Sub
#     Private Sub Check_Action_Forcedoutage(ByRef A_Action As EngineAction, ByRef MSGNR As Integer, ByRef Message_Time As Date, ByRef row As Integer)
#         Try
#             Select Case MSGNR
#                 Case MSG_Trigger.BWS_Hand, MSG_Trigger.BWS_AUS, MSG_Trigger.BWS_Auto
#                     A_Action.Action_To = Message_Time
#                     Store_Action(Engine_Action.Troubleshooting, row, Delay_Ckeck.NoCheck)

#                 Case MSG_Trigger.Bereit_Automatic_Ein
#                     A_Action.Action_To = Message_Time
#                     Store_Action(Engine_Action.Ready, row, Delay_Ckeck.RemoteReset)
#                 'Case MSG_Trigger.Netzparallelbetrieb
#                 '    A_Action.Action_To = Message_Time
#                 '    Store_Action(Engine_Action.RampUp_Mains_Parallel_Operation, row, Delay_Ckeck.Betrieb_NetzOrInsel)
#                 'Case MSG_Trigger.Inselbetrieb
#                 '    A_Action.Action_To = Message_Time
#                 '    Store_Action(Engine_Action.RampUp_Island_Operation, row, Delay_Ckeck.Betrieb_NetzOrInsel)
#                 Case MSG_Trigger.Anforderung_Ein
#                     A_Action.Action_To = Message_Time
#                     Store_Action(Engine_Action.Start_Preparation, row, Delay_Ckeck.NoCheck)
#                 Case MSG_Trigger.Starter_Ein
#                     A_Action.Action_To = Message_Time
#                     Store_Action(Engine_Action.Start, row, Delay_Ckeck.NoCheck)
#                     'Case MSG_Trigger.Synchronisieranforderung
#                     '    A_Action.Action_To = Message_Time
#                     '    Store_Action(Engine_Action.Synchronisation, row, Delay_Ckeck.NoCheck)
#                 Case MSG_Trigger.GS_Ein
#                     A_Action.Action_To = Message_Time
#                     Store_Action(Engine_Action.Operation, row, Delay_Ckeck.Betrieb_NetzOrInsel)
#                 Case Else

#             End Select
#         Catch ex As Exception
#             BGWCalcState.ReportProgress(-1, "Error Check_forced_outage " & ex.Message + " in row " + row.ToString + " Date: " + AMM_List(row).MsgDate.ToString)
#         End Try
#     End Sub
#     Private Sub Check_Action_Troubleshooting(ByRef A_Action As EngineAction, ByRef MSGNR As Integer, ByRef Message_Time As Date, ByRef row As Integer)
#         Try
#             Select Case MSGNR
#                 Case MSG_Trigger.Bereit_Automatic_Ein
#                     A_Action.Action_To = Message_Time
#                     Store_Action(Engine_Action.Ready, row, Delay_Ckeck.NoCheck)
#                 Case MSG_Trigger.Netzparallelbetrieb
#                     A_Action.Action_To = Message_Time
#                     Store_Action(Engine_Action.RampUp_Mains_Parallel_Operation, row, Delay_Ckeck.Betrieb_NetzOrInsel)
#                 Case MSG_Trigger.Inselbetrieb
#                     A_Action.Action_To = Message_Time
#                     Store_Action(Engine_Action.RampUp_Island_Operation, row, Delay_Ckeck.Betrieb_NetzOrInsel)
#                 Case MSG_Trigger.Anforderung_Ein
#                     A_Action.Action_To = Message_Time
#                     Store_Action(Engine_Action.Start_Preparation, row, Delay_Ckeck.NoCheck)
#                 Case MSG_Trigger.Starter_Ein
#                     A_Action.Action_To = Message_Time
#                     Store_Action(Engine_Action.Start, row, Delay_Ckeck.NoCheck)
#                 Case MSG_Trigger.Synchronisieranforderung
#                     A_Action.Action_To = Message_Time
#                     Store_Action(Engine_Action.Synchronisation, row, Delay_Ckeck.NoCheck)
#                 Case MSG_Trigger.Idle
#                     A_Action.Action_To = Message_Time
#                     Store_Action(Engine_Action.Idle, row, Delay_Ckeck.NoCheck)
#                     'Case MSG_Trigger.BWS_Hand
#                     '    A_Action.Action_To = Message_Time
#                     '    Store_Action(Engine_Action.Troubleshooting, row, Delay_Ckeck.NoCheck)
#                     '    E_Action.ServiceSelectorSwitch = ServiceSelectorSwitch_States.MAN
#                     'Case MSG_Trigger.BWS_AUS
#                     '    A_Action.Action_To = Message_Time
#                     '    Store_Action(Engine_Action.Troubleshooting, row, Delay_Ckeck.NoCheck)
#                     '    E_Action.ServiceSelectorSwitch = ServiceSelectorSwitch_States.OFF
#                     'Case MSG_Trigger.BWS_Auto
#                     '    A_Action.Action_To = Message_Time
#                     '    Store_Action(Engine_Action.Troubleshooting, row, Delay_Ckeck.NoCheck)
#                     '    E_Action.ServiceSelectorSwitch = ServiceSelectorSwitch_States.AUTO

#             End Select
#         Catch ex As Exception
#             BGWCalcState.ReportProgress(-1, "Error Check_Action_Troubleshooting " & ex.Message + " in row " + row.ToString + " Date: " + AMM_List(row).MsgDate.ToString)
#         End Try
#     End Sub

#     Private Sub Check_Action_NetzStörung(ByRef A_Action As EngineAction, ByRef MSGNR As Integer, ByRef Message_Time As Date, ByRef row As Integer)
#         Try
#             If A_Action.Trip_List.Count > 0 Then
#                 A_Action.Action_To = A_Action.Trip_List(0).MsgDate
#                 Store_Action(Engine_Action.Forced_Outage, row, Delay_Ckeck.NetzStörung)
#             Else
#                 Select Case MSGNR
#                     Case MSG_Trigger.Bereit_Automatic_Ein
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.Ready, row, Delay_Ckeck.NoCheck)
#                     Case MSG_Trigger.Anforderung_Ein
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.Start_Preparation, row, Delay_Ckeck.NoCheck)
#                     Case MSG_Trigger.Starter_Ein
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.Start, row, Delay_Ckeck.NoCheck)
#                     Case MSG_Trigger.Synchronisieranforderung
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.Synchronisation, row, Delay_Ckeck.NoCheck)
#                     Case MSG_Trigger.Netzparallelbetrieb
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.RampUp_Mains_Parallel_Operation, row, Delay_Ckeck.NoCheck)
#                     Case MSG_Trigger.Inselbetrieb
#                         A_Action.Action_To = Message_Time
#                         Store_Action(Engine_Action.RampUp_Island_Operation, row, Delay_Ckeck.NoCheck)
#                 End Select
#             End If
#         Catch ex As Exception
#             BGWCalcState.ReportProgress(-1, "Error Check_Action_Netzstörung " & ex.Message + " in row " + row.ToString + " Date: " + AMM_List(row).MsgDate.ToString)
#         End Try
#     End Sub
#     Private Function Select_Primary_MSG(ByRef AT_MSG As List(Of MSG)) As MSG
#         Dim _MSG As New List(Of MSG)
#         Dim FirstMSGD As DateTime
#         Select_Primary_MSG = Nothing
#         Try
#             If AT_MSG.Count > 0 Then
#                 FirstMSGD = AT_MSG(0).MsgDate
#             Else
#                 Exit Function
#             End If
#             'Check ob ein Kaltstart oder ein Sicherungsfall in den Meldungen enthalten ist
#             For Each row As MSG In AT_MSG
#                 If (row.MsgNo = MSG_Trigger.ColdStartCPU_1254) OrElse (row.MsgNo = MSG_Trigger.WarmStartCPU_1253) OrElse (row.MsgNo = MSG_Trigger.OPC_Server_start_9004) OrElse (row.MsgNo = MSG_Trigger.Sicherungsfall_Modulinterfaceschrank_1140) Then
#                     Select_Primary_MSG = row
#                     Select_Primary_MSG.MsgType = "P"
#                     Exit Function
#                 Else
#                     If (row.MsgDate - FirstMSGD).TotalMilliseconds < 1000 Then
#                         Dim result
#                         result = _MSG.Find(Function(x) x.MsgNo = row.MsgNo.ToString)
#                         If result Is Nothing Then
#                             _MSG.Add(row)
#                         Else
#                             result = result
#                         End If
#                     Else
#                         Exit For
#                     End If
#                 End If
#             Next

#             If _MSG.Count = 1 Then
#                 Select_Primary_MSG = _MSG(0)
#                 Exit Function
#             ElseIf _MSG.Count > 1 Then
#                 'Dim DSL As New List(Of MSG)

#                 _MSG.Sort(Function(x, y) x.MsgNo.CompareTo(y.MsgNo))
#                 Dim PM_String As New StringBuilder
#                 'Erstelle Primary Matrix
#                 For Each a As MSG In _MSG
#                     PM_String.Append(a.MsgNo.ToString + "_")
#                 Next
#                 PM_String = PM_String.Remove(PM_String.Length - 1, 1)
#                 Dim result As PrimaryMSG
#                 result = PrimaryMSGList.Find(Function(x) x.MSGCombination = PM_String.ToString)

#                 If Not result Is Nothing Then
#                     Dim Result2 As MSG
#                     Result2 = _MSG.Find(Function(x) x.MsgNo = result.MsgNo)
#                     Result2.MsgType = "P"
#                     Select_Primary_MSG = Result2
#                 Else
#                     If Not Silence Then 'Keine Primary gefunden
#                         Dim sb As New StringBuilder
#                         Dim Trip As String = "{0} | {1} | {2}"
#                         For Each a As MSG In _MSG
#                             sb.AppendFormat(Trip, a.MsgNo.ToString, String.Format("{0:dd/MM/yy H:mm:ss} {1} ", a.MsgDate, a.MsgDate.Subtract(_MSG(0).MsgDate).TotalMilliseconds), a.MsgText.ToString)
#                             sb.AppendLine()
#                         Next
#                         RaiseEvent PrimaryMessageNotfound(sb.ToString)
#                     End If
#                     If _MSG.Count > 1 Then
#                         For Each a As MSG In _MSG
#                             If a.MsgNo <> 1056 Then
#                                 Select_Primary_MSG = a
#                                 Exit For
#                             End If
#                         Next
#                     Else
#                         Select_Primary_MSG = _MSG(0)
#                     End If
#                 End If
#             End If
#             If Select_Primary_MSG Is Nothing Then
#                 Select_Primary_MSG = Select_Primary_MSG
#             End If
#         Catch ex As Exception
#             BGWCalcState.ReportProgress(-1, "Primary Message Error: " & ex.Message)
#             Select_Primary_MSG = Nothing
#         End Try
#     End Function
#     Private Function CheckBWSNotAuto(ByVal Row As Integer) As Boolean
#         Dim _LastMSG_Date As DateTime = AMM_List(Row).MsgDate
#         CheckBWSNotAuto = False
#         For i = Row - 1 To 0 Step -1
#             If (_LastMSG_Date - AMM_List(i).MsgDate).TotalMilliseconds < 1500 Then
#                 If AMM_List(i).MsgNo = MSG_Trigger.BWS_Hand Or AMM_List(i).MsgNo = MSG_Trigger.BWS_AUS Then
#                     CheckBWSNotAuto = True
#                     Exit For
#                 End If
#             Else
#                 Exit For
#             End If
#         Next

#         If Not CheckBWSNotAuto Then
#             For i = Row + 1 To AMM_List.Count - 1
#                 If (AMM_List(i).MsgDate - _LastMSG_Date).TotalMilliseconds < 1500 Then
#                     If AMM_List(i).MsgNo = MSG_Trigger.BWS_Hand Or AMM_List(i).MsgNo = MSG_Trigger.BWS_AUS Then
#                         CheckBWSNotAuto = True
#                         Exit For
#                     End If
#                 Else
#                     Exit For
#                 End If
#             Next
#         End If
#     End Function
#     Private Sub BGWCalcState_ProgressChanged(sender As Object, e As ProgressChangedEventArgs) Handles BGWCalcState.ProgressChanged
#         If e.ProgressPercentage < 0 Then
#             RaiseEvent ErrorInClass(e.UserState.ToString)
#         End If
#     End Sub
# #End Region
# #End Region
# End Class
