Imports System.Net
Imports System.Threading
Imports Newtonsoft.Json

Module Main

#Region "VARs"

    Dim tFileProccess() As Thread
    Dim tCapabiltyProcess() As Thread
    Dim intRCSCapabilty As UInt32 = 100
    Dim strURL As String = ""
    Dim intRCSCapabilityTPM As UInt16 = 0
#End Region

#Region "Structure"
    Structure fileStruct
        Dim sno As Int64
        Dim uid As Int64
        Dim Mobile_nums As String
        Dim msg As String
        Dim strService_File As String
        Dim No_Of_SMSes_Processed As Int64
        Dim msg_from As Byte
        Dim Image_Mapped As String
    End Structure

#End Region

#Region "Check_capabilty"
    Public Class bulk_check_capability
        Public Property reachableUsers As String()
        Public Property rcsEnabledContacts As String()
    End Class
#End Region
    Sub Start_Main()
        Dim i As Byte, j As Int64 = 0, a As Int64 = 0
        Dim ds As DataSet = Nothing
        Dim rowsCnt As Int32 = 0
        Dim arr1() As String = Nothing, ax As Byte = 0
        Dim objFileStruct As fileStruct = New fileStruct
        Dim bytThreadsCnt As Byte = 0
        Dim bytBulkInsert As Byte = 0
        Try
            Dim doc As New System.Xml.XmlDocument
            Dim strDirPath As String = System.IO.Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().GetName().CodeBase)

            Dim os As OperatingSystem = Environment.OSVersion
            If os.VersionString.Contains("Windows") = False Then
                strDirPath = strDirPath.Replace("file:/", "/")
            Else
                strDirPath = strDirPath.Replace("file:\", "")
                IsOSWindows = True
            End If
            doc.Load(strDirPath & "/Threads.xml")

            Dim list = doc.GetElementsByTagName("Threads")
            For Each item As System.Xml.XmlElement In list
                bytThreadsCnt = CByte(item.InnerText)
            Next
            Dim list1 = doc.GetElementsByTagName("DBConStr")
            For Each item As System.Xml.XmlElement In list1
                strMySQLConn = item.InnerText
            Next
            Dim list2 = doc.GetElementsByTagName("BulkInsert")
            For Each item As System.Xml.XmlElement In list2
                bytBulkInsert = item.InnerText
            Next
            Dim list3 = doc.GetElementsByTagName("FetchRecords")
            For Each item As System.Xml.XmlElement In list3
                intFetchRecords = item.InnerText
            Next
            Dim list4 = doc.GetElementsByTagName("CommandTimeOut")
            For Each item As System.Xml.XmlElement In list4
                intCommandTimeOut = item.InnerText
            Next
            Dim list5 = doc.GetElementsByTagName("RCSCapability")
            For Each item As System.Xml.XmlElement In list5
                intRCSCapabilty = item.InnerText
            Next
            Dim list6 = doc.GetElementsByTagName("URL")
            For Each item As System.Xml.XmlElement In list6
                strURL = item.InnerText
            Next
            Dim list7 = doc.GetElementsByTagName("RCSCapabilityTPM")
            For Each item As System.Xml.XmlElement In list7
                intRCSCapabilityTPM = item.InnerText
            Next

            Call update_mnos_sms_data_IsPicked()
            ReDim tFileProccess(bytThreadsCnt - 1)
            'ReDim tCapabiltyProcess(bytThreadsCnt - 1)
            ReDim tCapabiltyProcess(0)

            tCapabiltyProcess(0) = New Threading.Thread(AddressOf tProcessRCS_Capabilty_Check)

            Dim tCapabiltyChecker As Thread = New Threading.Thread(AddressOf Capability_Checker)
            tCapabiltyChecker.Start()

            For i = 0 To tFileProccess.Count() - 1
                tFileProccess(i) = New Threading.Thread(AddressOf tProcessRCS_BULK)
                'tCapabiltyProcess(i) = New Threading.Thread(AddressOf tProcessRCS_Capabilty_Check)
            Next

            addLog("RCS Process Started on (" & Now() & ")")
            While 1
                i = 0
                ds = get_FileSMS_Data()
                Try
                    rowsCnt = ds.Tables(0).Rows.Count
                Catch ex As Exception
                    rowsCnt = 0
                    addLog(ex.ToString())
                End Try
                While i < rowsCnt
                    For ax = 0 To tFileProccess.Count() - 1
                        If tFileProccess(ax).ThreadState = 8 Or tFileProccess(ax).ThreadState = 16 Then
                            objFileStruct.sno = ds.Tables(0).Rows(i).Item("sno")
                            objFileStruct.uid = ds.Tables(0).Rows(i).Item("user_id")
                            objFileStruct.Mobile_nums = ds.Tables(0).Rows(i).Item("Mobile_nums")
                            objFileStruct.msg = ds.Tables(0).Rows(i).Item("msg")
                            objFileStruct.strService_File = ds.Tables(0).Rows(i).Item("service_file")
                            objFileStruct.No_Of_SMSes_Processed = ds.Tables(0).Rows(i).Item("No_Of_SMSes_Processed")
                            objFileStruct.msg_from = ds.Tables(0).Rows(i).Item("msg_from")
                            objFileStruct.Image_Mapped = ds.Tables(0).Rows(i).Item("image_mapped")
                            'If bytBulkInsert = 0 Then
                            '    tFileProccess(ax) = New Threading.Thread(AddressOf tProcessFileUpload)
                            'Else
                            '    tFileProccess(ax) = New Threading.Thread(AddressOf tProcessFileUpload_BULK)
                            'End If
                            tFileProccess(ax) = New Threading.Thread(AddressOf tProcessRCS_BULK)
                            tFileProccess(ax).Start(objFileStruct)
                            i = i + 1
                            If i >= rowsCnt Then Exit While
                        End If
                    Next
                    Thread.Sleep(1000)
                End While
                Threading.Thread.Sleep(1000)
            End While
        Catch ex As Exception
            addLog("Main: " & ex.ToString())
        End Try
    End Sub

    Private Sub Capability_Checker()
        Dim i As Byte, j As Int64 = 0, a As Int64 = 0
        Dim ds As DataSet = Nothing
        Dim rowsCnt As Int32 = 0
        Dim arr1() As String = Nothing, ax As Byte = 0
        Dim objFileStruct As fileStruct = New fileStruct
        Dim bytThreadsCnt As Byte = 0
        Try
            While 1
                i = 0
                ds = get_capabilty_Data()
                Try
                    rowsCnt = ds.Tables(0).Rows.Count
                Catch ex As Exception
                    rowsCnt = 0
                    addLog(ex.ToString())
                End Try
                While i < rowsCnt
                    For ax = 0 To tCapabiltyProcess.Count() - 1
                        If tCapabiltyProcess(ax).ThreadState = 8 Or tCapabiltyProcess(ax).ThreadState = 16 Then
                            objFileStruct.sno = ds.Tables(0).Rows(i).Item("sno")
                            objFileStruct.uid = ds.Tables(0).Rows(i).Item("user_id")
                            objFileStruct.Mobile_nums = ds.Tables(0).Rows(i).Item("Mobile_nums")
                            objFileStruct.No_Of_SMSes_Processed = ds.Tables(0).Rows(i).Item("No_Of_SMSes_Processed")

                            tCapabiltyProcess(ax) = New Threading.Thread(AddressOf tProcessRCS_Capabilty_Check)
                            tCapabiltyProcess(ax).Start(objFileStruct)
                            i = i + 1
                            If i >= rowsCnt Then Exit While
                        End If
                    Next
                    Thread.Sleep(100)
                End While
                Threading.Thread.Sleep(100)
            End While
        Catch ex As Exception
            addLog(ex.ToString())
        End Try
    End Sub

    Public Sub tProcessRCS_BULK(ByVal objFileStruct As fileStruct)
        Try
            Dim Lsno As Int64 = objFileStruct.sno
            Dim Luid As Int64 = objFileStruct.uid
            Dim LMobile_nums As String = objFileStruct.Mobile_nums
            Dim Lmsg As String = objFileStruct.msg
            Dim Lservice_file As String = objFileStruct.strService_File
            Dim LNo_Of_SMSes_Processed As Int64 = objFileStruct.No_Of_SMSes_Processed
            Dim Lmsg_from As Byte = objFileStruct.msg_from
            Dim Limage_mapped As String = objFileStruct.Image_Mapped
            Dim arr1() As String = Nothing, j As Int64, IsToAdd_One As Boolean = False, lngMno As UInt64 = 0
            Dim col1 As Int64 = 0, col2 As Int64 = 0, rVal As String = ""
            Dim strColsArr() As String = Nothing, strRetValArr() As String = Nothing
            Dim strServiceType As String = ""
            Dim strMNOs As String = "", ax As Int16 = 0
            Dim lMNOCount As UInt32 = 0

            If InStr(1, LMobile_nums, vbCrLf) > 0 Then
                arr1 = Split(LMobile_nums, vbCrLf)
            ElseIf InStr(1, LMobile_nums, vbLf) > 0 Then
                arr1 = Split(LMobile_nums, vbLf)
            Else
                arr1 = Split(LMobile_nums, ",")
            End If
            lMNOCount = arr1.Length()
            If lMNOCount <= 10 Then IsToAdd_One = True
            strServiceType = "C"
            Select Case Lmsg_from
                Case 2
                    strServiceType = "H"
            End Select

            addLog(Lsno & "- (" & lMNOCount & ") BULK Process Started @ (" & Now() & ")")
            'getLow_User_Cols(Lsno, col1, col2)
            For j = LNo_Of_SMSes_Processed To lMNOCount - 1
                If arr1(j).Trim() <> "" Then
                    If IsNumeric(arr1(j).Trim()) Then
                        Try
                            'If Mid(arr1(j), 1, 2) = "91" And arr1(j).Length > 11 Then
                            'arr1(j) = Right(arr1(j), 10)
                            'End If
                            'lngMno = CLng(Mid("91" & CStr(arr1(j).Trim()).Replace(Space(1), ""), 1, 12))


                            'If IsToAdd_One = True Then lngMno = CLng("1" & lngMno.ToString())
                            ''getLow_User_Cols(Lsno, col1, col2)
                            'If col1 > 0 Then
                            '    If col2 >= col1 Then
                            '        lngMno = CLng("3" & lngMno.ToString())
                            '    End If
                            'End If
                            lngMno = CLng(arr1(j).Trim())
                            If strMNOs = "" Then
                                strMNOs = CStr(lngMno)
                            Else
                                strMNOs = strMNOs & "," & CStr(lngMno)
                            End If
                            If ax >= 1000 Or j = lMNOCount - 1 Then
                                addLog(Lsno & " - Bulk Insert Process started for (" & ax & ") @ " & Now())
                                Call process_RCS_Data_BULK(strServiceType, Luid, strMNOs, Lmsg, Lsno, Limage_mapped, rVal)
                                addLog(Lsno & " - Bulk Insert Process ended for (" & ax & ") @ " & Now())
                                ax = 0
                                strMNOs = ""
                                'getLow_User_Cols(Lsno, col1, col2)
                            Else
                                ax = ax + 1
                            End If
                        Catch ex As Exception
                            ax = ax + 1
                            addLog(ex.ToString())
                        End Try
                    Else
                        addLog("Mobile Number Not A Numeric (" & CStr(arr1(j).Trim()).Replace(Space(1), "") & ")")
                        ax = ax + 1
                    End If
                End If
            Next

            Try
                If strMNOs <> "" Then
                    addLog("Bulk Insert Process started for (" & ax & ") @ " & Now())
                    Call process_RCS_Data_BULK(strServiceType, Luid, strMNOs, Lmsg, Lsno, Limage_mapped, rVal)
                    addLog("Bulk Insert Process ended for (" & ax & ") @ " & Now())
                End If
            Catch ex As Exception
                addLog(ex.ToString())
            End Try
            Call Delete_FileUploadData(Lsno)
            addLog("(" & lMNOCount & ") BULK Deleted File @ (" & Now() & ")")
            Erase arr1
        Catch ex As Exception
            addLog("Main: " & ex.ToString())
        End Try
    End Sub

    Public Sub tProcessRCS_Capabilty_Check(ByVal objFileStruct As fileStruct)
        Try
            Dim Lsno As Int64 = objFileStruct.sno
            Dim Luid As Int64 = objFileStruct.uid
            Dim LMobile_nums As String = objFileStruct.Mobile_nums

            Dim LNo_Of_SMSes_Processed As Int64 = objFileStruct.No_Of_SMSes_Processed
            Dim Lmsg_from As Byte = objFileStruct.msg_from
            Dim arr1() As String = Nothing, j As Int64, lngMno As UInt64 = 0

            Dim strColsArr() As String = Nothing, strRetValArr() As String = Nothing
            Dim strServiceType As String = ""
            Dim strMNOs As String = "", ax As Int16 = 0
            Dim lMNOCount As UInt32 = 0

            If InStr(1, LMobile_nums, vbCrLf) > 0 Then
                arr1 = Split(LMobile_nums, vbCrLf)
            ElseIf InStr(1, LMobile_nums, vbLf) > 0 Then
                arr1 = Split(LMobile_nums, vbLf)
            Else
                arr1 = Split(LMobile_nums, ",")
            End If


            strServiceType = "C"
            Select Case Lmsg_from
                Case 2
                    strServiceType = "H"
            End Select
            lMNOCount = arr1.Length()
            addLog(Lsno & "- (" & lMNOCount & ") Capabilty Process Started @ (" & Now() & ")")

            For j = LNo_Of_SMSes_Processed To lMNOCount - 1
                If arr1(j).Trim() <> "" Then
                    If IsNumeric(arr1(j).Trim()) Then
                        Try
                            If arr1(j).Trim().Length = 10 Then
                                lngMno = CLng("91" & arr1(j).Trim())
                            End If
                            If strMNOs = "" Then
                                strMNOs = "['" & CStr("+" & lngMno) & "'"
                            Else
                                strMNOs = strMNOs & ",'" & CStr("+" & lngMno) & "'"
                            End If
                            If ax >= intRCSCapabilty Or j = lMNOCount - 1 Then
                                strMNOs = strMNOs & "]"

                                addLog(Lsno & " - Bulk capability Process started for (" & ax & ") @ " & Now())
                                If ax >= intRCSCapabilty Then
                                    Call make_batch_cap_request(Luid, strMNOs, Lsno, 0, ax)
                                Else
                                    Call make_batch_cap_request(Luid, strMNOs, Lsno, 1, ax)
                                End If

                                addLog(Lsno & " - Bulk capability Process ended for (" & ax & ") @ " & Now())
                                ax = 0
                                strMNOs = ""
                                'Thread.Sleep(1000)
                                Thread.Sleep(60 * 1000 / intRCSCapabilityTPM)
                            Else
                                ax = ax + 1
                            End If
                        Catch ex As Exception
                            ax = ax + 1
                            addLog(ex.ToString())
                        End Try
                    Else
                        addLog("Mobile Number Not A Numeric (" & CStr(arr1(j).Trim()).Replace(Space(1), "") & ")")
                        ax = ax + 1
                    End If
                End If
            Next

            Try
                If strMNOs <> "" Then
                    strMNOs = strMNOs & "]"
                    addLog("Bulk capability Process started for (" & ax & ") @ " & Now())
                    Call make_batch_cap_request(Luid, strMNOs, Lsno, 1, ax)
                    addLog("Bulk capability Process ended for (" & ax & ") @ " & Now())
                End If
            Catch ex As Exception
                addLog(ex.ToString())
            End Try
            'Call Delete_FileUploadData(Lsno)
            'addLog("(" & arr1.Length & ") BULK Deleted File @ (" & Now() & ")")
            Erase arr1
        Catch ex As Exception
            addLog("Main: " & ex.ToString())
        End Try
    End Sub

    Private Sub make_batch_cap_request(pUID As UInt32, strMSISDNs As String, pSNO As UInt32, pIsFinal As UInt16, pCnt As UInt32)
        Try

            pUID = 124543
            Dim strBearer As String = get_user_token(pUID)

            Dim client As WebClient = New WebClient()
            client.Headers.Add("Content-Type", "application/json")
            client.Headers.Add("Authorization", "Bearer " & strBearer)


            'Dim response As String = client.UploadString("https: //asia-rcsbusinessmessaging.googleapis.com/v1/users:batchGet", "POST", "{'users':" & strMSISDNs & "}")
            'Dim response As String = client.UploadString(strURL, "POST", "{'users':" & strMSISDNs & "}")
            Dim response As String = client.UploadString(strURL, "POST", "{""users"":" & strMSISDNs.Replace("'", """") & "}")
            Dim objBulkCapabilty As bulk_check_capability = JsonConvert.DeserializeObject(Of bulk_check_capability)(response)
            'Return objBulkCapabilty.reachableUsers
            'addLog(String.Join(",", objBulkCapabilty.reachableUsers.ToArray()))
            'Call update_rcs_cap(pSNO, String.Join(",", objBulkCapabilty.reachableUsers.ToArray()), pIsFinal, pCnt)
            Call update_rcs_cap(pSNO, String.Join(",", objBulkCapabilty.rcsEnabledContacts.ToArray()), pIsFinal, pCnt)

        Catch ex1 As WebException
            addLog(ex1.ToString())
        Catch ex As Exception
            addLog(ex.ToString())
        End Try
    End Sub

End Module
