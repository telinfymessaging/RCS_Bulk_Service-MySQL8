#Region "Imports"
Imports System
Imports System.Collections.Generic
Imports System.Linq
Imports System.Text
Imports MySql.Data
Imports MySql.Data.MySqlClient
Imports System.Data.SqlClient
#End Region
Module DBFunctions

    Public Function update_mnos_sms_data_IsPicked() As Boolean
        Dim con As MySqlConnection = New MySqlConnection(strMySQLConn)
        Try
            con.Open()
            Dim cmd As MySqlCommand = New MySqlCommand("rcs_update_mnos_data_IsPicked", con)
            cmd.CommandType = CommandType.StoredProcedure

            cmd.ExecuteNonQuery()
            addLog("Updated Is Picked Column")
            Return True
        Catch ex As Exception
            addLog("rcs_update_mnos_data_IsPicked:" & ex.ToString())
            Return False
        Finally
            If con.State = ConnectionState.Open Then con.Close()
        End Try
    End Function

    Public Function get_FileSMS_Data() As DataSet
        Dim con As MySqlConnection = New MySqlConnection(strMySQLConn)
        Dim da As MySqlDataAdapter = New MySqlDataAdapter
        Dim ds As DataSet = New DataSet
        Try
            con.Open()
            Dim cmd As MySqlCommand = New MySqlCommand("RCS_Get_BulkData", con)
            cmd.CommandType = CommandType.StoredProcedure

            cmd.Parameters.AddWithValue("@Num", intFetchRecords)
            cmd.Parameters("@Num").DbType = DbType.Int32
            cmd.Parameters("@Num").Direction = ParameterDirection.Input

            da.SelectCommand = cmd
            da.Fill(ds)
            Return ds
        Catch ex As Exception
            addLog("GFSD:" & ex.ToString)
            Return ds
        Finally
            If con.State = ConnectionState.Open Then con.Close()
        End Try
    End Function
    Public Sub Delete_FileUploadData(ByVal SN As Int64)
        Dim con As MySqlConnection = New MySqlConnection(strMySQLConn)
        Try
            con.Open()
            Dim cmd As MySqlCommand = New MySqlCommand("RCS_Delete_BulkData", con)
            cmd.CommandType = CommandType.StoredProcedure

            cmd.Parameters.AddWithValue("@slno", SN)
            cmd.Parameters("@slno").DbType = DbType.Int64
            cmd.Parameters("@slno").Direction = ParameterDirection.Input

            cmd.Parameters.AddWithValue("@action1", 1)
            cmd.Parameters("@action1").DbType = DbType.UInt16
            cmd.Parameters("@action1").Direction = ParameterDirection.Input

            cmd.ExecuteNonQuery()
        Catch ex As Exception
            addLog("DFUD:" & ex.ToString())
        Finally
            If con.State = ConnectionState.Open Then con.Close()
        End Try
    End Sub
    Public Function process_RCS_Data_BULK(ByVal strServiceType As String, ByVal Uid As Int64, ByVal MNOs As String, ByVal RCS_data As String, intSLNO As UInt32, pImage_Mapped As String, ByRef rVal As String) As Boolean
        Dim con As MySqlConnection = New MySqlConnection(strMySQLConn)
        'SyncLock objLogSyncLock_1
        Try
            con.Open()
            Dim cmd As MySqlCommand = New MySqlCommand("RCS_BULK_COMPOSE_SMS", con)

            cmd.CommandType = CommandType.StoredProcedure
            cmd.CommandTimeout = intCommandTimeOut

            cmd.Parameters.AddWithValue("@ServiceType", strServiceType)
            cmd.Parameters("@ServiceType").DbType = DbType.String
            cmd.Parameters("@ServiceType").Direction = ParameterDirection.Input

            cmd.Parameters.AddWithValue("@UID", Uid)
            cmd.Parameters("@UID").DbType = DbType.UInt32
            cmd.Parameters("@UID").Direction = ParameterDirection.Input

            cmd.Parameters.AddWithValue("@MNOs", MNOs)
            cmd.Parameters("@MNOs").DbType = DbType.String
            cmd.Parameters("@MNOs").Direction = ParameterDirection.Input

            cmd.Parameters.AddWithValue("@rcs_data", RCS_data)
            cmd.Parameters("@rcs_data").DbType = DbType.String
            cmd.Parameters("@rcs_data").Direction = ParameterDirection.Input


            cmd.Parameters.AddWithValue("@pSNO", intSLNO)
            cmd.Parameters("@pSNO").DbType = DbType.UInt32
            cmd.Parameters("@pSNO").Direction = ParameterDirection.Input

            cmd.Parameters.AddWithValue("@pImage_Mapped", pImage_Mapped)
            cmd.Parameters("@pImage_Mapped").DbType = DbType.String
            cmd.Parameters("@pImage_Mapped").Direction = ParameterDirection.Input


            cmd.Parameters.Add("@RetVal", MySqlDbType.String)
            cmd.Parameters("@RetVal").DbType = DbType.String
            cmd.Parameters("@RetVal").Direction = ParameterDirection.Output

            cmd.ExecuteNonQuery()

            rVal = cmd.Parameters("@RetVal").Value.ToString()
            If InStr(1, rVal, "Error:") > 0 Then addLog(rVal)
            Return True
        Catch ex As Exception
            addLog("BULK INSERT:" & ex.ToString())
            addLog("MNos: " & MNOs)
            addLog("Message: " & RCS_data)
            Return False
        Finally
            If con.State = ConnectionState.Open Then con.Close()
        End Try
        'End SyncLock
    End Function

    Public Function get_capabilty_Data() As DataSet
        Dim con As MySqlConnection = New MySqlConnection(strMySQLConn)
        Dim da As MySqlDataAdapter = New MySqlDataAdapter
        Dim ds As DataSet = New DataSet
        Try
            con.Open()
            Dim cmd As MySqlCommand = New MySqlCommand("RCS_Get_CapabiltyData", con)
            cmd.CommandType = CommandType.StoredProcedure

            cmd.Parameters.AddWithValue("@Num", intFetchRecords)
            cmd.Parameters("@Num").DbType = DbType.Int32
            cmd.Parameters("@Num").Direction = ParameterDirection.Input

            da.SelectCommand = cmd
            da.Fill(ds)
            Return ds
        Catch ex As Exception
            addLog("GCD:" & ex.ToString)
            Return ds
        Finally
            If con.State = ConnectionState.Open Then con.Close()
        End Try
    End Function

    Public Function get_user_token(pUID As UInt32) As String
        Dim con As MySqlConnection = New MySqlConnection(strMySQLConn)
        Dim da As MySqlDataAdapter = New MySqlDataAdapter
        Dim ds As DataSet = New DataSet
        Try
            con.Open()
            Dim cmd As MySqlCommand = New MySqlCommand("rcs_get_bearer", con)
            cmd.CommandType = CommandType.StoredProcedure

            cmd.Parameters.AddWithValue("@pUID", pUID)
            cmd.Parameters("@pUID").DbType = DbType.UInt32
            cmd.Parameters("@pUID").Direction = ParameterDirection.Input

            Dim returnParam As MySqlParameter = cmd.Parameters.Add("@RETURN_VALUE", MySqlDbType.String)
            returnParam.Direction = System.Data.ParameterDirection.ReturnValue
            cmd.ExecuteNonQuery()

            Return returnParam.Value
        Catch ex As Exception
            addLog("GUT:" & ex.ToString)
            Return ""
        Finally
            If con.State = ConnectionState.Open Then con.Close()
        End Try
    End Function

    Public Function update_rcs_cap(pSNO As UInt32, pMNOs As String, pIsFinal As UInt16, pCNT As UInt32) As Boolean
        Dim con As MySqlConnection = New MySqlConnection(strMySQLConn)
        Try

            con.Open()
            Dim cmd As MySqlCommand = New MySqlCommand("rcs_update_make_batch_cap", con)
            cmd.CommandType = CommandType.StoredProcedure
            cmd.Parameters.AddWithValue("@pSNO", pSNO)
            cmd.Parameters("@pSNO").DbType = DbType.UInt32
            cmd.Parameters("@pSNO").Direction = ParameterDirection.Input

            cmd.Parameters.AddWithValue("@pMNOs", pMNOs)
            cmd.Parameters("@pMNOs").DbType = DbType.String
            cmd.Parameters("@pMNOs").Direction = ParameterDirection.Input

            cmd.Parameters.AddWithValue("@isFinal", pIsFinal)
            cmd.Parameters("@isFinal").DbType = DbType.UInt16
            cmd.Parameters("@isFinal").Direction = ParameterDirection.Input

            cmd.Parameters.AddWithValue("@pCNT", pCNT)
            cmd.Parameters("@pCNT").DbType = DbType.UInt16
            cmd.Parameters("@pCNT").Direction = ParameterDirection.Input

            cmd.ExecuteNonQuery()
            Return True
        Catch ex As Exception
            addLog("update_rcs_cap:" & ex.ToString())
            Return False
        Finally
            If con.State = ConnectionState.Open Then con.Close()
        End Try
    End Function

End Module
