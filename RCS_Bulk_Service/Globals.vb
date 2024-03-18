Imports System.IO
Module Globals
    Public objLogSyncLock As Object = New Object()
    Public objLogSyncLock_1 As Object = New Object()
    Public strMySQLConn As String = ""
    Public intFetchRecords As Int16 = 10
    Public intCommandTimeOut As Int16 = 60
    Public IsOSWindows As Boolean = False

    Public Sub addLog(ByVal strMsg As String)
        Try
            SyncLock objLogSyncLock
                Try
                    'If Smpp_Log(strMsg, GID) = 0 Then Exit Sub
                Catch ex As Exception
                    '
                End Try
                Dim swriter As StreamWriter
                Dim strDirPath As String = System.IO.Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().GetName().CodeBase)
                If IsOSWindows = False Then
                    strDirPath = strDirPath.Replace("file:/", "/")
                Else
                    strDirPath = strDirPath.Replace("file:\", "")
                End If
                'strDirPath = strDirPath.Replace("file:/", "")
                swriter = File.AppendText(strDirPath & "/Log" & Now().ToString("yyyyMMdd") & ".txt")
                swriter.WriteLine(strMsg)
                swriter.Close()
            End SyncLock
        Catch ex As Exception
            '
        End Try
    End Sub


End Module
