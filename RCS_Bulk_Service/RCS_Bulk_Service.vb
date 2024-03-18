Public Class RCS_Bulk_Service
    Dim t As Threading.Thread = New Threading.Thread(AddressOf Start_Main)
    Protected Overrides Sub OnStart(ByVal args() As String)
        ' Add code here to start your service. This method should set things
        ' in motion so your service can do its work.
        t.Start()
    End Sub

    Protected Overrides Sub OnStop()
        ' Add code here to perform any tear-down necessary to stop your service.
        End
    End Sub

End Class
