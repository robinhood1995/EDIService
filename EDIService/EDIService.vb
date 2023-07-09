Option Strict On
Imports log4net
Imports log4net.Config
Imports System.Configuration
Imports System.IO
Imports System.Reflection
Imports System.Security.AccessControl
Imports System.Security.Principal
Imports System.Threading
Imports System.Data.SqlClient
Imports Microsoft

Public Class EDIService
    Implements IDisposable

    Private Shared ReadOnly _log As ILog = LogManager.GetLogger(GetType(EDIService))

    Dim strStorageFolder As String = Environment.GetFolderPath(Environment.SpecialFolder.CommonApplicationData)
    Dim strWorkFolder As String = strStorageFolder & "\EDIService"
    Dim strLogFolder As String = strWorkFolder & "\Log"
    Dim intMonth As Integer = CInt(My.Settings.PurgeLogFilesMonth)

    Dim objDSMS As DataSet
    Dim objDTMS As DataTable
    Dim objDSMT As DataSet
    Dim objDTMT As DataTable
    Dim objDSP As DataSet
    Dim objDTP As DataTable

    Private stopping As Boolean
    Private stoppedEvent As ManualResetEvent


    Public Sub New()
        InitializeComponent()
        'Init the log for net settings (must have in 4.0 Framework)
        log4net.Config.XmlConfigurator.Configure()
        Me.stopping = False
        Me.stoppedEvent = New ManualResetEvent(False)

        StartUp()

    End Sub

#Region " On Start "
    ''' <summary>
    ''' The function is executed when a Start command is sent to the service
    ''' by the SCM or when the operating system starts (for a service that 
    ''' starts automatically). It specifies actions to take when the service 
    ''' starts. In this code sample, OnStart logs a service-start message to 
    ''' the Application log, and queues the main service function for 
    ''' execution in a thread pool worker thread.
    ''' </summary>
    ''' <param name="args">Command line arguments</param>
    ''' <remarks>
    ''' A service application is designed to be long running. Therefore, it 
    ''' usually polls or monitors something in the system. The monitoring is 
    ''' set up in the OnStart method. However, OnStart does not actually do 
    ''' the monitoring. The OnStart method must return to the operating 
    ''' system after the service's operation has begun. It must not loop 
    ''' forever or block. To set up a simple monitoring mechanism, one 
    ''' general solution is to create a timer in OnStart. The timer would 
    ''' then raise events in your code periodically, at which time your 
    ''' service could do its monitoring. The other solution is to spawn a 
    ''' new thread to perform the main service functions, which is 
    ''' demonstrated in this code sample.
    ''' </remarks>
    Protected Overrides Sub OnStart(ByVal args() As String)
        ' Log a service start message to the Application log.
        _log.Info("EDI Service in OnStart.")

        ' Queue the main service function for execution in a worker thread.
        ThreadPool.QueueUserWorkItem(New WaitCallback(AddressOf ServiceWorkerThread))

    End Sub
#End Region

#Region " Startup "
    Public Sub StartUp()
        Try

            'Dim applicationName As String = Environment.GetCommandLineArgs()(0)
            'Dim exePath As String = System.IO.Path.Combine(Environment.CurrentDirectory, applicationName)
            'Dim cfg As Configuration = ConfigurationManager.OpenExeConfiguration(exePath)
            ''XmlConfigurator.Configure(New System.IO.FileInfo(Application.ExecutablePath + ".config"))
            '_log.Info("Got the config file " & cfg.ToString)

            _log.Info("--------------------------------------------------------------------------------")
            _log.Info("New Startup Date: " & Format(Now(), "yyyy-MM-dd HH:mm:ss"))
            _log.Info("--------------------------------------------------------------------------------")

            'set folder to be accessable from authenticated users
            Dim folderinfolog As DirectoryInfo = New DirectoryInfo(strLogFolder)
            Dim folderinfowork As DirectoryInfo = New DirectoryInfo(strWorkFolder)

            'Check to make sure log folder is there first
            If Not folderinfolog.Exists Then
                Directory.CreateDirectory(strLogFolder)
                _log.Info("Created Folder " & folderinfolog.ToString)
            End If

            If Not folderinfowork.Exists Then
                Directory.CreateDirectory(strWorkFolder)
                _log.Info("Created Folder " & folderinfowork.ToString)
            End If

            _log.Info("Setting folder and files permissions")
            Dim folderacllog As New DirectorySecurity(strLogFolder, AccessControlSections.Access)
            folderacllog.AddAccessRule(New FileSystemAccessRule(New SecurityIdentifier(WellKnownSidType.BuiltinUsersSid, Nothing), FileSystemRights.FullControl, InheritanceFlags.ContainerInherit Or InheritanceFlags.ObjectInherit, PropagationFlags.None, AccessControlType.Allow))
            folderinfolog.SetAccessControl(folderacllog)

            Dim folderaclwork As New DirectorySecurity(strWorkFolder, AccessControlSections.Access)
            folderaclwork.AddAccessRule(New FileSystemAccessRule(New SecurityIdentifier(WellKnownSidType.BuiltinUsersSid, Nothing), FileSystemRights.FullControl, InheritanceFlags.ContainerInherit Or InheritanceFlags.ObjectInherit, PropagationFlags.None, AccessControlType.Allow))
            folderinfowork.SetAccessControl(folderaclwork)


            ' Queue the main service function for execution in a worker thread.
            ' Uncomment to run manually
            'ThreadPool.QueueUserWorkItem(New WaitCallback(AddressOf ServiceWorkerThread))

        Catch ex As Exception
            _log.Error(ex.ToString & vbCrLf & ex.StackTrace.ToString)

        End Try
    End Sub
#End Region

#Region "Loop Message Systems"
    Public Sub LoopMessageSystems()
        Try

            Dim builder As New SqlConnectionStringBuilder()
            builder.DataSource = My.Settings.MsSQLHostName
            _log.Info("Set the MsSQL Hostname to " & My.Settings.MsSQLHostName)
            builder.InitialCatalog = My.Settings.MsSQLDataBaseName
            _log.Info("Set the MsSQL Database Name to " & My.Settings.MsSQLDataBaseName)
            builder.IntegratedSecurity = True
            'builder.MultipleActiveResultSets = True

            Using comm As New SqlConnection(builder.ConnectionString)
                Try
                    comm.Open()
                    If comm.State.Open = 1 Then
                        _log.Info("Connected to SQL Database...")
                    Else
                        _log.Error("Not connected to SQL Database...")
                        Return
                    End If

                    'Clean up logs files
                    Dim logFiles As String() = Directory.GetFiles(strLogFolder)
                    For Each logFile As String In logFiles
                        Dim fileInfo As New FileInfo(logFile)

                        'set file to be accessable from authenticated users
                        'Dim fileacl As New FileSecurity(fileInfo.FullName, AccessControlSections.Access)
                        'fileacl.AddAccessRule(New FileSystemAccessRule(New SecurityIdentifier(WellKnownSidType.AuthenticatedUserSid, Nothing), FileSystemRights.FullControl, AccessControlType.Allow))
                        'fileInfo.SetAccessControl(fileacl)

                        If fileInfo.CreationTime < DateTime.Now.AddMonths(intMonth * -1) Then
                            _log.Info("Found Log file(s) that are more then " & intMonth * -1 & " month old to delete " & fileInfo.ToString)
                            fileInfo.Delete()
                        Else
                            _log.Info("This file" & fileInfo.Name.ToString & " is not older then " & My.Settings.PurgeLogFilesMonth & " month")
                        End If

                    Next

                    'Clean up work files
                    Dim strFiles As String() = Directory.GetFiles(strWorkFolder)
                    For Each strFile As String In strFiles
                        Dim fileInfo As New FileInfo(strFile)

                        'set file to be accessable from authenticated users
                        'Dim fileacl As New FileSecurity(fileInfo.FullName, AccessControlSections.Access)
                        'fileacl.AddAccessRule(New FileSystemAccessRule(New SecurityIdentifier(WellKnownSidType.AuthenticatedUserSid, Nothing), FileSystemRights.FullControl, AccessControlType.Allow))
                        'fileInfo.SetAccessControl(fileacl)

                        If fileInfo.CreationTime < DateTime.Now.AddMonths(intMonth * -1) Then
                            _log.Info("Found file(s) that are more then " & intMonth * -1 & " month old to delete " & fileInfo.ToString)
                            fileInfo.Delete()
                            _log.Info("Deleted the file " & fileInfo.Name.ToString)
                        End If
                    Next

                    _log.Info("Building a list of active message systems")
                    Dim command As SqlCommand = Nothing
                    Dim sqlda As SqlDataAdapter = Nothing

                    Dim strSQL = "select * from mscmessagesystem where adapter='Folder' and status =0"
                    '_log.Info(strSQL)

                    command = New SqlCommand(strSQL, comm)
                    command.CommandType = CommandType.Text
                    command.CommandTimeout = 0
                    sqlda = New SqlDataAdapter
                    sqlda.SelectCommand = command
                    objDSMS = New DataSet
                    sqlda.Fill(objDSMS)

                    objDTMS = objDSMS.Tables(0)
                    objDTP = New DataTable
                    objDTP.Columns.Add("SectionID", Type.GetType("System.Int32"))
                    objDTP.Columns.Add("Name", Type.GetType("System.String"))
                    objDTP.Columns.Add("Value", Type.GetType("System.String"))

                    Dim pr As DataRow = objDTP.NewRow

                    Dim intInputFileCount As Int32 = 0
                    Dim strInputFolder As String = ""
                    Dim strEDIServiceFolder As String

                    For Each msr As DataRow In objDTMS.Rows

                        _log.Info("Looking at message system parameter called " & msr.Item("Name").ToString)

                        strSQL = "select sectionID,[Name],Value from infParameter where sectionID = " & msr.Item("sectionID").ToString & " and ([name] like 'Input Folder Name%' or [name] like 'EDIService Folder%') order by ID asc"
                        '_log.Info(strSQL)

                        command = New SqlCommand(strSQL, comm)
                        command.CommandType = CommandType.Text
                        command.CommandTimeout = 0
                        sqlda = New SqlDataAdapter
                        sqlda.SelectCommand = command
                        objDSP = New DataSet
                        sqlda.Fill(objDSP)
                        objDTP = objDSP.Tables(0)

                        For Each r As DataRow In objDTP.Rows

                            If objDTP.Rows.Count < 2 Then
                                _log.Info("Looks like we do not have a EDIService Folder setup")
                                strSQL = "INSERT INTO [dbo].[infParameter]
                                       ([mainttime]
                                       ,[userID]
                                       ,[name]
                                       ,[scope]
                                       ,[value]
                                       ,[comments]
                                       ,[sectionID]
                                       ,[helpcontextid]
                                       ,[isaudited]
                                       ,[datatype]
                                       ,[allowpersonalvalues]
                                       ,[allowgroupvalues]
                                       ,[allowplantvalues]
                                       ,[parametertype]
                                       ,[obsolete]
                                       ,[enumeration]
                                       ,[businessClassID]
                                       ,[configurationchangereference]
                                       ,[configurationnotes])
                                 VALUES
                                       (getdate(),system_user,'EDIService Folder',0,'',null," & r.Item("SectionID").ToString & ",null,-1,0,null,null,null,0,null,null,null,null,null)"
                                command = New SqlCommand(strSQL, comm)
                                command.CommandType = CommandType.Text
                                command.CommandTimeout = 0
                                command.ExecuteNonQuery()
                                _log.Info("Added the EDIService folder to the section ID " & r.Item("SectionID").ToString)
                                Exit For
                            End If

                            _log.Info("Looking at infparameter row ID " & String.Join(", ", r.ItemArray))


                            If r.Item("Name").ToString = "Input Folder Name" Then
                                If IsDBNull(r.Item("value")) Then
                                    _log.Info("The input folder is null for message system named " & msr.Item("Name").ToString & ", you must have it set moving on to the next one")
                                    Exit For
                                End If
                                If r.Item("Name").ToString = "Input Folder Name" AndAlso r.Item("Value").ToString = Nothing Then
                                    _log.Info("The input folder does not have a value for message system named " & msr.Item("Name").ToString & ", you must have it set")
                                    Exit For
                                Else
                                    _log.Info("The input folder does have a value set as " & r.Item("Value").ToString & " continue to verify the Input Folder")
                                    strInputFolder = r.Item("value").ToString
                                    If Not strInputFolder = Nothing Then
                                        If Directory.Exists(strInputFolder) = False Then
                                            _log.Info("Creating directory " & strInputFolder)
                                            Directory.CreateDirectory(strInputFolder)
                                            _log.Info("Created directory " & strInputFolder)
                                        Else
                                            _log.Info("Folder already exists " & strInputFolder)
                                        End If
                                    Else
                                        _log.Info("Folder value is blank, quiting and moving on to the next folder")
                                        Exit For
                                    End If

                                    'look if file exist to be processed
                                    _log.Info("Looking in folder: " & strInputFolder)

                                    _log.Info("Sorting the files in write time ascending order")
                                    Dim files = From file In New DirectoryInfo(strInputFolder).GetFileSystemInfos Where file.Name Like "*.*"
                                                Order By file.LastWriteTime Ascending Select file
                                    _log.Info("Done sorting the files in write time ascending order")

                                    If files.Count > 0 Then
                                        intInputFileCount = files.Count
                                        _log.Info("Found " & files.Count.ToString & " files..., so we are exiting for this message system until files are processed by KMC")
                                    Else
                                        _log.Info("Did not find any files..., lets go look to see if there are any files waiting to be moved")
                                    End If
                                End If
                            End If


                            If r.Item("Name").ToString = "EDIService Folder" AndAlso intInputFileCount = 0 Then
                                If IsDBNull(r.Item("value")) Then
                                    _log.Info("The EDIService folder is null for message system named " & msr.Item("Name").ToString & ", you must have it set moving on to the next one")
                                    Exit For
                                End If
                                If r.Item("Name").ToString = "EDIService Folder" AndAlso r.Item("Value").ToString = Nothing Then
                                    _log.Info("The EDIService folder does not have a value for message system named " & msr.Item("Name").ToString & ", you must have it set")
                                    Exit For
                                Else
                                    _log.Info("The EDIService folder does have a value set as " & r.Item("Value").ToString & " continue to verify the ESIService Folder")
                                    strEDIServiceFolder = r.Item("value").ToString
                                    If Not strEDIServiceFolder = Nothing Then
                                        If Directory.Exists(strEDIServiceFolder) = False Then
                                            _log.Info("Creating directory " & strEDIServiceFolder)
                                            Directory.CreateDirectory(strEDIServiceFolder)
                                            _log.Info("Created directory " & strEDIServiceFolder)
                                        Else
                                            _log.Info("Folder already exists " & strEDIServiceFolder)
                                        End If
                                    Else
                                        _log.Info("Folder value is blank, quiting and moving on to the next folder")
                                        Exit For
                                    End If

                                    'look if file exist to be processed
                                    _log.Info("Looking in folder: " & strEDIServiceFolder)

                                    Dim files = From file In New DirectoryInfo(strEDIServiceFolder).GetFileSystemInfos Where file.Name Like "*.*"
                                                Order By file.LastWriteTime Ascending Select file

                                    If files.Count > 0 Then
                                        _log.Info("Found " & files.Count.ToString & " files... going to move on file ")
                                        File.Copy(strEDIServiceFolder & "\" & files(0).Name, strInputFolder & "\" & files(0).Name)
                                        _log.Info("Copied file " & strEDIServiceFolder & "\" & files(0).Name & " to " & strInputFolder & "\" & files(0).Name)
                                        File.Delete(strEDIServiceFolder & "\" & files(0).Name)
                                        _log.Info("Deleted file " & strEDIServiceFolder & "\" & files(0).Name)
                                    Else
                                        _log.Info("Did not find any files to process...")
                                    End If
                                End If
                            Else
                                intInputFileCount = 0
                                _log.Info("Resetting the file count in that folder of " & intInputFileCount & " to 0")
                            End If

                            'Dim _Dir As New DirectoryInfo(r.Item("value"))

                            'If Directory.Exists(r.Item("value")) Then
                            '    _log.Info("Looking in folder: " & _Dir.ToString)
                            '    Dim files = From file In _Dir.GetFileSystemInfos Where file.Name Like "*.*"
                            '                Order By file.CreationTime Ascending Select file

                            '    _log.Info("Found " & files.Count.ToString & " files...")


                            'End If

                        Next
                        objDSP.Clear()
                        objDTP.Clear()

                    Next

                Catch ex As Exception
                    _log.Error(ex.ToString & vbCrLf & ex.StackTrace.ToString)
                Finally
                    comm.Close()
                    _log.Info("Closed the connection")
                End Try
                GC.Collect()
                GC.WaitForPendingFinalizers()
            End Using

        Catch ex As Exception
            _log.Error(ex.ToString & vbCrLf & ex.StackTrace.ToString)
        Finally

        End Try

    End Sub
#End Region

#Region " Service Work Thread "
    ''' <summary>
    ''' The method performs the main function of the service. It runs on a 
    ''' thread pool worker thread.
    ''' </summary>
    ''' <param name="state"></param>
    Private Sub ServiceWorkerThread(ByVal state As Object)
        ' Periodically check if the service is stopping.
        Do While Not Me.stopping

            _log.Info("We are starting to loop through the message systems")
            LoopMessageSystems()
            _log.Info("We are finished looping through the message systems")
            _log.Info("Sent to garbage collector")

            Dispose(True)
            ' Perform main service function here...
            Dim intMin = CInt(My.Settings.TimerMilliseconds)
            _log.Info("Sleeping for " & intMin / 1000 & " seconds")
            Thread.Sleep(intMin)  ' Simulate some lengthy operations.
        Loop

        ' Signal the stopped event.
        Me.stoppedEvent.Set()

    End Sub
#End Region

#Region " On Stop "
    ''' <summary>
    ''' The function is executed when a Stop command is sent to the service 
    ''' by SCM. It specifies actions to take when a service stops running. In 
    ''' this code sample, OnStop logs a service-stop message to the 
    ''' Application log, and waits for the finish of the main service 
    ''' function.
    ''' </summary>
    Protected Overrides Sub OnStop()
        ' Log a service stop message to the Application log.
        _log.Info("EDIService in OnStop.")

        ' Indicate that the service is stopping and wait for the finish of 
        ' the main service function (ServiceWorkerThread).
        Me.stopping = True
        Me.stoppedEvent.WaitOne()
    End Sub
#End Region

End Class
