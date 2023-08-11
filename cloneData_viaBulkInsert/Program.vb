Imports System
Imports System.Data
Imports System.Data.Common
Imports System.IO
Imports System.Threading
Imports MySqlConnector
Imports Mysqlx

Module Program

    'Public connStr_From As String = "server=10.10.60.104;userid=root;password=P@ssw0rD;database=ordering-sg-2ndwos"
    ''Public connStr_From As String = "server=10.10.60.224;userid=root;password=P@ssw0rd;database=ordering-sg-224"
    'Public connStr_To As String = "server=10.10.60.224;userid=root;password=P@ssw0rd;database=ecommerce"

    '' Production
    'Public connStr_From As String = "server=10.74.201.244;userid=2ndWOS;password=Ujmnhy78;database=ordering-sg"
    'Public connStr_To As String = "server=172.24.13.21;userid=root;password=P@ssw0rd;database=ecommerce"

    Public connStr_From As String = "server=10.74.201.154;userid=2ndWOS;password=Qazxsw21;database=ordering-sg"
    Public connStr_To As String = "server=172.24.13.21;userid=root;password=P@ssw0rd;database=ecommerce"

    Public countingTime As String = "TotalSeconds"

    Sub Main(args As String())
        Console.WriteLine("im - Main.")
        Dim work_on_table As String
        Dim new_tb_name As String

        ' 1. Get Data
        ' 2. sent DT to > bulkCopy.WriteToServer

        Dim dt As DataTable
        Dim res As Boolean

        Dim startTime_AtStep As DateTime
        Dim startTime_OverAll As DateTime = DateTime.Now

        work_on_table = "shoppingcart"
        Console.WriteLine("\n {0}", work_on_table)
        startTime_AtStep = DateTime.Now
        startTime_AtStep = DateTime.Now
        Console.WriteLine(String.Format("0-1-10 Connect for get data : Table: >{0}<", work_on_table))
        dt = SQL_db_dumptable_as_datatable(work_on_table)
        Console.WriteLine(String.Format("0-1-11 Step Get Data ({0} Rows), Use Time ({1} {2})", dt.Rows.Count, (DateTime.Now - startTime_AtStep).TotalSeconds, countingTime))

        startTime_AtStep = DateTime.Now
        new_tb_name = "0" & work_on_table & "_new"
        Console.WriteLine(String.Format("0-1-20 Connect for Insert ! data : Table: >{0}<", new_tb_name))
        res = sql_db_write_new_datatable_to_db(dt, new_tb_name)
        Console.WriteLine(String.Format("0-1-21 Step Insert Result >{0}<, Use Time ({1} {2})", res.ToString & IIf(res, "", "!!!!!!!!!!!!!!!!!"), (DateTime.Now - startTime_AtStep).TotalSeconds, countingTime))

        ' ---------------------------------------------------

        work_on_table = "supshoppingcart"
        Console.WriteLine("\n {0}", work_on_table)
        startTime_AtStep = DateTime.Now
        startTime_AtStep = DateTime.Now
        Console.WriteLine(String.Format("0-2-10 Connect for get data : Table: >{0}<", work_on_table))
        dt = SQL_db_dumptable_as_datatable(work_on_table)
        Console.WriteLine(String.Format("0-2-11 Step Get Data ({0} Rows), Use Time ({1} {2})", dt.Rows.Count, (DateTime.Now - startTime_AtStep).TotalSeconds, countingTime))

        startTime_AtStep = DateTime.Now
        new_tb_name = "0" & work_on_table & "_new"
        Console.WriteLine(String.Format("0-2-20 Connect for Insert ! data : Table: >{0}<", new_tb_name))
        res = sql_db_write_new_datatable_to_db(dt, new_tb_name)
        Console.WriteLine(String.Format("0-2-21 Step Insert Result >{0}<, Use Time ({1} {2})", res.ToString & IIf(res, "", "!!!!!!!!!!!!!!!!!"), (DateTime.Now - startTime_AtStep).TotalSeconds, countingTime))

        ' ---------------------------------------------------

        'work_on_table = "supcatalogue"
        'Console.WriteLine("\n {0}", work_on_table)
        'startTime_AtStep = DateTime.Now
        'startTime_AtStep = DateTime.Now
        'Console.WriteLine(String.Format("0-1-10 Connect for get data : Table: >{0}<", work_on_table))
        'dt = SQL_db_dumptable_as_datatable(work_on_table)
        'Console.WriteLine(String.Format("0-1-11 Step Get Data ({0} Rows), Use Time ({1} {2})", dt.Rows.Count, (DateTime.Now - startTime_AtStep).TotalSeconds, countingTime))

        'startTime_AtStep = DateTime.Now
        'new_tb_name = "0" & work_on_table & "_new"
        'Console.WriteLine(String.Format("0-1-20 Connect for Insert ! data : Table: >{0}<", new_tb_name))
        'res = sql_db_write_new_datatable_to_db(dt, new_tb_name)
        'Console.WriteLine(String.Format("0-1-21 Step Insert Result >{0}<, Use Time ({1} {2})", res.ToString & IIf(res, "", "!!!!!!!!!!!!!!!!!"), (DateTime.Now - startTime_AtStep).TotalSeconds, countingTime))

        '' ---------------------------------------------------

        'work_on_table = "supprice"
        'Console.WriteLine("\n {0}", work_on_table)
        'startTime_AtStep = DateTime.Now
        'startTime_AtStep = DateTime.Now
        'Console.WriteLine(String.Format("0-2-10 Connect for get data : Table: >{0}<", work_on_table))
        'dt = SQL_db_dumptable_as_datatable(work_on_table)
        'Console.WriteLine(String.Format("0-2-11 Step Get Data ({0} Rows), Use Time ({1} {2})", dt.Rows.Count, (DateTime.Now - startTime_AtStep).TotalSeconds, countingTime))

        'startTime_AtStep = DateTime.Now
        'new_tb_name = "0" & work_on_table & "_new"
        'Console.WriteLine(String.Format("0-2-20 Connect for Insert ! data : Table: >{0}<", new_tb_name))
        'res = sql_db_write_new_datatable_to_db(dt, new_tb_name)
        'Console.WriteLine(String.Format("0-2-21 Step Insert Result >{0}<, Use Time ({1} {2})", res.ToString & IIf(res, "", "!!!!!!!!!!!!!!!!!"), (DateTime.Now - startTime_AtStep).TotalSeconds, countingTime))

        '' ---------------------------------------------------

        'work_on_table = "bm008pr"
        'Console.WriteLine("\n {0}", work_on_table)
        'startTime_AtStep = DateTime.Now
        'startTime_AtStep = DateTime.Now
        'Console.WriteLine(String.Format("1-1-10 Connect for get data : Table: >{0}<", work_on_table))
        'dt = SQL_db_dumptable_as_datatable(work_on_table)
        'Console.WriteLine(String.Format("1-1-11 Step Get Data ({0} Rows), Use Time ({1} {2})", dt.Rows.Count, (DateTime.Now - startTime_AtStep).TotalSeconds, countingTime))

        'startTime_AtStep = DateTime.Now
        'new_tb_name = "0" & work_on_table & "_new"
        'Console.WriteLine(String.Format("1-1-20 Connect for Insert ! data : Table: >{0}<", new_tb_name))
        'res = sql_db_write_new_datatable_to_db(dt, new_tb_name)
        'Console.WriteLine(String.Format("1-1-21 Step Insert Result >{0}<, Use Time ({1} {2})", res.ToString & IIf(res, "", "!!!!!!!!!!!!!!!!!"), (DateTime.Now - startTime_AtStep).TotalSeconds, countingTime))

        '' ---------------------------------------------------

        'work_on_table = "catalogue"
        'Console.WriteLine("\n {0}", work_on_table)
        'startTime_AtStep = DateTime.Now
        'startTime_AtStep = DateTime.Now
        'Console.WriteLine(String.Format("1-2-10 Connect for get data : Table: >{0}<", work_on_table))
        'dt = SQL_db_dumptable_as_datatable(work_on_table)
        'Console.WriteLine(String.Format("1-2-11 Step Get Data ({0} Rows), Use Time ({1} {2})", dt.Rows.Count, (DateTime.Now - startTime_AtStep).TotalSeconds, countingTime))

        'startTime_AtStep = DateTime.Now
        'new_tb_name = "0" & work_on_table & "_new"
        'Console.WriteLine(String.Format("1-2-20 Connect for Insert ! data : Table: >{0}<", new_tb_name))
        'res = sql_db_write_new_datatable_to_db(dt, new_tb_name)
        'Console.WriteLine(String.Format("1-2-21 Step Insert Result >{0}<, Use Time ({1} {2})", res.ToString & IIf(res, "", "!!!!!!!!!!!!!!!!!"), (DateTime.Now - startTime_AtStep).TotalSeconds, countingTime))

        '' ---------------------------------------------------

        'work_on_table = "sellprice"
        'Console.WriteLine("\n {0}", work_on_table)
        'startTime_AtStep = DateTime.Now
        'startTime_AtStep = DateTime.Now
        'Console.WriteLine(String.Format("1-3-10 Connect for get data : Table: >{0}<", work_on_table))
        'dt = SQL_db_dumptable_as_datatable(work_on_table)
        'Console.WriteLine(String.Format("1-3-11 Step Get Data ({0} Rows), Use Time ({1} {2})", dt.Rows.Count, (DateTime.Now - startTime_AtStep).TotalSeconds, countingTime))

        'startTime_AtStep = DateTime.Now
        'new_tb_name = "0" & work_on_table & "_new"
        'Console.WriteLine(String.Format("1-3-20 Connect for Insert ! data : Table: >{0}<", new_tb_name))
        'res = sql_db_write_new_datatable_to_db(dt, new_tb_name)
        'Console.WriteLine(String.Format("1-3-21 Step Insert Result >{0}<, Use Time ({1} {2})", res.ToString & IIf(res, "", "!!!!!!!!!!!!!!!!!"), (DateTime.Now - startTime_AtStep).TotalSeconds, countingTime))

        '' ---------------------------------------------------

        dt = New DataTable
        Console.WriteLine(String.Format("++++++++ Step Over All, Use Time ({1} {2})", dt.Rows.Count, (DateTime.Now - startTime_OverAll).TotalSeconds, countingTime))
        'Thread.Sleep(500000)
    End Sub

    Public Function SQL_db_dumptable_as_datatable(db_working_table As String) As DataTable
        Dim sql_connection As String = connStr_From

        Dim op As String = ""
        Dim dt_op As New DataTable
        Dim dt_table_schema As New DataTable
        Dim row As DataRow

        Dim db_connection As MySqlConnection = New MySqlConnection(sql_connection)

        Using db_connection
            db_connection.Open()
            Dim db_table_select As String = Trim("select * from " + db_working_table + ";")
            Dim db_command As MySqlCommand = New MySqlCommand(db_table_select, db_connection)
            Dim reader As MySqlDataReader
            reader = db_command.ExecuteReader()

            dt_table_schema = reader.GetSchemaTable()

            For Each row_in_schema As DataRow In dt_table_schema.Rows
                dt_op.Columns.Add(New DataColumn(row_in_schema("ColumnName"), row_in_schema("DataType")))
            Next

            While reader.Read()
                row = dt_op.NewRow()
                For j As Integer = 0 To (reader.VisibleFieldCount - 1)
                    row.Item(j) = reader.GetValue(j)
                Next
                dt_op.Rows.Add(row)
            End While
            db_connection.Close()
        End Using

        Return dt_op
    End Function

    Public Function sql_db_write_new_datatable_to_db(wr_dt As DataTable, db_working_table_name As String) As Boolean
        Dim op As Boolean = False

        Dim sql_connection As String = connStr_To
        Dim MARIA_DB_VARCHAR_string_lenght = 2000

        Dim db_connection As MySqlConnection = New MySqlConnection(sql_connection + ";AllowLoadLocalInfile=true")
        db_connection.Open()
        Try
            Using db_connection

                ' TRUNCATE 0bm008pr;
                Dim db_command As New MySqlCommand
                db_command.CommandText = String.Format("TRUNCATE {0};", db_working_table_name) 'Sql_command_create_table
                db_command.Connection = db_connection
                db_command.ExecuteNonQuery()

                Dim bulkCopy As MySqlBulkCopy = New MySqlBulkCopy(db_connection)
                'MySqlBulkLoader.Local = True

                bulkCopy.DestinationTableName = db_working_table_name
                '------------------------------------------------
                bulkCopy.WriteToServer(wr_dt)  ''  
                '------------------------------------------------
            End Using
            op = True
        Catch ex As Exception
            op = False
        End Try

        db_connection.Close()

        Return op

    End Function

    Public Function does_db_table_exist(table_name As String) As Boolean
        Dim sql_connection As String = connStr_To

        Dim op As Boolean = False

        Dim db_connection As MySqlConnection = New MySqlConnection(sql_connection + ";AllowLoadLocalInfile=true")

        Dim Sql_command_create_table As String = "SHOW TABLES;"

        Dim opx As String = ""

        db_connection.Open()
        Try
            Using db_connection
                Dim db_command As New MySqlCommand
                Dim reader As MySqlDataReader
                Dim db_activity_result As Stream
                db_command.CommandText = Sql_command_create_table
                db_command.Connection = db_connection
                reader = db_command.ExecuteReader
                While reader.Read()
                    For j As Integer = 0 To (reader.VisibleFieldCount - 1)
                        opx = opx + reader.GetValue(j).ToString() + vbCrLf
                    Next
                End While
                '------------------------------------------------
                ' Debug.WriteLine(opx)
                'Debug.WriteLine(vbCrLf + vbCrLf + table_name)
                'Debug.WriteLine(vbCrLf + vbCrLf + opx.IndexOf(table_name).ToString + vbCrLf)
            End Using
            If opx.IndexOf(table_name) >= 0 Then op = True Else op = False
        Catch ex As Exception
            op = False
        End Try

        db_connection.Close()

        Return op
    End Function

End Module
