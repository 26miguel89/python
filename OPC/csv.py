"""
=====================================================================================================================
Script Python para leer datos desde Base de datos y generar archivo csv para adquision de datos
Miguel Alanis
=====================================================================================================================
"""
import pypyodbc, csv, time, os, sys, re



print "iniciando espera 30s......."
for i in range(5+1):
    time.sleep(1)
    sys.stdout.write(('='*i)+(''*(100-i))+("\r [ %d"%i+"s ] "))
    sys.stdout.flush()

class ODBCtoCSV(object):

    def __init__(self, connect='Driver={SQL Server};Server=IE11WIN7\SQLEXPRESS;Database=OPC;UID=miguel;PWD=*****'):
        self.connect_string = connect

    def dump(self, sql, filename, include_headers=True):
        f = csv.writer(file(filename, 'wb'))

        cnxn = pypyodbc.connect(self.connect_string)
        c = cnxn.cursor()
        c.execute(sql)

        if include_headers:
            f.writerow([d[0] for d in c.description])

        f.writerows(c.fetchall())

def is_open(file_name):
    if os.path.exists(file_name):
        try:
            os.rename(file_name, file_name) #can't rename an open file so an error will be thrown
            return False
        except:
            return True
    raise NameError

def quality():
    sql_con = pypyodbc.connect('Driver={SQL Server};Server=IE11WIN7\SQLEXPRESS;Database=OPC;UID=miguel;PWD=******')
    cur = sql_con.cursor()
    cur.execute("SELECT DATATIME,[Quality] FROM dbo.DATOS_PLC$ WHERE [DATATIME]=(SELECT MAX([DATATIME]) FROM dbo.DATOS_PLC$)")
    resultado = cur.fetchone()
    return resultado


good = True
while good:
    Q = quality()
    pattern = r"Good"
    if re.match(pattern, Q[1]):
        good = True
        print "Comunicacion OK!!"
    else:
        print "Perdida de conexion!!!1"
        good = False


    fecha = time.strftime("%d-%m-%y")
    nombre = "C:/Users/IEUser/Desktop/1rep/" + 'VarPlcM ' + fecha + ".csv"
    if is_open(nombre) ==True:
        print "El archivo se esta usando esperando a que se desocupe...."
    else:
        if __name__ == '__main__':
            query = ODBCtoCSV()
            print "Escribiendo Archivo..."
            query.dump("""SELECT * FROM dbo.[DATOS_PLC$]
                        WHERE dbo.[DATOS_PLC$].DATATIME = CONVERT (date, SYSDATETIME())
                        ORDER BY dbo.[DATOS_PLC$].DATATIME DESC""",nombre)
        print "Archivo escrito.... "+nombre
    print "1 segundo para volver a escribir..."
    time.sleep(1)
    os.system ("cls")
