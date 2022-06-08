def load_data(host, port, db_name, user, password, name):
    conexion = mysql.connector.connect(host="localhost",user="root", passwd="1234", db="prueba")
    cur = conexion.cursor()
    eng= 'mysql+mysqldb+://root:1234@host:3306/aerolinea'
    #url completa dde la base de datos
    info_transform_flights.to_sql(name='vuelo', con=eng, if_exists='append', index=False)
    #cur.execute('select nombre from ciudad')
    #for nombre in cur.fetchall() :
    #    print(nombre)
    conexion.close()
    print('version')
    #print(cur.fetchall())

load_data("localhost","3306","Local instance MySQL80", "root", "1234", "prueba");