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

    conn = psycopg2.connect(host="localhost", port="5432", dbname="BINoche", user="postgres", password="boxer2004")
    cur = conn.cursor()
    engine = create_engine('postgresql://root:1234@host:3306/aerolinea')
    info_transform_flights.to_sql(name="flight", con=engine, if_exists='append', index=False)
    info_transform_airlines.to_sql(name="airlines", con=engine, if_exists='append', index=False)
    data_transform_airports.to_sql(name="airports", con=engine, if_exists='append', index=False)
    print('posgreSQL database version:')
    cur.execute('SELECT version()')
    db_version = cur.fetchone()
    print(db_version)
    cur.close()

