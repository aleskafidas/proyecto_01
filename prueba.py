import pandas as pd
import sqlalchemy as db

database_username='root' # Nombre de cliente en MySQL Workbrench
database_password='072030' # Contraseña de MySQL Workbrench
database_ip='localhost'
database_name='proyecto_1' # Nombre de Base de datos a donde nos conectaremos
database_conection=db.create_engine(f'mysql+pymysql://{database_username}:{database_password}@{database_ip}/{database_name}')
conexion=database_conection.connect()
metadata=db.MetaData()

def normalizar(id):
    if(type(id)!= str):
        return id

    caracteres = len(id)
    if (len(id)< 13):
        id = '0'*(13-caracteres)+id
    if('-' in id):
        id = id[id.rfind('-')+1:caracteres]
    if ('.' in id):
        id = id[0 :id.find('.')]
    return id

def mod_id_suc (x):
    if ' 00:00:00' in x:
        x = x.replace(' 00:00:00','')
        val_1 = x.split('-')[0]
        val_2 = x.split('-')[1]
        val_3 = x.split('-')[2]
       
        x = val_3 + '-' + val_2 + '-' + val_1
    return x
#cargando archivos a nuestra base de datos
#archivo 413
precios_413 = pd.read_csv(r'C:\Users\kfln0\OneDrive\Desktop\proyecto 1\PI01_DATA_ENGINEERING\Datasets\precios_semana_20200413.csv', encoding='utf-16 LE').dropna(axis = 0, how = 'any').drop_duplicates()
precios_413 = precios_413.rename(columns= {'sucursal_id': 'idSucursal', 'producto_id': 'idProducto'})
precios_413.to_sql(name='precios_413', con=conexion)

#archivo json 503
ver_503 = pd.read_csv(r'C:\Users\kfln0\OneDrive\Desktop\proyecto 1\PI01_DATA_ENGINEERING\Datasets\precios_503_1.csv')
ver_503 = ver_503.rename(columns= {'sucursal_id': 'idSucursal', 'producto_id': 'idProducto'})
media_precio_503 = ver_503['precio'].mean()
ver_503['precio'].fillna(value=media_precio_503, inplace=True)
ver_503.dropna(axis = 0, how = 'any').drop_duplicates()
ver_503.to_sql(name='precios_503', con=conexion)


#archivo sucursal
sucursal = pd.read_csv(r'C:\Users\kfln0\OneDrive\Desktop\proyecto 1\PI01_DATA_ENGINEERING\Datasets\sucursal.csv')
sucursal.rename(columns={'lng':'long', 'banderaDescripcion': 'BanderaDescripcion', 'comercioRazonSocial':'ComercioRazonSocial', 'sucursalNombre': 'SucursalNombre'}, inplace=True)
sucursal.dropna(axis = 0, how = 'any').drop_duplicates()
sucursal.to_sql(name='sucursal', con=conexion)



#archivo 419-426
ar_419_426 = pd.read_csv(r'C:\Users\kfln0\OneDrive\Desktop\proyecto 1\PI01_DATA_ENGINEERING\Datasets\precios_semanas_20200419_20200426.csv',low_memory=False)
ar_419_426.dropna(axis = 0, how = 'any').drop_duplicates()
ar_419_426 = ar_419_426.rename(columns= {'sucursal_id': 'idSucursal', 'producto_id': 'idProducto'})
ar_419_426.to_sql(name='precios_419_426', con=conexion)

#archivo parquet
file_parquet = pd.read_csv(r'C:\Users\kfln0\OneDrive\Desktop\proyecto 1\PI01_DATA_ENGINEERING\Datasets\producto.csv',low_memory=False)
file_parquet=file_parquet.drop(columns=['categoria1', 'categoria2', 'categoria3'])
file_parquet = file_parquet.fillna('Dato Faltante')
file_parquet.dropna(axis = 0, how = 'any').drop_duplicates()
file_parquet.drop(['Unnamed: 0'], axis=1)
file_parquet.to_sql(name='parquet', con=conexion)

#Funcion carga incremental
def carga_incremental(rutaArchivo):
    ar_518 = pd.read_csv(f'{rutaArchivo}',  sep= '|')
    #normalizacion de datos
    ar_518 = ar_518.rename(columns= {'sucursal_id': 'idSucursal', 'producto_id': 'idProducto'})
    ar_518.drop_duplicates(inplace=True)
    ar_518.drop(ar_518[ar_518.idSucursal.isnull() == True].index, inplace=True)
    ar_518.precio.fillna(0.0, inplace=True)
    for i in range(1,3):
        ar_518.iloc[:,i] = ar_518.iloc[:,i].astype('string')
    database_conection = db.create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}"
                       .format(user="root",
                               pw="072030",
                               db="proyecto_1"))
    conexion=database_conection.connect()
    ar_518.to_sql('precios_518', con =conexion, if_exists='append')

carga_incremental(r'C:\Users\kfln0\OneDrive\Desktop\proyecto 1\PI01_DATA_ENGINEERING\Datasets\precios_semana_20200518.csv')
