# Proyecto Individual - HENRY
![image](https://github.com/aleskafidas/proyecto_01/blob/main/henry.png)

## Descripción 

Dicho proyecto llevado a cabo es realizar un proceso ETL(Extract, Transform, Load) a partir de un series de datos, utilizando diversas herramientas y habilidades, siendo un proyecto para el area de Data Engiennering.

Las herramientas utilizadas durante el proyecto son:
- Visual Studio Code
- Mysql Workbech
- Power bi

## Propuestas para llevar a cabo

- Procesar los diferentes datasets. 
- Crear un archivo DB con el motor de SQL que quieran. Pueden usar SQLAlchemy por ejemplo.
- Realizar en draw.io un diagrama de flujo de trabajo del ETL y explicarlo en vivo.
- Realizar una carga incremental de los archivos que se tienen durante el video.
- Realizar una query en el video, para comprobar el funcionamiento de todo su trabajo. La query a armar es la siguiente: Precio promedio de la sucursal 9-1-688.

## Desarrollo

Primero cambie los archivos que nos dieron a CSV ya que estaban en diferentes formatos.
Luego trabaje el codigo para normalizar los datos, y conectar el python con el Mysql.
Yo utilice Power Bi para normalizar algunas tablas ya que es muy amigable. La carga incremental se realizo por medio de una funcion, de modo que al ingresar una nueva tabla,
automaticamente lo cargue a la base de datos.
![image](https://github.com/aleskafidas/proyecto_01/blob/main/diagrama%20de%20flujo.jpeg)
