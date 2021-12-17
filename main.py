#------------------------------------------------------------------------
# NAME:    main.py
# AUTH: Heidy Castellanos
#------------------------------------------------------------------------
import os
import pyodbc
from pathlib import Path
import pandas as pd
from io import StringIO
from datetime import datetime
import csv

def generar_reporte(contenido):
    try:
        salida=str(Path(__file__).parent.absolute())+"\\Consultas.txt"
        file = open(salida, "w")
        file.write(contenido)
        file.close()
    except Exception as e:
        print(e)
        
def generacion_archivo(opcion,datos):
    file=""
    if opcion == 1:
        file = file + "#######################Consulta No.1#######################\n"
        file = file + "| Table_name | Row_count |\n"
        for row in datos:
            if  row[1] != "sysdiagrams":
                file = file + "|------------------------|\n"
                file = file + f'''| {row[1]} | {row[2]} |\n'''
    elif opcion == 2:
        file = file + "#######################Consulta No.2#######################\n"
        encabezados = 0
        contador = 0
        auxiliar =""
        cuerpo = ""
        for row in datos:
            if row[0] != auxiliar:
                auxiliar = row[0]
                cuerpo = cuerpo + "\n|------------------------------------------------------------------------|\n"
                cuerpo = cuerpo + f'''| {row[0]} | {row[1]} |'''
                if contador > encabezados:
                    encabezados = contador
                contador = 1
            else:
                cuerpo = cuerpo + f''' {row[1]} |'''
                contador += 1
        file = file + "| Año |"
        for i in range(encabezados):
            file = file + f''' Pais{i+1} |'''
        file = file + cuerpo + "\n"
    elif opcion == 3:
        file = file + "#######################Consulta No.3#######################\n"
        encabezados = 0
        contador = 0
        auxiliar =""
        cuerpo = ""
        for row in datos:
            if row[0] != auxiliar:
                auxiliar = row[0]
                cuerpo = cuerpo + "\n|------------------------------------------------------------------------|\n"
                cuerpo = cuerpo + f'''| {row[0]} | {row[1]} |'''
                if contador > encabezados:
                    encabezados = contador
                contador = 1
            else:
                cuerpo = cuerpo + f''' {row[1]} |'''
                contador += 1
        file = file + "| Pais |"
        for i in range(encabezados):
            file = file + f''' Año{i+1} |'''
        file = file + cuerpo + "\n"
    if opcion == 4:
        file = file + "#######################Consulta No.4#######################\n"
        file = file + "| Pais | Promedio Total_Damage |\n"
        for row in datos:
            file = file + "|------------------------|\n"
            file = file + f'''| {row[0]} | {row[1]} |\n'''
    if opcion == 5:
        file = file + "#######################Consulta No.5#######################\n"
        file = file + "| Pais | Total_Muertes |\n"
        for row in datos:
            file = file + "|------------------------|\n"
            file = file + f'''| {row[0]} | {row[1]} |\n'''
    if opcion == 6:
        file = file + "#######################Consulta No.6#######################\n"
        file = file + "| Año | Total_Muertes |\n"
        for row in datos:
            file = file + "|------------------------|\n"
            file = file + f'''| {row[0]} | {row[1]} |\n'''
    if opcion == 7:
        file = file + "#######################Consulta No.7#######################\n"
        file = file + "| Año | Total_tsunamis |\n"
        for row in datos:
            file = file + "|------------------------|\n"
            file = file + f'''| {row[0]} | {row[1]} |\n'''
    if opcion == 8:
        file = file + "#######################Consulta No.8#######################\n"
        file = file + "| Pais | Total_Casas_Destruidas |\n"
        for row in datos:
            file = file + "|------------------------|\n"
            file = file + f'''| {row[0]} | {row[1]} |\n'''
    if opcion == 9:
        file = file + "#######################Consulta No.9#######################\n"
        file = file + "| Pais | Total_Casas_Dañadas |\n"
        for row in datos:
            file = file + "|------------------------|\n"
            file = file + f'''| {row[0]} | {row[1]} |\n'''
    if opcion == 10:
        file = file + "#######################Consulta No.10#######################\n"
        file = file + "| Pais | Promedio_Altura_Agua |\n"
        for row in datos:
            file = file + "|------------------------|\n"
            file = file + f'''| {row[0]} | {row[1]} |\n'''
    return file

def extraer_0():
    print('Extrayendo informacion ...')
    print('...')
    try:
        print(reader)
        bitacora.append({'hora':str(now.time()),'fecha': str(now.date()),'tipo':'extraer','descripcion':'Se extrajo la informacion de covid19'})
        print('')
        print('***** ---La extraccion se realizado con exito--- *****\n')

        print(reader2)
        bitacora.append({'hora':str(now.time()),'fecha': str(now.date()),'tipo':'extraer','descripcion':'Se extrajo la informacion de economia'})
        print('')
        print('***** ---La extraccion se realizado con exito--- *****\n')
        
        _=input('Enter para continuar--$>')
        return ''
    except Exception as e:
        print('')
        print(f'Error al ejecutar el script: {e}')
        _=input('Enter para continuar--$>')
        return ''

def transform_1():
    global reader
    global reader2
    
    print('Realizando la transformacion de los datos ...')
    print('...')

    try:
        #COVID
        print(reader.isnull().sum().sort_values(ascending=False))
        reader.drop(columns=['weekly_icu_admissions'],inplace=True)
        reader.drop(columns=['weekly_icu_admissions_per_million'],inplace=True)
        reader.drop(columns=['weekly_hosp_admissions'],inplace=True)
        reader.drop(columns=['weekly_hosp_admissions_per_million'],inplace=True)
        reader.drop(columns=['excess_mortality_cumulative_per_million'],inplace=True)
        reader.drop(columns=['excess_mortality_cumulative_absolute'],inplace=True)
        reader.drop(columns=['excess_mortality_cumulative'],inplace=True)
        reader.drop(columns=['total_boosters_per_hundred'],inplace=True)
        reader.drop(columns=['total_cases_per_million'],inplace=True)
        reader.drop(columns=['icu_patients_per_million'],inplace=True)
        reader.drop(columns=['hosp_patients_per_million'],inplace=True)
        reader.drop(columns=['people_fully_vaccinated_per_hundred'],inplace=True)
        reader.drop(columns=['people_vaccinated_per_hundred'],inplace=True)
        reader.drop(columns=['total_vaccinations_per_hundred'],inplace=True)
        reader.drop(columns=['new_tests_per_thousand'],inplace=True)
        reader.drop(columns=['total_tests_per_thousand'],inplace=True)
        reader.drop(columns=['total_deaths_per_million'],inplace=True)
        reader.drop(columns=['new_deaths_per_million'],inplace=True)
        reader.drop(columns=['new_cases_per_million'],inplace=True)
        bitacora.append({'hora':str(now.time()),'fecha': str(now.date()),'tipo':'transformar','descripcion':'Se limpiaron las columnas de covid'})
        #replazar nan por 0 en las columans vacias tipo float
        reader = reader.fillna(0)
        reader['continent'] = reader['continent'].replace(0, '')
        reader['tests_units'] = reader['tests_units'].replace(0, '')
        bitacora.append({'hora':str(now.time()),'fecha': str(now.date()),'tipo':'transformar','descripcion':'Se corrigio el formato'})
        #Data Limpia
        reader.to_csv('clean_data.csv', encoding='utf-8', index=False)        
        print('')
        print('***** ---Transformacion realizado con exito--- *****\n')

        #ECONOMIA
        print(reader2.isnull().sum().sort_values(ascending=False))
        reader2.drop(columns=['Indicator Name'],inplace=True)
        reader2.drop(columns=['Indicator Code'],inplace=True)
        bitacora.append({'hora':str(now.time()),'fecha': str(now.date()),'tipo':'transformar','descripcion':'Se limpiaron las columnas de economia'})
        #eliminar filas con nulos
        reader2.drop([2,4],axis=0)
        bitacora.append({'hora':str(now.time()),'fecha': str(now.date()),'tipo':'transformar','descripcion':'Se limpiaron filas de economia'})
        #Data Limpia
        reader2.to_csv('clean_data_econo.csv', encoding='utf-8', index=False)
        print('')
        print('***** ---Transformacion realizado con exito--- *****\n')
        
        _=input('Enter para continuar--$>')
        return ''
    except Exception as e:
        print('')
        print(f'Error al ejecutar el script: {e}')
        _=input('Enter para continuar--$>')
        return ''

def cargar_temp_2():
    print('Realizando Carga a las tablas temporales ...')
    print('...')
    try:
        conn = pyodbc.connect(conn_data)
        #Creacion para tabla temporal
        inputdir0 = Path(__file__).with_name('SQL_esquema.sql')
        with inputdir0.open('r') as creats0:
            sqlScript0 = creats0.read()
            for statement0 in sqlScript0.split(';'):
                if statement0:
                    with conn.cursor() as cur2:
                        cur2.execute(statement0)
        bitacora.append({'hora':str(now.time()),'fecha': str(now.date()),'tipo':'transformar','descripcion':'Se creo la tabla temporal'})
        #Insercion  para la temporal
        inputdir = Path(__file__).with_name('clean_data.csv')
        with inputdir.open('r') as creats:
            reader = csv.reader(creats)
            contador = 0
            for row in reader:
                if contador != 0:
                    data =f"""INSERT INTO Temporal (iso_code, continent,locationn,datee,total_cases,new_cases,total_deaths,new_deaths,reproduction_rate,icu_patients,hosp_patients,new_tests,
                            total_tests,positive_rate,tests_per_case,tests_units,total_vaccinations,people_vaccinated,people_fully_vaccinated,total_boosters,new_vaccinations,
                            stringency_index,populationn,population_density,median_age,aged_65_older,aged_70_older,gdp_per_capita,extreme_poverty,cardiovasc_death_rate,diabetes_prevalence,female_smokers,
                            male_smokers,handwashing_facilities,hospital_beds_per_thousand,life_expectancy,human_development_index,excess_mortality) 
                            VALUES ('{row[0].replace("'","")}', '{row[1].replace("'","")}','{row[2].replace("'","")}', '{row[3].replace("'","")}',{row[4]}, {row[5]},{row[6]}, {row[7]},{row[8]},{row[9]},{row[10]},
                            {row[11]}, {row[12]},{row[13]}, {row[14]},'{row[15].replace("'","")}', {row[16]},{row[17]}, {row[18]},{row[19]},{row[20]},{row[21]},
                            {row[22]},{row[23]},{row[24]},{row[25]},{row[26]},{row[27]},{row[28]},{row[29]},{row[30]},{row[31]},{row[32]},{row[33]},{row[34]},{row[35]},
                            {row[36]},{row[37]})"""
                    with conn.cursor() as cur:
                        cur.execute(data)
                else:
                    contador+=1
        conn.close()
        bitacora.append({'hora':str(now.time()),'fecha': str(now.date()),'tipo':'transformar','descripcion':'Se cargaron los datos a la tabla temporal'})
        print('')
        print('***** ---Carga realizado con exito--- *****')
        _=input('Enter para continuar--$>')
        return ''
    except Exception as e:
        print('')
        print(f'Error al ejecutar el script: {e}')
        _=input('Enter para continuar--$>')
        return ''


def cerrando_programa():
    return 'Terminando ETL ....'


def llama_la(opcion='x'):
    func = opciones[opcion]['Funcion']
    return func()

def cls():
    _=os.system("cls")

def menu(accion='',s=45):
    print('')
    print('Universidad de San Carlos de Guatemala')
    print('Facultad de Ingenieria')
    print('Escuela en Ciencias y Sistemas')
    print('Seminario de Sistemas 2')
    print('Opciones del programa del programa:','\n','-'*s)
    for op in opciones:
        print('\t',op,' : ',opciones[op]['Des'])
    op=input('Opcion--$>').lower()
    if op in opciones:
        print(llama_la(opcion=op))
        if op=='x':
            return op
    else:
        print('#--Opcion inexistente---',op,'---intente de nuevo--#')
        _=input('Enter para continuar--$>')
        return ''
    return op

reader = pd.read_csv(Path(__file__).with_name('Dataset Covid19.csv'))
reader2 = pd.read_csv(Path(__file__).with_name('Dataset Economia.csv'))
bitacora = []
now = datetime.now()
server='DESKTOP-6RRBLEB\SQLEXPRESS'
bd = 'tnami'
user = 'hdb1'
password = '1234'
conn_data = 'DRIVER={ODBC Driver 17 for SQL server}; SERVER='+server+'; DATABASE='+bd+'; UID='+user+'; PWD='+password

opciones ={
    '0' : {'Des': 'Extraer informacion', 'Funcion': extraer_0, 'Param1':'p1','Param2' :'p2'},
    '1' : {'Des': 'Transformacion de Informacion', 'Funcion': transform_1, 'Param1':'p1','Param2' :'p2'},
    '2' : {'Des': 'Carga', 'Funcion': cargar_temp_2, 'Param1':'p1','Param2' :'p2'},
    '3' : {'Des': 'Crear Modelo', 'Funcion': cargar_temp_2, 'Param1':'p1','Param2' :'p2'},
    '4' : {'Des': 'Crear Datamarts', 'Funcion': cargar_temp_2, 'Param1':'p1','Param2' :'p2'},
    '5' : {'Des': 'Realizar Consultas', 'Funcion': cargar_temp_2, 'Param1':'p1','Param2' :'p2'},
    'x' : {'Des': 'Salir', 'Funcion': cerrando_programa, 'Param1':'p1','Param2' :'p2'}
    }

op,accion,s='','',25
while op!='x':
    op=menu(accion,s)
    accion=accion +' ' + op
    cls()
print('-'*s,'Fin del programa --$> ',accion)
