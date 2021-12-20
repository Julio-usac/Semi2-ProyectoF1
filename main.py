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

def generar_reporte(contenido, nombre=""):
    try:
        salida=str(Path(__file__).parent.absolute())+"\\"+nombre+".txt"
        file = open(salida, "w")
        file.write(contenido)
        file.close()
    except Exception as e:
        print(e)

def generar_bitacora():
    try:
        file=""
        file = file + "#######################-----BITACORA----#######################\n"
        file = file + "|       HORA      |    FECHA   |   TIPO  |              DESCRIPCION             |\n"
        for value in bitacora:
            file = file + "|-------------------------------------------------------------------------------|\n"
            file = file + f'''| {value['hora']} | {value['fecha']} | {value['tipo']} | {value['descripcion']} |\n'''
        return file
    except Exception as e:
        print(e)

def reporte_bitacora():
    try:
        generar_reporte(generar_bitacora(),"RBitacora")
        print('***** ---Report bitacora realizado con exito--- *****\n')

        _=input('Enter para continuar--$>')
        return ''
    except Exception as e:
        print('')
        print(f'Error al ejecutar el reporte: {e}')
        _=input('Enter para continuar--$>')
        return ''

def generacion_archivo():
    
    conn = pyodbc.connect(conn_data)
    cursor = conn.cursor()

    f=open("consultas.txt","w")

    f.write("Consulta 1\n")
    f.write("-----------------------------------------------------------\n")
    cursor.execute('''select Continente2.nombre as Continente, sum(new_deaths) as Muertes FROM Continente2, Pais2, Covid_data2
                        WHERE Continente2.id_continente= Pais2.id_continente and Continente2.nombre!=''
                        and Pais2.id_Pais=Covid_data2.id_Pais 
                        Group by Continente2.nombre;''')
    f.write("Continente, Muertes\n")
    for i in cursor:
         f.write(str(i[0])+"||"+str(i[1])+"\n")
  
    f.write("\n-----------------------------------------------------------\n")

    f.write("Consulta 2\n")
    f.write("-----------------------------------------------------------\n")

    cursor.execute('''select year(datee) as año, total_cases from Covid_data2, Pais2
                        WHERE Covid_data2.id_Pais= Pais2.id_Pais and Covid_data2.datee='09/12/2021'and  Pais2.nombre='World'
                        union
                        select year(datee) as año, total_cases from Covid_data2, Pais2
                        WHERE Covid_data2.id_Pais= Pais2.id_Pais and Covid_data2.datee='09/12/2020'and  Pais2.nombre='World';''')
    f.write("Año,  Total de casos\n")
    for i in cursor:
        f.write(str(i[0])+"||"+str(i[1])+"\n")
    
    f.write("\n-----------------------------------------------------------\n")

    f.write("Consulta 3\n")
    f.write("-----------------------------------------------------------\n")

    cursor.execute('''select TOP 10 Pais2.nombre, Pais2.median_age, sum(new_deaths) as Muertes FROM Pais2,Covid_data2
                        WHERE Pais2.id_Pais=Covid_data2.id_Pais
                        group by nombre,median_age
                        ORDER BY median_age desc;''')
    f.write("Nombre,  Promedio edad, Muertes\n")
    for i in cursor:
        f.write(str(i[0])+"||"+str(i[1])+"||"+str(i[2])+"\n")
    
    f.write("\n-----------------------------------------------------------\n")

    f.write("Consulta 4\n")
    f.write("-----------------------------------------------------------\n")

    cursor.execute('''select TOP 5 Pais2.nombre,year(datee) as año, avg(new_vaccinations) as vacunaciones from Pais2, Covid_data2
                        where Pais2.id_Pais= Covid_data2.id_Pais and Pais2.nombre!='World' and Pais2.nombre!='Asia' and Pais2.nombre!='Upper middle income'
                        and Pais2.nombre!='Lower middle income' and Pais2.nombre!='High income' and Pais2.nombre!='Europe' and Pais2.nombre!='North America'
                        and Pais2.nombre!='European Union' and Pais2.nombre!='South America'
                        group by Pais2.nombre, year(datee) 
                        order by vacunaciones desc;''')
    f.write("Nombre,  Año, Vacunaciones\n")
    for i in cursor:
        f.write(str(i[0])+"||"+str(i[1])+"||"+str(i[2])+"\n")
    
    f.write("\n-----------------------------------------------------------\n")

    f.write("Consulta 5\n")
    f.write("-----------------------------------------------------------\n")
    cursor.execute('''SELECT Pais, a2018, a2020, (((a2020-a2018)/a2020)*100) AS Cambio_Relativo FROM (
                          SELECT Pais_SG.nombre AS Pais, PIB_SG.pib AS a2018 FROM Pais_SG
                          INNER JOIN PIB_SG ON PIB_SG.iso=Pais_SG.iso
                          WHERE PIB_SG.yearr=2018 and PIB_SG.pib!=0
                      ) con1 INNER JOIN (
                          SELECT Pais_SG.nombre AS Pais1, PIB_SG.pib AS a2020 FROM Pais_SG
                          INNER JOIN PIB_SG ON PIB_SG.iso=Pais_SG.iso
                          WHERE PIB_SG.yearr=2020 and PIB_SG.pib!=0
                      )con2 ON con1.Pais=con2.Pais1;''')
    f.write("Pais, 2018, 2019, Cambio Relativo\n")
    for i in cursor:
         f.write(str(i[0])+"||"+str(i[1])+"||"+str(i[2])+"||"+str(i[3])+"\n")
  
    f.write("\n-----------------------------------------------------------\n")

    f.write("Consulta 6\n")
    f.write("-----------------------------------------------------------\n")
    cursor.execute('''SELECT TOP 7 Pais, a2019, a2020, ((ABS(a2020-a2019)/a2020)*100) AS Cambio_Absoluto FROM (
                          SELECT Pais_SG.nombre AS Pais, PIB_SG.pib AS a2019 FROM Pais_SG
                          INNER JOIN PIB_SG ON PIB_SG.iso=Pais_SG.iso
                          WHERE PIB_SG.yearr=2019 and PIB_SG.pib!=0
                      ) con1 INNER JOIN (
                          SELECT Pais_SG.nombre AS Pais1, PIB_SG.pib AS a2020 FROM Pais_SG
                          INNER JOIN PIB_SG ON PIB_SG.iso=Pais_SG.iso
                          WHERE PIB_SG.yearr=2020 and PIB_SG.pib!=0
                      )con2 ON con1.Pais=con2.Pais1
                          ORDER BY Cambio_Absoluto DESC;''')
    f.write("Pais, 2019, 2020, Cambio Absoluto\n")
    for i in cursor:
         f.write(str(i[0])+"||"+str(i[1])+"||"+str(i[2])+"||"+str(i[3])+"\n")
  
    f.write("\n-----------------------------------------------------------\n")

    f.write("Consulta 7\n")
    f.write("-----------------------------------------------------------\n")
    cursor.execute('''SELECT Pais.nombre, AVG(Covid_data.new_cases) AS Promedio FROM Pais
                      INNER JOIN Covid_data ON Covid_data.iso=Pais.iso
                      WHERE Covid_data.new_cases!=0
                      GROUP BY Pais.nombre;''')
    f.write("Pais, Promedio contagios por dia\n")
    for i in cursor:
         f.write(str(i[0])+"||"+str(i[1])+"\n")
  
    f.write("\n-----------------------------------------------------------\n")

    f.write("Consulta 8\n")
    f.write("-----------------------------------------------------------\n")
    cursor.execute('''SELECT Pais.nombre, AVG(Covid_data.new_deaths) AS Promedio FROM Pais
                      INNER JOIN Covid_data ON Covid_data.iso=Pais.iso
                      WHERE Covid_data.new_deaths!=0
                      GROUP BY Pais.nombre;''')
    f.write("Pais, Promedio de muertes por dia\n")
    for i in cursor:
         f.write(str(i[0])+"||"+str(i[1])+"\n")
  
    f.write("\n-----------------------------------------------------------\n")

    f.write("Consulta 9\n")
    f.write("-----------------------------------------------------------\n")
    cursor.execute('''SELECT TOP 10 t2.CANTIDAD_2020, t3.CANTIDAD_2019 
                        FROM (
                            SELECT cantidad_pib AS CANTIDAD_2020,cyear_id FROM Calculo_year WHERE cyear_id=(SELECT id_year FROM Cyear WHERE n_year='2020')
                            ) t2
                        INNER JOIN (
                                SELECT cantidad_pib AS CANTIDAD_2019,cyear_id FROM Calculo_year WHERE cyear_id=(SELECT id_year FROM Cyear WHERE n_year='2019')
                            )  t3
                        ON t2.cyear_id<>t3.cyear_id ORDER BY t2.CANTIDAD_2020 DESC, t3.CANTIDAD_2019 DESC;''')
    f.write("CANTIDAD_2020, CANTIDAD_2019\n")
    for i in cursor:
         f.write(str(i[0])+"||"+str(i[1])+"\n")
  
    f.write("\n-----------------------------------------------------------\n")

    f.write("Consulta 10\n")
    f.write("-----------------------------------------------------------\n")
    cursor.execute('''SELECT  t2.Pais, (t2.CANTIDAD_2020 - t3.CANTIDAD_2018) AS DIFERENCIA
                        FROM (
                            SELECT cantidad_pib AS CANTIDAD_2020,b.nombre AS Pais  FROM Calculo_year t
                            INNER JOIN PIB3 a ON a.id_pib = t.pib_id
                            INNER JOIN Pais3 b ON b.id_Pais = a.pais_id
                            WHERE cyear_id=(SELECT id_year FROM Cyear WHERE n_year='2020')
                            ) t2
                        INNER JOIN (
                                SELECT cantidad_pib AS CANTIDAD_2018,b.nombre AS Pais FROM Calculo_year t
                                INNER JOIN PIB3 a ON a.id_pib = t.pib_id
                                INNER JOIN Pais3 b ON b.id_Pais = a.pais_id
                                WHERE cyear_id=(SELECT id_year FROM Cyear WHERE n_year='2018')
                            )  t3
                            ON t2.Pais=t3.Pais
                            WHERE t2.CANTIDAD_2020 !=0 AND t3.CANTIDAD_2018 !=0
                            ORDER BY t2.CANTIDAD_2020 ASC, t3.CANTIDAD_2018 ASC;''')
    f.write("Pais, DIFERENCIA\n")
    for i in cursor:
         f.write(str(i[0])+"||"+str(i[1])+"\n")
  
    f.write("\n-----------------------------------------------------------\n")

    f.write("Consulta 11\n")
    f.write("-----------------------------------------------------------\n")
    cursor.execute('''SELECT a1.nombre AS Pais,ROUND(SUM(b.people_fully_vaccinated)/
                        (SELECT R2.cantidad FROM
                        (SELECT r.pais, COUNT(r.pais) AS cantidad FROM (
                            SELECT a.nombre AS Pais,b.people_fully_vaccinated AS total_Pais  FROM Covid_PIB t
                                INNER JOIN Pais a ON a.id_Pais = t.id_Pais
                                INNER JOIN Covid_data b ON b.id_covid = t.id_covid
                                WHERE t.people_fully_vaccinated is not null
                                GROUP BY a.nombre,b.people_fully_vaccinated ) AS R
                        WHERE r.pais= a1.nombre GROUP BY r.pais) AS R2),2) AS Promedio
                    FROM Covid_PIB t1
                    INNER JOIN Pais a1 ON a1.id_Pais = t1.id_Pais
                    INNER JOIN Covid_data b ON b.id_covid = t1.id_covid
                    WHERE t1.people_fully_vaccinated is not null
                    GROUP BY a1.nombre ORDER BY a1.nombre ASC;''')
    f.write("Pais, Promedio\n")
    for i in cursor:
         f.write(str(i[0])+"||"+str(i[1])+"\n")
  
    f.write("\n-----------------------------------------------------------\n")

    f.write("Consulta 12\n")
    f.write("-----------------------------------------------------------\n")
    cursor.execute('''SELECT a1.nombre AS Pais,ROUND(SUM(b.total_deaths)/
                        (SELECT R2.cantidad FROM
                        (SELECT r.pais, COUNT(r.pais) AS cantidad FROM (
                            SELECT a.nombre AS Pais,b.total_deaths AS total_Pais  FROM Covid_PIB t
                                INNER JOIN Pais a ON a.id_Pais = t.id_Pais
                                INNER JOIN Covid_data b ON b.id_covid = t.id_covid
                                WHERE b.total_deaths is not null AND a.cardiovasc_death_rate > 0 AND a.diabetes_prevalence > 0
                                GROUP BY a.nombre,b.total_deaths ) AS R
                    WHERE r.pais= a1.nombre GROUP BY r.pais) AS R2),2) AS Promedio
                    FROM Covid_PIB t1
                    INNER JOIN Pais a1 ON a1.id_Pais = t1.id_Pais
                    INNER JOIN Covid_data b ON b.id_covid = t1.id_covid
                    WHERE b.total_deaths is not null AND a1.cardiovasc_death_rate > 0 AND a1.diabetes_prevalence > 0
                    GROUP BY a1.nombre ORDER BY a1.nombre ASC;''')
    f.write("Pais, Promedio\n")
    for i in cursor:
         f.write(str(i[0])+"||"+str(i[1])+"\n")
  
    f.write("\n-----------------------------------------------------------\n")
    
    
    f.close()

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
        reader2.drop([1,3],inplace=True)
        #replazar nan por 0 en las columans vacias tipo float
        reader2 = reader2.fillna(0)
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


def crear_datamarts_0():
    print('Realizando Creacion de datamarts ...')
    print('...')
    try:
        conn = pyodbc.connect(conn_data)
        #Creacion de datamart covid
        inputdir1 = Path(__file__).with_name('SQL_datamart_covid.sql')
        with inputdir1.open('r') as creats1:
            sqlScript1 = creats1.read()
            for statement0 in sqlScript1.split(';'):
                if statement0:
                    with conn.cursor() as cur3:
                        cur3.execute(statement0)
        bitacora.append({'hora':str(now.time()),'fecha': str(now.date()),'tipo':'Crear','descripcion':'Se creo el datamart covid'})
        #Insercion de datamart covid
        cursor = conn.cursor()
        cursor.execute('''INSERT INTO Continente2(nombre) SELECT DISTINCT continent FROM Temporal''')
        conn.commit()
        cursor.execute('''INSERT INTO Pais2(iso,nombre,populationn,population_density,median_age,aged_65_older,aged_70_older,gdp_per_capita,
                            extreme_poverty,cardiovasc_death_rate,diabetes_prevalence,female_smokers,
                            male_smokers,handwashing_facilities,hospital_beds_per_thousand,life_expectancy,human_development_index,id_continente)
                        SELECT DISTINCT iso_code, locationn,populationn,population_density,median_age,aged_65_older,aged_70_older,gdp_per_capita,
                        extreme_poverty,cardiovasc_death_rate,diabetes_prevalence,female_smokers, male_smokers,handwashing_facilities,
                        hospital_beds_per_thousand,life_expectancy,human_development_index,id_continente FROM Temporal, Continente2
                        WHERE Temporal.continent=Continente2.nombre;
                       ''')
        conn.commit()
        cursor.execute('''INSERT INTO Covid_data2(datee,total_cases,new_cases,total_deaths,new_deaths,reproduction_rate,icu_patients,hosp_patients,new_tests,
                            total_tests,positive_rate,tests_per_case,tests_units,total_vaccinations,people_vaccinated,people_fully_vaccinated,total_boosters,new_vaccinations,
                            stringency_index, excess_mortality,id_Pais)
                        SELECT datee,total_cases,new_cases,total_deaths,new_deaths,reproduction_rate,icu_patients,hosp_patients,new_tests,
                            total_tests,positive_rate,tests_per_case,tests_units,total_vaccinations,people_vaccinated,people_fully_vaccinated,total_boosters,new_vaccinations,
                            stringency_index, excess_mortality, id_Pais FROM Temporal, Pais2
                            WHERE Temporal.locationn=Pais2.nombre;
                       ''')
        conn.commit()
        conn.close()
        #Creacion de datamart impacto
        conn = pyodbc.connect(conn_data)
        #Creacion de datamart covid
        inputdir2 = Path(__file__).with_name('SQL_datamart_impacto.sql')
        with inputdir2.open('r') as creats2:
            sqlScript2 = creats2.read()
            for statement2 in sqlScript2.split(';'):
                if statement2:
                    with conn.cursor() as cur2:
                        cur2.execute(statement2)
        bitacora.append({'hora':str(now.time()),'fecha': str(now.date()),'tipo':'Crear','descripcion':'Se creo el datamart impacto'})
        #Insercion de datamart covid
        inputdir3 = Path(__file__).with_name('SQL_carga_impacto.sql')
        with inputdir3.open('r') as creats3:
            sqlScript3 = creats3.read()
            for statement3 in sqlScript3.split(';'):
                if statement3:
                    with conn.cursor() as cur3:
                        cur3.execute(statement3)
        bitacora.append({'hora':str(now.time()),'fecha': str(now.date()),'tipo':'Crear','descripcion':'Se inserto data en el datamart impacto'})
        conn.commit()
        conn.close()
        #Datamart combinado
        conn = pyodbc.connect(conn_data)
        #Creacion de datamart combinado
        inputdirS = Path(__file__).with_name('SQL_datamart_combi.sql')
        with inputdirS.open('r') as creatsS:
            sqlScriptS = creatsS.read()
            for statementS in sqlScriptS.split(';'):
                if statementS:
                    with conn.cursor() as curS:
                        curS.execute(statementS)
        bitacora.append({'hora':str(now.time()),'fecha': str(now.date()),'tipo':'Crear','descripcion':'Se creo el datamart combinado'})
        #Insercion en datamart combinado
        inputdirG = Path(__file__).with_name('SQL_carga_combi.sql')
        with inputdirG.open('r') as creatsG:
            sqlScriptG = creatsG.read()
            for statementG in sqlScriptG.split(';'):
                if statementG:
                    with conn.cursor() as curG:
                        curG.execute(statementG)
        bitacora.append({'hora':str(now.time()),'fecha': str(now.date()),'tipo':'Crear','descripcion':'Se inserto data en el datamart combinado'})
        conn.commit()
        conn.close()
        print('***** ---Datamarts creados y cargados con exito--- *****')
        _=input('Enter para continuar--$>')
        return ''
    except Exception as e:
        print('')
        print(f'Error al ejecutar el script: {e}')
        _=input('Enter para continuar--$>')
        return ''

def crear_modelo_0():
    print('Realizando Creacion de modelo ...')
    print('...')
    try:
        conn = pyodbc.connect(conn_data)
        #Creacion de modelo
        inputdir1 = Path(__file__).with_name('SQL_modelo.sql')
        with inputdir1.open('r') as creats1:
            sqlScript1 = creats1.read()
            for statement0 in sqlScript1.split(';'):
                if statement0:
                    with conn.cursor() as cur3:
                        cur3.execute(statement0)
        bitacora.append({'hora':str(now.time()),'fecha': str(now.date()),'tipo':'Crear','descripcion':'Se creo el modelo'})
        cursor = conn.cursor()
        cursor.execute('''INSERT INTO Continente(nombre) SELECT DISTINCT continent FROM Temporal''')
        conn.commit()
        cursor.execute('''INSERT INTO Pais(iso,nombre,populationn,population_density,median_age,aged_65_older,aged_70_older,gdp_per_capita,
                            extreme_poverty,cardiovasc_death_rate,diabetes_prevalence,female_smokers,
                            male_smokers,handwashing_facilities,hospital_beds_per_thousand,life_expectancy,human_development_index,id_continente)
                        SELECT DISTINCT iso_code, locationn,populationn,population_density,median_age,aged_65_older,aged_70_older,gdp_per_capita,
                        extreme_poverty,cardiovasc_death_rate,diabetes_prevalence,female_smokers, male_smokers,handwashing_facilities,
                        hospital_beds_per_thousand,life_expectancy,human_development_index,id_continente FROM Temporal, Continente
                        WHERE Temporal.continent=Continente.nombre;
                       ''')
        conn.commit()
        cursor.execute('''INSERT INTO Covid_data(iso,datee,total_cases,new_cases,total_deaths,new_deaths,reproduction_rate,icu_patients,hosp_patients,new_tests,
                            total_tests,positive_rate,tests_per_case,tests_units,total_vaccinations,people_vaccinated,people_fully_vaccinated,total_boosters,new_vaccinations,
                            stringency_index, excess_mortality)
                        SELECT iso_code,datee,total_cases,new_cases,total_deaths,new_deaths,reproduction_rate,icu_patients,hosp_patients,new_tests,
                            total_tests,positive_rate,tests_per_case,tests_units,total_vaccinations,people_vaccinated,people_fully_vaccinated,total_boosters,new_vaccinations,
                            stringency_index, excess_mortality FROM Temporal;
                       ''')
        conn.commit()

        cursor.execute('''INSERT INTO PIB_Data(yearr, iso, pib) SELECT '1960', iso, ano_1960 FROM Temporal_PIB''')
        cursor.execute('''INSERT INTO PIB_Data(yearr, iso, pib) SELECT '1961', iso, ano_1961 FROM Temporal_PIB''')
        cursor.execute('''INSERT INTO PIB_Data(yearr, iso, pib) SELECT '1962', iso, ano_1962 FROM Temporal_PIB''')
        cursor.execute('''INSERT INTO PIB_Data(yearr, iso, pib) SELECT '1963', iso, ano_1963 FROM Temporal_PIB''')
        cursor.execute('''INSERT INTO PIB_Data(yearr, iso, pib) SELECT '1964', iso, ano_1964 FROM Temporal_PIB''')
        cursor.execute('''INSERT INTO PIB_Data(yearr, iso, pib) SELECT '1965', iso, ano_1965 FROM Temporal_PIB''')
        cursor.execute('''INSERT INTO PIB_Data(yearr, iso, pib) SELECT '1966', iso, ano_1966 FROM Temporal_PIB''')
        cursor.execute('''INSERT INTO PIB_Data(yearr, iso, pib) SELECT '1967', iso, ano_1967 FROM Temporal_PIB''')
        cursor.execute('''INSERT INTO PIB_Data(yearr, iso, pib) SELECT '1968', iso, ano_1968 FROM Temporal_PIB''')
        cursor.execute('''INSERT INTO PIB_Data(yearr, iso, pib) SELECT '1969', iso, ano_1969 FROM Temporal_PIB''')
        cursor.execute('''INSERT INTO PIB_Data(yearr, iso, pib) SELECT '1970', iso, ano_1970 FROM Temporal_PIB''')
        cursor.execute('''INSERT INTO PIB_Data(yearr, iso, pib) SELECT '1971', iso, ano_1971 FROM Temporal_PIB''')
        cursor.execute('''INSERT INTO PIB_Data(yearr, iso, pib) SELECT '1972', iso, ano_1972 FROM Temporal_PIB''')
        cursor.execute('''INSERT INTO PIB_Data(yearr, iso, pib) SELECT '1973', iso, ano_1973 FROM Temporal_PIB''')
        cursor.execute('''INSERT INTO PIB_Data(yearr, iso, pib) SELECT '1974', iso, ano_1974 FROM Temporal_PIB''')
        cursor.execute('''INSERT INTO PIB_Data(yearr, iso, pib) SELECT '1975', iso, ano_1975 FROM Temporal_PIB''')
        cursor.execute('''INSERT INTO PIB_Data(yearr, iso, pib) SELECT '1976', iso, ano_1976 FROM Temporal_PIB''')
        cursor.execute('''INSERT INTO PIB_Data(yearr, iso, pib) SELECT '1977', iso, ano_1977 FROM Temporal_PIB''')
        cursor.execute('''INSERT INTO PIB_Data(yearr, iso, pib) SELECT '1978', iso, ano_1978 FROM Temporal_PIB''')
        cursor.execute('''INSERT INTO PIB_Data(yearr, iso, pib) SELECT '1979', iso, ano_1979 FROM Temporal_PIB''')
        cursor.execute('''INSERT INTO PIB_Data(yearr, iso, pib) SELECT '1980', iso, ano_1980 FROM Temporal_PIB''')
        cursor.execute('''INSERT INTO PIB_Data(yearr, iso, pib) SELECT '1981', iso, ano_1981 FROM Temporal_PIB''')
        cursor.execute('''INSERT INTO PIB_Data(yearr, iso, pib) SELECT '1982', iso, ano_1982 FROM Temporal_PIB''')
        cursor.execute('''INSERT INTO PIB_Data(yearr, iso, pib) SELECT '1983', iso, ano_1983 FROM Temporal_PIB''')
        cursor.execute('''INSERT INTO PIB_Data(yearr, iso, pib) SELECT '1984', iso, ano_1984 FROM Temporal_PIB''')
        cursor.execute('''INSERT INTO PIB_Data(yearr, iso, pib) SELECT '1985', iso, ano_1985 FROM Temporal_PIB''')
        cursor.execute('''INSERT INTO PIB_Data(yearr, iso, pib) SELECT '1986', iso, ano_1986 FROM Temporal_PIB''')
        cursor.execute('''INSERT INTO PIB_Data(yearr, iso, pib) SELECT '1987', iso, ano_1987 FROM Temporal_PIB''')
        cursor.execute('''INSERT INTO PIB_Data(yearr, iso, pib) SELECT '1988', iso, ano_1988 FROM Temporal_PIB''')
        cursor.execute('''INSERT INTO PIB_Data(yearr, iso, pib) SELECT '1989', iso, ano_1989 FROM Temporal_PIB''')
        cursor.execute('''INSERT INTO PIB_Data(yearr, iso, pib) SELECT '1990', iso, ano_1990 FROM Temporal_PIB''')
        cursor.execute('''INSERT INTO PIB_Data(yearr, iso, pib) SELECT '1991', iso, ano_1991 FROM Temporal_PIB''')
        cursor.execute('''INSERT INTO PIB_Data(yearr, iso, pib) SELECT '1992', iso, ano_1992 FROM Temporal_PIB''')
        cursor.execute('''INSERT INTO PIB_Data(yearr, iso, pib) SELECT '1993', iso, ano_1993 FROM Temporal_PIB''')
        cursor.execute('''INSERT INTO PIB_Data(yearr, iso, pib) SELECT '1994', iso, ano_1994 FROM Temporal_PIB''')
        cursor.execute('''INSERT INTO PIB_Data(yearr, iso, pib) SELECT '1995', iso, ano_1995 FROM Temporal_PIB''')
        cursor.execute('''INSERT INTO PIB_Data(yearr, iso, pib) SELECT '1996', iso, ano_1996 FROM Temporal_PIB''')
        cursor.execute('''INSERT INTO PIB_Data(yearr, iso, pib) SELECT '1997', iso, ano_1997 FROM Temporal_PIB''')
        cursor.execute('''INSERT INTO PIB_Data(yearr, iso, pib) SELECT '1998', iso, ano_1998 FROM Temporal_PIB''')
        cursor.execute('''INSERT INTO PIB_Data(yearr, iso, pib) SELECT '1999', iso, ano_1999 FROM Temporal_PIB''')
        cursor.execute('''INSERT INTO PIB_Data(yearr, iso, pib) SELECT '2000', iso, ano_2000 FROM Temporal_PIB''')
        cursor.execute('''INSERT INTO PIB_Data(yearr, iso, pib) SELECT '2001', iso, ano_2001 FROM Temporal_PIB''')
        cursor.execute('''INSERT INTO PIB_Data(yearr, iso, pib) SELECT '2002', iso, ano_2002 FROM Temporal_PIB''')
        cursor.execute('''INSERT INTO PIB_Data(yearr, iso, pib) SELECT '2003', iso, ano_2003 FROM Temporal_PIB''')
        cursor.execute('''INSERT INTO PIB_Data(yearr, iso, pib) SELECT '2004', iso, ano_2004 FROM Temporal_PIB''')
        cursor.execute('''INSERT INTO PIB_Data(yearr, iso, pib) SELECT '2005', iso, ano_2005 FROM Temporal_PIB''')
        cursor.execute('''INSERT INTO PIB_Data(yearr, iso, pib) SELECT '2006', iso, ano_2006 FROM Temporal_PIB''')
        cursor.execute('''INSERT INTO PIB_Data(yearr, iso, pib) SELECT '2007', iso, ano_2007 FROM Temporal_PIB''')
        cursor.execute('''INSERT INTO PIB_Data(yearr, iso, pib) SELECT '2008', iso, ano_2008 FROM Temporal_PIB''')
        cursor.execute('''INSERT INTO PIB_Data(yearr, iso, pib) SELECT '2009', iso, ano_2009 FROM Temporal_PIB''')
        cursor.execute('''INSERT INTO PIB_Data(yearr, iso, pib) SELECT '2010', iso, ano_2010 FROM Temporal_PIB''')
        cursor.execute('''INSERT INTO PIB_Data(yearr, iso, pib) SELECT '2011', iso, ano_2011 FROM Temporal_PIB''')
        cursor.execute('''INSERT INTO PIB_Data(yearr, iso, pib) SELECT '2012', iso, ano_2012 FROM Temporal_PIB''')
        cursor.execute('''INSERT INTO PIB_Data(yearr, iso, pib) SELECT '2013', iso, ano_2013 FROM Temporal_PIB''')
        cursor.execute('''INSERT INTO PIB_Data(yearr, iso, pib) SELECT '2014', iso, ano_2014 FROM Temporal_PIB''')
        cursor.execute('''INSERT INTO PIB_Data(yearr, iso, pib) SELECT '2015', iso, ano_2015 FROM Temporal_PIB''')
        cursor.execute('''INSERT INTO PIB_Data(yearr, iso, pib) SELECT '2016', iso, ano_2016 FROM Temporal_PIB''')
        cursor.execute('''INSERT INTO PIB_Data(yearr, iso, pib) SELECT '2017', iso, ano_2017 FROM Temporal_PIB''')
        cursor.execute('''INSERT INTO PIB_Data(yearr, iso, pib) SELECT '2018', iso, ano_2018 FROM Temporal_PIB''')
        cursor.execute('''INSERT INTO PIB_Data(yearr, iso, pib) SELECT '2019', iso, ano_2019 FROM Temporal_PIB''')
        cursor.execute('''INSERT INTO PIB_Data(yearr, iso, pib) SELECT '2020', iso, ano_2020 FROM Temporal_PIB''')
        conn.commit()
        cursor.execute('''INSERT INTO Covid_PIB(id_Pais, id_covid, id_pib) SELECT Pais.id_Pais, Covid_data.id_covid, PIB_data.id_pib FROM Pais
                                                                           INNER JOIN Covid_data ON Covid_data.iso=Pais.iso
                                                                           INNER JOIN PIB_data ON PIB_data.iso=Pais.iso''')
        conn.commit()
        conn.close()
        print('***** ---Modelo creado y cargado con exito--- *****')
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
        #COVID
        conn = pyodbc.connect(conn_data)
        #Creacion para tabla temporal
        inputdir0 = Path(__file__).with_name('SQL_esquema.sql')
        with inputdir0.open('r') as creats0:
            sqlScript0 = creats0.read()
            for statement0 in sqlScript0.split(';'):
                if statement0:
                    with conn.cursor() as cur2:
                        cur2.execute(statement0)
        bitacora.append({'hora':str(now.time()),'fecha': str(now.date()),'tipo':'transformar','descripcion':'Se creo la tabla temporal covid'})
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
        bitacora.append({'hora':str(now.time()),'fecha': str(now.date()),'tipo':'transformar','descripcion':'Se cargaron los datos a la tabla temporal covid'})
        print('')
        print('***** ---Carga realizada con exito--- *****\n')

        #ECONOMIA
        conn = pyodbc.connect(conn_data)
        #Creacion para tabla temporal
        inputdir1 = Path(__file__).with_name('SQL_PIB.sql')
        with inputdir1.open('r') as creats1:
            sqlScript1 = creats1.read()
            for statement1 in sqlScript1.split(';'):
                if statement1:
                    with conn.cursor() as cur2:
                        cur2.execute(statement1)
        bitacora.append({'hora':str(now.time()),'fecha': str(now.date()),'tipo':'transformar','descripcion':'Se creo la tabla temporal economia'})
        #Insercion  para la temporal
        inputdir2 = Path(__file__).with_name('clean_data_econo.csv')
        with inputdir2.open('r',encoding="utf8") as creats2:
            reader2 = csv.reader(creats2)
            contador = 0
            for row in reader2:
                if contador != 0:
                    data =f"""INSERT INTO Temporal_PIB (pais,iso,ano_1960,ano_1961,ano_1962,ano_1963,ano_1964,ano_1965,ano_1966,ano_1967,ano_1968,ano_1969,ano_1970,ano_1971,
                              ano_1972,ano_1973,ano_1974,ano_1975,ano_1976,ano_1977,ano_1978,ano_1979,ano_1980,ano_1981,ano_1982,ano_1983,ano_1984,ano_1985,ano_1986,ano_1987,
                              ano_1988,ano_1989,ano_1990,ano_1991,ano_1992,ano_1993,ano_1994,ano_1995,ano_1996,ano_1997,ano_1998,ano_1999,ano_2000,ano_2001,ano_2002,ano_2003,
                              ano_2004,ano_2005,ano_2006,ano_2007,ano_2008,ano_2009,ano_2010,ano_2011,ano_2012,ano_2013,ano_2014,ano_2015,ano_2016,ano_2017,ano_2018,ano_2019,
                              ano_2020) 
                            VALUES ('{row[0].replace("'","")}', '{row[1].replace("'","")}',{row[2]}, {row[3]},{row[4]}, {row[5]},{row[6]},
                            {row[7]},{row[8]},{row[9]},{row[10]},{row[11]}, {row[12]},{row[13]}, {row[14]},'{row[15].replace("'","")}', {row[16]},{row[17]}, {row[18]},{row[19]},
                            {row[20]},{row[21]},{row[22]},{row[23]},{row[24]},{row[25]},{row[26]},{row[27]},{row[28]},{row[29]},{row[30]},{row[31]},{row[32]},{row[33]},{row[34]},
                            {row[35]},{row[36]},{row[37]},{row[38]},{row[39]},{row[40]},{row[41]},{row[42]},{row[43]},{row[44]},{row[45]},{row[46]},{row[47]},{row[48]},{row[49]},
                            {row[50]},{row[51]},{row[52]},{row[53]},{row[54]},{row[55]},{row[56]},{row[57]},{row[58]},{row[59]},{row[60]},{row[61]},{row[62]})"""
                    with conn.cursor() as cur:
                        cur.execute(data)
                else:
                    contador+=1
        conn.close()
        bitacora.append({'hora':str(now.time()),'fecha': str(now.date()),'tipo':'transformar','descripcion':'Se cargaron los datos a la tabla temporal economia'})
        print('')
        print('***** ---Carga realizado con exito--- *****\n')


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
bd = 'covid'
user = 'hdb1'
password = '1234'
conn_data = 'DRIVER={ODBC Driver 17 for SQL server}; SERVER='+server+'; DATABASE='+bd+'; UID='+user+'; PWD='+password
conn_data='Driver={SQL Server};''Server=DJULIO\MSSQLSERVER01;''Database=covid;''Trusted_Connection=yes;'
opciones ={
    '0' : {'Des': 'Extraer informacion', 'Funcion': extraer_0, 'Param1':'p1','Param2' :'p2'},
    '1' : {'Des': 'Transformacion de Informacion', 'Funcion': transform_1, 'Param1':'p1','Param2' :'p2'},
    '2' : {'Des': 'Carga', 'Funcion': cargar_temp_2, 'Param1':'p1','Param2' :'p2'},
    '3' : {'Des': 'Crear Modelo', 'Funcion': crear_modelo_0, 'Param1':'p1','Param2' :'p2'},
    '4' : {'Des': 'Crear Datamarts', 'Funcion': crear_datamarts_0, 'Param1':'p1','Param2' :'p2'},
    '5' : {'Des': 'Realizar Consultas', 'Funcion': generacion_archivo, 'Param1':'p1','Param2' :'p2'},
    '6' : {'Des': 'Realizar bitacora', 'Funcion': reporte_bitacora, 'Param1':'p1','Param2' :'p2'},
    'x' : {'Des': 'Salir', 'Funcion': cerrando_programa, 'Param1':'p1','Param2' :'p2'}
    }

op,accion,s='','',25
while op!='x':
    op=menu(accion,s)
    accion=accion +' ' + op
    cls()
print('-'*s,'Fin del programa --$> ',accion)
