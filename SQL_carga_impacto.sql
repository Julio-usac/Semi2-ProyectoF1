USE covid;
INSERT INTO Pais3(nombre)
SELECT DISTINCT Pais.nombre
FROM Pais
WHERE Pais.nombre is not null;
INSERT INTO Cyear(n_year)
SELECT DISTINCT PIB_data.yearr
FROM PIB_data
WHERE PIB_data.yearr is not null;
INSERT INTO PIB3(iso,pais_id)
SELECT DISTINCT PIB_data.iso, Pais3.id_Pais
FROM PIB_data, Pais3, Temporal
WHERE PIB_data.iso is not null AND Pais3.nombre is not null AND PIB_data.id_pib IN 
(SELECT Covid_PIB.id_pib FROM Covid_PIB WHERE Covid_PIB.id_Pais IN (SELECT Pais.id_Pais  FROM Pais WHERE Pais.nombre =Pais3.nombre));
INSERT INTO Calculo_year(cantidad_pib,pib_id,cyear_id)
SELECT DISTINCT PIB_data.pib,PIB3.id_pib ,Cyear.id_year
FROM PIB_data, Cyear,PIB3
WHERE PIB_data.pib is not null AND Cyear.id_year is not null AND PIB_data.iso =PIB3.iso AND Cyear.n_year = PIB_data.yearr;