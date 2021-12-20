USE covid;
DROP TABLE IF EXISTS Calculo_year;
DROP TABLE IF EXISTS PIB3;
DROP TABLE IF EXISTS Pais3;
DROP TABLE IF EXISTS Cyear;
CREATE TABLE Pais3(
	id_Pais int IDENTITY(1,1) PRIMARY KEY,
	nombre varchar(255),

);
CREATE TABLE Cyear(
	id_year int IDENTITY(1,1) PRIMARY KEY,
	n_year varchar(255),

);
CREATE TABLE PIB3(
	id_pib int IDENTITY(1,1) PRIMARY KEY,
	iso varchar(255),
	pais_id int,
	FOREIGN KEY (pais_id) REFERENCES Pais3(id_Pais)
);
CREATE TABLE Calculo_year(
	id_cyear int IDENTITY(1,1) PRIMARY KEY,
	cantidad_pib float,
	pib_id int,
	cyear_id int,
	FOREIGN KEY (pib_id) REFERENCES PIB3(id_pib),
	FOREIGN KEY (cyear_id) REFERENCES Cyear(id_year)
);
