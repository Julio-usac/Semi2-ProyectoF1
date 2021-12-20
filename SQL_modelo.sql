use covid;
DROP TABLE IF EXISTS Covid_PIB;
DROP TABLE IF EXISTS PIB_data;
DROP TABLE IF EXISTS Covid_data;
DROP TABLE IF EXISTS Pais;
DROP TABLE IF EXISTS Continente;
CREATE TABLE Continente(
	id_continente INT PRIMARY KEY IDENTITY (1, 1),
	nombre varchar(100)
);

CREATE TABLE Pais(
	id_Pais INT PRIMARY KEY IDENTITY (1, 1),
	iso varchar(100),
	nombre varchar(100),
	populationn float,
	population_density float,
	median_age float,
	aged_65_older float,
	aged_70_older float,
	gdp_per_capita float,
	extreme_poverty float,
	cardiovasc_death_rate float,
	diabetes_prevalence float,
	female_smokers float,
	male_smokers float,
	handwashing_facilities float,
	hospital_beds_per_thousand float,
	life_expectancy float,
	human_development_index float,
	id_continente INT,
    FOREIGN KEY (id_continente) REFERENCES Continente(id_continente)
);

CREATE TABLE Covid_data(
	id_covid INT PRIMARY KEY IDENTITY (1, 1),
	iso varchar(100),
	datee varchar(255),
	total_cases float,
	new_cases float,
	total_deaths float,
	new_deaths float,
	reproduction_rate float,
	icu_patients float,
	hosp_patients float,
	new_tests float,
	total_tests float,
	positive_rate float,
	tests_per_case float,
	tests_units varchar(255),
	total_vaccinations float,
	people_vaccinated float,
	people_fully_vaccinated float,
	total_boosters float,
	new_vaccinations float,
	stringency_index float,
	excess_mortality float
);

CREATE TABLE PIB_data(
	id_pib INT PRIMARY KEY IDENTITY (1, 1),
	yearr int,
	pib float,
	iso varchar(100)
);

CREATE TABLE Covid_PIB(
	id_pais INT,
	id_pib INT,
	id_covid INT,
    FOREIGN KEY (id_pais) REFERENCES Pais(id_Pais),
	FOREIGN KEY (id_pib) REFERENCES PIB_data(id_pib),
	FOREIGN KEY (id_covid) REFERENCES Covid_data(id_covid)
);