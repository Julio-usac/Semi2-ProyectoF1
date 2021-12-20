use covid;
DROP TABLE IF EXISTS Covid_PIB_SG;
DROP TABLE IF EXISTS PIB_SG;
DROP TABLE IF EXISTS Covid_SG;
DROP TABLE IF EXISTS Pais_SG;
CREATE TABLE PIB_SG(
	id_pib INT PRIMARY KEY IDENTITY (1, 1),
	yearr int,
	pib float,
	iso varchar(100)
);
CREATE TABLE Covid_SG(
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
CREATE TABLE Pais_SG(
	id_pais INT PRIMARY KEY IDENTITY (1, 1),
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
	human_development_index float
);
CREATE TABLE Covid_PIB_SG(
	id_pais INT,
	id_pib INT,
	id_covid INT,
    FOREIGN KEY (id_pais) REFERENCES Pais_SG(id_pais),
	FOREIGN KEY (id_pib) REFERENCES PIB_SG(id_pib),
	FOREIGN KEY (id_covid) REFERENCES Covid_SG(id_covid)
);