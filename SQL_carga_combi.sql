USE covid;
INSERT INTO Pais_SG(iso,nombre,populationn,population_density,median_age,aged_65_older,aged_70_older,gdp_per_capita,
extreme_poverty,cardiovasc_death_rate,diabetes_prevalence,female_smokers,male_smokers,handwashing_facilities,
hospital_beds_per_thousand,life_expectancy,human_development_index)
SELECT DISTINCT iso,nombre,populationn,population_density,median_age,aged_65_older,aged_70_older,gdp_per_capita,
extreme_poverty,cardiovasc_death_rate,diabetes_prevalence,female_smokers,male_smokers,handwashing_facilities,
hospital_beds_per_thousand,life_expectancy,human_development_index FROM Pais;       ;
INSERT INTO Covid_SG(iso,datee,total_cases,new_cases,total_deaths,new_deaths,reproduction_rate,icu_patients,hosp_patients,new_tests,
total_tests,positive_rate,tests_per_case,tests_units,total_vaccinations,people_vaccinated,people_fully_vaccinated,total_boosters,
new_vaccinations,stringency_index, excess_mortality)
SELECT iso,datee,total_cases,new_cases,total_deaths,new_deaths,reproduction_rate,icu_patients,hosp_patients,new_tests,
total_tests,positive_rate,tests_per_case,tests_units,total_vaccinations,people_vaccinated,people_fully_vaccinated,total_boosters,
new_vaccinations,stringency_index, excess_mortality FROM Covid_data;
INSERT INTO PIB_SG(yearr, iso, pib) SELECT yearr, iso, pib FROM PIB_data;
INSERT INTO Covid_PIB_SG(id_Pais, id_covid, id_pib) SELECT id_Pais, id_covid, id_pib FROM Covid_PIB;