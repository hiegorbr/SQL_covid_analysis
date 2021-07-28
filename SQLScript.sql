-- Criando tabela de apoio para a importação dos dados de vacinação do Covid 19

CREATE TABLE CovidVaccinations 
						(iso_code VARCHAR(50),	
						  continent VARCHAR(50),	
						  location VARCHAR(50),	
						  date DATE,
						  new_tests INT,
						  total_tests INT,
						  total_tests_per_thousand NUMERIC,
						  new_tests_per_thousand NUMERIC,
						  new_tests_smoothed NUMERIC,
						  new_tests_smoothed_per_thousand NUMERIC,
						  positive_rate NUMERIC,
						  tests_per_case NUMERIC,
						  tests_units VARCHAR(50),
						  total_vaccinations BIGINT,
						  people_vaccinated INT,
						  people_fully_vaccinated INT,
						  new_vaccinations INT,
						  new_vaccinations_smoothed NUMERIC,
						  total_vaccinations_per_hundred NUMERIC,
						  people_vaccinated_per_hundred NUMERIC,
						  people_fully_vaccinated_per_hundred NUMERIC,
						  new_vaccinations_smoothed_per_million NUMERIC,
						  stringency_index NUMERIC,
						  population_density NUMERIC,
						  median_age NUMERIC,
						  aged_65_older NUMERIC,
						  aged_70_older NUMERIC,
						  gdp_per_capita NUMERIC,
						  extreme_poverty NUMERIC,
						  cardiovasc_death_rate NUMERIC,
						  diabetes_prevalence NUMERIC,
						  female_smokers NUMERIC,
						  male_smokers NUMERIC,
						  handwashing_facilities NUMERIC,
						  hospital_beds_per_thousand NUMERIC,
						  life_expectancy NUMERIC,
						  human_development_index NUMERIC,
						  excess_mortality NUMERIC
);


-- Criando tabela de apoio para a importação dos dados de mortes por Covid 19


CREATE TABLE coviddeaths
						(iso_code VARCHAR(50),
						 continent VARCHAR(50),
						 location VARCHAR(50),
						 date DATE,
						 population	BIGINT, 
						 total_cases NUMERIC,
						 new_cases NUMERIC,
						 new_cases_smoothed	NUMERIC, 
						 total_deaths NUMERIC, 
						 new_deaths NUMERIC,
						 new_deaths_smoothed NUMERIC,
						 total_cases_per_million NUMERIC,
						 new_cases_per_million NUMERIC,
						 new_cases_smoothed_per_million NUMERIC,
						 total_deaths_per_million NUMERIC,
						 new_deaths_per_million NUMERIC,
						 new_deaths_smoothed_per_million NUMERIC,
						 reproduction_rate NUMERIC,
						 icu_patients NUMERIC,
						 icu_patients_per_million NUMERIC,
						 hosp_patients NUMERIC,
						 hosp_patients_per_million NUMERIC,
						 weekly_icu_admissions NUMERIC,
						 weekly_icu_admissions_per_million NUMERIC,
						 weekly_hosp_admissions NUMERIC,
						 weekly_hosp_admissions_per_million NUMERIC
 );
 


-- Importando os dados para as nossas tabelas

COPY coviddeaths (iso_code,	continent,	location,	date,	population,	total_cases,	new_cases,	new_cases_smoothed,	total_deaths,	new_deaths,	new_deaths_smoothed,	total_cases_per_million,	new_cases_per_million,	new_cases_smoothed_per_million,	total_deaths_per_million,	new_deaths_per_million,	new_deaths_smoothed_per_million,	reproduction_rate,	icu_patients,	icu_patients_per_million,	hosp_patients,	hosp_patients_per_million,	weekly_icu_admissions,	weekly_icu_admissions_per_million,	weekly_hosp_admissions,	weekly_hosp_admissions_per_million)

FROM 'C:/filepath/CovidDeaths.csv'

DELIMITER ';'

CSV HEADER;


COPY covidvaccinations 

FROM 'C:/filepath/CovidVaccinations.csv'

DELIMITER ';'

CSV HEADER;


-- Selecionando os dados que utilizaremos para entendermos como estão expressados

SELECT location, date, total_cases, new_cases, total_deaths, population

FROM coviddeaths

ORDER BY 1, 2;


-- Analisando o Total de casos x Total de mortes
-- Queremos o índice de mortalidade por dia no Brasil

SELECT location, date, total_cases, total_deaths,  ROUND((total_deaths/total_cases)*100, 2) AS DeathPercentage

FROM coviddeaths

WHERE location iLIKE 'Brazil'

ORDER BY 1, 2;


-- Analisando o Total de casos x População
-- Mostra a porcentagem da população infectada pelo Covid no Brasil

SELECT location, date, total_cases, population,  ROUND((total_cases/population)*100, 4) AS PercentofPopulationInfected

FROM coviddeaths

WHERE location iLIKE 'Brazil'

ORDER BY 1, 2;


-- Analisando os países com o maior índice de infecção comparado com a sua população

SELECT location, population, MAX(total_cases) AS HighestInfectionCount, ROUND(MAX((total_cases/population))*100, 2) AS PercentofPopulationInfected

FROM coviddeaths

GROUP BY 1, 2

HAVING MAX((total_cases/population)) > 0

ORDER BY 4 DESC;


-- Analisando os países com o maior número de mortes comparado com a sua população

SELECT location, population, MAX(total_deaths) AS TotalDeathCount

FROM coviddeaths

WHERE continent IS NOT NULL 

GROUP BY 1, 2

HAVING MAX(total_deaths) > 0

ORDER BY 3 DESC;


-- Vamos analisar mais a fundo cada continente

SELECT location, MAX(total_deaths) AS TotalDeathCount

FROM coviddeaths

WHERE continent IS NULL

GROUP BY 1

HAVING MAX(total_deaths) > 0

ORDER BY 2 DESC;


-- Números Globais

-- 1. Por dia

SELECT date, SUM(new_cases) as NewCases, SUM(new_deaths) AS NewDeaths, ROUND(SUM(new_deaths)/SUM(new_cases)*100, 2) as DeathPercentage

FROM coviddeaths

WHERE continent IS NOT NULL 

GROUP BY date

ORDER BY date;

-- 2. Geral

SELECT SUM(new_cases) as NewCases, SUM(new_deaths) AS NewDeaths, ROUND(SUM(new_deaths)/SUM(new_cases)*100, 2) as DeathPercentage

FROM coviddeaths

WHERE continent IS NOT NULL


--  Unindo as 2 tabelas para retirar mais informações

SELECT *

FROM coviddeaths cd

JOIN covidvaccinations cv
	ON cd.location = cv.location
	and cd.date = cv.date

-- Analisando o andamento da vacinação por país

SELECT cd.continent, cd.location, cd.date, cd.population, cv.new_vaccinations, SUM(cv.new_vaccinations) OVER (PARTITION BY cd.location ORDER BY cd.date) AS CummulativeVaccinations

FROM coviddeaths cd

JOIN covidvaccinations cv
	ON cd.location = cv.location
	AND cd.date = cv.date

WHERE cd.continent IS NOT NULL

ORDER BY 2, 3;


-- Comparando a população com o andamento da vacinação

WITH vacXpop (Continent, Location, Date, Population, New_Vaccinations, CummulativeVaccinations)
AS (
SELECT cd.continent, 
		cd.location, 
		cd.date, 
		cd.population, 
		cv.new_vaccinations, 
		SUM(cv.new_vaccinations) OVER (PARTITION BY cd.location ORDER BY cd.date) AS CummulativeVaccinations
FROM coviddeaths cd

JOIN covidvaccinations cv
	ON cd.location = cv.location
	AND cd.date = cv.date

WHERE cd.continent IS NOT NULL
)

SELECT *, ROUND((CAST(CummulativeVaccinations AS NUMERIC)/Population)*100, 2) as PercentVaccination

FROM vacXpop;