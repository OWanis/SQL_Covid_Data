-- CREATING TABLE FOR COVID DEATHS
CREATE TABLE coviddeaths
(
	iso_code VARCHAR(25),
    continent VARCHAR(25),
    location VARCHAR(50),
    date DATE,
    population BIGINT,
    total_cases BIGINT,
    new_cases INT,
    new_cases_smoothed DOUBLE,
    total_deaths INT,
    new_deaths INT,
    new_deaths_smoothed DOUBLE,
    total_cases_per_million DOUBLE,
    new_cases_per_million DOUBLE,
    new_cases_smoothed_per_million DOUBLE,
    total_deaths_per_million DOUBLE,
    new_deaths_per_million DOUBLE,
    new_deaths_smoothed_per_million DOUBLE,
    reproduction_rate DOUBLE,
    icu_patients INT,
    icu_patients_per_million DOUBLE,
    hosp_patients INT,
    hosp_patients_per_million DOUBLE,
    weekly_icu_admissions INT,
    weekly_icu_admissions_per_million DOUBLE,
    weekly_hosp_admissions INT,
    weekly_hosp_admissions_per_million DOUBLE,
    total_tests BIGINT
);

-- FIXING PROBLEMS WITH IMPORTING
SHOW VARIABLES LIKE 'secure_file_priv';

-- IMPORTING DATA FOR COVID DEATHS
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/CovidDeaths_0.csv' INTO TABLE coviddeaths
FIELDS TERMINATED BY ','
IGNORE 1 LINES;

-- CHECKING COVID DEATHS DATA
SELECT *
FROM coviddeaths
ORDER BY 3,4;

-- CREATING TABLE FOR COVID VACCINES
CREATE TABLE covidvaccines
(
	iso_code VARCHAR(25),
    continent VARCHAR(25),
    location VARCHAR(50),
    date DATE,
    total_tests BIGINT,
	new_tests INT,
	total_tests_per_thousand DOUBLE,
	new_tests_per_thousand DOUBLE,
	new_tests_smoothed INT,
	new_tests_smoothed_per_thousand DOUBLE,
	positive_rate DOUBLE,
	tests_per_case DOUBLE,
	tests_units VARCHAR(50),
	total_vaccinations BIGINT,
	people_vaccinated BIGINT,
	people_fully_vaccinated BIGINT,
	total_boosters BIGINT,
	new_vaccinations INT,
	new_vaccinations_smoothed INT,
	total_vaccinations_per_hundred DOUBLE,
	people_vaccinated_per_hundred DOUBLE,
	people_fully_vaccinated_per_hundred DOUBLE,
	total_boosters_per_hundred DOUBLE,
	new_vaccinations_smoothed_per_million DOUBLE,
	new_people_vaccinated_smoothed INT,
	new_people_vaccinated_smoothed_per_hundred DOUBLE,
	stringency_index DOUBLE,
	population_density DOUBLE,
	median_age DOUBLE,
	aged_65_older DOUBLE,
	aged_70_older DOUBLE,
	gdp_per_capita DOUBLE,
	extreme_poverty DOUBLE,
	cardiovasc_death_rate DOUBLE,
	diabetes_prevalence DOUBLE,
	female_smokers DOUBLE,
	male_smokers DOUBLE,
	handwashing_facilities DOUBLE,
	hospital_beds_per_thousand DOUBLE,
	life_expectancy DOUBLE,
	human_development_index DOUBLE,
	excess_mortality_cumulative_absolute DOUBLE,
	excess_mortality_cumulative DOUBLE,
	excess_mortality DOUBLE,
	excess_mortality_cumulative_per_million DOUBLE
);

-- IMPORTING DATA FOR COVID VACCINES
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/CovidVaccines_0.csv' INTO TABLE covidvaccines
FIELDS TERMINATED BY ','
IGNORE 1 LINES;

-- CHANGING COLUMN TYPES THAT DON'T FIT
ALTER TABLE covidvaccines
MODIFY COLUMN total_boosters BIGINT;

-- CHECKING COVID VACCINES DATA
SELECT *
FROM covidvaccines
ORDER BY 3,4;

-- SELECTING THE COLUMNS WE NEED
SELECT location, date, total_cases, new_cases, total_deaths, population
FROM coviddeaths
ORDER BY 1,2;

-- LOOKGING AT TOTAL CASES VS TOTAL DEATHS FOR UNITED STATES ONLY
SELECT location, date, total_cases, total_deaths, (total_deaths/total_cases)*100 AS death_percentage
FROM coviddeaths
WHERE location LIKE '%states'
ORDER BY death_percentage DESC;

-- LOOKING AT TOTAL CASES VS POPULATION FOR UNITED STATES ONLY
SELECT location, date, total_cases, population, (total_cases/population)*100 AS infected_percentage
FROM coviddeaths
WHERE location LIKE '%states'
ORDER BY infected_percentage DESC;

-- LOOKING AT COUNTRIES WITH HIGHEST INFECTION RATE COMPARED TO POPULATION
SELECT location, population, MAX(total_cases) as highest_infection, MAX((total_cases/population)*100) AS infected_percentage
FROM coviddeaths
GROUP BY location, population
ORDER BY highest_infection DESC;

-- LOOKING AT COUNTRIES WITH HIGHEST DEATHS COUNT/RATE COMPARED TO POPULATION
SELECT location, population, MAX(total_deaths) as total_death_count, MAX((total_deaths/population)*100) AS death_percentage
FROM coviddeaths
GROUP BY location, population
ORDER BY death_percentage DESC;

-- BREAKING DOWN BY CONTINENT
SELECT continent, SUM(population) as total_population, MAX(total_deaths) as total_death_count, MAX((total_deaths/population)*100) AS death_percentage
FROM coviddeaths
WHERE continent <> '0'
GROUP BY continent
ORDER BY total_death_count DESC;

-- BREAKING DOWN SOME GLOBAL NUMBERS
SELECT date, SUM(new_cases) as new_daily_cases, SUM(total_cases) as total_cases, SUM(new_deaths) as new_daily_deaths, SUM(total_deaths) as total_deaths
FROM coviddeaths
GROUP BY date
ORDER BY date ASC;

-- LARGEST NUMBERS
SELECT MAX(total_cases) as total_cases, MAX(total_deaths) as total_deaths
FROM coviddeaths;

-- JOINING WITH VACCINATIONS, USING CTE AND PARTITIONING
WITH pop_vs_vac (location, date, population, new_vaccination, total_vaccinations_per_date_per_country)
as
(
SELECT dea.location, dea.date, dea.population, vac.new_vaccinations,
SUM(vac.new_vaccinations) OVER (PARTITION BY dea.location ORDER BY dea.location, dea.date) as total_vaccinations_per_date_per_country
FROM coviddeaths dea
JOIN covidvaccines vac
ON dea.location = vac.location
AND dea.date = vac.date
WHERE dea.continent <> '0'
ORDER BY dea.location, dea.date
)
SELECT *, (total_vaccinations_per_date_per_country/population)*100 as pop_vs_vac
FROM pop_vs_vac
WHERE location = 'Albania';

-- CREATING VIEWS TO STORE DATA FOR LATER VIZ
CREATE VIEW percentpopulationvaccinate as
SELECT dea.location, dea.date, dea.population, vac.new_vaccinations,
SUM(vac.new_vaccinations) OVER (PARTITION BY dea.location ORDER BY dea.location, dea.date) as total_vaccinations_per_date_per_country
FROM coviddeaths dea
JOIN covidvaccines vac
ON dea.location = vac.location
AND dea.date = vac.date
WHERE dea.continent <> '0'
ORDER BY dea.location, dea.date;

SELECT *
FROM percentpopulationvaccinate;