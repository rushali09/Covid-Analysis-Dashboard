







--DROP TABLE IF EXISTS covid_dataset;



CREATE TABLE covid_dataset (
    iso_code TEXT,
    continent TEXT,
    location TEXT,
    date DATE,
    population BIGINT,

    total_cases DOUBLE PRECISION,
    new_cases DOUBLE PRECISION,
    new_cases_smoothed DOUBLE PRECISION,
    total_deaths DOUBLE PRECISION,
    new_deaths DOUBLE PRECISION,
    new_deaths_smoothed DOUBLE PRECISION,

    total_cases_per_million DOUBLE PRECISION,
    new_cases_per_million DOUBLE PRECISION,
    new_cases_smoothed_per_million DOUBLE PRECISION,
    total_deaths_per_million DOUBLE PRECISION,
    new_deaths_per_million DOUBLE PRECISION,
    new_deaths_smoothed_per_million DOUBLE PRECISION,

    reproduction_rate DOUBLE PRECISION,
    icu_patients DOUBLE PRECISION,
    icu_patients_per_million DOUBLE PRECISION,
    hosp_patients DOUBLE PRECISION,
    hosp_patients_per_million DOUBLE PRECISION,
    weekly_icu_admissions DOUBLE PRECISION,
    weekly_icu_admissions_per_million DOUBLE PRECISION,
    weekly_hosp_admissions DOUBLE PRECISION,
    weekly_hosp_admissions_per_million DOUBLE PRECISION
);



SELECT * FROM COVID_DATASET;
SELECT * FROM COVID_VACCINE_DATA;


CREATE TABLE covid_vaccine_data (
    iso_code TEXT,
    continent TEXT,
    location TEXT,
    date DATE,

    new_tests DOUBLE PRECISION,
    total_tests DOUBLE PRECISION,
    total_tests_per_thousand DOUBLE PRECISION,
    new_tests_per_thousand DOUBLE PRECISION,
    new_tests_smoothed DOUBLE PRECISION,
    new_tests_smoothed_per_thousand DOUBLE PRECISION,
    positive_rate DOUBLE PRECISION,
    tests_per_case DOUBLE PRECISION,
    tests_units TEXT,

    total_vaccinations DOUBLE PRECISION,
    people_vaccinated DOUBLE PRECISION,
    people_fully_vaccinated DOUBLE PRECISION,
    new_vaccinations DOUBLE PRECISION,
    new_vaccinations_smoothed DOUBLE PRECISION,
    total_vaccinations_per_hundred DOUBLE PRECISION,
    people_vaccinated_per_hundred DOUBLE PRECISION,
    people_fully_vaccinated_per_hundred DOUBLE PRECISION,
    new_vaccinations_smoothed_per_million DOUBLE PRECISION,

    stringency_index DOUBLE PRECISION,
    population DOUBLE PRECISION,
    population_density DOUBLE PRECISION,
    median_age DOUBLE PRECISION,
    aged_65_older DOUBLE PRECISION,
    aged_70_older DOUBLE PRECISION,
    gdp_per_capita DOUBLE PRECISION,
    extreme_poverty DOUBLE PRECISION,
    cardiovasc_death_rate DOUBLE PRECISION,
    diabetes_prevalence DOUBLE PRECISION,
    female_smokers DOUBLE PRECISION,
    male_smokers DOUBLE PRECISION,
    handwashing_facilities DOUBLE PRECISION,
    hospital_beds_per_thousand DOUBLE PRECISION,
    life_expectancy DOUBLE PRECISION,
    human_development_index DOUBLE PRECISION
);




Select *
From COVID_DATASET
Where continent is not null 
order by 3,4


-- Select Data that we are going to be starting with

Select Location, date, total_cases, new_cases, total_deaths, population
From COVID_DATASET
Where continent is not null 
order by 1,2


-- Total Cases vs Total Deaths
-- Shows likelihood of dying if you contract covid in your country

Select Location, date, total_cases,total_deaths, (total_deaths/total_cases)*100 as DeathPercentage
From COVID_DATASET
Where location like '%states%'
and continent is not null 
order by 1,2


-- Total Cases vs Population
-- Shows what percentage of population infected with Covid

Select Location, date, Population, total_cases,  (total_cases/population)*100 as PercentPopulationInfected
From COVID_DATASET
--Where location like '%states%'
order by 1,2
--**--

-- Countries with Highest Infection Rate compared to Population

Select Location, Population, MAX(total_cases) as HighestInfectionCount,  Max((total_cases/population))*100 as PercentPopulationInfected
From COVID_DATASET
--Where location like '%states%'
Group by Location, Population
order by PercentPopulationInfected desc


-- Countries with Highest Death Count per Population

Select Location, MAX(cast(Total_deaths as int)) as TotalDeathCount
From COVID_DATASET
--Where location like '%states%'
Where continent is not null 
Group by Location
order by TotalDeathCount desc



-- BREAKING THINGS DOWN BY CONTINENT

-- Showing contintents with the highest death count per population

Select continent, MAX(cast(Total_deaths as int)) as TotalDeathCount
From COVID_DATASET
--Where location like '%states%'
Where continent is not null 
Group by continent
order by TotalDeathCount desc



-- GLOBAL NUMBERS

Select SUM(new_cases) as total_cases, SUM(cast(new_deaths as int)) as total_deaths, SUM(cast(new_deaths as int))/SUM(New_Cases)*100 as DeathPercentage
From COVID_DATASET
--Where location like '%states%'
where continent is not null 
--Group By date
order by 1,2



-- Total Population vs Vaccinations
-- Shows Percentage of Population that has recieved at least one Covid Vaccine

SELECT
    dea.continent,
    dea.location,
    dea.date,
    dea.population,
    vac.new_vaccinations,
    SUM(CAST(vac.new_vaccinations AS INTEGER)) OVER (
        PARTITION BY dea.location
        ORDER BY dea.date
    ) AS RollingPeopleVaccinated
-- , (RollingPeopleVaccinated / population) * 100
FROM COVID_DATASET dea
JOIN COVID_VACCINE_DATA vac
    ON dea.location = vac.location
   AND dea.date = vac.date
WHERE dea.continent IS NOT NULL
ORDER BY dea.location, dea.date;



-- Using CTE to perform Calculation on Partition By in previous query
-- Using CTE to perform Calculation on Partition By in previous query
WITH PopvsVac (Continent, Location, Date, Population, New_Vaccinations, RollingPeopleVaccinated) AS (
    SELECT
        dea.continent,
        dea.location,
        dea.date,
        dea.population,
        vac.new_vaccinations,
        SUM(CAST(vac.new_vaccinations AS INTEGER)) OVER (
            PARTITION BY dea.location
            ORDER BY dea.date
        ) AS RollingPeopleVaccinated
    FROM COVID_DATASET dea
    JOIN COVID_VACCINE_DATA vac
        ON dea.location = vac.location
       AND dea.date = vac.date
    WHERE dea.continent IS NOT NULL
    ORDER BY 2, 3
)

SELECT *,
       (RollingPeopleVaccinated::FLOAT / Population) * 100 AS VaccinatedPercentage
FROM PopvsVac;




-- Using Temp Table to perform Calculation on Partition By in previous query
-- Drop table if exists
DROP TABLE IF EXISTS percent_population_vaccinated;

-- Create a temp table (optional: can use CREATE TEMP TABLE)
CREATE TEMP TABLE percent_population_vaccinated (
    continent TEXT,
    location TEXT,
    date DATE,
    population DOUBLE PRECISION,
    new_vaccinations DOUBLE PRECISION,
    rolling_people_vaccinated DOUBLE PRECISION
);

-- Insert data using corrected syntax
INSERT INTO percent_population_vaccinated
SELECT
    dea.continent,
    dea.location,
    dea.date,
    dea.population,
    vac.new_vaccinations,
    SUM(CAST(vac.new_vaccinations AS INTEGER)) OVER (
        PARTITION BY dea.location
        ORDER BY dea.date
    ) AS rolling_people_vaccinated
FROM covid_dataset dea
JOIN covid_vaccine_data vac
    ON dea.location = vac.location
   AND dea.date = vac.date;

-- Now select with percentage calculation
SELECT *,
       (rolling_people_vaccinated::FLOAT / population) * 100 AS percent_vaccinated
FROM percent_population_vaccinated;



-- Creating View to store data for later visualizations
-- Creating View to store data for later visualizations
CREATE VIEW percent_population_vaccinated AS
SELECT
    dea.continent,
    dea.location,
    dea.date,
    dea.population,
    vac.new_vaccinations,
    SUM(CAST(vac.new_vaccinations AS INTEGER)) OVER (
        PARTITION BY dea.location
        ORDER BY dea.date
    ) AS rolling_people_vaccinated
    -- , (rolling_people_vaccinated / population) * 100
FROM covid_dataset dea
JOIN covid_vaccine_data vac
    ON dea.location = vac.location
   AND dea.date = vac.date
WHERE dea.continent IS NOT NULL;

/*

Queries used for Tableau Project

*/



-- 1. 

Select SUM(new_cases) as total_cases, SUM(cast(new_deaths as int)) as total_deaths, SUM(cast(new_deaths as int))/SUM(New_Cases)*100 as DeathPercentage
From COVID_DATASET
--Where location like '%states%'
where continent is not null 
--Group By date
order by 1,2

-- Just a double check based off the data provided
-- numbers are extremely close so we will keep them - The Second includes "International"  Location


--Select SUM(new_cases) as total_cases, SUM(cast(new_deaths as int)) as total_deaths, SUM(cast(new_deaths as int))/SUM(New_Cases)*100 as DeathPercentage
--From PortfolioProject..CovidDeaths
----Where location like '%states%'
--where location = 'World'
----Group By date
--order by 1,2


-- 2. 

-- We take these out as they are not inluded in the above queries and want to stay consistent
-- European Union is part of Europe

Select location, SUM(cast(new_deaths as int)) as TotalDeathCount
From COVID_DATASET
--Where location like '%states%'
Where continent is null 
and location not in ('World', 'European Union', 'International')
Group by location
order by TotalDeathCount desc


-- 3.

Select Location, Population, MAX(total_cases) as HighestInfectionCount,  Max((total_cases/population))*100 as PercentPopulationInfected
From COVID_DATASET
--Where location like '%states%'
Group by Location, Population
order by PercentPopulationInfected desc


-- 4.


Select Location, Population,date, MAX(total_cases) as HighestInfectionCount,  Max((total_cases/population))*100 as PercentPopulationInfected
From COVID_DATASET
--Where location like '%states%'
Group by Location, Population, date
order by PercentPopulationInfected desc












--------**********--------------


-- 1.

Select dea.continent, dea.location, dea.date, dea.population
, MAX(vac.total_vaccinations) as RollingPeopleVaccinated
--, (RollingPeopleVaccinated/population)*100
From PortfolioProject..CovidDeaths dea
Join PortfolioProject..CovidVaccinations vac
	On dea.location = vac.location
	and dea.date = vac.date
where dea.continent is not null 
group by dea.continent, dea.location, dea.date, dea.population
order by 1,2,3




-- 2.
Select SUM(new_cases) as total_cases, SUM(cast(new_deaths as int)) as total_deaths, SUM(cast(new_deaths as int))/SUM(New_Cases)*100 as DeathPercentage
From PortfolioProject..CovidDeaths
--Where location like '%states%'
where continent is not null 
--Group By date
order by 1,2


-- Just a double check based off the data provided
-- numbers are extremely close so we will keep them - The Second includes "International"  Location


--Select SUM(new_cases) as total_cases, SUM(cast(new_deaths as int)) as total_deaths, SUM(cast(new_deaths as int))/SUM(New_Cases)*100 as DeathPercentage
--From PortfolioProject..CovidDeaths
----Where location like '%states%'
--where location = 'World'
----Group By date
--order by 1,2


-- 3.

-- We take these out as they are not inluded in the above queries and want to stay consistent
-- European Union is part of Europe

Select location, SUM(cast(new_deaths as int)) as TotalDeathCount
From PortfolioProject..CovidDeaths
--Where location like '%states%'
Where continent is null 
and location not in ('World', 'European Union', 'International')
Group by location
order by TotalDeathCount desc



-- 4.

Select Location, Population, MAX(total_cases) as HighestInfectionCount,  Max((total_cases/population))*100 as PercentPopulationInfected
From PortfolioProject..CovidDeaths
--Where location like '%states%'
Group by Location, Population
order by PercentPopulationInfected desc



-- 5.

--Select Location, date, total_cases,total_deaths, (total_deaths/total_cases)*100 as DeathPercentage
--From PortfolioProject..CovidDeaths
----Where location like '%states%'
--where continent is not null 
--order by 1,2

-- took the above query and added population
Select Location, date, population, total_cases, total_deaths
From PortfolioProject..CovidDeaths
--Where location like '%states%'
where continent is not null 
order by 1,2


-- 6. 


With PopvsVac (Continent, Location, Date, Population, New_Vaccinations, RollingPeopleVaccinated)
as
(
Select dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations
, SUM(CONVERT(int,vac.new_vaccinations)) OVER (Partition by dea.Location Order by dea.location, dea.Date) as RollingPeopleVaccinated
--, (RollingPeopleVaccinated/population)*100
From PortfolioProject..CovidDeaths dea
Join PortfolioProject..CovidVaccinations vac
	On dea.location = vac.location
	and dea.date = vac.date
where dea.continent is not null 
--order by 2,3
)
Select *, (RollingPeopleVaccinated/Population)*100 as PercentPeopleVaccinated
From PopvsVac


-- 7. 

Select Location, Population,date, MAX(total_cases) as HighestInfectionCount,  Max((total_cases/population))*100 as PercentPopulationInfected
From PortfolioProject..CovidDeaths
--Where location like '%states%'
Group by Location, Population, date
order by PercentPopulationInfected desc























