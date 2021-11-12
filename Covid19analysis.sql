/*
	Special thanks to 
	Research and data: Hannah Ritchie, Edouard Mathieu, Lucas Rodés-Guirao, Cameron Appel, Charlie Giattino, Esteban Ortiz-Ospina, Joe Hasell, Bobbie MacDonald, Diana Beltekian, Saloni Dattani and Max Roser
	ref: https://ourworldindata.org/covid-deaths free dataset for all
	sql scripting : Md. Masududzaman Khan (KHAN THE ANALYST)
	inspired by: ALEX THE ANALYST
*/

--Now we are going to analyse the data for many answers

--unmute it and create databse and import xlsx file into it
--create database DataAnalystProject;
/*SELECT * INTO MasterCovid19 FROM OPENROWSET('Microsoft.ACE.OLEDB.12.0',
                        'Excel 12.0;Database=C:\yourlocation\owid-covid-data.xlsx',
                        'SELECT * FROM [sheet1$]')
						
						or use sql import wizard to do that for you... your call, cheers
						*/

--select which database are going to use in this project

use DataAnalystProject;

--to read all the data from covid 19 master table

Select * from MasterCovid19
order by 1,2,3

--Now find out total covid infected by continental

select continent as Continent, MAX(total_cases) as Total_Cases from 
MasterCovid19 where continent is not Null group by continent;



--Now find out total Death vs total population by continent

select continent as Continent, MAX(cast(population as int)) as Population, MAX(total_cases) as Total_Cases 
from 
MasterCovid19 where continent is not Null group by continent;



--Now find out how many percentage are infected by continent

select continent as Continent, MAX(cast(population as int)) as Population, MAX(total_cases) as Total_Cases
, Max((total_cases/population))*100 as Infected_Per
from 
MasterCovid19 where continent is not Null group by continent;



--Now find out death ratio over population by continent

select continent as Continent
, MAX(cast(population as int)) as Population
, MAX(cast(total_cases as int)) as Total_Cases
, MAX(cast(total_deaths as int)) as Total_Death
, Max((total_cases/population))*100 as Infected_Ratio
, MAX((total_deaths/population))*100 as Total_Death_Ratio
from 
MasterCovid19 where continent is not Null group by continent order by 1,2,3;


--simplest way of doing thing
-- unless you use first database

--Now find out death ratio over total cases vs total death by location and date wise

select location, date, population, total_cases, total_deaths, (total_deaths/total_cases)*100 as Death_Percentage
from DataAnalystProject..MasterCovid19 
order by 1,2 desc



--Now find out death ratio over total cases by location wise filter (Exp: Bangladesh)

select location, date, population, total_cases, total_deaths, (total_deaths/total_cases)*100 as Death_Percentage
from DataAnalystProject..MasterCovid19 
where location Like '%Desh%' order by 5,6 desc



--Now find out Countries are highest infection rate over on population

select Location, Population, MAX(total_cases) as HighestInfectionCount, MAX(total_cases/population)*100 as PercentagePopulationInfected
from DataAnalystProject..MasterCovid19 
group by location, population
order by PercentagePopulationInfected desc



--Now find out highest death count on location

select Location, Max(cast(total_deaths as int)) as Total_Death_Count 
from DataAnalystProject..MasterCovid19
where continent is not null
group by location
order by Total_Death_Count desc



--Now find out highest death count on Continent also death ratio over population

select continent, max(cast(Population as int)) as population, Max(cast(total_deaths as int)) as Total_Death_Count, max((total_deaths/population)*100) as total_death_Percentage 
from DataAnalystProject..MasterCovid19
where continent is not null
group by continent
order by total_death_Percentage desc



/*
 Now we going to make a copy of this table and set all the data into it and name it CovidVaccinations not by using wizard
 then we will have two diffent tables to digg and make relationship to retrive answer
*/

select * into DataAnalystProject..CovidVaccinations from DataAnalystProject..MasterCovid19



--lets join two table and find out new vacination flow over population location wise

select mc.continent, mc.location,mc.date,mc.population,cv.new_vaccinations,
Sum(convert(int,cv.new_vaccinations)) over (Partition by mc.location order by mc.location, mc.date)
as Total_Veccination
from DataAnalystProject..MasterCovid19 mc 
join DataAnalystProject..CovidVaccinations cv 
on mc.location = cv.location 
and mc.date = cv.date
where mc.continent is not null and mc.location is not null
order by 2,3


/*
in this query ...after execution, you will have warning
like : "Null value is eliminated by an aggregate or other SET operation."
the error says, NULLs are being ignored because we are using aggregate function (SUM, AVG). 
To avoid the warning we can use “set ansi_warnings off” before the script. Here is the modified script.

also you will get warnings like : Arithmetic overflow error converting expression to data type int.
--just change datatype int to bigint
*/

SET ANSI_WARNINGS OFF;
GO
select mc.continent, mc.location,mc.date,mc.population,cv.new_vaccinations,
Sum(CAST(cv.new_vaccinations as bigint)) over (Partition by mc.location order by mc.location, mc.date)
as Total_Veccination
from DataAnalystProject..MasterCovid19 mc 
join DataAnalystProject..CovidVaccinations cv 
on mc.location = cv.location 
and mc.date = cv.date
where mc.continent is not null and mc.location is not null
order by 2,3


--using CTE 

SET ANSI_WARNINGS OFF;
GO

with PopulationVSVaccination (Continent, Location, Date, Population,New_Vaccination,Total_Rolling_Vaccination) as
(
select mc.continent, mc.location,mc.date,mc.population,cv.new_vaccinations,
Sum(CAST(cv.new_vaccinations as bigint)) over (Partition by mc.location order by mc.location, mc.date)
as Total_Rolling_Vaccination
from DataAnalystProject..MasterCovid19 mc 
join DataAnalystProject..CovidVaccinations cv 
on mc.location = cv.location 
and mc.date = cv.date
where mc.continent is not null and mc.location is not null
)
select *, (Total_Rolling_Vaccination/Population)*100 as Vaccination_Percatage from PopulationVSVaccination

--and  creating TEMP table, droping TEMP table


--Creating View 
--on highest death count on location

create view HighestDeathLocation as
select Location, Max(cast(total_deaths as int)) as Total_Death_Count 
from DataAnalystProject..MasterCovid19
where continent is not null
group by location



--on highest death count on Continent wise

create view HighestDeathContinent as
select continent, max(cast(Population as int)) as population, Max(cast(total_deaths as int)) as Total_Death_Count, max((total_deaths/population)*100) as total_death_Percentage 
from DataAnalystProject..MasterCovid19
where continent is not null
group by continent


--Create a view find out death ratio over total cases by location wise filter (Exp: Bangladesh)
create view DeathRationLocationFilter as
select location, date, population, total_cases, total_deaths, (total_deaths/total_cases)*100 as Death_Percentage
from DataAnalystProject..MasterCovid19 
where location Like '%Desh%'