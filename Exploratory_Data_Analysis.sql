SELECT * 
FROM layoffs_staging2;

SELECT max(total_laid_off)
FROM layoffs_staging2;

SELECT *
FROM layoffs_staging2
WHERE  total_laid_off =12000;


SELECT * 
FROM layoffs_staging2
WHERE percentage_laid_off =1
ORDER by total_laid_off DESC ;


SELECT * 
FROM layoffs_staging2
WHERE percentage_laid_off =1
ORDER by funds_raised_millions DESC ;


SELECT company,SUM(total_laid_off)
FROM layoffs_staging2
group by company 
ORDER by 2 DESC 
limit 0,100;



SELECT industry ,SUM(total_laid_off)
FROM layoffs_staging2
group by industry 
ORDER by 2 DESC 
limit 0,100;


SELECT country ,SUM(total_laid_off)
FROM layoffs_staging2
group by country 
ORDER by 2 DESC 
limit 0,100;



SELECT `date` ,SUM(total_laid_off)
FROM layoffs_staging2
group by `date` 
ORDER by 1 DESC 
limit 0,100;


SELECT YEAR(`date`),SUM(total_laid_off)
FROM layoffs_staging2
group by YEAR(`date`) 
ORDER by 1 DESC 
limit 0,100;


 





-- rolling over data


SELECT SUBSTRING(`date`,6,2)  `month` ,SUM(total_laid_off)
FROM layoffs_staging2
group by `month` 
ORDER by 2 DESC 
limit 0,100;



SELECT SUBSTRING(`date`,1,7)  `year/month` ,SUM(total_laid_off)
FROM layoffs_staging2
group by `year/month` 
ORDER by 1  
limit 0,100;



with Rolling_Total As
(
SELECT SUBSTRING(`date`,1,7)  `year/month` , SUM(total_laid_off) AS total_off
FROM layoffs_staging2
group by `year/month` 
ORDER by 1  
)
SELECT `year/month`,total_off, SUM(total_off) over(ORDER by `year/month`) As roll_total
from Rolling_Total;




with Rolling_Total As
(
SELECT SUBSTRING(`date`,1,7)  `year/month` , SUM(total_laid_off) AS total_off
FROM layoffs_staging2
group by `year/month` 
ORDER by 1  
)
SELECT `year/month`, SUM(total_off) over(ORDER by SUBSTRING(`year/month`,1,4)) As roll_total
from Rolling_Total;



SELECT company,YEAR (`date`), SUM(total_laid_off)
FROM layoffs_staging2
group by company,YEAR (`date`)
order BY 3 DESC ;


with company_year(company,years,total_laid_off) As
(
SELECT company,YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
group by company,YEAR(`date`)
)
SELECT *, Dense_rank() over(partition by years order by total_laid_off desc) AS Ranking
FROM company_year
order by ranking;



with company_year(company,years,total_laid_off) As
(
SELECT company,YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
group by company,YEAR(`date`)
),
company_year_Ranking AS 
(
SELECT *, Dense_rank() over(partition by years order by total_laid_off desc) AS Ranking
FROM company_year
order by Ranking
)
SELECT *
FROM company_year_Ranking
where Ranking<=5;


SELECT YEAR (`date`) year , SUM(total_laid_off)  total_laid_offs
FROM layoffs_staging2
group by YEAR (`date`);


with years_total AS
(
SELECT YEAR (`date`) year , SUM(total_laid_off)  total_laid_offs
FROM layoffs_staging2
group by YEAR (`date`)
)
SELECT *, DENSE_RANK () over( order by total_laid_offs DESC) As ranking
FROM years_total;
