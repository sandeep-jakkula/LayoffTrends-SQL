SELECT * 
FROM world_layoffs.layoffs;


CREATE TABLE world_layoffs.layoffs_staging 
LIKE world_layoffs.layoffs;



INSERT layoffs_staging 
SELECT * FROM world_layoffs.layoffs;

SELECT *
FROM world_layoffs.layoffs_staging
;



SELECT *
FROM (
	SELECT company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		world_layoffs.layoffs_staging
		
) duplicates
WHERE 
	row_num > 1;




WITH DELETE_CTE AS 
(
SELECT *
FROM (
	SELECT company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		world_layoffs.layoffs_staging
) duplicates
WHERE 
	row_num > 1
)
DELETE
FROM DELETE_CTE
;
	





WITH DELETE_CTE AS (
	SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions, 
    ROW_NUMBER() OVER (PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
	FROM world_layoffs.layoffs_staging
)
DELETE FROM world_layoffs.layoffs_staging
WHERE (company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions, row_num) IN (
	SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions, row_num
	FROM DELETE_CTE
) AND row_num > 1;


ALTER TABLE world_layoffs.layoffs_staging ADD row_num INT;



SELECT *
FROM world_layoffs.layoffs_staging
;





CREATE TABLE `world_layoffs`.`layoffs_staging2` (
`company` text,
`location`text,
`industry`text,
`total_laid_off` int DEFAULT NULL,
`percentage_laid_off` text,
`date` text,
`stage`text,
`country` text,
`funds_raised_millions` int DEFAULT NULL,
row_num INT
);




INSERT INTO world_layoffs.layoffs_staging2 (
    company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions, row_num
)
SELECT 
    company, 
    location, 
    industry, 
    total_laid_off, 
    percentage_laid_off, 
    date, 
    stage, 
    country, 
    funds_raised_millions,
    ROW_NUMBER() OVER (
        PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions
    ) AS row_num
FROM world_layoffs.layoffs_staging
WHERE total_laid_off IS NOT NULL;







SELECT total_laid_off, COUNT(*)
FROM world_layoffs.layoffs_staging
WHERE total_laid_off IS NULL OR total_laid_off NOT REGEXP '^[0-9]+$'
GROUP BY total_laid_off;




INSERT INTO world_layoffs.layoffs_staging2 (
    company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions, row_num
)
SELECT 
    company, 
    location, 
    industry, 
    total_laid_off, 
    percentage_laid_off, 
    date, 
    stage, 
    country, 
    funds_raised_millions,
    ROW_NUMBER() OVER (
        PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions
    ) AS row_num
FROM world_layoffs.layoffs_staging
WHERE total_laid_off IS NOT NULL AND total_laid_off REGEXP '^[0-9]+$' AND funds_raised_millions IS NOT NULL
  AND funds_raised_millions REGEXP '^[0-9]+$';
	

 
 
 SELECT *
FROM world_layoffs.layoffs_staging2
where total_laid_off IS NULL AND percentage_laid_off is NULL ;


DELETE FROM world_layoffs.layoffs_staging2
WHERE row_num >= 2;


SELECT *
FROM world_layoffs.layoffs_staging2
WHERE row_num >= 2;



-- standardizing the data

SELECT company, TRIM(company) 
FROM world_layoffs.layoffs_staging2;


update world_layoffs.layoffs_staging2
SET company =TRIM(company);

SELECT company 
FROM world_layoffs.layoffs_staging;

SELECT DISTINCT industry 
FROM world_layoffs.layoffs_staging2;

UPDATE world_layoffs.layoffs_staging2
SET industry ='Crypto'
WHERE industry LIKE 'Crypto%';


SELECT DISTINCT country 
FROM world_layoffs.layoffs_staging2;



UPDATE world_layoffs.layoffs_staging2
SET country =TRIM(TRAILING '.' FROM country ) 
WHERE country LIKE 'United States%';



update world_layoffs.layoffs_staging2
SET `date`= STR_TO_DATE(`date` , '%m/%d/%Y');

ALTER TABLE world_layoffs.layoffs_staging2
modify column  `date`  DATE;




SELECT * 
FROM world_layoffs.layoffs_staging2
WHERE industry IS NULL OR industry ="";



SELECT * 
FROM world_layoffs.layoffs_staging2
WHERE company ="Airbnb";


SELECT t1.industry,t2.industry
FROM world_layoffs.layoffs_staging2  t1
join world_layoffs.layoffs_staging2  t2
     on t1.company = t2.company 
     WHERE (t1.industry is NULL or t1.industry ="")
     AND t2.industry is NOT NULL ;
    
    
    
    UPDATE world_layoffs.layoffs_staging2  t1
     join world_layoffs.layoffs_staging2  t2
        on t1.company = t2.company 
     SET t1.industry= t2.industry
     WHERE (t1.industry is NULL or t1.industry ="")
     AND t2.industry is NOT NULL ;
    
    
    
    UPDATE world_layoffs.layoffs_staging2
    SET industry = NULL
    WHERE industry ="";
   
   
   
   SELECT t1.industry,t2.industry
FROM world_layoffs.layoffs_staging2  t1
join world_layoffs.layoffs_staging2  t2
     on t1.company = t2.company 
     WHERE (t1.industry is NULL or t1.industry ="")
     AND t2.industry is NOT NULL ;
    
    
   
   UPDATE world_layoffs.layoffs_staging2  t1
     join world_layoffs.layoffs_staging2  t2
        on t1.company = t2.company 
     SET t1.industry= t2.industry
     WHERE (t1.industry is NULL )
     AND t2.industry is NOT NULL ;
    
    
    
 SELECT * 
FROM world_layoffs.layoffs_staging2
WHERE company ="Airbnb";


SELECT * 
FROM world_layoffs.layoffs_staging2
WHERE industry IS NULL OR industry ="";


SELECT * 
FROM world_layoffs.layoffs_staging2;


ALTER Table world_layoffs.layoffs_staging2
drop column row_num;

SELECT * 
FROM world_layoffs.layoffs_staging2;

