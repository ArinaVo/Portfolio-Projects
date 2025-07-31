-- SQL Project - Data Cleaning
-- https://www.kaggle.com/datasets/swaptr/layoffs-2022



SELECT *
FROM world_layoffs.layoffs;

SELECT COUNT(*)
FROM world_layoffs.layoffs;


-- First thing we want to do is to create a staging table. This is the one we will work in and clean the data. We want a table with the raw data in case something happens. 
-- Furthomore we will be mentioning tables without world_layoffs schema in the beginning. (After clicking on it in our schemas list it automatically asumes we use the one we clicked on)

CREATE TABLE layoffs_staging
LIKE layoffs;

INSERT layoffs_staging
SELECT *
FROM layoffs;

SELECT COUNT(*)
FROM layoffs_staging;


-- Now when we are data cleaning we usually follow a few steps:
-- 1. Check for duplicates and remove any
-- 2. Standardize data and fix errors
-- 3. Look at null and blank values
-- 4. Remove any columns and rows that are not necessary - few ways



#1. Removing Duplicates
-- First let's check for duplicates

SELECT *
FROM layoffs_staging;

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layoffs_staging;

-- Let's just look at "Oda" to confirm

SELECT *
FROM layoffs_staging
WHERE company = 'Oda';

-- It looks like these are all legitimate entries and shouldn't be deleted. We need to really look at every single row to be accurate

-- These are our real duplicates 

SELECT *
FROM (
	SELECT company, industry, total_laid_off,`date`,
		ROW_NUMBER() OVER (
			PARTITION BY company, industry, total_laid_off,`date`
			) AS row_num
	FROM 
		layoffs_staging
) duplicates
WHERE 
	row_num > 1;
    

-- These are the ones we want to delete where the row number is > 1

-- Next steps:

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, 
total_laid_off, percentage_laid_off, `date`,
stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

    
WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, 
total_laid_off, percentage_laid_off, `date`,
stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
DELETE
FROM duplicate_cte
WHERE row_num > 1;


-- One solution, which I think is a good one. It is to create a new column and add those row numbers in. 
-- Then delete where row numbers are greater than 1, then delete that column

ALTER TABLE layoffs_staging ADD row_num INT;

SELECT *
FROM layoffs_staging;


CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` INT,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` INT,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, 
total_laid_off, percentage_laid_off, `date`,
stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

SELECT *
FROM layoffs_staging2;

-- Now that we have this we can delete rows where row_num is greater than 1

SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

DELETE
FROM layoffs_staging2
WHERE row_num > 1;



#2. Standardizing Data

SELECT *
FROM layoffs_staging2;

-- We can see that some company names have additional spaces bafore. So we will use Trim in order to fix it

SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT DISTINCT(company)
FROM layoffs_staging2;

-- Now it looks good. Let's look at industry column

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

-- We can see that the Crypto has multiple different variations. We need to standardize that - let's say all to Crypto

SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

-- Now it's fixed. We may check location 

SELECT DISTINCT location
FROM layoffs_staging2
ORDER BY 1;

-- Location looks fine. Let's check country

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;

-- Everything looks good except apparently we have some "United States" and some "United States." with a period at the end. Let's standardize this

SELECT *
FROM layoffs_staging2
WHERE country LIKE 'United States%'
ORDER BY 1;

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';


-- After running it again we can see that it's fixed
SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;


-- Let's also fix the date column:

SELECT `date`
FROM layoffs_staging2;

-- We can use str to date to update this field

SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- After running it again we can see that it's fixed
SELECT `date`
FROM layoffs_staging2;


-- Now we can convert the data type properly

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

SELECT *
FROM world_layoffs.layoffs_staging2;

--  If we look at industry it looks like we have some null and empty rows, let's take a look at these

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

-- Let's take a look at these

SELECT *
FROM layoffs_staging2
WHERE company LIKE 'Bally%';

-- Nothing wrong here

SELECT * 
FROM layoffs_staging2
WHERE company = 'Airbnb';


-- It looks like "Airbnb" is a Travel, but this one just isn't populated.
-- I'm sure it's the same for the others. What we can do is
-- to write a query that if there is another row with the same company name, it will update it to the non-null industry values.
-- Makes it easy so if there were thousands we wouldn't have to manually check them all

SELECT t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;


UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;


-- We should set the blanks to nulls since those are typically easier to work with

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- And if we check it looks like Bally's was the only one without a populated row to populate this null values

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;



#3. Looking at Null and Blank Values

-- The null values in total_laid_off, percentage_laid_off, and funds_raised_millions all look normal. I don't think I want to change that
-- I like having them null because it makes it easier for calculations during the EDA phase.
-- So there isn't anything I want to change with the null values



#4. Removing any columns and rows that are not necessary

SELECT *
FROM layoffs_staging2; 

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL;

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;


-- Deleting useless data we can't really use

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

SELECT * 
FROM world_layoffs.layoffs_staging2;