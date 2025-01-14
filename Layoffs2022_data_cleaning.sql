-- SQL Project: Data Cleaning for Layoffs Dataset

-- Step 1: Create a staging table for cleaning data
CREATE TABLE world_layoffs.layoffs_staging LIKE world_layoffs.layoffs;
INSERT INTO world_layoffs.layoffs_staging SELECT * FROM world_layoffs.layoffs;

-- Step 2: Check for duplicates
-- Identify duplicate rows using ROW_NUMBER
WITH duplicate_rows AS (
    SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions,
           ROW_NUMBER() OVER (
               PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
           ) AS row_num
    FROM world_layoffs.layoffs_staging
)
SELECT * FROM duplicate_rows WHERE row_num > 1;

-- Remove duplicates from the staging table
WITH duplicate_rows AS (
    SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions,
           ROW_NUMBER() OVER (
               PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
           ) AS row_num
    FROM world_layoffs.layoffs_staging
)
DELETE FROM world_layoffs.layoffs_staging
WHERE (company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions, row_num) IN (
    SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions, row_num
    FROM duplicate_rows
) AND row_num > 1;

-- Step 3: Standardize data
-- Handle NULL or empty values in the "industry" column
UPDATE world_layoffs.layoffs_staging
SET industry = NULL
WHERE industry = '';

-- Populate missing "industry" values using other rows for the same company
UPDATE world_layoffs.layoffs_staging t1
JOIN world_layoffs.layoffs_staging t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL AND t2.industry IS NOT NULL;

-- Standardize variations in the "industry" column
UPDATE world_layoffs.layoffs_staging
SET industry = 'Crypto'
WHERE industry IN ('Crypto Currency', 'CryptoCurrency');

-- Standardize "country" values to remove trailing periods
UPDATE world_layoffs.layoffs_staging
SET country = TRIM(TRAILING '.' FROM country);

-- Format and update the "date" column to a proper DATE format
UPDATE world_layoffs.layoffs_staging
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- Modify the "date" column data type
ALTER TABLE world_layoffs.layoffs_staging
MODIFY COLUMN `date` DATE;

-- Step 4: Handle NULL values and clean up unnecessary data
-- Review rows with missing "total_laid_off" and "percentage_laid_off" values
SELECT * FROM world_layoffs.layoffs_staging
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;

-- Remove rows where "total_laid_off" and "percentage_laid_off" are both NULL
DELETE FROM world_layoffs.layoffs_staging
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;

-- Final cleanup: Drop any temporary columns
ALTER TABLE world_layoffs.layoffs_staging
DROP COLUMN row_num;

-- Review the cleaned data
SELECT * FROM world_layoffs.layoffs_staging;
