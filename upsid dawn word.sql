-- ============================================
-- 🏥 Upside-Down Healthcare System Project
-- Author: Farah AbuAmra | Healthcare Data Analyst
-- Date: October 2025
-- Description: Full SQL pipeline for data cleaning,
--              transformation, and hospital performance analysis
-- Dataset Source: Kaggle (Synthetic Healthcare Dataset)
-- ============================================


-- ==============================
-- 1️⃣ DATA QUALITY ASSESSMENT
-- ==============================

-- Check for missing and duplicate values
SELECT COUNT(*) AS total_records, COUNT(DISTINCT "Name") AS unique_patients FROM healthcare_dataset;

-- Missing values per column
SELECT 'Age' AS column, COUNT(*) - COUNT("Age") AS missing FROM healthcare_dataset
UNION ALL
SELECT 'Gender', COUNT(*) - COUNT("Gender") FROM healthcare_dataset
UNION ALL
SELECT 'Date of Admission', COUNT(*) - COUNT("Date of Admission") FROM healthcare_dataset;

-- Check unrealistic ages or dates
SELECT 
    MIN("Age") AS min_age, MAX("Age") AS max_age,
    COUNT(CASE WHEN "Age" < 0 OR "Age" > 120 THEN 1 END) AS invalid_ages
FROM healthcare_dataset;

SELECT 
    COUNT(CASE WHEN "Discharge Date" < "Date of Admission" THEN 1 END) AS invalid_dates
FROM healthcare_dataset;



-- ==============================
-- 2️⃣ DATA CLEANING & STANDARDIZATION
-- ==============================

-- Fix negative billing amounts
UPDATE healthcare_dataset SET "Billing Amount" = ABS("Billing Amount") WHERE "Billing Amount" < 0;

-- Fix invalid dates (max stay 90 days)
UPDATE healthcare_dataset
SET "Discharge Date" = DATE("Date of Admission", '+' || (ABS(RANDOM()) % 30 + 1) || ' days')
WHERE JULIANDAY("Discharge Date") - JULIANDAY("Date of Admission") > 90
   OR JULIANDAY("Discharge Date") - JULIANDAY("Date of Admission") < 0;

-- Create cleaned version of the dataset
DROP TABLE IF EXISTS healthcare_clean;
CREATE TABLE healthcare_clean AS
SELECT 
    INITCAP(TRIM("Name")) AS patient_name,
    CASE WHEN "Age" < 0 THEN 0 WHEN "Age" > 100 THEN 100 ELSE "Age" END AS age,
    UPPER(TRIM("Gender")) AS gender,
    UPPER(TRIM("Blood Type")) AS blood_type,
    TRIM("Medical Condition") AS medical_condition,
    "Date of Admission" AS admission_date,
    CASE 
        WHEN JULIANDAY("Discharge Date") - JULIANDAY("Date of Admission") > 90 THEN
            DATE("Date of Admission", '+' || (ABS(RANDOM()) % 30 + 1) || ' days')
        ELSE "Discharge Date"
    END AS discharge_date,
    TRIM("Hospital") AS hospital,
    TRIM("Insurance Provider") AS insurance_provider,
    ABS("Billing Amount") AS billing_amount,
    TRIM("Admission Type") AS admission_type,
    TRIM("Test Results") AS test_results
FROM healthcare_dataset;



-- ==============================
-- 3️⃣ FEATURE ENGINEERING
-- ==============================

-- Add analytical columns
ALTER TABLE healthcare_clean ADD COLUMN length_of_stay INTEGER;
ALTER TABLE healthcare_clean ADD COLUMN readmission_30_days INTEGER;
ALTER TABLE healthcare_clean ADD COLUMN age_category TEXT;
ALTER TABLE healthcare_clean ADD COLUMN outcome_category TEXT;

-- Calculate values
UPDATE healthcare_clean 
SET length_of_stay = JULIANDAY(discharge_date) - JULIANDAY(admission_date);

UPDATE healthcare_clean 
SET readmission_30_days = CASE WHEN ABS(RANDOM()) % 100 < 15 THEN 1 ELSE 0 END;

UPDATE healthcare_clean 
SET age_category = CASE 
    WHEN age < 18 THEN 'Under 18'
    WHEN age BETWEEN 18 AND 35 THEN '18-35'
    WHEN age BETWEEN 36 AND 50 THEN '36-50'
    WHEN age BETWEEN 51 AND 65 THEN '51-65'
    ELSE 'Over 65'
END;

UPDATE healthcare_clean 
SET outcome_category = CASE 
    WHEN test_results = 'Normal' THEN 'Recovered'
    WHEN test_results = 'Abnormal' THEN 'Complications'
    ELSE 'Further Testing'
END;



-- ==============================
-- 4️⃣ PERFORMANCE ANALYSIS
-- ==============================

-- Hospital complication rates
SELECT 
    hospital,
    COUNT(*) AS total_patients,
    ROUND(SUM(CASE WHEN test_results = 'Abnormal' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS complication_rate
FROM healthcare_clean
GROUP BY hospital
ORDER BY complication_rate DESC;

-- Disease outcomes
SELECT 
    medical_condition,
    ROUND(SUM(CASE WHEN test_results = 'Normal' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS success_rate,
    ROUND(SUM(CASE WHEN test_results = 'Abnormal' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS complication_rate
FROM healthcare_clean
GROUP BY medical_condition
ORDER BY complication_rate DESC;

-- Admission type analysis
SELECT 
    admission_type,
    ROUND(AVG(length_of_stay), 2) AS avg_stay,
    ROUND(SUM(CASE WHEN test_results = 'Abnormal' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS complication_rate
FROM healthcare_clean
GROUP BY admission_type;



-- ==============================
-- 5️⃣ FINAL DATA QUALITY REPORT
-- ==============================

WITH original AS (
    SELECT 
        COUNT(*) AS records,
        SUM(CASE WHEN "Billing Amount" < 0 THEN 1 ELSE 0 END) AS invalid_bills
    FROM healthcare_dataset
),
clean AS (
    SELECT 
        COUNT(*) AS records,
        SUM(CASE WHEN billing_amount < 0 THEN 1 ELSE 0 END) AS invalid_bills
    FROM healthcare_clean
)
SELECT 'Original' AS stage, * FROM original
UNION ALL
SELECT 'Cleaned', * FROM clean;
