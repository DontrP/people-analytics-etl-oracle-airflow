/*
===============================================================================
DDL Script: Create Silver Tables (Oracle Compatible)
===============================================================================
Author: Dollaya Piumsuwan
Date: 2025-08-21
Version: 1.0

Purpose:
    Create tables in the 'SILVER' schema in Oracle.
    Drops existing tables if they already exist.
    Silver tables store cleaned, standardized, and enriched data 
    derived from the raw Bronze layer for downstream analytics.

Notes:
    - Bronze tables must exist before running this script.
    - Silver tables typically include data type normalization, 
      derived columns, and auditing fields (e.g., dwh_create_date).
    - Run this script to re-define the DDL structure of Silver tables.
===============================================================================
*/


-- Drop tables if they exist
declare
   table_not_exist exception;
   pragma exception_init ( table_not_exist,-942 );
begin
   begin
      execute immediate 'DROP TABLE silver.employee_data CASCADE CONSTRAINTS';
   exception
      when table_not_exist then
         null;
   end;
   begin
      execute immediate 'DROP TABLE silver.employee_engagement_survey_data CASCADE CONSTRAINTS';
   exception
      when table_not_exist then
         null;
   end;
   begin
      execute immediate 'DROP TABLE silver.recruitment_data CASCADE CONSTRAINTS';
   exception
      when table_not_exist then
         null;
   end;
   begin
      execute immediate 'DROP TABLE silver.training_and_development_data CASCADE CONSTRAINTS';
   exception
      when table_not_exist then
         null;
   end;
end;
/

-- Create employee_data table
create table silver.employee_data (
   emp_id               nvarchar2(50),
   emp_firstname        nvarchar2(50),
   emp_lastname         nvarchar2(50),
   emp_startdate        date,
   emp_exitdate         date,
   emp_position         nvarchar2(50),
   emp_supervisor       nvarchar2(50),
   emp_email            nvarchar2(50),
   emp_unit             nvarchar2(50),
   emp_status           nvarchar2(50),
   emp_type             nvarchar2(50),
   emp_payzone          nvarchar2(50),
   emp_classification   nvarchar2(50),
   emp_termination_type nvarchar2(50),
   emp_department       nvarchar2(50),
   emp_division         nvarchar2(50),
   emp_dob              date,
   emp_state            nvarchar2(50),
   emp_job_function     nvarchar2(50),
   emp_gender           nvarchar2(50),
   emp_location         nvarchar2(50),
   emp_race             nvarchar2(50),
   emp_marital          nvarchar2(50),
   emp_score            nvarchar2(50),
   emp_rating           number,
   dwh_create_date      timestamp default systimestamp
);

-- Create employee_engagement_survey_data table
create table silver.employee_engagement_survey_data (
   emp_id                        nvarchar2(50),
   emp_engagement_survey_date    date,
   emp_engagement_survey_score   number,
   emp_satisfaction_survey_score number,
   emp_work_life_balance_score   number,
   dwh_create_date               timestamp default systimestamp
);

-- Create recruitment_data table
create table silver.recruitment_data (
   applicant_id             nvarchar2(50),
   application_date         date,
   applicant_firstname      nvarchar2(50),
   applicant_lastname       nvarchar2(50),
   applicant_gender         nvarchar2(50),
   applicant_dob            date,
   applicant_phone          nvarchar2(255),
   applicant_email          nvarchar2(50),
   applicant_address        nvarchar2(100),
   applicant_city           nvarchar2(50),
   applicant_state          nvarchar2(50),
   applicant_zip            nvarchar2(20),
   applicant_country        nvarchar2(150),
   applicant_education      nvarchar2(50),
   applicant_experience     number,
   applicant_desired_salary number(18,2),
   applicant_job_title      nvarchar2(150),
   applicant_status         nvarchar2(50),
   dwh_create_date          timestamp default systimestamp
);

-- Create training_and_development_data table
create table silver.training_and_development_data (
   emp_id                nvarchar2(50),
   training_date         date,
   training_program_name nvarchar2(100),
   training_type         nvarchar2(50),
   training_outcome      nvarchar2(100),
   location              nvarchar2(100),
   trainer               nvarchar2(100),
   training_duration     number,  -- Duration in days
   training_cost         number(18,2),
   dwh_create_date       timestamp default systimestamp
);