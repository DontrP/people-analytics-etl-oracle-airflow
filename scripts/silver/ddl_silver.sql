/*
===============================================================================
DDL Script: Create Silver Tables (Oracle Compatible)
===============================================================================
Author: Dollaya Piumsuwan
Date: 2025-08-11
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

comment on column silver.employee_data.emp_id is
   'Employee ID from source system';
comment on column silver.employee_data.emp_firstname is
   'Employee first name';
comment on column silver.employee_data.emp_lastname is
   'Employee last name';
comment on column silver.employee_data.emp_startdate is
   'Employee start date';
comment on column silver.employee_data.emp_exitdate is
   'Employee exit date';
comment on column silver.employee_data.emp_position is
   'Employee job position';
comment on column silver.employee_data.emp_supervisor is
   'Supervisor name';
comment on column silver.employee_data.emp_email is
   'Corporate email address';
comment on column silver.employee_data.emp_unit is
   'Business unit';
comment on column silver.employee_data.emp_status is
   'Employment status';
comment on column silver.employee_data.emp_type is
   'Employee type';
comment on column silver.employee_data.emp_payzone is
   'Pay zone';
comment on column silver.employee_data.emp_classification is
   'Employee classification';
comment on column silver.employee_data.emp_termination_type is
   'Termination type';
comment on column silver.employee_data.emp_department is
   'Department name';
comment on column silver.employee_data.emp_division is
   'Division name';
comment on column silver.employee_data.emp_dob is
   'Date of birth';
comment on column silver.employee_data.emp_state is
   'Work state';
comment on column silver.employee_data.emp_job_function is
   'Job function';
comment on column silver.employee_data.emp_gender is
   'Employee gender';
comment on column silver.employee_data.emp_location is
   'Work location';
comment on column silver.employee_data.emp_race is
   'Employee race';
comment on column silver.employee_data.emp_marital is
   'Marital status';
comment on column silver.employee_data.emp_score is
   'Performance score in text';
comment on column silver.employee_data.emp_rating is
   'Performance rating (numeric)';
comment on column silver.employee_data.dwh_create_date is
   'Record creation timestamp in data warehouse';

-- Create employee_engagement_survey_data table
create table silver.employee_engagement_survey_data (
   emp_id                        nvarchar2(50),
   emp_engagement_survey_date    date,
   emp_engagement_survey_score   number,
   emp_satisfaction_survey_score number,
   emp_work_life_balance_score   number,
   dwh_create_date               timestamp default systimestamp
);

comment on column silver.employee_engagement_survey_data.emp_id is
   'Employee ID';
comment on column silver.employee_engagement_survey_data.emp_engagement_survey_date is
   'Survey date';
comment on column silver.employee_engagement_survey_data.emp_engagement_survey_score is
   'Engagement survey score';
comment on column silver.employee_engagement_survey_data.emp_satisfaction_survey_score is
   'Satisfaction survey score';
comment on column silver.employee_engagement_survey_data.emp_work_life_balance_score is
   'Work-life balance survey score';
comment on column silver.employee_engagement_survey_data.dwh_create_date is
   'Record creation timestamp in data warehouse';

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

comment on column silver.recruitment_data.applicant_id is
   'Applicant ID';
comment on column silver.recruitment_data.application_date is
   'Application date';
comment on column silver.recruitment_data.applicant_firstname is
   'Applicant first name';
comment on column silver.recruitment_data.applicant_lastname is
   'Applicant last name';
comment on column silver.recruitment_data.applicant_gender is
   'Applicant gender';
comment on column silver.recruitment_data.applicant_dob is
   'Applicant date of birth';
comment on column silver.recruitment_data.applicant_phone is
   'Applicant phone number';
comment on column silver.recruitment_data.applicant_email is
   'Applicant email address';
comment on column silver.recruitment_data.applicant_address is
   'Applicant address';
comment on column silver.recruitment_data.applicant_city is
   'Applicant city';
comment on column silver.recruitment_data.applicant_state is
   'Applicant state';
comment on column silver.recruitment_data.applicant_zip is
   'Applicant ZIP code';
comment on column silver.recruitment_data.applicant_country is
   'Applicant country';
comment on column silver.recruitment_data.applicant_education is
   'Applicant education level';
comment on column silver.recruitment_data.applicant_experience is
   'Years of experience of applicant';
comment on column silver.recruitment_data.applicant_desired_salary is
   'Desired salary of applicant';
comment on column silver.recruitment_data.applicant_job_title is
   'Job title applied for';
comment on column silver.recruitment_data.applicant_status is
   'Application status';
comment on column silver.recruitment_data.dwh_create_date is
   'Record creation timestamp in data warehouse';


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

comment on column silver.training_and_development_data.emp_id is
   'Employee ID';
comment on column silver.training_and_development_data.training_date is
   'Date of training';
comment on column silver.training_and_development_data.training_program_name is
   'Training program name';
comment on column silver.training_and_development_data.training_type is
   'Training type';
comment on column silver.training_and_development_data.training_outcome is
   'Outcome of the training';
comment on column silver.training_and_development_data.location is
   'Location of training';
comment on column silver.training_and_development_data.trainer is
   'Trainer name';
comment on column silver.training_and_development_data.training_duration is
   'Duration of training in days';
comment on column silver.training_and_development_data.training_cost is
   'Cost of training';
comment on column silver.training_and_development_data.dwh_create_date is
   'Record creation timestamp in data warehouse';