/*
===============================================================
DDL Script: Create Bronze Tables for Oracle
===============================================================
Author: Dollaya Piumsuwan
Date: 2025-08-10
Version: 1.0

Purpose:
    Creates raw (Bronze) tables in the 'BRONZE' schema.
    Drops existing tables if they exist. These tables store 
    source-level data before any transformation.

Notes:
    - BRONZE tables are raw and untransformed.
    - Each table corresponds to a source system/entity.
===============================================================================
*/

-- Drop tables if they exist
declare
   table_not_exist exception;
   pragma exception_init ( table_not_exist,-942 );
begin
    -- Drop employee_data table if exists
   begin
      execute immediate 'DROP TABLE bronze.employee_data CASCADE CONSTRAINTS';
      dbms_output.put_line('Dropped table: bronze.employee_data');
   exception
      when table_not_exist then
         dbms_output.put_line('Table bronze.employee_data does not exist');
   end;
    
    -- Drop employee_engagement_survey_data table if exists
   begin
      execute immediate 'DROP TABLE bronze.employee_engagement_survey_data CASCADE CONSTRAINTS';
      dbms_output.put_line('Dropped table: bronze.employee_engagement_survey_data');
   exception
      when table_not_exist then
         dbms_output.put_line('Table bronze.employee_engagement_survey_data does not exist');
   end;
    
    -- Drop recruitment_data table if exists
   begin
      execute immediate 'DROP TABLE bronze.recruitment_data CASCADE CONSTRAINTS';
      dbms_output.put_line('Dropped table: bronze.recruitment_data');
   exception
      when table_not_exist then
         dbms_output.put_line('Table bronze.recruitment_data does not exist');
   end;
    
    -- Drop training_and_development_data table if exists
   begin
      execute immediate 'DROP TABLE bronze.training_and_development_data CASCADE CONSTRAINTS';
      dbms_output.put_line('Dropped table: bronze.training_and_development_data');
   exception
      when table_not_exist then
         dbms_output.put_line('Table bronze.training_and_development_data does not exist');
   end;
end;
/

-- Create employee_data table
create table bronze.employee_data (
   emp_id               nvarchar2(50),
   emp_firstname        nvarchar2(50),
   emp_lastname         nvarchar2(50),
   emp_startdate        nvarchar2(50),
   emp_exitdate         nvarchar2(50),
   emp_position         nvarchar2(50),
   emp_supervisor       nvarchar2(50),
   emp_email            nvarchar2(50),
   emp_unit             nvarchar2(50),
   emp_status           nvarchar2(50),
   emp_type             nvarchar2(50),
   emp_payzone          nvarchar2(50),
   emp_classification   nvarchar2(50),
   emp_termination_type nvarchar2(50),
   emp_termination_desc nvarchar2(50),
   emp_department       nvarchar2(50),
   emp_division         nvarchar2(50),
   emp_dob              nvarchar2(50),
   emp_state            nvarchar2(50),
   emp_job_function     nvarchar2(50),
   emp_gender           nvarchar2(50),
   emp_location         nvarchar2(50),
   emp_race             nvarchar2(50),
   emp_marital          nvarchar2(50),
   emp_score            nvarchar2(50),
   emp_rating           number
);

-- Create employee_engagement_survey_data table
create table bronze.employee_engagement_survey_data (
   emp_id                        nvarchar2(50),
   emp_engagement_survey_date    nvarchar2(50),
   emp_engagement_survey_score   number,
   emp_satisfaction_survey_score number,
   emp_work_life_balance_score   number
);

-- Create recruitment_data table
create table bronze.recruitment_data (
   applicant_id             nvarchar2(50),
   application_date         nvarchar2(50),
   applicant_firstname      nvarchar2(50),
   applicant_lastname       nvarchar2(50),
   applicant_gender         nvarchar2(50),
   applicant_dob            nvarchar2(50),
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
   applicant_job_title      nvarchar2(100),
   applicant_status         nvarchar2(100)
);

-- Create training_and_development_data table
create table bronze.training_and_development_data (
   emp_id                nvarchar2(50),
   training_date         nvarchar2(50),
   training_program_name nvarchar2(100),
   training_type         nvarchar2(50),
   training_outcome      nvarchar2(100),
   location              nvarchar2(100),
   trainer               nvarchar2(100),
   training_duration     number,  -- Duration in days
   training_cost         number(18,2)
);