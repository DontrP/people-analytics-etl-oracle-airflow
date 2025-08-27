/*
===============================================================================
DDL Script: GOLD Star Schema for HR Analytics (Oracle)
===============================================================================
Author: Dollaya Piumsuwan
Date: 2025-08-12
Version: 1.0

Purpose:
    This script creates a star schema in the 'GOLD' schema for HR analytics.
    It includes denormalized dimensions, fact tables, and aggregate tables for
    pre-calculated KPIs to accelerate reporting.

Actions:
    - Drops existing GOLD tables if they exist (facts, dimensions, aggregates).
    - Creates denormalized dimension tables (employee, date, training programs, trainers, education).
    - Creates fact tables for employee engagement, recruitment, and training.
    - Creates aggregate tables for HR analytics by department, job, location, education, and other KPIs.

Dependencies:
    - Silver layer tables must exist and be populated before running this script.
    - Bronze layer tables provide upstream data to Silver; GOLD depends on Silver.
    - Users executing this script should have CREATE TABLE and DROP TABLE privileges in GOLD schema.

Privileges:
    - GOLD users should only access GOLD tables for reporting.
    - Silver users may have read-only privileges if needed for verification or incremental ETL.

Notes:
    - Aggregate tables are created as empty tables with CAST on all columns to avoid ORA-01723 errors.
    - FACT tables are dropped first due to foreign key dependencies on DIMENSION tables.
    - This DDL is intended to support downstream analytics and dashboards.
===============================================================================
*/


/* Exception for table not existing*/
declare
   table_not_exist exception;
   pragma exception_init ( table_not_exist,-942 );
begin
   -- Drop FACT tables first (because of FK dependencies)
   begin
      execute immediate 'DROP TABLE gold.fact_training_star CASCADE CONSTRAINTS';
   exception
      when table_not_exist then
         null;
   end;
   begin
      execute immediate 'DROP TABLE gold.fact_recruitment_star CASCADE CONSTRAINTS';
   exception
      when table_not_exist then
         null;
   end;
   begin
      execute immediate 'DROP TABLE gold.fact_employee_engagement_star CASCADE CONSTRAINTS';
   exception
      when table_not_exist then
         null;
   end;

   -- Drop DIMENSIONS (denormalized)
   begin
      execute immediate 'DROP TABLE gold.dim_employee_star CASCADE CONSTRAINTS';
   exception
      when table_not_exist then
         null;
   end;
   begin
      execute immediate 'DROP TABLE gold.dim_date CASCADE CONSTRAINTS';
   exception
      when table_not_exist then
         null;
   end;
   begin
      execute immediate 'DROP TABLE gold.dim_training_program_star CASCADE CONSTRAINTS';
   exception
      when table_not_exist then
         null;
   end;
   begin
      execute immediate 'DROP TABLE gold.dim_education_star CASCADE CONSTRAINTS';
   exception
      when table_not_exist then
         null;
   end;

   -- Drop AGGREGATE tables
   begin
      execute immediate 'DROP TABLE gold.agg_engagement_department_month';
   exception
      when table_not_exist then
         null;
   end;
   begin
      execute immediate 'DROP TABLE gold.agg_engagement_job_month';
   exception
      when table_not_exist then
         null;
   end;
   begin
      execute immediate 'DROP TABLE gold.agg_engagement_location_month';
   exception
      when table_not_exist then
         null;
   end;
   begin
      execute immediate 'DROP TABLE gold.agg_recruitment_department_month';
   exception
      when table_not_exist then
         null;
   end;
   begin
      execute immediate 'DROP TABLE gold.agg_recruitment_education_month';
   exception
      when table_not_exist then
         null;
   end;
   begin
      execute immediate 'DROP TABLE gold.agg_recruitment_job_month';
   exception
      when table_not_exist then
         null;
   end;
   begin
      execute immediate 'DROP TABLE gold.agg_training_program_month';
   exception
      when table_not_exist then
         null;
   end;
   begin
      execute immediate 'DROP TABLE gold.agg_training_employee_month';
   exception
      when table_not_exist then
         null;
   end;
   begin
      execute immediate 'DROP TABLE gold.agg_training_department_month';
   exception
      when table_not_exist then
         null;
   end;
   begin
      execute immediate 'DROP TABLE gold.agg_employee_gender_month';
   exception
      when table_not_exist then
         null;
   end;
   begin
      execute immediate 'DROP TABLE gold.agg_employee_race_month';
   exception
      when table_not_exist then
         null;
   end;
   begin
      execute immediate 'DROP TABLE gold.agg_employee_joblevel_month';
   exception
      when table_not_exist then
         null;
   end;
   begin
      execute immediate 'DROP TABLE gold.agg_turnover_department_month';
   exception
      when table_not_exist then
         null;
   end;
   begin
      execute immediate 'DROP TABLE gold.agg_turnover_job_month';
   exception
      when table_not_exist then
         null;
   end;
   begin
      execute immediate 'DROP TABLE gold.agg_turnover_location_month';
   exception
      when table_not_exist then
         null;
   end;
   begin
      execute immediate 'DROP TABLE gold.agg_training_trainer_month';
   exception
      when table_not_exist then
         null;
   end;
end;
/

/*------------------------------------------------------------------------------*/
/* DIMENSIONS*/
/*------------------------------------------------------------------------------*/

-- Employee Dimension (Flattened)
create table gold.dim_employee_star (
   employee_key         number
      generated always as identity start with 1 increment by 1
   primary key,
   emp_id               nvarchar2(50) not null,
   emp_firstname        nvarchar2(50),
   emp_lastname         nvarchar2(50),
   emp_email            nvarchar2(50),
   emp_dob              date,
   emp_startdate        date,
   emp_exitdate         date,
   emp_supervisor       nvarchar2(50),
   emp_score            nvarchar2(50),
   emp_rating           number,
   emp_termination_type nvarchar2(50),
   emp_type             nvarchar2(50),
   gender               nvarchar2(50),
   race                 nvarchar2(50),
   marital_status       nvarchar2(50),
   department           nvarchar2(50),
   division             nvarchar2(50),
   unit                 nvarchar2(50),
   job_title            nvarchar2(150),
   job_function         nvarchar2(50),
   classification       nvarchar2(50),
   payzone              nvarchar2(50),
   emp_status           nvarchar2(50),
   location             nvarchar2(100),
   state                nvarchar2(50),
   dwh_create_date      timestamp default systimestamp
);

comment on column gold.dim_employee_star.employee_key is
   'Primary surrogate key for the employee dimension';
comment on column gold.dim_employee_star.emp_id is
   'Employee ID from source system';
comment on column gold.dim_employee_star.emp_firstname is
   'Employee first name';
comment on column gold.dim_employee_star.emp_lastname is
   'Employee last name';
comment on column gold.dim_employee_star.emp_email is
   'Corporate email address';
comment on column gold.dim_employee_star.emp_dob is
   'Date of birth in DD-MON-YYYY format';
comment on column gold.dim_employee_star.emp_startdate is
   'Employee start date in DD-MON-YYYY format';
comment on column gold.dim_employee_star.emp_exitdate is
   'Employee exit date in DD-MON-YYYY format';
comment on column gold.dim_employee_star.emp_supervisor is
   'Supervisor first and last name';
comment on column gold.dim_employee_star.emp_score is
   'Performance score in text including PIP, Needs Improvement, Fully Meets, Exceeds';
comment on column gold.dim_employee_star.emp_rating is
   'Performance rating (numeric) from 1 - 5';
comment on column gold.dim_employee_star.emp_termination_type is
   'Termination type (Resignation, Retirement, Involuntary, Voluntary)';
comment on column gold.dim_employee_star.emp_type is
   'Employee type (Contract, Part-time, Full-time)';
comment on column gold.dim_employee_star.gender is
   'Employee gender (Male, Female)';
comment on column gold.dim_employee_star.race is
   'Employee race (Black, Asian, Other, White, Hispanic)';
comment on column gold.dim_employee_star.marital_status is
   'Marital status (Widowed, Single, Married, Divorced)';
comment on column gold.dim_employee_star.department is
   'Department name';
comment on column gold.dim_employee_star.division is
   'Division name';
comment on column gold.dim_employee_star.unit is
   'Business unit';
comment on column gold.dim_employee_star.job_title is
   'Job title';
comment on column gold.dim_employee_star.job_function is
   'Job function';
comment on column gold.dim_employee_star.classification is
   'Employee classification (Part-time, Full-time, Temporary)';
comment on column gold.dim_employee_star.payzone is
   'Pay zone (Zone A, Zone B, Zone C)';
comment on column gold.dim_employee_star.emp_status is
   'Employment status (Active, Inactive)';
comment on column gold.dim_employee_star.location is
   'Work location (zip code)';
comment on column gold.dim_employee_star.state is
   'State';
comment on column gold.dim_employee_star.dwh_create_date is
   'Record creation timestamp in data warehouse';

-- Date Dimension
create table gold.dim_date (
   date_key     number primary key,   -- YYYYMMDD
   full_date    date not null,
   day          number,
   month        number,
   month_name   varchar2(20),
   quarter      number,
   year         number,
   day_of_week  number,
   day_name     varchar2(20),
   week_of_year number,
   is_weekend   char(1),
   is_holiday   char(1)
);

comment on column gold.dim_date.date_key is
   'Primary key representing date in YYYYMMDD format';
comment on column gold.dim_date.full_date is
   'Full date value';
comment on column gold.dim_date.day is
   'Day of the month';
comment on column gold.dim_date.month is
   'Month number (1-12)';
comment on column gold.dim_date.month_name is
   'Full month name (e.g., January)';
comment on column gold.dim_date.quarter is
   'Quarter of the year (1-4)';
comment on column gold.dim_date.year is
   'Year';
comment on column gold.dim_date.day_of_week is
   'Numeric day of the week (1=Monday, 7=Sunday)';
comment on column gold.dim_date.day_name is
   'Day of the week name (e.g., Monday)';
comment on column gold.dim_date.week_of_year is
   'Week number of the year';
comment on column gold.dim_date.is_weekend is
   'Y if weekend, N otherwise';
comment on column gold.dim_date.is_holiday is
   'Y if public holiday, N otherwise';

-- Training Program Dimension 
create table gold.dim_training_program_star (
   training_program_key  number
      generated always as identity start with 1 increment by 1
   primary key,
   training_program_name nvarchar2(100) not null,
   training_type         nvarchar2(50) not null,
   dwh_create_date       timestamp default systimestamp,
   constraint uq_training_program unique ( training_program_name,
                                           training_type )
);

comment on column gold.dim_training_program_star.training_program_key is
   'Primary surrogate key for the training program dimension';
comment on column gold.dim_training_program_star.training_program_name is
   'Name of the training program';
comment on column gold.dim_training_program_star.training_type is
   'Type of the training program';
comment on column gold.dim_training_program_star.dwh_create_date is
   'Record creation timestamp in data warehouse';

-- Training Trainer Dimension
create table gold.dim_trainer_star (
   trainer_key     number
      generated always as identity
   primary key,
   trainer_name    nvarchar2(100) not null,
   location        nvarchar2(100),
   dwh_create_date timestamp default systimestamp,
   constraint uq_trainer unique ( trainer_name,
                                  location )
);

comment on column gold.dim_trainer_star.trainer_key is
   'Primary surrogate key for the trainer dimension';
comment on column gold.dim_trainer_star.trainer_name is
   'Trainer full name';
comment on column gold.dim_trainer_star.location is
   'Training location';
comment on column gold.dim_trainer_star.dwh_create_date is
   'Record creation timestamp in data warehouse';

-- Education Dimension*/
create table gold.dim_education_star (
   education_key number
      generated always as identity
   primary key,
   education     nvarchar2(50)
);

comment on column gold.dim_education_star.education_key is
   'Primary surrogate key for the education dimension';
comment on column gold.dim_education_star.education is
   'Education level or qualification description';

/*------------------------------------------------------------------------------*/
/* FACT TABLES*/
/*------------------------------------------------------------------------------*/

-- Employee Engagement Fact
create table gold.fact_employee_engagement_star (
   fact_id                 number
      generated always as identity start with 1 increment by 1
   primary key,
   employee_key            number
      references gold.dim_employee_star ( employee_key ),
   survey_date_key         number
      references gold.dim_date ( date_key ),
   engagement_score        number,
   satisfaction_score      number,
   work_life_balance_score number,
   dwh_create_date         timestamp default systimestamp
);

comment on column gold.fact_employee_engagement_star.fact_id is
   'Primary surrogate key for the employee engagement fact table';
comment on column gold.fact_employee_engagement_star.employee_key is
   'Foreign key to the employee dimension';
comment on column gold.fact_employee_engagement_star.survey_date_key is
   'Foreign key to the date dimension representing survey date in DD-MON-YYYY format';
comment on column gold.fact_employee_engagement_star.engagement_score is
   'Engagement score of the employee (from 1 to 5)';
comment on column gold.fact_employee_engagement_star.satisfaction_score is
   'Satisfaction score of the employee (from 1 to 5)';
comment on column gold.fact_employee_engagement_star.work_life_balance_score is
   'Work-life balance score of the employee (from 1 to 5)';
comment on column gold.fact_employee_engagement_star.dwh_create_date is
   'Record creation timestamp in data warehouse';

-- Recruitment Fact
create table gold.fact_recruitment_star (
   fact_id              number
      generated always as identity
   primary key,
   applicant_id         nvarchar2(50) not null,
   application_date_key number
      references gold.dim_date ( date_key ),
   employee_key         number
      references gold.dim_employee_star ( employee_key ),
   education_key        number
      references gold.dim_education_star ( education_key ),
   job_title            nvarchar2(150),
   desired_salary       number(18,2),
   applicant_status     nvarchar2(50),
   applicant_experience number,
   dwh_create_date      timestamp default systimestamp
);

comment on column gold.fact_recruitment_star.fact_id is
   'Primary surrogate key for the recruitment fact table';
comment on column gold.fact_recruitment_star.applicant_id is
   'Applicant ID from source system';
comment on column gold.fact_recruitment_star.application_date_key is
   'Foreign key to date dimension representing application date';
comment on column gold.fact_recruitment_star.employee_key is
   'Foreign key to employee dimension if applicant became an employee';
comment on column gold.fact_recruitment_star.education_key is
   'Foreign key to education dimension';
comment on column gold.fact_recruitment_star.job_title is
   'Previous job title';
comment on column gold.fact_recruitment_star.desired_salary is
   'Desired salary of the applicant';
comment on column gold.fact_recruitment_star.applicant_status is
   'Status of the applicant';
comment on column gold.fact_recruitment_star.applicant_experience is
   'Years of experience of the applicant';
comment on column gold.fact_recruitment_star.dwh_create_date is
   'Record creation timestamp in data warehouse';

-- Training Fact
create table gold.fact_training_star (
   fact_id              number
      generated always as identity start with 1 increment by 1
   primary key,
   employee_key         number
      references gold.dim_employee_star ( employee_key ),
   training_program_key number
      references gold.dim_training_program_star ( training_program_key ),
   trainer_key          number
      references gold.dim_trainer_star ( trainer_key ),
   training_date_key    number
      references gold.dim_date ( date_key ),
   training_duration    number,
   training_cost        number(18,2),
   training_outcome     nvarchar2(100),
   dwh_create_date      timestamp default systimestamp
);

comment on column gold.fact_training_star.fact_id is
   'Primary surrogate key for the training fact table';
comment on column gold.fact_training_star.employee_key is
   'Foreign key to employee dimension';
comment on column gold.fact_training_star.training_program_key is
   'Foreign key to training program dimension';
comment on column gold.fact_training_star.trainer_key is
   'Foreign key to trainer dimension';
comment on column gold.fact_training_star.training_date_key is
   'Foreign key to date dimension representing training date';
comment on column gold.fact_training_star.training_duration is
   'Duration of the training in days';
comment on column gold.fact_training_star.training_cost is
   'Cost of the training';
comment on column gold.fact_training_star.training_outcome is
   'Outcome of the training (e.g., Passed, Completed, Failed)';
comment on column gold.fact_training_star.dwh_create_date is
   'Record creation timestamp in data warehouse';

/*------------------------------------------------------------------------------*/
/* AGGREGATE TABLES (HR Analytics)*/
/* All columns defined with CAST to avoid ORA-01723*/
/*------------------------------------------------------------------------------*/

-- Engagement by Department / Month
create table gold.agg_engagement_department_month
   as
      select cast(null as nvarchar2(50)) as department,
             cast(null as nvarchar2(50)) as division,
             cast(null as number) as year,
             cast(null as number) as month,
             cast(null as number) as avg_engagement,
             cast(null as number) as avg_satisfaction,
             cast(null as number) as avg_work_life_balance,
             cast(null as number) as num_surveys
        from dual
       where 1 = 0;

/* Engagement by Job / Month*/
create table gold.agg_engagement_job_month
   as
      select cast(null as nvarchar2(150)) as job_title,
             cast(null as number) as year,
             cast(null as number) as month,
             cast(null as number) as avg_engagement,
             cast(null as number) as avg_satisfaction,
             cast(null as number) as avg_work_life_balance,
             cast(null as number) as num_surveys
        from dual
       where 1 = 0;

/* Engagement by Location / Month*/
create table gold.agg_engagement_location_month
   as
      select cast(null as nvarchar2(100)) as location,
             cast(null as nvarchar2(50)) as state,
             cast(null as number) as year,
             cast(null as number) as month,
             cast(null as number) as avg_engagement,
             cast(null as number) as avg_satisfaction,
             cast(null as number) as avg_work_life_balance,
             cast(null as number) as num_surveys
        from dual
       where 1 = 0;

/* Recruitment by Department / Month*/
create table gold.agg_recruitment_department_month
   as
      select cast(null as nvarchar2(50)) as department,
             cast(null as nvarchar2(50)) as division,
             cast(null as number) as year,
             cast(null as number) as month,
             cast(null as number) as num_applicants,
             cast(null as number) as avg_desired_salary,
             cast(null as number) as avg_experience
        from dual
       where 1 = 0;

/* Recruitment by Education / Month*/
create table gold.agg_recruitment_education_month
   as
      select cast(null as nvarchar2(50)) as education,
             cast(null as number) as year,
             cast(null as number) as month,
             cast(null as number) as num_applicants,
             cast(null as number) as avg_desired_salary,
             cast(null as number) as avg_experience
        from dual
       where 1 = 0;

/* Recruitment by Job / Month*/
create table gold.agg_recruitment_job_month
   as
      select cast(null as nvarchar2(150)) as job_title,
             cast(null as number) as year,
             cast(null as number) as month,
             cast(null as number) as num_applicants,
             cast(null as number) as avg_desired_salary,
             cast(null as number) as avg_experience
        from dual
       where 1 = 0;

/* Training by Program / Month*/
create table gold.agg_training_program_month
   as
      select cast(null as nvarchar2(100)) as program_name,
             cast(null as nvarchar2(50)) as training_type,
             cast(null as number) as year,
             cast(null as number) as month,
             cast(null as number) as total_hours,
             cast(null as number) as total_cost,
             cast(null as number) as avg_duration
        from dual
       where 1 = 0;

/* Training by Employee / Month*/
create table gold.agg_training_employee_month
   as
      select cast(null as number) as employee_key,
             cast(null as number) as year,
             cast(null as number) as month,
             cast(null as number) as total_hours,
             cast(null as number) as total_cost,
             cast(null as number) as num_passed,
             cast(null as number) as num_completed,
             cast(null as number) as num_failed
        from dual
       where 1 = 0;


/* Training by Department / Month*/
create table gold.agg_training_department_month
   as
      select cast(null as nvarchar2(50)) as department,
             cast(null as nvarchar2(50)) as division,
             cast(null as number) as year,
             cast(null as number) as month,
             cast(null as number) as total_hours,
             cast(null as number) as total_cost,
             cast(null as number) as num_participants
        from dual
       where 1 = 0;

/* Trainer by Month*/
create table gold.agg_training_trainer_month
   as
      select cast(null as number) as trainer_key,
             cast(null as nvarchar2(100)) as trainer_name,
             cast(null as nvarchar2(100)) as location,
             cast(null as number) as year,
             cast(null as number) as month,
             cast(null as number) as total_hours,
             cast(null as number) as total_cost,
             cast(null as number) as num_participants
        from dual
       where 1 = 0;

/* Employee Gender Distribution / Month*/
create table gold.agg_employee_gender_month
   as
      select cast(null as nvarchar2(50)) as gender,
             cast(null as number) as year,
             cast(null as number) as month,
             cast(null as number) as num_employees
        from dual
       where 1 = 0;

/* Employee Race Distribution / Month*/
create table gold.agg_employee_race_month
   as
      select cast(null as nvarchar2(50)) as race,
             cast(null as number) as year,
             cast(null as number) as month,
             cast(null as number) as num_employees
        from dual
       where 1 = 0;

/* Employee Job Level / Month*/
create table gold.agg_employee_joblevel_month
   as
      select cast(null as nvarchar2(50)) as classification,
             cast(null as number) as year,
             cast(null as number) as month,
             cast(null as number) as num_employees
        from dual
       where 1 = 0;

/* Turnover by Department / Month*/
create table gold.agg_turnover_department_month
   as
      select cast(null as nvarchar2(50)) as department,
             cast(null as nvarchar2(50)) as division,
             cast(null as number) as year,
             cast(null as number) as month,
             cast(null as number) as num_exits,
             cast(null as number) as avg_tenure
        from dual
       where 1 = 0;

/* Turnover by Job / Month*/
create table gold.agg_turnover_job_month
   as
      select cast(null as nvarchar2(150)) as job_title,
             cast(null as number) as year,
             cast(null as number) as month,
             cast(null as number) as num_exits,
             cast(null as number) as avg_tenure
        from dual
       where 1 = 0;

/* Turnover by Location / Month*/
create table gold.agg_turnover_location_month
   as
      select cast(null as nvarchar2(100)) as location,
             cast(null as nvarchar2(50)) as state,
             cast(null as number) as year,
             cast(null as number) as month,
             cast(null as number) as num_exits,
             cast(null as number) as avg_tenure
        from dual
       where 1 = 0;