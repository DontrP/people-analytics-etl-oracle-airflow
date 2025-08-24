/*
===============================================================================
DDL Script: Create Gold Tables (Oracle)
===============================================================================
Purpose:
    This script creates the tables in the 'gold' schema (Star Schema).
    If tables already exist, they are dropped and re-created.
    This defines the Gold layer DDL structure for reporting (Star Schema).
===============================================================================
*/

-- Drop tables if they exist
declare
   table_not_exist exception;
   pragma exception_init ( table_not_exist,-942 );
begin
   -- Drop FACT tables first (because of FKs)
   begin
      execute immediate 'DROP TABLE gold.fact_training CASCADE CONSTRAINTS';
      dbms_output.put_line('Dropped table: gold.fact_training');
   exception
      when table_not_exist then
         dbms_output.put_line('Table gold.fact_training does not exist');
   end;

   begin
      execute immediate 'DROP TABLE gold.fact_recruitment CASCADE CONSTRAINTS';
      dbms_output.put_line('Dropped table: gold.fact_recruitment');
   exception
      when table_not_exist then
         dbms_output.put_line('Table gold.fact_recruitment does not exist');
   end;

   begin
      execute immediate 'DROP TABLE gold.fact_employee_engagement CASCADE CONSTRAINTS';
      dbms_output.put_line('Dropped table: gold.fact_employee_engagement');
   exception
      when table_not_exist then
         dbms_output.put_line('Table gold.fact_employee_engagement does not exist');
   end;

   -- Drop DIMENSION tables
   begin
      execute immediate 'DROP TABLE gold.dim_training CASCADE CONSTRAINTS';
      dbms_output.put_line('Dropped table: gold.dim_training');
   exception
      when table_not_exist then
         dbms_output.put_line('Table gold.dim_training does not exist');
   end;

   begin
      execute immediate 'DROP TABLE gold.dim_date CASCADE CONSTRAINTS';
      dbms_output.put_line('Dropped table: gold.dim_date');
   exception
      when table_not_exist then
         dbms_output.put_line('Table gold.dim_date does not exist');
   end;

   begin
      execute immediate 'DROP TABLE gold.dim_employee CASCADE CONSTRAINTS';
      dbms_output.put_line('Dropped table: gold.dim_employee');
   exception
      when table_not_exist then
         dbms_output.put_line('Table gold.dim_employee does not exist');
   end;
end;
/
--------------------------------------------------------------------------------
-- DIMENSION TABLES
--------------------------------------------------------------------------------

-- Dimension: Employee
create table gold.dim_employee (
   employee_key       number
      generated always as identity
   primary key,
   emp_id             nvarchar2(50),
   emp_firstname      nvarchar2(50),
   emp_lastname       nvarchar2(50),
   emp_position       nvarchar2(50),
   emp_supervisor     nvarchar2(50),
   emp_email          nvarchar2(50),
   emp_unit           nvarchar2(50),
   emp_department     nvarchar2(50),
   emp_division       nvarchar2(50),
   emp_state          nvarchar2(50),
   emp_job_function   nvarchar2(50),
   emp_gender         nvarchar2(50),
   emp_location       nvarchar2(50),
   emp_race           nvarchar2(50),
   emp_marital        nvarchar2(50),
   emp_type           nvarchar2(50),
   emp_status         nvarchar2(50),
   emp_payzone        nvarchar2(50),
   emp_classification nvarchar2(50),
   emp_dob            date,
   dwh_create_date    timestamp default systimestamp
);

-- Dimension: Date
create table gold.dim_date (
   date_key     number primary key,   -- YYYYMMDD format
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

-- Dimension: Training
create table gold.dim_training (
   training_key     number
      generated always as identity
   primary key,
   training_program nvarchar2(100),
   training_type    nvarchar2(50),
   trainer          nvarchar2(100),
   location         nvarchar2(100)
);

--------------------------------------------------------------------------------
-- FACT TABLES
--------------------------------------------------------------------------------

-- Fact: Employee Engagement
create table gold.fact_employee_engagement (
   fact_id                 number
      generated always as identity
   primary key,
   employee_key            number
      references gold.dim_employee ( employee_key ),
   survey_date_key         number
      references gold.dim_date ( date_key ),
   engagement_score        number,
   satisfaction_score      number,
   work_life_balance_score number,
   dwh_create_date         timestamp default systimestamp
);

-- Fact: Recruitment
create table gold.fact_recruitment (
   fact_id              number
      generated always as identity
   primary key,
   applicant_id         nvarchar2(20),
   application_date_key number
      references gold.dim_date ( date_key ),
   applicant_gender     nvarchar2(50),
   applicant_dob        date,
   applicant_city       nvarchar2(50),
   applicant_state      nvarchar2(50),
   applicant_country    nvarchar2(255),
   applicant_education  nvarchar2(50),
   applicant_experience number,
   desired_salary       number(18,2),
   job_title            nvarchar2(150),
   applicant_status     nvarchar2(50),
   dwh_create_date      timestamp default systimestamp
);

-- Fact: Training
create table gold.fact_training (
   fact_id           number
      generated always as identity
   primary key,
   employee_key      number
      references gold.dim_employee ( employee_key ),
   training_key      number
      references gold.dim_training ( training_key ),
   training_date_key number
      references gold.dim_date ( date_key ),
   training_duration number,
   training_cost     number(18,2),
   training_outcome  nvarchar2(100),
   dwh_create_date   timestamp default systimestamp
);