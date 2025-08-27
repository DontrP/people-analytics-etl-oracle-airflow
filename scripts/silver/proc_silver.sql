/*
===============================================================================
Procedure Name: SILVER.LOAD_SILVER
===============================================================================
Author: Dollaya Piumsuwan
Date: 2025-08-21
Version: 1.0

Purpose:
    ETL procedure to populate Silver schema tables from Bronze tables.
    Performs minimal transformations: trimming, type casting, basic cleansing,
    and status mapping.

Actions:
    - Truncates Silver tables.
    - Inserts transformed data from Bronze to Silver.
    - Ensures data consistency for dates, numeric fields, and status values.

Dependencies:
    - Bronze tables must exist.
    - Silver schema tables must be created before execution.

Parameters:
    None

Usage:
    EXEC SILVER.LOAD_SILVER;

Notes:
    This procedure is designed to be part of the ETL workflow from Bronze to Silver.
===============================================================================
*/


create or replace procedure silver.load_silver as
begin
    -- Employee Data
   execute immediate 'TRUNCATE TABLE silver.employee_data';
   insert into silver.employee_data (
      emp_id,
      emp_firstname,
      emp_lastname,
      emp_startdate,
      emp_exitdate,
      emp_position,
      emp_supervisor,
      emp_email,
      emp_unit,
      emp_status,
      emp_type,
      emp_payzone,
      emp_classification,
      emp_termination_type,
      emp_department,
      emp_division,
      emp_dob,
      emp_state,
      emp_job_function,
      emp_gender,
      emp_location,
      emp_race,
      emp_marital,
      emp_score,
      emp_rating
   )
      select emp_id,
             to_nchar(trim(emp_firstname)),
             to_nchar(trim(emp_lastname)),
             case
                when emp_startdate is not null then
                   to_date(emp_startdate,
                           'DD-MON-RR')
             end,
             case
                when emp_exitdate is not null then
                   to_date(emp_exitdate,
                           'DD-MON-RR')
             end,
             to_nchar(trim(emp_position)),
             to_nchar(trim(emp_supervisor)),
             to_nchar(trim(emp_email)),
             to_nchar(trim(emp_unit)),
             case
                when emp_exitdate is not null then
                   N'Inactive'
                when upper(emp_status) = N'ACTIVE' then
                   N'Active'
                else
                   N'Inactive'
             end,
             to_nchar(trim(emp_type)),
             to_nchar(trim(emp_payzone)),
             to_nchar(trim(emp_classification)),
             case
                when upper(emp_termination_type) = N'UNK' then
                   N'n/a'
                when emp_exitdate is not null then
                   N'n/a'
                else
                   to_nchar(trim(emp_termination_type))
             end,
             to_nchar(trim(emp_department)),
             to_nchar(trim(emp_division)),
             case
                when emp_dob is not null then
                   to_date(emp_dob,
                           'DD-MM-YYYY')
             end,
             to_nchar(trim(emp_state)),
             to_nchar(trim(emp_job_function)),
             to_nchar(trim(emp_gender)),
             to_nchar(trim(emp_location)),
             to_nchar(trim(emp_race)),
             to_nchar(trim(emp_marital)),
             to_nchar(trim(emp_score)),
             case
                when emp_rating is not null then
                   to_number(emp_rating)
             end
        from bronze.employee_data
       where emp_id is not null;

    -- Employee Engagement Survey Data
   execute immediate 'TRUNCATE TABLE silver.employee_engagement_survey_data';
   insert into silver.employee_engagement_survey_data (
      emp_id,
      emp_engagement_survey_date,
      emp_engagement_survey_score,
      emp_satisfaction_survey_score,
      emp_work_life_balance_score
   )
      select emp_id,
             case
                when emp_engagement_survey_date is not null then
                   to_date(emp_engagement_survey_date,
                           'DD-MM-YYYY')
             end,
             to_number(emp_engagement_survey_score),
             to_number(emp_satisfaction_survey_score),
             to_number(emp_work_life_balance_score)
        from bronze.employee_engagement_survey_data;

    -- Recruitment Data
   execute immediate 'TRUNCATE TABLE silver.recruitment_data';
   insert into silver.recruitment_data (
      applicant_id,
      application_date,
      applicant_firstname,
      applicant_lastname,
      applicant_gender,
      applicant_dob,
      applicant_phone,
      applicant_email,
      applicant_address,
      applicant_city,
      applicant_state,
      applicant_zip,
      applicant_country,
      applicant_education,
      applicant_experience,
      applicant_desired_salary,
      applicant_job_title,
      applicant_status
   )
      select applicant_id,
             case
                when application_date is not null then
                   to_date(application_date,
                           'DD-MON-RR')
             end,
             to_nchar(trim(applicant_firstname)),
             to_nchar(trim(applicant_lastname)),
             to_nchar(trim(applicant_gender)),
             case
                when applicant_dob is not null then
                   to_date(applicant_dob,
                           'DD-MM-YYYY')
             end,
             regexp_replace(
                replace(
                   replace(
                      upper(applicant_phone),
                      'X',
                      'EXT.'
                   ),
                   'x',
                   'ext.'
                ),
                '[^0-9EXT.]',  -- Remove everything except digits and 'EXT.'
                ''
             ) as applicant_phone,
             to_nchar(trim(applicant_email)),
             to_nchar(trim(applicant_address)),
             to_nchar(trim(applicant_city)),
             to_nchar(trim(applicant_state)),
             to_nchar(trim(applicant_zip)),
             to_nchar(trim(applicant_country)),
             to_nchar(trim(applicant_education)),
             applicant_experience,
             applicant_desired_salary,
             to_nchar(trim(applicant_job_title)),
             to_nchar(trim(applicant_status))
        from bronze.recruitment_data;

    -- Training and Development Data
   execute immediate 'TRUNCATE TABLE silver.training_and_development_data';
   insert into silver.training_and_development_data (
      emp_id,
      training_date,
      training_program_name,
      training_type,
      training_outcome,
      location,
      trainer,
      training_duration,
      training_cost
   )
      select emp_id,
             case
                when training_date is not null then
                   to_date(training_date,
                           'DD-MM-RR')
             end,
             to_nchar(trim(training_program_name)),
             to_nchar(trim(training_type)),
             to_nchar(trim(training_outcome)),
             to_nchar(trim(location)),
             to_nchar(trim(trainer)),
             training_duration,
             training_cost
        from bronze.training_and_development_data;

   commit;
end;
/