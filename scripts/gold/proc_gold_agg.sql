/*
===============================================================================
Oracle Procedure: GOLD.LOAD_AGGREGATE_TABLES
===============================================================================
Author: Dollaya Piumsuwan
Date: 2025-08-12
Version: 1.0

Purpose:
    Populate all GOLD aggregate tables for HR analytics from fact and 
    dimension tables. Pre-calculates KPIs to improve reporting efficiency.

Workflow / Logic:
    1. Engagement Aggregates: Department, Job, Location (by Month)
    2. Recruitment Aggregates: Department (by Month)
    3. Training Aggregates: Program and Department (by Month)
    4. Employee Demographics Aggregates: Gender, Race, Job Level (by Month)
    5. Turnover Aggregates: Department, Job, Location (by Month)
    6. Truncate all aggregate tables before insertion using EXECUTE IMMEDIATE.
    7. Handle exceptions and rollback if errors occur.

Parameters:
    - None

Dependencies:
    - GOLD dimension and fact tables must exist.
    - KPIs are derived from pre-populated Gold fact tables.

Privileges:
    - Must have INSERT and TRUNCATE privileges on GOLD aggregate tables.
    - EXECUTE privilege required for calling the procedure.

Notes:
    - Pre-truncation ensures fresh data for each run.
    - Uses dynamic SQL (EXECUTE IMMEDIATE) to avoid errors when truncating empty tables.
    - Procedure should be called after Gold fact tables are fully loaded.

Schedule:
    - Called daily via Airflow DAG: load_gold_layer
    - Can also be executed manually in SQL*Plus or via Oracle client.
===============================================================================

*/

create or replace procedure gold.load_aggregate_tables is
begin
   /*-------------------------------------------------------------------------*/
   /* 1. Engagement Aggregates */
   /*-------------------------------------------------------------------------*/
   execute immediate 'TRUNCATE TABLE gold.agg_engagement_department_month';
   insert into gold.agg_engagement_department_month (
      department,
      division,
      year,
      month,
      avg_engagement,
      avg_satisfaction,
      avg_work_life_balance,
      num_surveys
   )
      select e.department,
             e.division,
             extract(year from d.full_date),
             extract(month from d.full_date),
             avg(f.engagement_score),
             avg(f.satisfaction_score),
             avg(f.work_life_balance_score),
             count(*)
        from gold.fact_employee_engagement_star f
        join gold.dim_employee_star e
      on f.employee_key = e.employee_key
        join gold.dim_date d
      on f.survey_date_key = d.date_key
       group by e.department,
                e.division,
                extract(year from d.full_date),
                extract(month from d.full_date);

   execute immediate 'TRUNCATE TABLE gold.agg_engagement_job_month';
   insert into gold.agg_engagement_job_month (
      job_title,
      year,
      month,
      avg_engagement,
      avg_satisfaction,
      avg_work_life_balance,
      num_surveys
   )
      select e.job_title,
             extract(year from d.full_date),
             extract(month from d.full_date),
             avg(f.engagement_score),
             avg(f.satisfaction_score),
             avg(f.work_life_balance_score),
             count(*)
        from gold.fact_employee_engagement_star f
        join gold.dim_employee_star e
      on f.employee_key = e.employee_key
        join gold.dim_date d
      on f.survey_date_key = d.date_key
       group by e.job_title,
                extract(year from d.full_date),
                extract(month from d.full_date);

   execute immediate 'TRUNCATE TABLE gold.agg_engagement_location_month';
   insert into gold.agg_engagement_location_month (
      location,
      state,
      year,
      month,
      avg_engagement,
      avg_satisfaction,
      avg_work_life_balance,
      num_surveys
   )
      select e.location,
             e.state,
             extract(year from d.full_date),
             extract(month from d.full_date),
             avg(f.engagement_score),
             avg(f.satisfaction_score),
             avg(f.work_life_balance_score),
             count(*)
        from gold.fact_employee_engagement_star f
        join gold.dim_employee_star e
      on f.employee_key = e.employee_key
        join gold.dim_date d
      on f.survey_date_key = d.date_key
       group by e.location,
                e.state,
                extract(year from d.full_date),
                extract(month from d.full_date);

   /*-------------------------------------------------------------------------*/
   /* 2. Recruitment Aggregates */
   /*-------------------------------------------------------------------------*/
   execute immediate 'TRUNCATE TABLE gold.agg_recruitment_department_month';
   insert into gold.agg_recruitment_department_month (
      department,
      division,
      year,
      month,
      num_applicants,
      avg_desired_salary,
      avg_experience
   )
      select e.department,
             e.division,
             extract(year from d.full_date),
             extract(month from d.full_date),
             count(r.applicant_id),
             avg(r.desired_salary),
             avg(r.applicant_experience)
        from gold.fact_recruitment_star r
        left join gold.dim_employee_star e
      on r.employee_key = e.employee_key
        left join gold.dim_date d
      on r.application_date_key = d.date_key
       group by e.department,
                e.division,
                extract(year from d.full_date),
                extract(month from d.full_date);

   execute immediate 'TRUNCATE TABLE gold.agg_recruitment_education_month';
   insert into gold.agg_recruitment_education_month (
      education,
      year,
      month,
      num_applicants,
      avg_desired_salary,
      avg_experience
   )
      select ed.education,
             extract(year from d.full_date),
             extract(month from d.full_date),
             count(r.applicant_id),
             avg(r.desired_salary),
             avg(r.applicant_experience)
        from gold.fact_recruitment_star r
        left join gold.dim_education_star ed
      on r.education_key = ed.education_key
        left join gold.dim_date d
      on r.application_date_key = d.date_key
       group by ed.education,
                extract(year from d.full_date),
                extract(month from d.full_date);

   execute immediate 'TRUNCATE TABLE gold.agg_recruitment_job_month';
   insert into gold.agg_recruitment_job_month (
      job_title,
      year,
      month,
      num_applicants,
      avg_desired_salary,
      avg_experience
   )
      select r.job_title,
             extract(year from d.full_date),
             extract(month from d.full_date),
             count(r.applicant_id),
             avg(r.desired_salary),
             avg(r.applicant_experience)
        from gold.fact_recruitment_star r
        left join gold.dim_date d
      on r.application_date_key = d.date_key
       group by r.job_title,
                extract(year from d.full_date),
                extract(month from d.full_date);

   /*-------------------------------------------------------------------------*/
   /* 3. Training Aggregates */
   /*-------------------------------------------------------------------------*/
   execute immediate 'TRUNCATE TABLE gold.agg_training_program_month';
   insert into gold.agg_training_program_month (
      program_name,
      training_type,
      year,
      month,
      total_hours,
      total_cost,
      avg_duration
   )
      select tp.training_program_name,
             tp.training_type,
             extract(year from d.full_date),
             extract(month from d.full_date),
             sum(f.training_duration),
             sum(f.training_cost),
             avg(f.training_duration)
        from gold.fact_training_star f
        join gold.dim_training_program_star tp
      on f.training_program_key = tp.training_program_key
        join gold.dim_date d
      on f.training_date_key = d.date_key
       group by tp.training_program_name,
                tp.training_type,
                extract(year from d.full_date),
                extract(month from d.full_date);

   execute immediate 'TRUNCATE TABLE gold.agg_training_department_month';
   insert into gold.agg_training_department_month (
      department,
      division,
      year,
      month,
      total_hours,
      total_cost,
      num_participants
   )
      select e.department,
             e.division,
             extract(year from d.full_date),
             extract(month from d.full_date),
             sum(f.training_duration),
             sum(f.training_cost),
             count(distinct f.employee_key)
        from gold.fact_training_star f
        join gold.dim_employee_star e
      on f.employee_key = e.employee_key
        join gold.dim_date d
      on f.training_date_key = d.date_key
       group by e.department,
                e.division,
                extract(year from d.full_date),
                extract(month from d.full_date);

   execute immediate 'TRUNCATE TABLE gold.agg_training_employee_month';
   insert into gold.agg_training_employee_month (
      employee_key,
      year,
      month,
      total_hours,
      total_cost,
      num_passed,
      num_completed,
      num_failed
   )
      select f.employee_key,
             extract(year from d.full_date),
             extract(month from d.full_date),
             sum(f.training_duration),
             sum(f.training_cost),
             sum(
                case
                   when f.training_outcome = 'Passed' then
                      1
                   else
                      0
                end
             ),
             sum(
                case
                   when f.training_outcome = 'Completed' then
                      1
                   else
                      0
                end
             ),
             sum(
                case
                   when f.training_outcome = 'Failed' then
                      1
                   else
                      0
                end
             )
        from gold.fact_training_star f
        join gold.dim_date d
      on f.training_date_key = d.date_key
       group by f.employee_key,
                extract(year from d.full_date),
                extract(month from d.full_date);

   execute immediate 'TRUNCATE TABLE gold.agg_training_trainer_month';
   insert into gold.agg_training_trainer_month (
      trainer_key,
      trainer_name,
      location,
      year,
      month,
      total_hours,
      total_cost,
      num_participants
   )
      select t.trainer_key,
             t.trainer_name,
             t.location,
             extract(year from d.full_date),
             extract(month from d.full_date),
             sum(f.training_duration),
             sum(f.training_cost),
             count(distinct f.employee_key)
        from gold.fact_training_star f
        join gold.dim_trainer_star t
      on f.trainer_key = t.trainer_key
        join gold.dim_date d
      on f.training_date_key = d.date_key
       group by t.trainer_key,
                t.trainer_name,
                t.location,
                extract(year from d.full_date),
                extract(month from d.full_date);

   /*-------------------------------------------------------------------------*/
   /* 4. Employee Demographics Aggregates */
   /*-------------------------------------------------------------------------*/
   execute immediate 'TRUNCATE TABLE gold.agg_employee_gender_month';
   insert into gold.agg_employee_gender_month (
      gender,
      year,
      month,
      num_employees
   )
      select gender,
             extract(year from emp_startdate),
             extract(month from emp_startdate),
             count(*)
        from gold.dim_employee_star
       group by gender,
                extract(year from emp_startdate),
                extract(month from emp_startdate);

   execute immediate 'TRUNCATE TABLE gold.agg_employee_race_month';
   insert into gold.agg_employee_race_month (
      race,
      year,
      month,
      num_employees
   )
      select race,
             extract(year from emp_startdate),
             extract(month from emp_startdate),
             count(*)
        from gold.dim_employee_star
       group by race,
                extract(year from emp_startdate),
                extract(month from emp_startdate);

   execute immediate 'TRUNCATE TABLE gold.agg_employee_joblevel_month';
   insert into gold.agg_employee_joblevel_month (
      classification,
      year,
      month,
      num_employees
   )
      select classification,
             extract(year from emp_startdate),
             extract(month from emp_startdate),
             count(*)
        from gold.dim_employee_star
       group by classification,
                extract(year from emp_startdate),
                extract(month from emp_startdate);

   /*-------------------------------------------------------------------------*/
   /* 5. Turnover Aggregates */
   /*-------------------------------------------------------------------------*/
   execute immediate 'TRUNCATE TABLE gold.agg_turnover_department_month';
   insert into gold.agg_turnover_department_month (
      department,
      division,
      year,
      month,
      num_exits,
      avg_tenure
   )
      select e.department,
             e.division,
             extract(year from e.emp_exitdate),
             extract(month from e.emp_exitdate),
             count(*),
             avg(months_between(
                e.emp_exitdate,
                e.emp_startdate
             ))
        from gold.dim_employee_star e
       where e.emp_exitdate is not null
       group by e.department,
                e.division,
                extract(year from e.emp_exitdate),
                extract(month from e.emp_exitdate);

   execute immediate 'TRUNCATE TABLE gold.agg_turnover_job_month';
   insert into gold.agg_turnover_job_month (
      job_title,
      year,
      month,
      num_exits,
      avg_tenure
   )
      select e.job_title,
             extract(year from e.emp_exitdate),
             extract(month from e.emp_exitdate),
             count(*),
             avg(months_between(
                e.emp_exitdate,
                e.emp_startdate
             ))
        from gold.dim_employee_star e
       where e.emp_exitdate is not null
       group by e.job_title,
                extract(year from e.emp_exitdate),
                extract(month from e.emp_exitdate);

   execute immediate 'TRUNCATE TABLE gold.agg_turnover_location_month';
   insert into gold.agg_turnover_location_month (
      location,
      state,
      year,
      month,
      num_exits,
      avg_tenure
   )
      select e.location,
             e.state,
             extract(year from e.emp_exitdate),
             extract(month from e.emp_exitdate),
             count(*),
             avg(months_between(
                e.emp_exitdate,
                e.emp_startdate
             ))
        from gold.dim_employee_star e
       where e.emp_exitdate is not null
       group by e.location,
                e.state,
                extract(year from e.emp_exitdate),
                extract(month from e.emp_exitdate);

   commit;
   dbms_output.put_line('Aggregate tables populated successfully.');
exception
   when others then
      rollback;
      dbms_output.put_line('Error in loading aggregates: ' || sqlerrm);
      raise;
end;
/