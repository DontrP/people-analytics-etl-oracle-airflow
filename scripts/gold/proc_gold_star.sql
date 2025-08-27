/*
===============================================================================
Procedure Name: gold.cleanup_fact_tables, gold.add_default_dimension_records,
                gold.load_dim_date, gold.load_dim_employee,
                gold.load_dim_education_star, gold.load_dim_training_program_star,
                gold.load_dim_trainer_star, gold.load_fact_employee_engagement,
                gold.load_fact_recruitment, gold.load_fact_training,
                gold.load_gold_layer
===============================================================================
Author: Dollaya Piumsuwan
Date: 2025-08-12
Version: 1.1

Purpose:
    ETL procedures to populate Gold schema tables from Silver tables.
    Populates dimension and fact tables, handles default/unknown records,
    and maintains surrogate keys for referential integrity.

Dependencies:
    - Silver tables must exist and be populated:
        * silver.employee_data
        * silver.employee_engagement_survey_data
        * silver.recruitment_data
        * silver.training_and_development_data
    - Gold schema tables (dimension and fact) must exist.
    - Gold schema must have SELECT privileges on Silver tables.

Privileges:
    - INSERT, DELETE, TRUNCATE, and EXECUTE privileges on Gold tables.
    - SELECT privileges on required Silver tables.

Parameters:
    None

Usage without Airflow:
    EXEC GOLD.LOAD_GOLD_LAYER;

Notes:
    - Default/unknown dimension records prevent foreign key violations.
    - Surrogate keys link Gold facts to dimensions.
    - Truncation ensures fresh data for each run.
    - Should be executed after Silver ETL is complete.
===============================================================================
*/


/* 1. Cleanup fact tables*/
create or replace procedure gold.cleanup_fact_tables is
begin
   execute immediate 'DELETE FROM gold.fact_training_star';
   execute immediate 'DELETE FROM gold.fact_recruitment_star';
   execute immediate 'DELETE FROM gold.fact_employee_engagement_star';
   commit;
end;
/

/* 2. Add default/unknown records to dimensions*/
create or replace procedure gold.add_default_dimension_records is
begin
    /* This ensures every dimension has a default record for unknown references*/
    /* Employee dimension*/
   insert into gold.dim_employee_star (
      emp_id,
      emp_firstname,
      emp_lastname,
      job_title,
      gender,
      location,
      department
   )
      select 'Unknown',
             'Unknown',
             'Unknown',
             'Unknown',
             'Unknown',
             'Unknown',
             'Unknown'
        from dual
       where not exists (
         select 1
           from gold.dim_employee_star
          where emp_id = 'Unknown'
      );

    /* Education dimension*/
   insert into gold.dim_education_star ( education )
      select 'Unknown'
        from dual
       where not exists (
         select 1
           from gold.dim_education_star
          where education = 'Unknown'
      );

    /* Trainer dimension*/
   insert into gold.dim_trainer_star (
      trainer_name,
      location
   )
      select 'Unknown',
             'Unknown'
        from dual
       where not exists (
         select 1
           from gold.dim_trainer_star
          where trainer_name = 'Unknown'
      );

   commit;
end;
/

/* 3. Load dim_date*/
create or replace procedure gold.load_dim_date is
   v_date date := date '2000-01-01';
   v_end  date := date '2050-12-31';
begin
   delete from gold.dim_date;

   while v_date <= v_end loop
      insert into gold.dim_date (
         date_key,
         full_date,
         day,
         month,
         month_name,
         quarter,
         year,
         day_of_week,
         day_name,
         week_of_year,
         is_weekend,
         is_holiday
      ) values ( to_number(to_char(
         v_date,
         'YYYYMMDD'
      )),
                 v_date,
                 to_number(to_char(
                    v_date,
                    'DD'
                 )),
                 to_number(to_char(
                    v_date,
                    'MM'
                 )),
                 trim(to_char(
                    v_date,
                    'Month'
                 )),
                 to_number(to_char(
                    v_date,
                    'Q'
                 )),
                 to_number(to_char(
                    v_date,
                    'YYYY'
                 )),
                 to_number(to_char(
                    v_date,
                    'D'
                 )),
                 trim(to_char(
                    v_date,
                    'Day'
                 )),
                 to_number(to_char(
                    v_date,
                    'IW'
                 )),
                 case
                    when to_char(
                       v_date,
                       'DY',
                       'NLS_DATE_LANGUAGE=ENGLISH'
                    ) in ( 'SAT',
                           'SUN' ) then
                       'Y'
                    else
                       'N'
                 end,
                 'N' );

      v_date := v_date + 1;
   end loop;

   commit;
end;
/

/* 4. Load dim_employee*/
create or replace procedure gold.load_dim_employee is
begin
   delete from gold.dim_employee_star;

   insert into gold.dim_employee_star (
      emp_id,
      emp_firstname,
      emp_lastname,
      emp_startdate,
      emp_exitdate,
      emp_dob,
      job_title,
      emp_supervisor,
      emp_email,
      department,
      division,
      unit,
      gender,
      location,
      race,
      marital_status,
      emp_score,
      emp_rating,
      emp_type,
      payzone,
      emp_status,
      classification,
      emp_termination_type,
      state,
      job_function
   )
      select distinct emp_id,
                      emp_firstname,
                      emp_lastname,
                      emp_startdate,
                      emp_exitdate,
                      emp_dob,
                      emp_position as job_title,
                      emp_supervisor,
                      emp_email,
                      emp_department as department,
                      emp_division as division,
                      emp_unit as unit,
                      nvl(
                         emp_gender,
                         'Unknown'
                      ) as gender,
                      nvl(
                         emp_location,
                         'Unknown'
                      ) as location,
                      nvl(
                         emp_race,
                         'Unknown'
                      ) as race,
                      nvl(
                         emp_marital,
                         'Unknown'
                      ) as marital_status,
                      emp_score,
                      emp_rating,
                      emp_type,
                      emp_payzone as payzone,
                      emp_status,
                      emp_classification as classification,
                      emp_termination_type,
                      emp_state as state,
                      emp_job_function as job_function
        from silver.employee_data;

   commit;
end;
/

/* 5.Load dim_education_star*/

create or replace procedure gold.load_dim_education_star is
begin
   delete from gold.dim_education_star;

   insert into gold.dim_education_star ( education )
      select distinct trim(applicant_education)
        from silver.recruitment_data
       where applicant_education is not null;

   commit;
   dbms_output.put_line(sql%rowcount || ' rows inserted into dim_education_star.');
end;
/

/* 6. Load dim_training_program_star*/
create or replace procedure gold.load_dim_training_program_star is
begin
   delete from gold.dim_training_program_star;

    /* Insert distinct programs from source*/
   insert into gold.dim_training_program_star (
      training_program_name,
      training_type
   )
      select distinct t.training_program_name,
                      t.training_type
        from silver.training_and_development_data t
       where t.training_program_name is not null
         and t.training_program_name != 'Unknown';

   dbms_output.put_line(sql%rowcount || ' rows inserted into dim_training_program_star.');
end;
/

/*7. Load dim_trainer_star*/
create or replace procedure gold.load_dim_trainer_star is
begin
    /* Remove existing data*/
   delete from gold.dim_trainer_star;

    /* Insert distinct trainers and their locations*/
   insert into gold.dim_trainer_star (
      trainer_name,
      location
   )
      select distinct t.trainer,
                      t.location
        from silver.training_and_development_data t
       where t.trainer is not null;

   commit;
   dbms_output.put_line(sql%rowcount || ' rows inserted into dim_trainer_star.');
end;
/

/* 8. Load fact_employee_engagement*/
create or replace procedure gold.load_fact_employee_engagement is
begin
   delete from gold.fact_employee_engagement_star;

   insert into gold.fact_employee_engagement_star (
      employee_key,
      survey_date_key,
      engagement_score,
      satisfaction_score,
      work_life_balance_score
   )
      select e.employee_key,  /* surrogate key*/
             to_number(to_char(
                s.emp_engagement_survey_date,
                'YYYYMMDD'
             )) as survey_date_key,
             s.emp_engagement_survey_score,
             s.emp_satisfaction_survey_score,
             s.emp_work_life_balance_score
        from silver.employee_engagement_survey_data s
        join gold.dim_employee_star e
      on e.emp_id = s.emp_id
       where s.emp_engagement_survey_date is not null;

   commit;
end;
/

/* 9. Load fact_recruitment*/
create or replace procedure gold.load_fact_recruitment is
begin
   delete from gold.fact_recruitment_star;

   insert into gold.fact_recruitment_star (
      applicant_id,
      application_date_key,
      employee_key,
      education_key,
      job_title,
      desired_salary,
      applicant_status,
      applicant_experience
   )
      select r.applicant_id,
             to_number(to_char(
                r.application_date,
                'YYYYMMDD'
             )),
             e.employee_key,  /* join with employee if exists*/
             d.education_key,
             nvl(
                r.applicant_job_title,
                'Unknown'
             ),
             r.applicant_desired_salary,
             nvl(
                r.applicant_status,
                'Unknown'
             ),
             r.applicant_experience
        from silver.recruitment_data r
        left join gold.dim_education_star d
      on d.education = nvl(
         r.applicant_education,
         'Unknown'
      )
        left join gold.dim_employee_star e
      on e.emp_email = r.applicant_email;

   commit;
end;
/

/* 10. Load fact_training*/
create or replace procedure gold.load_fact_training is
begin
   delete from gold.fact_training_star;

   insert into gold.fact_training_star (
      employee_key,
      training_program_key,
      trainer_key,
      training_date_key,
      training_duration,
      training_cost,
      training_outcome
   )
      select e.employee_key,
             coalesce(
                tp.training_program_key,
                (
                   select training_program_key
                     from gold.dim_training_program_star
                    where training_program_name = 'Unknown'
                )
             ) as training_program_key,
             coalesce(
                tr.trainer_key,
                (
                   select trainer_key
                     from gold.dim_trainer_star
                    where trainer_name = 'Unknown'
                )
             ) as trainer_key,
             to_number(to_char(
                t.training_date,
                'YYYYMMDD'
             )) as training_date_key,
             t.training_duration,
             t.training_cost,
             t.training_outcome
        from silver.training_and_development_data t
        left join gold.dim_employee_star e
      on e.emp_id = t.emp_id
        left join gold.dim_training_program_star tp
      on tp.training_program_name = t.training_program_name
        left join gold.dim_trainer_star tr
      on tr.trainer_name = t.trainer;

   commit;
end;
/

/* 11. Master ETL procedure*/
create or replace procedure gold.load_gold_layer is
begin
   gold.cleanup_fact_tables();
   gold.load_dim_date();
   gold.add_default_dimension_records();
   gold.load_dim_employee();
   gold.load_dim_training_program_star();
   gold.load_dim_education_star();
   gold.load_dim_trainer_star();
   gold.load_fact_employee_engagement();
   gold.load_fact_recruitment();
   gold.load_fact_training();
   commit;
   dbms_output.put_line('Gold Layer ETL completed successfully.');
exception
   when others then
      rollback;
      dbms_output.put_line('Error in ETL: ' || sqlerrm);
      raise;
end;
/