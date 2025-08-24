--------------------------------------------------------------------------------
-- GOLD LAYER STAR SCHEMA - FIXED PROCEDURES
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- PROCEDURE: Load Date Dimension
--------------------------------------------------------------------------------
create or replace procedure gold.load_dim_date is
   v_start_date date := date '2000-01-01';
   v_end_date   date := date '2050-12-31';
   v_date       date;
   v_count      number;
begin
   -- Get number of rows in dim_date
   select count(*)
     into v_count
     from gold.dim_date;

   -- Only insert if table is empty
   if v_count = 0 then
      v_date := v_start_date;
      while v_date <= v_end_date loop
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
   end if;
end;
/


--------------------------------------------------------------------------------
-- PROCEDURE: Load Employee Dimension
--------------------------------------------------------------------------------
create or replace procedure gold.load_dim_employee is
begin
   -- Delete dependent fact table rows first
   execute immediate 'DELETE FROM gold.fact_employee_engagement';
   execute immediate 'DELETE FROM gold.fact_training';

   -- Clear existing data in employee dimension
   execute immediate 'DELETE FROM gold.dim_employee';

   -- Load data from Silver
   insert into gold.dim_employee (
      emp_id,
      emp_firstname,
      emp_lastname,
      emp_position,
      emp_supervisor,
      emp_email,
      emp_unit,
      emp_department,
      emp_division,
      emp_state,
      emp_job_function,
      emp_gender,
      emp_location,
      emp_race,
      emp_marital,
      emp_type,
      emp_status,
      emp_payzone,
      emp_classification,
      emp_dob
   )
      select distinct emp_id,
                      emp_firstname,
                      emp_lastname,
                      emp_position,
                      emp_supervisor,
                      emp_email,
                      emp_unit,
                      emp_department,
                      emp_division,
                      emp_state,
                      emp_job_function,
                      emp_gender,
                      emp_location,
                      emp_race,
                      emp_marital,
                      emp_type,
                      emp_status,
                      emp_payzone,
                      emp_classification,
                      emp_dob
        from silver.employee_data;

   commit;
end;
/

--------------------------------------------------------------------------------
-- PROCEDURE: Load Training Dimension
--------------------------------------------------------------------------------
create or replace procedure gold.load_dim_training is
begin
   -- Delete dependent fact rows first
   execute immediate 'DELETE FROM gold.fact_training';

   -- Clear existing data in training dimension
   execute immediate 'DELETE FROM gold.dim_training';

   -- Load data from Silver training table
   insert into gold.dim_training (
      training_program,
      training_type,
      trainer,
      location
   )
      select distinct training_program_name,
                      training_type,
                      trainer,
                      location
        from silver.training_and_development_data;

   commit;
end;
/

--------------------------------------------------------------------------------
-- PROCEDURE: Load Fact - Employee Engagement
--------------------------------------------------------------------------------
create or replace procedure gold.load_fact_employee_engagement is
begin
   execute immediate 'DELETE FROM gold.fact_employee_engagement';
   insert into gold.fact_employee_engagement (
      employee_key,
      survey_date_key,
      engagement_score,
      satisfaction_score,
      work_life_balance_score
   )
      select (
         select employee_key
           from gold.dim_employee d
          where d.emp_id = s.emp_id
      ),
             to_number(to_char(
                emp_engagement_survey_date,
                'YYYYMMDD'
             )),
             emp_engagement_survey_score,
             emp_satisfaction_survey_score,
             emp_work_life_balance_score
        from silver.employee_engagement_survey_data s;
   commit;
end;
/
--------------------------------------------------------------------------------
-- PROCEDURE: Load Fact - Recruitment
--------------------------------------------------------------------------------
create or replace procedure gold.load_fact_recruitment is
begin
   execute immediate 'DELETE FROM gold.fact_recruitment';
   insert into gold.fact_recruitment (
      applicant_id,
      application_date_key,
      applicant_gender,
      applicant_dob,
      applicant_city,
      applicant_state,
      applicant_country,
      applicant_education,
      applicant_experience,
      desired_salary,
      job_title,
      applicant_status
   )
      select applicant_id,
             to_number(to_char(
                application_date,
                'YYYYMMDD'
             )),
             applicant_gender,
             applicant_dob,
             applicant_city,
             applicant_state,
             applicant_country,
             applicant_education,
             applicant_experience,
             applicant_desired_salary,
             applicant_job_title,
             applicant_status
        from silver.recruitment_data;
   commit;
end;
/
--------------------------------------------------------------------------------
-- PROCEDURE: Load Fact - Training
--------------------------------------------------------------------------------
create or replace procedure gold.load_fact_training is
begin
   execute immediate 'DELETE FROM gold.fact_training';
   insert into gold.fact_training (
      employee_key,
      training_key,
      training_date_key,
      training_duration,
      training_cost,
      training_outcome
   )
      select (
         select employee_key
           from gold.dim_employee d
          where d.emp_id = t.emp_id
      ),
             (
                select training_key
                  from gold.dim_training tr
                 where tr.training_program = t.training_program_name
                   and tr.training_type = t.training_type
                   and tr.trainer = t.trainer
                   and tr.location = t.location
             ),
             to_number(to_char(
                training_date,
                'YYYYMMDD'
             )),
             training_duration,
             training_cost,
             training_outcome
        from silver.training_and_development_data t;
   commit;
end;
/