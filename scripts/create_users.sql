/*
=====================================================
Create Schemas (Users) for Data Warehouse
=====================================================

Script Purpose:
    This script creates three Oracle schemas (users) to simulate
    a Data Warehouse environment with layered architecture:
        - BRONZE : Raw/staging data
        - SILVER : Cleaned/standardized data
        - GOLD   : Aggregated/business-ready data

Behavior:
    - If schemas already exist, they will be dropped (CASCADE) 
      along with all their objects.
    - New schemas are created with basic privileges and 
      unlimited quota on the USERS tablespace.

WARNING:
    Running this script will permanently drop the BRONZE, SILVER,
    and GOLD schemas if they exist, including all contained data.
    Ensure you have proper backups before execution.
=====================================================
*/

-- Drop schemas if they already exist
-- Drop BRONZE user if exists
begin
   execute immediate 'DROP USER BRONZE CASCADE';
exception
   when others then
      if sqlcode != -01918 then -- user does not exist
         raise;
      end if;
end;
/

-- Drop SILVER user if exists
begin
   execute immediate 'DROP USER SILVER CASCADE';
exception
   when others then
      if sqlcode != -01918 then
         raise;
      end if;
end;
/

-- Drop GOLD user if exists
begin
   execute immediate 'DROP USER GOLD CASCADE';
exception
   when others then
      if sqlcode != -01918 then
         raise;
      end if;
end;
/

-- Create schemas (users)
create user bronze identified by bronze123;
create user silver identified by silver123;
create user gold identified by gold123;

-- Grant privileges
grant connect,resource to bronze;
grant connect,resource to silver;
grant connect,resource to gold;

-- Allow unlimited storage in USERS tablespace
alter user bronze
   quota unlimited on users;
alter user silver
   quota unlimited on users;
alter user gold
   quota unlimited on users;