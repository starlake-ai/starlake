#Assuming we are running in QA, the requests below are rewritten as follows:

select * from myds.mytable1; # in the sql.j2
-- is run as
select * from `starlake-qa`.myds.my_table1_original;

select * from XYZ.2020_01_01; # in the sql.j2
-- is run as
select * from `starlake-qa`.myds2.2020_01_01;


select * from myds31.mytable1; # in the sql.j2
-- is run as
select starlake-qa.myds31.mytable1 from `starlake-qa`.myds31.mytable1;




