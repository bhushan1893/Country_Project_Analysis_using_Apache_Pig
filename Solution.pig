--First need to create directory in HDFS.Creating a directory called country_project

--bin/hadoop fs -mkdir /country_project

--Putting DataFile(Country.txt) into HDFS by Using below command

--bin/hadoop fs -put /home/bhushan/Country_Project_Analyzing_using_Apache_Pig/Country.txt /country_project

--(Now, Execute each usecase by removing comment)



--1) Count number of countries based on landmass

A = load '/country_project/Country.txt' USING PigStorage(',') as (name:chararray,landmass:int,zone:int,area:int,popul:int,lang:int,religion:int,bar:int,strips:int,colours:int,red:int,green:int,blue:int,gold:int,white:int,black:int,orange:int,manhue:chararray,circle:int,crosses:int,saltires:int,quaters:int,sunstars:int,cresent:int,triangle:int,icon:int,animate:int,text:int,topl:chararray,topr:chararray);
B = group A by landmass;
C = foreach B generate A.name, COUNT(A.landmass);
dump C; 


--2) Count of countries with icon

--A = load '/country_project/Country.txt' USING PigStorage(',') as (name:chararray,landmass:int,zone:int,area:int,popul:int,lang:int,religion:int,bar:int,strips:int,colours:int,red:int,green:int,blue:int,gold:int,white:int,black:int,orange:int,manhue:chararray,circle:int,crosses:int,saltires:int,quaters:int,sunstars:int,cresent:int,triangle:int,icon:int,animate:int,text:int,topl:chararray,topr:chararray);
--B1 = filter A by $25 == 1;
--C1 = group B1 all;
--D1 = foreach C1 generate COUNT(B1);
--Dump D1;



--3) Count of countries which have same top left and top right color in flag

--A = load '/country_project/Country.txt' USING PigStorage(',') as (name:chararray,landmass:int,zone:int,area:int,popul:int,lang:int,religion:int,bar:int,strips:int,colours:int,red:int,green:int,blue:int,gold:int,white:int,black:int,orange:int,manhue:chararray,circle:int,crosses:int,saltires:int,quaters:int,sunstars:int,cresent:int,triangle:int,icon:int,animate:int,text:int,topl:chararray,topr:chararray);
--B2 = filter A by $28 == $29;
--C2 = group B2 all;
--D2 = foreach C2 generate COUNT(B2);
--dump D2;



--4) Count number of countries based on zone

--A = load '/country_project/Country.txt' USING PigStorage(',') as (name:chararray,landmass:int,zone:int,area:int,popul:int,lang:int,religion:int,bar:int,strips:int,colours:int,red:int,green:int,blue:int,gold:int,white:int,black:int,orange:int,manhue:chararray,circle:int,crosses:int,saltires:int,quaters:int,sunstars:int,cresent:int,triangle:int,icon:int,animate:int,text:int,topl:chararray,topr:chararray);
--B3 = group A by zone;
--C3 = foreach B3 generate A.name, COUNT(A.zone);
--dump C3;



--5) Find out largest county in terms of area in NE zone.

--A = load '/country_project/Country.txt' USING PigStorage(',') as (name:chararray,landmass:int,zone:int,area:int,popul:int,lang:int,religion:int,bar:int,strips:int,colours:int,red:int,green:int,blue:int,gold:int,white:int,black:int,orange:int,manhue:chararray,circle:int,crosses:int,saltires:int,quaters:int,sunstars:int,cresent:int,triangle:int,icon:int,animate:int,text:int,topl:chararray,topr:chararray);
--B4 = filter A by zone == 1;
--C4 = ORDER B4 by area desc;
--D4 = limit C4 1;
--dump D4;



--6) Find out least populated country in South America landmass

--A = load '/country_project/Country.txt' USING PigStorage(',') as (name:chararray,landmass:int,zone:int,area:int,popul:int,lang:int,religion:int,bar:int,strips:int,colours:int,red:int,green:int,blue:int,gold:int,white:int,black:int,orange:int,manhue:chararray,circle:int,crosses:int,saltires:int,quaters:int,sunstars:int,cresent:int,triangle:int,icon:int,animate:int,text:int,topl:chararray,topr:chararray);
--B5 = filter A by landmass == 2;
--C5 = ORDER B5 by popul asc;
--D5 = limit C5 1;
--Dump D5;



--7) Sum of all circles present in all country flags

--A = load '/country_project/Country.txt' USING PigStorage(',') as (name:chararray,landmass:int,zone:int,area:int,popul:int,lang:int,religion:int,bar:int,strips:int,colours:int,red:int,green:int,blue:int,gold:int,white:int,black:int,orange:int,manhue:chararray,circle:int,crosses:int,saltires:int,quaters:int,sunstars:int,cresent:int,triangle:int,icon:int,animate:int,text:int,topl:chararray,topr:chararray);
--B6 = group A by circle;
--C6 = foreach B6 generate A.name, SUM(A.circle);
--dump C6;




--8) Count of countries which have both icon and text in flag

--A = load '/country_project/Country.txt' USING PigStorage(',') as (name:chararray,landmass:int,zone:int,area:int,popul:int,lang:int,religion:int,bar:int,strips:int,colours:int,red:int,green:int,blue:int,gold:int,white:int,black:int,orange:int,manhue:chararray,circle:int,crosses:int,saltires:int,quaters:int,sunstars:int,cresent:int,triangle:int,icon:int,animate:int,text:int,topl:chararray,topr:chararray);
--B7 = filter A by $25 == 1 and $27 == 1;
--C7 = group B7 all;
--D7 = foreach C7 generate COUNT(B7);
--dump D7;




--9) Find out largest speaking language among all countries.

--A = load '/country_project/Country.txt' USING PigStorage(',') as (name:chararray,landmass:int,zone:int,area:int,popul:int,lang:int,religion:int,bar:int,strips:int,colours:int,red:int,green:int,blue:int,gold:int,white:int,black:int,orange:int,manhue:chararray,circle:int,crosses:int,saltires:int,quaters:int,sunstars:int,cresent:int,triangle:int,icon:int,animate:int,text:int,topl:chararray,topr:chararray);
--l1 = filter A by lang == 1;
--groupl1 = group l1 all;
--d1 = foreach groupl1 generate COUNT(l1), 'ENGLISH';
--l2 = filter A by lang == 2;
--groupl2 = group l2 all;
--d2 = foreach groupl2 generate COUNT(l2), 'SPANISH';
-- l3 = filter A by lang == 3;
-- groupl3 = group l3 all;
-- d3 = foreach groupl3 generate COUNT(l3), 'FRENCH';
-- l4 = filter A by lang == 4;
-- groupl4 = group l4 all;
-- d4 = foreach groupl4 generate COUNT(l4), 'GERMAN';
-- l5 = filter A by lang == 5;
-- groupl5 = group l5 all;
-- d5 = foreach groupl5 generate COUNT(l5), 'SLAVIC';
-- l6 = filter A by lang == 6;
-- groupl6 = group l6 all;
-- d6 = foreach groupl6 generate COUNT(l6), 'OTHER INDO-EUROPEAN';
-- l7 = filter A by lang == 7;
-- groupl7 = group l7 all;
-- d7 = foreach groupl7 generate COUNT(l7), 'chinese';
-- l8 = filter A by lang == 8;
-- groupl8 = group l8 all;
-- d8 = foreach groupl8 generate COUNT(l8), 'ARABIC';
--l9 = filter A by lang == 9;
-- groupl9 = group l9 all;
-- d9 = foreach groupl9 generate COUNT(l9), 'JAPANESE/TURKISH/FINNISH/MAGYAR';
-- l10 = filter A by lang == 10;
--groupl10 = group l10 all;
--d10 = foreach groupl10 generate COUNT(l10), 'OTHERS';

--F = union d1,d2,d3,d4,d5,d6,d7,d8,d9,d10;
--sort = ORDER F by $0 desc;
--final = limit sort 1;
--dump final;





--10) Find most common color among flags from all countries.

--A = load '/country_project/Country.txt' USING PigStorage(',') as (name:chararray,landmass:int,zone:int,area:int,popul:int,lang:int,religion:int,bar:int,strips:int,colours:int,red:int,green:int,blue:int,gold:int,white:int,black:int,orange:int,manhue:chararray,circle:int,crosses:int,saltires:int,quaters:int,sunstars:int,cresent:int,triangle:int,icon:int,animate:int,text:int,topl:chararray,topr:chararray);
--z = filter A by red == 1; 
--y = group z all;
--c1 = foreach y generate COUNT(z), 'RED';

--z2 = filter A by green == 1; 
--y2 = group z2 all;
--c2 = foreach y2 generate COUNT(z2), 'GREEN';

--z3 = filter A by blue == 1; 
--y3 = group z3 all;
--c3 = foreach y3 generate COUNT(z3), 'BLUE';

--z4 = filter A by gold == 1; 
--y4 = group z4 all;
--c4 = foreach y4 generate COUNT(z4), 'GOLD';

--z5 = filter A by white == 1; 
--y5 = group z5 all;
--c5 = foreach y5 generate COUNT(z5), 'WHITE';

--z6 = filter A by black == 1; 
--y6 = group z6 all;
--c6 = foreach y6 generate COUNT(z6), 'BLACK';

--z7 = filter A by orange == 1; 
--y7 = group z7 all;
--c7 = foreach y7 generate COUNT(z7), 'ORANGE';

--K = union c1,c2,c3,c4,c5,c6,c7;
--sortf = ORDER K by $0 desc;
--lim = limit sortf 1;
--dump lim;



