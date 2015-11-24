REGISTER myUDF.jar;
--set pig.splitCombination false;
Fulload = load '/pigdata' using PigStorage(',','-tagFile') ;
relevantdata = foreach Fulload generate $0 as stock_name, $1 as stock_date, $7 as Adj_close;
--dump relevantdata;
filta = FILTER relevantdata by stock_date!='Date' and stock_date IS NOT NULL;
datasplit = foreach filta generate stock_name, FLATTEN(STRSPLIT(stock_date, '-')) as (Year,Month,Day), Adj_close;
sorteddata =  GROUP datasplit by (stock_name, Year , Month);
dump sorteddata;
xiinput = foreach sorteddata generate pkgUDF.XiUDF($1) as t1;
--dump xiinput;

valuesplit = FOREACH xiinput GENERATE FLATTEN(t1) as (name,xi);
-- filterxi = FILTER valuesplit by xi 
groupXibyname = GROUP valuesplit by name;
--dump groupXibyname;

Vstockpair = foreach groupXibyname generate pkgUDF.Top10Last10V($1) as nameV;
Vstokpair = Filter Vstockpair by nameV is not null;
--dump Vstokpair;
vname = FOREACH Vstokpair GENERATE FLATTEN(nameV) as (name,V);
FinalVal = Order vname By V ASC ;
Final10 = LIMIT FinalVal 10 ;
FinalVal1 = Order vname By V DESC ;
HighestFinal10 = LIMIT FinalVal1 10;
finalAns =  UNION Final10, HighestFinal10;
dump finalAns;
store finalAns into 'hdfs:///pigdata/hw3_out';
/*
Stringrep = FOREACH xiinput GENERATE CONCAT(CONCAT($0,','),$1);
--
dump Vstokpair
--namexisplit = foreach xiinput generate FLATTEN(STRSPLIT($0,',')) as (stockname,xival);
--dump groupXibyname;
--B = FOREACH xiinput GENERATE FLATTEN(STRSPLIT(stock_name,'.')) as (stockname:chararray, csv:chararray),xi;
--C = FOREACH B GENERATE stockname,xi;

Vstokpair = foreach groupXibyname generate pkgUDF.Top10Last10V($0) as stok_nameV; 
filterV = filter Vstokpair by V is not null and V > 0;
dump filterV;

SortV = ORDER filterV by V
dump filterV;
*/

