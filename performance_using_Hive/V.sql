DROP table stock_prices; CREATE EXTERNAL TABLE stock_prices (stock_date STRING, Open FLOAT, High FLOAT, Low FLOAT, Close FLOAT, Volume FLOAT, Adj_Close FLOAT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/'
tblproperties ("skip.header.line.count"="1");


drop table reqddata;create table reqddata(stock_name STRING, stock_date STRING, month STRING, Adj_Close FLOAT);
insert overwrite table reqddata select regexp_replace( regexp_replace(INPUT__FILE__NAME,'.*/',''),'\\..*','') as stock_name, stock_date, substr(stock_date,0,7) as month, Adj_Close from stock_prices;

drop table begin;create table begin as
select stock_name, min(stock_date) as stock_date, month
from reqddata
group by stock_name, month;


drop table last;
create table last as
select stock_name, max(stock_date) as l_stock_date, month as l_month
from reqddata
group by stock_name, month;

drop table mapbegin;
create table mapbegin as
SELECT
B.stock_name, B.month, A.Adj_close AS adj
FROM
reqddata AS A,begin AS B
WHERE
A.stock_date = B.stock_date AND A.stock_name = B.stock_name;


drop table mapend;
create table mapend as
SELECT
B.stock_name, B.l_month, A.Adj_close AS adj
FROM
reqddata AS A,last AS B
WHERE
A.stock_date = B.l_stock_date AND A.stock_name = B.stock_name;

drop table XiVals;
create table XiVals as
SELECT
A.stock_name, A.l_month, (A.adj - B.adj)/B.adj AS difference
FROM
mapend AS A,mapbegin AS B
WHERE
A.stock_name = B.stock_name AND A.l_month = B.month;


drop table average;
create table average as
select stock_name, stddev_samp(difference) as mean from XiVals group by stock_name;

SELECT * FROM average
where mean>0 and mean is not null
ORDER BY mean DESC LIMIT 10;



SELECT * FROM average
where mean>0 and mean is not null
ORDER BY mean ASC LIMIT 10;
