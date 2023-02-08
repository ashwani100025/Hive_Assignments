Hive assignment_01_solutions
============================

Ashwani Kumar (ashwani100025@gmail.com)

1. Download vechile sales data -> https://github.com/shashank-mishra219/Hive-Class/blob/main/sales_order_data.csv
=
 sales_order_data.csv is present in my local 'file:///home/ashok/Documents/Ashwani/iNeuron Big Data Course/Hive/Hive Assignment Projects & Interview Questions/sales_order_data.csv'


2. Store raw data into hdfs location
=
$ hadoop fs -put 'file:///home/ashok/Documents/Ashwani/iNeuron Big Data Course/Hive/Hive Assignment Projects & Interview Questions/sales_order_data.csv' /tmp

$ hadoop fs -ls /tmp
#Found 2 items
#drwx-wx-wx   - ashok supergroup          0 2023-02-08 19:52 /tmp/hive
#-rw-r--r--   1 ashok supergroup     360233 2023-02-08 19:55 /tmp/sales_order_data.csv


3. Create a internal hive table "sales_order_csv" which will store csv data sales_order_csv .. make sure to skip header row while creating table
=
hive> create database hive_db1;
#OK
#Time taken: 1.444 seconds

use hive_db1;
#OK
#Time taken: 0.045 seconds

create table sales_order_csv(
ORDERNUMBER int,
QUANTITYORDERED int,
PRICEEACH float,
ORDERLINENUMBER int,
SALES float,
STATUS string,
QTR_ID int,
MONTH_ID int,
YEAR_ID int,
PRODUCTLINE string,
MSRP int,
PRODUCTCODE string,
PHONE string,
CITY string,
STATE string,
POSTALCODE string,
COUNTRY string,
TERRITORY string,
CONTACTLASTNAME string,
CONTACTFIRSTNAME string,
DEALSIZE string)
row format delimited
fields terminated by ','
tblproperties("skip.header.line.count"="1");
#OK
#Time taken: 1.034 seconds


4. Load data from hdfs path into "sales_order_csv"
=
load data inpath '/tmp/sales_order_data.csv' into table sales_order_csv;
#Loading data to table hive_db1.sales_order_csv
#OK
#Time taken: 1.402 seconds

select * from sales_order_csv limit 5;
#OK
#10107	30	95.7	2	2871.0	Shipped	1	2	2003	Motorcycles	95	S10_1678	2125557818	NYC	NY	10022	USA	NA	Yu	Kwai	Small
#10121	34	81.35	5	2765.9	Shipped	2	5	2003	Motorcycles	95	S10_1678	26.47.1555	Reims		51100	France	EMEA	Henriot	Paul	Small
#10134	41	94.74	2	3884.34	Shipped	3	7	2003	Motorcycles	95	S10_1678	+33 1 46 62 7555	Paris		75508	France	EMEADa Cunha	Daniel	Medium
#10145	45	83.26	6	3746.7	Shipped	3	8	2003	Motorcycles	95	S10_1678	6265557265	Pasadena	CA	90003	USA	NA	Young	Julie	Medium
#10159	49	100.0	14	5205.27	Shipped	4	10	2003	Motorcycles	95	S10_1678	6505551386	San Francisco	CA		USA	NA	Brown	Julie	Medium
#Time taken: 1.172 seconds, Fetched: 5 row(s)


5. Create an internal hive table which will store data in ORC format "sales_order_orc"
=

create table sales_order_orc(
ORDERNUMBER int,
QUANTITYORDERED int,
PRICEEACH float,
ORDERLINENUMBER int,
SALES float,
STATUS string,
QTR_ID int,
MONTH_ID int,
YEAR_ID int,
PRODUCTLINE string,
MSRP int,
PRODUCTCODE string,
PHONE string,
CITY string,
STATE string,
POSTALCODE string,
COUNTRY string,
TERRITORY string,
CONTACTLASTNAME string,
CONTACTFIRSTNAME string,
DEALSIZE string)
stored as orc;
#OK
#Time taken: 1.432 seconds


6. Load data from "sales_order_csv" into "sales_order_orc"
=
from sales_order_csv insert overwrite table sales_order_orc select *;
#Query ID = ashok_20230208205036_cb4a42a4-ef6d-4692-9e25-081f9a648c73
#Total jobs = 1
#Launching Job 1 out of 1
#Number of reduce tasks determined at compile time: 1
#In order to change the average load for a reducer (in bytes):
#  set hive.exec.reducers.bytes.per.reducer=<number>
#In order to limit the maximum number of reducers:
#  set hive.exec.reducers.max=<number>
#In order to set a constant number of reducers:
#  set mapreduce.job.reduces=<number>
#Starting Job = job_1675868703285_0001, Tracking URL = http://ashok-PC:8088/proxy/application_1675868703285_0001/
#Kill Command = /home/ashok/hadoop-3.3.4//bin/mapred job  -kill job_1675868703285_0001
#Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 1
#2023-02-08 20:51:12,302 Stage-1 map = 0%,  reduce = 0%
#2023-02-08 20:51:35,462 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 5.06 sec
#2023-02-08 20:51:45,909 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 8.25 sec
#MapReduce Total cumulative CPU time: 8 seconds 250 msec
#Ended Job = job_1675868703285_0001
#Stage-4 is selected by condition resolver.
#Stage-3 is filtered out by condition resolver.
#Stage-5 is filtered out by condition resolver.
#Moving data to directory hdfs://localhost:9000/user/hive/warehouse/hive_db1.db/sales_order_orc/.hive-staging_hive_2023-02-08_20-50-36_677_3607795840760344766-1/-ext-10000
#Loading data to table hive_db1.sales_order_orc
#MapReduce Jobs Launched: 
#Stage-Stage-1: Map: 1  Reduce: 1   Cumulative CPU: 8.25 sec   HDFS Read: 400484 HDFS Write: 50362 SUCCESS
#Total MapReduce CPU Time Spent: 8 seconds 250 msec
#OK
#Time taken: 82.238 seconds


Perform below menioned queries on "sales_order_orc" table :

a. Calculatye total sales per year
=
hive> set hive.cli.print.header=true;

select sum(sales) as total_sales from sales_order_orc;
#Query ID = ashok_20230208205353_c9fb4811-7d43-4c11-b9ae-1e3a52f89d51
#Total jobs = 1
#Launching Job 1 out of 1
#Number of reduce tasks determined at compile time: 1
#In order to change the average load for a reducer (in bytes):
#  set hive.exec.reducers.bytes.per.reducer=<number>
#In order to limit the maximum number of reducers:
#  set hive.exec.reducers.max=<number>
#In order to set a constant number of reducers:
#  set mapreduce.job.reduces=<number>
#Starting Job = job_1675868703285_0002, Tracking URL = http://ashok-PC:8088/proxy/application_1675868703285_0002/
#Kill Command = /home/ashok/hadoop-3.3.4//bin/mapred job  -kill job_1675868703285_0002
#Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 1
#2023-02-08 20:54:08,296 Stage-1 map = 0%,  reduce = 0%
#2023-02-08 20:54:15,525 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 2.72 sec
#2023-02-08 20:54:20,666 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 5.45 sec
#MapReduce Total cumulative CPU time: 5 seconds 450 msec
#Ended Job = job_1675868703285_0002
#MapReduce Jobs Launched: 
#Stage-Stage-1: Map: 1  Reduce: 1   Cumulative CPU: 5.45 sec   HDFS Read: 43371 HDFS Write: 118 SUCCESS
#Total MapReduce CPU Time Spent: 5 seconds 450 msec
OK
total_sales
1.00326288493042E7
Time taken: 29.586 seconds, Fetched: 1 row(s)


b. Find a product for which maximum orders were placed
=

select PRODUCTLINE, MSRP, PRODUCTCODE, QUANTITYORDERED
from sales_order_orc
where QUANTITYORDERED = (select max(QUANTITYORDERED) from sales_order_orc);
#Query ID = ashok_20230208210656_ad0f37b5-9dce-4f75-b196-d0404e602662
#Total jobs = 2
#Launching Job 1 out of 2
#Number of reduce tasks determined at compile time: 1...
#...
OK
productline		msrp	productcode		quantityordered
Classic Cars	115		S12_4675		97


c. Calculate the total sales for each quarter
=

'in correct order of data'
select QTR_ID, sum(SALES) as total_sales
from sales_order_orc
group by QTR_ID
order by QTR_ID;
#Query ID = ashok_20230208212104_51380a6f-23e0-433f-8fb1-6bc9ab159a35
#Total jobs = 2
#Launching Job 1 out of 2....
#....
OK
qtr_id	total_sales
1		2350817.726501465
2		2048120.3029174805
3		1758910.808959961
4		3874780.010925293


d. In which quarter sales was minimum
=
select QTR_ID, sum(SALES) as total_sales
from sales_order_orc
group by QTR_ID
order by total_sales
limit 1;
#Query ID = ashok_20230208214857_7b9eb634-7572-4b8d-8576-d731d095dc71
#Total jobs = 2
#Launching Job 1 out of 2...
#....
OK
qtr_id	total_sales
3		1758910.808959961


e. In which country sales was maximum and in which country sales was minimum
=
'minimum country sales'
select COUNTRY, sum(SALES) as total_sales
from sales_order_orc
group by COUNTRY
order by total_sales
limit 1;

OK
country	total_sales
Ireland	57756.43029785156

'maximum country sales'
select COUNTRY, sum(SALES) as total_sales
from sales_order_orc
group by COUNTRY
order by total_sales desc
limit 1;

OK
country	total_sales
USA		3627982.825744629


f. Calculate quartelry sales for each city
=

select CITY, QTR_ID, sum(SALES) as total_sales
from sales_order_orc
group by CITY, QTR_ID
order by CITY, QTR_ID;

OK
city	qtr_id	total_sales
Aaarhus	4	100595.5498046875
Allentown	2	6166.7998046875
Allentown	3	71930.61041259766
Allentown	4	44040.729736328125
Barcelona	2	4219.2001953125
Barcelona	4	74192.66003417969
Bergamo	1	56181.320068359375
Bergamo	4	81774.40008544922
Bergen	3	16363.099975585938
Bergen	4	95277.17993164062
Boras	1	31606.72021484375
Boras	3	53941.68981933594
Boras	4	48710.92053222656
Boston	2	74994.240234375
Boston	3	15344.640014648438
Boston	4	63730.7802734375
Brickhaven	1	31474.7802734375
Brickhaven	2	7277.35009765625
Brickhaven	3	114974.53967285156
Brickhaven	4	11528.52978515625
Bridgewater	2	75778.99060058594
Bridgewater	4	26115.800537109375
Brisbane	1	16118.479858398438
Brisbane	3	34100.030029296875
Bruxelles	1	18800.089721679688
Bruxelles	2	8411.949829101562
Bruxelles	3	47760.479736328125
Burbank	1	37850.07958984375
Burbank	4	8234.559936523438
Burlingame	1	13529.570190429688
Burlingame	3	42031.83020019531
Burlingame	4	65221.67004394531
Cambridge	1	21782.699951171875
Cambridge	2	14380.920043945312
Cambridge	3	48828.71942138672
Cambridge	4	54251.659912109375
Charleroi	1	16628.16015625
Charleroi	2	1711.260009765625
Charleroi	3	1637.199951171875
Charleroi	4	13463.480224609375
Chatswood	2	43971.429931640625
Chatswood	3	69694.40002441406
Chatswood	4	37905.14990234375
Cowes	1	26906.68017578125
Cowes	4	51334.15966796875
Dublin	1	38784.470458984375
Dublin	3	18971.959838867188
Espoo	1	51373.49072265625
Espoo	2	31018.230102539062
Espoo	3	31569.430053710938
Frankfurt	1	48698.82922363281
Frankfurt	4	36472.76025390625
Gensve	1	50432.549560546875
Gensve	3	67281.00903320312
Glen Waverly	2	14378.089965820312
Glen Waverly	3	12334.819580078125
Glen Waverly	4	37878.54992675781
Glendale	1	3987.199951171875
Glendale	2	20350.949768066406
Glendale	3	7600.1201171875
Glendale	4	34485.49987792969
Graz	1	8775.159912109375
Graz	4	43488.740234375
Helsinki	1	26422.819458007812
Helsinki	3	42744.0595703125
Helsinki	4	42083.499755859375
Kobenhavn	1	58871.110107421875
Kobenhavn	2	62091.880615234375
Kobenhavn	4	24078.610107421875
Koln	4	100306.58020019531
Las Vegas	2	33847.61975097656
Las Vegas	3	34453.84973144531
Las Vegas	4	14449.609741210938
Lille	1	20178.1298828125
Lille	4	48874.28088378906
Liverpool	2	91211.0595703125
Liverpool	4	26797.210083007812
London	1	8477.219970703125
London	2	32376.29052734375
London	4	83970.029296875
Los Angeles	1	23889.320068359375
Los Angeles	4	24159.14013671875
Lule	1	9748.999755859375
Lule	4	66005.8798828125
Lyon	1	101339.13977050781
Lyon	4	41535.11022949219
Madrid	1	357668.4899291992
Madrid	2	339588.0513305664
Madrid	3	69714.09008789062
Madrid	4	315580.80963134766
Makati City	1	55245.02014160156
Makati City	4	38770.71032714844
Manchester	1	51017.919860839844
Manchester	4	106789.88977050781
Marseille	1	2317.43994140625
Marseille	2	52481.840087890625
Marseille	4	20136.859985351562
Melbourne	1	49637.57067871094
Melbourne	2	60135.84033203125
Melbourne	4	91221.99914550781
Minato-ku	1	38191.38977050781
Minato-ku	2	26482.700256347656
Minato-ku	4	55888.65026855469
Montreal	2	58257.50012207031
Montreal	4	15947.290405273438
Munich	3	34993.92004394531
NYC	1	32647.809814453125
NYC	2	165100.33947753906
NYC	3	63027.92004394531
NYC	4	300011.6999511719
Nantes	1	59617.39978027344
Nantes	2	60344.990173339844
Nantes	3	61310.880126953125
Nantes	4	23031.589599609375
Nashua	1	12133.25
Nashua	4	119552.04949951172
New Bedford	1	48578.95935058594
New Bedford	3	45738.38952636719
New Bedford	4	113557.509765625
New Haven	2	36973.309814453125
New Haven	4	42498.760498046875
Newark	1	8722.1201171875
Newark	2	74506.06909179688
North Sydney	1	65012.41955566406
North Sydney	3	47191.76013183594
North Sydney	4	41791.949462890625
Osaka	1	50490.64013671875
Osaka	2	17114.43017578125
Oslo	3	34145.47021484375
Oslo	4	45078.759765625
Oulu	1	49055.40026855469
Oulu	2	17813.40008544922
Oulu	3	37501.580322265625
Paris	1	71494.17944335938
Paris	2	80215.4203491211
Paris	3	27798.480102539062
Paris	4	89436.60034179688
Pasadena	1	44273.359436035156
Pasadena	3	55776.119873046875
Pasadena	4	4512.47998046875
Philadelphia	1	27398.820434570312
Philadelphia	2	7287.240234375
Philadelphia	4	116503.07043457031
Reggio Emilia	2	41509.94006347656
Reggio Emilia	3	56421.650390625
Reggio Emilia	4	44669.740478515625
Reims	1	52029.07043457031
Reims	2	18971.959716796875
Reims	3	15146.31982421875
Reims	4	48895.59014892578
Salzburg	2	98104.24005126953
Salzburg	3	6693.2802734375
Salzburg	4	45001.10986328125
San Diego	1	87489.23010253906
San Francisco	1	72899.19995117188
San Francisco	4	151459.4805908203
San Jose	2	160010.27026367188
San Rafael	1	267315.2586669922
San Rafael	2	7261.75
San Rafael	3	216297.40063476562
San Rafael	4	163983.64880371094
Sevilla	4	54723.621154785156
Singapore	1	28395.18994140625
Singapore	2	92033.77014160156
Singapore	3	90250.07995605469
Singapore	4	77809.37023925781
South Brisbane	1	21730.029907226562
South Brisbane	3	10640.290161132812
South Brisbane	4	27098.800048828125
Stavern	1	54701.999755859375
Stavern	4	61897.19006347656
Strasbourg	2	80438.47985839844
Torino	3	94117.25988769531
Toulouse	1	15139.1201171875
Toulouse	3	17251.08056640625
Toulouse	4	38098.240234375
Tsawassen	2	31302.500244140625
Tsawassen	3	43332.349609375
Vancouver	4	75238.91955566406
Versailles	1	5759.419921875
Versailles	4	59074.90026855469
White Plains	4	85555.98962402344


h. Find a month for each year in which maximum number of quantities were sold
=
create table year_monthly_wise_quantities as
(select YEAR_ID, MONTH_ID, sum(QUANTITYORDERED) as quantities
from sales_order_orc
group by YEAR_ID, MONTH_ID
order by YEAR_ID, MONTH_ID);

select * from year_monthly_wise_quantities;
year_id	month_id  quantities
2003	1	1357
2003	2	1449
2003	3	1755
2003	4	1993
2003	5	2017
2003	6	1649
2003	7	1725
2003	8	1974
2003	9	2510
2003	10	5515
2003	11	10179
2003	12	2489
2004	1	3245
2004	2	3061
2004	3	1978
2004	4	2077
2004	5	2618
2004	6	2971
2004	7	3174
2004	8	4564
2004	9	3171
2004	10	5483
2004	11	10678
2004	12	3804
2005	1	3395
2005	2	3393
2005	3	3852
2005	4	2634
2005	5	4357

select YEAR_ID, MONTH_ID, quantities as max_quantities from
(select YEAR_ID, MONTH_ID, quantities,
row_number() over(partition by YEAR_ID order by quantities desc) as rn
from year_monthly_wise_quantities) tmp
where rn = 1;

OK
year_id	 month_id  max_quantities
2003	 11		   10179
2004	 11		   10678
2005	 5		   4357
