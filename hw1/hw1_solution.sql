--Create Clean Table--
CREATE TABLE sviridenko.kiva_loans_upd AS
    (SELECT * 
        FROM sviridenko.kiva_loans t
        WHERE CAST(t.dt AS date) IS NOT NULL)

--Excercise 1--

--Количество строк
--Интервал датасета (дата начала и конца выборки)
--Минимум и максимум по колонке с суммами займов
SELECT COUNT(*) AS rows_count, 
       MIN(t.dt) AS min_date, 
       MAX(t.dt) AS max_date, 
       MIN(t.loan_amount) AS min_amount, 
       MAX(t.loan_amount) AS max_amount 
   FROM sviridenko.kiva_loans_upd t; 
/**
 	rows_count	min_date	max_date	min_amount	max_amount
1	670311	        2014-01-01	2017-07-26	100.0	        9975.0
**/

--Наличие пропусков и null
SELECT COUNT(*) AS count_rows 
    FROM sviridenko.kiva_loans_upd
    WHERE funded_amount IS NULL;
/**
 	count_rows
1	0
**/

SELECT COUNT(*) AS count_rows 
    FROM sviridenko.kiva_loans_upd
    WHERE funded_time = '';
/**
 	count_rows
1	48239
**/

SELECT count(*) as count_rows 
    FROM sviridenko.kiva_loans_upd
    WHERE country_code = '';
/**
 	count_rows
1	8
**/

-- STRING -> TIMESTAMP
SELECT unix_timestamp(t.dt, 'yyyy-MM-dd') AS dt_unix,
       unix_timestamp(t.posted_time, "yyyy-MM-dd HH:mm:ss+00:00") AS posted_time_unix,
       unix_timestamp(t.disbursed_time, "yyyy-MM-dd HH:mm:ss+00:00") AS disbursed_time_unix,
       unix_timestamp(t.funded_time, "yyyy-MM-dd HH:mm:ss+00:00") AS funded_time_unix
       FROM sviridenko.kiva_loans_upd t;
/**
 	dt_unix	    posted_time_unix	disbursed_time_unix	funded_time_unix
1	1388534400  1388556759	        1387267200	        1388657192
2	1388534400  1388559068	        1387267200	        1388654243
3	1388534400  1388570287	        1387267200	        1388592096
4	1388534400  1388563391	        1387872000	        1388581200
5	1388534400  1388577199	        1387267200	        1388603931
**/
       
--Excercise 2--

--Количество займов в разрезе стран
SELECT country, COUNT(*) as loans_num
    FROM sviridenko.kiva_loans_upd
    GROUP BY country
    ORDER BY loans_num DESC;
/**
 	country	    loans_num
1	Philippines 160416
2	Kenya	    75770
3	El Salvador 39686
4	Cambodia    34835
5	Pakistan    26855
6	Peru	    22154
7	Colombia    21915
8	Uganda	    20596
9	Tajikistan  19570
10	Ecuador	    13504
**/

--Количество займов в разрезе макрорегионов
SELECT mpi.world_region, 
       COUNT(*) AS loans_num
    FROM kiva_loans_upd loans
    INNER JOIN kiva_mpi_region_locations mpi
    ON loans.country = mpi.country
    GROUP BY mpi.world_region
    ORDER BY loans_num DESC;
/**
 	mpi.world_region	        loans_num
1	East Asia and the Pacific	3659475
2	Latin America and Caribbean	2056527
3	Sub-Saharan Africa	        1769847
4	South Asia	                183227
5	Arab States	                119977
6	Europe and Central Asia	        117091
**/

--Люди какого пола обращаются за финансированием
SELECT  SUM(female_num) AS female_num,
        SUM(male_num) AS male_num
    FROM
    (SELECT country,
            length(regexp_replace(gender_str, '[^0]', '')) AS female_num,
            length(regexp_replace(gender_str, '[^1]', '')) AS male_num
        FROM
        (SELECT country, borrower_genders,
                regexp_replace(regexp_replace(borrower_genders, 'female', '0'), 'male', '1') as gender_str
            FROM kiva_loans_upd) gender_raw) gender_agg;
/**
 	female_num	male_num
1	1068745	        274399
**/
            

--В каких странах подавляющее большинство заемщиков - женщины?
SELECT  country,
        SUM(female_num) / (SUM(male_num) + SUM(female_num)) AS female_ratio
    FROM
    (SELECT country,
           length(regexp_replace(gender_str, '[^0]', '')) AS female_num,
           length(regexp_replace(gender_str, '[^1]', '')) AS male_num
        FROM
        (SELECT country, borrower_genders,
                regexp_replace(regexp_replace(borrower_genders, 'female', '0'), 'male', '1') as gender_str
            FROM kiva_loans_upd) gender_raw) gender_agg
    GROUP BY country
    HAVING SUM(female_num) > SUM(male_num)
    ORDER BY female_ratio DESC
/**
 	country	        female_ratio
1	Cote D'Ivoire	1
2	Virgin Islands	1
3	Turkey	        1
4	Afghanistan	1
5	Guam	        1
6	Bhutan	        1
7	Solomon Islands	1
8	Nepal	        0.9916317991631799
9	India	        0.990926045477369
10	Samoa	        0.9907734056987788
11	Malawi	        0.9880529544720698
12	Senegal	        0.980948857856284
13	Paraguay	0.97876421888672
14	Benin	        0.9786780383795309
15	Zimbabwe	0.9743114028828315
**/
    
    
--Excercise 3--

--Объемы займов в разрезе секторов
SELECT sector,
       sum(loan_amount) as loan_sum
    FROM sviridenko.kiva_loans_upd
    GROUP BY sector
    ORDER BY loan_sum DESC;
 
/**
 	sector	        loan_sum
1	Agriculture     142806250
2	Food	        121238875
3	Retail	        97937175
4	Services        47918850
5	Clothing        37225050
6	Education       30936425
7	Housing         23613400
8	Personal Use    14946450
9	Arts	        12225425
10	Transportation  11049875
11	Health	        9809225
12	Construction	6687525
13	Manufacturing	5442550
14	Entertainment	1377150
15	Wholesale	995200
**/

--Средние и медианные значения суммы займа по секторам
SELECT sector,
       avg(loan_amount) as loan_avg,
       percentile(cast(loan_amount as BIGINT), 0.5) as loan_median
    FROM sviridenko.kiva_loans_upd
    GROUP BY sector
    ORDER BY loan_avg DESC;
/** 
    sector          	loan_avg            loan_median
1   Entertainment   	1663.2246376811595	875
2	Wholesale   	1569.7160883280758	950
3	Clothing    	1138.6593050287531	600
4	Construction	1068.2947284345048	700
5	Health	        1065.6409560021727	725
6	Services	1063.5162127971237	550
7	Arts	        1014.9792444997925	475
8	Education	998.335646056538	725
9	Food	        888.3921374661097	450
10	Manufacturing	877.4060938255683	500
11	Agriculture	793.1917907131749	500
12	Retail	        787.5612158739094	425
13	Transportation	712.7112358101135	450
14	Housing	        701.6729563486168	500
15	Personal Use	410.8763779311103	200
**/


--Суммы займов по макрорегионам
SELECT mpi.world_region,
       SUM(loans.loan_amount) AS loan_sum
    FROM    sviridenko.kiva_loans_upd as loans
    INNER JOIN sviridenko.kiva_mpi_region_locations as mpi
    ON loans.country = mpi.country
    GROUP BY mpi.world_region
    ORDER BY loan_sum DESC;
/**
 	mpi.world_region	        loan_sum
1	Latin America and Caribbean	2062326400
2	East Asia and the Pacific	1538949850
3	Sub-Saharan Africa	        1034895275
4	Arab States	                128606100
5	Europe and Central Asia	        102337300
6	South Asia	                91962850
**/
