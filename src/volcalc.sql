drop table if exists tmp;
create external table tmp (date string,open double,high double,low double,close double,volume double,adjclose double) row format delimited fields terminated by ',' lines terminated by '\n'
location 'hdfs:///data'  
tblproperties ("skip.header.line.count"="1");

drop table if exists tmp2;
create external table tmp2 as select regexp_replace(INPUT__FILE__NAME,'.*/','') as filename,substr(date,0,7) as monthyear,substr(date,9,9) as day, adjclose from tmp; 


drop table if exists tmp3;
create table tmp3 as select filename,monthyear,max(day) as lastday,min(day) as firstday from tmp2 group by filename, monthyear;
drop table if exists tmp4;
create table tmp4 as select tmp3.filename,tmp3.monthyear,tmp2.adjclose as adjcloselast from tmp3,tmp2 where tmp2.day = tmp3.lastday and tmp2.monthyear = tmp3.monthyear and tmp3.filename = tmp2.filename;

drop table if exists tmp5;
create table tmp5 as select tmp3.filename,tmp3.monthyear,tmp2.adjclose as adjclosefirst from tmp3,tmp2 where tmp2.day = tmp3.firstday and tmp2.monthyear = tmp3.monthyear and tmp3.filename = tmp2.filename;



drop table if exists tmp6;
create table tmp6 as select tmp4.filename,tmp4.monthyear,(tmp4.adjcloselast-tmp5.adjclosefirst)/tmp5.adjclosefirst as xi from tmp4,tmp5 where tmp4.filename = tmp5.filename and tmp4.monthyear = tmp5.monthyear;




drop table if exists tmp7;
create table tmp7 as select filename,stddev_samp(xi) as volatility from tmp6 group by filename;



drop table if exists top10;
create table top10 as select filename,volatility from tmp7 sort by volatility desc limit 10;

drop table if exists bot10;
create table bot10 as select filename,volatility from tmp7 where volatility>0 and volatility is not null sort by volatility asc limit 10;








