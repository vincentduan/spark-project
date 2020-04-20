create table day_netType_access_topn_stat (
day varchar(8) not null,
uid bigint(10) not null,
times bigint(10) not null,
primary key (day, uid)
)

create table day_netType_city_access_topn_stat (
day varchar(8) not null,
uid bigint(10) not null,
city varchar(20) not null,
times bigint(10) not null,
times_rank  bigint(10) not null,
primary key (day, uid)
)

create table day_netType_city_traffics_topn_stat (
day varchar(8) not null,
uid bigint(10) not null,
traffics bigint(20) not null,
primary key (day, uid)
)