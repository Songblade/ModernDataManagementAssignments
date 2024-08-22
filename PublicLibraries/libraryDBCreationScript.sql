drop table lib2016;
drop table lib2017;
drop table lib2018;

create table lib2016 (STABR char(2), FSCSKEY char(6) PRIMARY KEY, LIBID varchar(20), LIBNAME varchar(100), ADDRESS varchar(100), CITY varchar(30), ZIP varchar(5), CNTY varchar(30), PHONE char(10), C_RELATN char(2), C_LEGBAS char(2), C_ADMIN char(2), C_FSCS char(1), GEOCODE char(3), LSABOUND char(1), STARTDAT varchar(10), ENDDAT varchar(10), POPU_LSA int, POPU_UND int, CENTLIB int, BRANLIB int, BKMOB int, TOTSTAFF double precision, BKVOL int, EBOOK int, AUDIO_PH int, AUDIO_DL int, VIDEO_PH int, VIDEO_DL int, EC_LO_OT int, SUBSCRIP int, HRS_OPEN int, VISITS int, REFERENC int, REGBOR int, TOTCIR int, KIDCIRCL int, TOTPRO int, GPTERMS int, PITUSR int, WIFI_SESS int, OBEREG varchar(3), STATSTRU varchar(3), STATNAME varchar(3), STATADDR varchar(3), LONGITUD double precision, LATITUDE double precision);

create table lib2017 (STABR char(2), FSCSKEY char(6) PRIMARY KEY, LIBID varchar(20), LIBNAME varchar(100), ADDRESS varchar(100), CITY varchar(30), ZIP varchar(5), CNTY varchar(30), PHONE char(10), C_RELATN char(2), C_LEGBAS char(2), C_ADMIN char(2), C_FSCS char(1), GEOCODE char(3), LSABOUND char(1), STARTDAT varchar(10), ENDDAT varchar(10), POPU_LSA int, POPU_UND int, CENTLIB int, BRANLIB int, BKMOB int, TOTSTAFF double precision, BKVOL int, EBOOK int, AUDIO_PH int, AUDIO_DL int, VIDEO_PH int, VIDEO_DL int, EC_LO_OT int, SUBSCRIP int, HRS_OPEN int, VISITS int, REFERENC int, REGBOR int, TOTCIR int, KIDCIRCL int, TOTPRO int, GPTERMS int, PITUSR int, WIFI_SESS int, OBEREG varchar(3), STATSTRU varchar(3), STATNAME varchar(3), STATADDR varchar(3), LONGITUD double precision, LATITUDE double precision);

create table lib2018 (STABR char(2), FSCSKEY char(6) PRIMARY KEY, LIBID varchar(20), LIBNAME varchar(100), ADDRESS varchar(100), CITY varchar(30), ZIP varchar(5), CNTY varchar(30), PHONE char(10), C_RELATN char(2), C_LEGBAS char(2), C_ADMIN char(2), C_FSCS char(1), GEOCODE char(3), LSABOUND char(1), STARTDAT varchar(10), ENDDAT varchar(10), POPU_LSA int, POPU_UND int, CENTLIB int, BRANLIB int, BKMOB int, TOTSTAFF double precision, BKVOL int, EBOOK int, AUDIO_PH int, AUDIO_DL int, VIDEO_PH int, VIDEO_DL int, EC_LO_OT int, SUBSCRIP int, HRS_OPEN int, VISITS int, REFERENC int, REGBOR int, TOTCIR int, KIDCIRCL int, TOTPRO int, GPTERMS int, PITUSR int, WIFI_SESS int, OBEREG varchar(3), STATSTRU varchar(3), STATNAME varchar(3), STATADDR varchar(3), LONGITUD double precision, LATITUDE double precision);

COPY lib2016  -- I know you want us to list all the columns, but there are too many
FROM '/mnt/c/users/shimm/coding/Greengart_Shimon_2018998974/ModernDataManagement/assignments/PublicLibraries/pls_fy2016_libraries.csv'
DELIMITER ','
CSV HEADER;

COPY lib2017
FROM '/mnt/c/users/shimm/coding/Greengart_Shimon_2018998974/ModernDataManagement/assignments/PublicLibraries/pls_fy2017_libraries.csv'
DELIMITER ','
CSV HEADER;

COPY lib2018
FROM '/mnt/c/users/shimm/coding/Greengart_Shimon_2018998974/ModernDataManagement/assignments/PublicLibraries/pls_fy2018_libraries.csv'
DELIMITER ','
CSV HEADER;

SELECT count(*), count(*) = 9252 from lib2016;
SELECT count(*), count(*) = 9245 from lib2017;
SELECT count(*), count(*) = 9261 from lib2018;