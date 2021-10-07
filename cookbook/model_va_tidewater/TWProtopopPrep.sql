--ALL LOCATIONS IN TW VA
drop table Tidewater_locations_home purge;
create table Tidewater_locations_home as select * from PROTOPOP.US_HOME_LOCATIONS_C2009 a where a.STATE='51' and a.COUNTY in
('175', '093', '800', '550', '810', '740', '710', '650', '700', '735', '199', '830', '095', '115', '073', '119', '101', '097', '103', '159', '057', '193', '133', '001', '131', '620');

drop table Tidewater_person_info purge;
create table Tidewater_person_info as
  select * from PROTOPOP.va_person_info_2009_1 where hid in 
    (select hid from protopop.va_household_2009_1 where county in
    (175, 093, 800, 550, 810, 740, 710, 650, 700, 735, 199, 830, 095, 115, 073, 119, 101, 097, 103, 159, 057, 193, 133, 001, 131, 620));


--CREATE LIST OF ALL HOMES AND ACTIVITY LOCATIONS WITH COORDS


drop table Tidewater_locations_nonhome purge;
create table Tidewater_locations_nonhome as
  select distinct a.location, c.latitude, c.longitude from PROTOPOP.va_activities_2009_1 a, Tidewater_person_info b, PROTOPOP.US_DNB_LOCS_W_NCES_SCHOOLS c
  where a.purpose!=0 and a.pid=b.pid and a.location=c.id;

drop table Tidewater_activities_home purge;    
create table Tidewater_activities_Home as
  select * from PROTOPOP.va_activities_2009_1 where purpose=0 and hid in 
    (select hid from protopop.va_household_2009_1 where county in
    (175, 093, 800, 550, 810, 740, 710, 650, 700, 735, 199, 830, 095, 115, 073, 119, 101, 097, 103, 159, 057, 193, 133, 001, 131, 620));

drop table Tidewater_activities_nonhome purge;
create table Tidewater_activities_NonHome as
  select * from PROTOPOP.va_activities_2009_1 where purpose!=0 and hid in 
    (select hid from protopop.va_household_2009_1 where county in
    (175, 093, 800, 550, 810, 740, 710, 650, 700, 735, 199, 830, 095, 115, 073, 119, 101, 097, 103, 159, 057, 193, 133, 001, 131, 620));
    
--CLEANUP Tidewater_locs




--For activities : pid, location_id, start_time, duration 
--/w bounding boxes for each


-------The land of forgotten code--------


--CLEANUP Tidewater_locs
--For locations : id, lat, lon
--alter table Tidewater_locs drop (Z, ZIPCODE, WORK, SCHOOL, ELEM_SCHOOL);
--  'MID_SCHOOL', 'HIGH_SCHOOL', 'DAYCARE', 'SHOPPING', 'OTHER', 'COLLEGE', 'FIPS_STATE_CODE', 'FIPS_COUNTY_CODE');


--select min(LONGITUDE), max(longitude) from Tidewater_locations_nonhome;

--select min(LONGITUDE), max(longitude) from PROTOPOP.US_HOME_LOCATIONS_C2009 a where a.STATE='51' and a.COUNTY in
--(175, 093, 800, 550, 810, 740, 710, 650, 700, 735, 199, 830, 095, 115, 073, 119, 101, 097, 103, 159, 057, 193, 133, 001, 131, 620);

--SELECT ALL PEOPLE WHO HAVE A HOME ID IN TABLE OF HOME IDS WITHIN ACCEPTED FIPS

--select min(LONGITUDE), max(longitude) from Tidewater_locations_nonhome;

--select min(LONGITUDE), max(longitude) from PROTOPOP.US_HOME_LOCATIONS_C2009 a where a.STATE='51' and a.COUNTY in
--(175, 093, 800, 550, 810, 740, 710, 650, 700, 735, 199, 830, 095, 115, 073, 119, 101, 097, 103, 159, 057, 193, 133, 001, 131, 620);

--SELECT ALL PEOPLE WHO HAVE A HOME ID IN TABLE OF HOME IDS WITHIN ACCEPTED FIPS

--create table Tidewater_locations_nonhome_fips as select * from PROTOPOP.US_DNB_LOCS_W_NCES_SCHOOLS a where a.FIPS_STATE_CODE='51' and a.FIPS_COUNTY_CODE in
--('175', '093', '800', '550', '810', '740', '710', '650', '700', '735', '199', '830', '095', '115', '073', '119', '101', '097', '103', '159', '057', '193', '133', '001', '131', '620');

--create table Tidewater_activities_NonHomeF as
--  select * from PROTOPOP.va_activities_2009_1 where purpose=0 and hid in 
--    (select hid from protopop.va_household_2009_1 where county in
--    (175, 093, 800, 550, 810, 740, 710, 650, 700, 735, 199, 830, 095, 115, 073, 119, 101, 097, 103, 159, 057, 193, 133, 001, 131, 620));
