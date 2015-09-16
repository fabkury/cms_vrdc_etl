/* CMS VRDC ETL to OHDSI CDM v5
 * -----------------------------
 * Fabricio Kury, MD, Vojtech Huser, MD, PhD
 * Lister Hill National Center For Biomedical Communications,
 * U.S. National Library of Medicine
 * September 14, 2015
 *
 * This program is intended to produce Observational Health Data Sciences and
 * Informatics (OHDSI) Common Data Model v5 tables from the data available
 * inside the Virtual Research Data Center of the U.S. Centers For Medicare &
 * Medicaid Services (CMS).
 *
 * Contact:
 * fabricio.kury@nih.gov
 * http://github.com/fabkury
 */

/** BEGIN CONSTANTS **/
%let year_list = 1999 2000 2001 2002 2003 2004 2005 2006 2007 2008 2009 2010
	2011 2012; /* year_list specifies the years of data to be considered for
 all data except Part D data. It should be tailored to your DUA and/or your
 intentions. */

%let drug_year_list = 6 7 8 9 10 11 12; /* drug_year_list specifies the years
 of data to be considered for Part D data. It should be tailored to your DUA
 and/or your intentions. */
 
%let maps_library = FKU838SL.; /* maps_library must point to the SAS library
 containing the vc5_* tables with the mappings from VRDC codes to OHDSI CDM v5
 codes. */

%let pde_library = IN026250.; /* pde_library must point to the SAS library
 containing the Part D Event (PDE) files. */

%let pde_request_suffix = _R3632; /* pde_request_suffix constains the suffix
 that is added to the PDE file names according to the number of the request
 that originated the files. */

%let part_d_enrollment_code = 44814722; /* Code used as concept_id for
 observation periods derived from Part D enrollment. */

%let part_d_coverage_regex = "/H|R|S|E|(X[A-Za-z0-9]{4})/"; /* This PERL
 regular expression identifies all codes that signify that the beneficiary
 had Part D enrollment in a specific month; as specified in the ResDAC website.

/** END CONSTANTS **/

/** BEGIN ETL CODE **/

/* PERSON table */
%macro etl_person;
proc sql;
create view PERSON as
/* The %do ... %to ... loop structure is used to replicate the SQL code for all
 years. */
%do YL=1 %to %sysfunc(countw(&year_list));
	%if &YL > 1 %then union;
	select BENE_ID as person_id, /* Notice that we are using the BENE_ID
 both as person_id and person_source_value, the only difference being the
 data type (number and text, respectively). */
		gender_concept_id, /* Comes from vc5_gender_map.csv */
		year(BENE_BIRTH_DT) as year_of_birth,
		month(BENE_BIRTH_DT) as month_of_birth,
		day(BENE_BIRTH_DT) as day_of_birth,
		race_concept_id, /* Comes from vc5_race_map.csv */
		ethnicity_concept_id, /* Comes from vc5_ethnicity_map.csv */
		put(BENE_ID, zn.) as person_source_value,
		a.BENE_SEX_IDENT_CD as gender_source_value,
		a.BENE_RACE_CD as race_source_value
	from BENE_CC.MBSF_AB_%scan(&year_list, &YL) a,
		&maps_library.vc5_gender_map b,
		&maps_library.vc5_race_map c,
		&maps_library.vc5_ethnicity_map d
	where b.BENE_SEX_IDENT_CD = a.BENE_SEX_IDENT_CD
		and c.BENE_RACE_CD = a.BENE_RACE_CD
		and d.BENE_RACE_CD = a.BENE_RACE_CD
%end;; /* This double ; is intentional and necessary. */ 
quit;
%mend;

/* DRUG_EXPOSURE table */
%macro etl_drug_exposure;
proc sql;
create view DRUG_EXPOSURE as
/* The %do ... %to ... loop structure is used to replicate the SQL code for all
 years. */
%do YL=1 %to %sysfunc(countw(&drug_year_list));
	%if &YL > 1 %then union;
	%let y = %scan(&drug_year_list, &YL);
	select PDE_ID as drug_exposure_id, /* The PDE_ID is a number that uniquely
 identifies the rows in the PDE files. */
		BENE_ID as person_id,
		0 as drug_concept_id,
		SRVC_DT as drug_exposure_start_date,
		0 as drug_type_concept_id,
		FILL_NUM as refills, /* TO DO: Verify if this interpretation of
 FILL_NUM is correct. */
		DAYS_SUPLY_NUM as days_supply,
		PROD_SRVC_ID as drug_source_value,
		0 as route_source_value,
		STR as dose_unit_source_value
	/* In 2012 they changed the file naming convention from starting with
 "PDESAF" to starting with "PDE" */
	from %if &y > 11 %then &pde_library.PDE&y.&pde_request_suffix;
		%else &pde_library.PDESAF%sysfunc(putn(&y, Z2.))&pde_request_suffix;
%end;; /* This double ; is intentional and necessary. */ 
quit;
%mend;

/* DEATH table */
%macro etl_death;
proc sql;
create view DEATH as
/* The %do ... %to ... loop structure is used to replicate the SQL code for all
 years. */
%do YL=1 %to %sysfunc(countw(&year_list));
	%if &YL > 1 %then union;
	select BENE_ID as person_id,
		BENE_DEATH_DT as death_date,
		0 as death_type_concept_id
	from BENE_CC.MBSF_AB_%scan(&year_list, &YL)
	where BENE_DEATH_DT is not null
%end;; /* This double ; is intentional and necessary. */ 
quit;
%mend;

/* OBSERVATION_PERIOD table */
%macro etl_observation_period;
/* PLEASE NOTICE that this code currently only considers periods of Part D 
 enrollment as observation periods; although the similar code could be used
 for Parts A and B. */

proc sql;
create view OBSERVATION_PERIOD as
/* The %do ... %to ... loop structure is used to replicate the SQL code for all
 years. */
%do YL=1 %to %sysfunc(countw(&drug_year_list));
	%let y = %eval(%scan(&drug_year_list, &YL)+2000);
	%if &YL > 1 %then union;
	/* This additional %do ... %to ... loop structure replicates the SQL code
for each month of the year (each has a separate column in the VRDC) */
	%do MO=1 %to 12;
		%if &MO > 1 %then union;
		select 0 as observation_period_id,
			BENE_ID as person_id,
			mdy(&MO, 1, &y) as observation_period_start_date,
			intnx('day', intnx('month', mdy(&MO, 1, &y), 1), -1)
				as observation_period_end_date,
			&part_d_enrollment_code as period_type_concept_id
		from BENE_CC.MBSF_D_&y
		where prxmatch(&part_d_coverage_regex,
			PTD_CNTRCT_ID_%sysfunc(putn(&MO, Z2.))) > 0
	%end;
%end;;
quit;
%mend;

%macro drop_all_etl_views;
proc sql;
drop view PERSON;
drop view DRUG_EXPOSURE;
drop view DEATH;
drop view OBSERVATION_PERIOD;
quit;
%mend;

/** END ETL CODE **/

/* Now, execute the ETL, i.e., create the SQL views. */
%etl_person;
%etl_drug_exposure;
%etl_death;
%etl_observation_period;

/** END ETL CODE **/

/* Uncomment the section below to test the ETL. */
/*
proc sql;
create table drug_exposure_mat as
select * from DRUG_EXPOSURE
where person_id < 50000;
quit;

proc sql;
create table person_mat as
select * from PERSON
where person_id < 50000;
quit;

proc sql;
create table death_mat as
select * from DEATH
where person_id < 50000;
quit;

proc sql;
create table observation_period_mat as
select * from OBSERVATION_PERIOD
where person_id < 50000;
quit;
*/

/* As a "safety" measure, we delete the ETL views after using them. Views
 are created/deleted virtually instantly, and the reason to delete them is to
 avoid the risk of accidentaly clicking, inside SAS, in any tab or button that
 shows you the content of the view. Such click would make SAS initiate ETL of
 the entire data files, a long operation that freezes the program. */
%drop_all_etl_views;
