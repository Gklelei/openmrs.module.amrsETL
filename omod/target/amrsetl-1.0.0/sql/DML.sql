
SET @OLD_SQL_MODE=@@SQL_MODE $$
SET SQL_MODE='' $$
DROP PROCEDURE IF EXISTS sp_populate_etl_patient_demographics $$
CREATE PROCEDURE sp_populate_etl_patient_demographics()
BEGIN
-- initial set up of etl_patient_demographics table
SELECT "Processing patient demographics data ", CONCAT("Time: ", NOW());

insert into amrsemr_etl.etl_patient_demographics(
    patient_id,
    uuid,
    given_name,
    middle_name,
    family_name,
    Gender,
    DOB,
    dead,
    date_created,
    date_last_modified,
    voided,
    death_date
    )
select
       p.person_id,
       p.uuid,
       p.given_name,
       p.middle_name,
       p.family_name,
       p.gender,
       p.birthdate,
       p.dead,
       p.date_created,
       if((p.date_last_modified='0000-00-00 00:00:00' or p.date_last_modified=p.date_created),NULL,p.date_last_modified) as date_last_modified,
       p.voided,
       p.death_date
FROM (
     select
            p.person_id,
            p.uuid,
            pn.given_name,
            pn.middle_name,
            pn.family_name,
            p.gender,
            p.birthdate,
            p.dead,
            p.date_created,
            greatest(ifnull(p.date_changed,'0000-00-00 00:00:00'),ifnull(pn.date_changed,'0000-00-00 00:00:00')) as date_last_modified,
            p.voided,
            p.death_date
     from amrs.person p
            left join amrs.patient pa on pa.patient_id=p.person_id
            left join amrs.person_name pn on pn.person_id = p.person_id and pn.voided=0
     where p.voided=0
     GROUP BY p.person_id
     ) p
ON DUPLICATE KEY UPDATE given_name = p.given_name, middle_name=p.middle_name, family_name=p.family_name;

-- update etl_patient_demographics with patient attributes: birthplace, citizenship, mother_name, phone number and kin's details
update amrsemr_etl.etl_patient_demographics d
left outer join
(
select
       pa.person_id,
       max(if(pat.uuid='8d8718c2-c2cc-11de-8d13-0010c6dffd0f', pa.value, null)) as birthplace,
       max(if(pat.uuid='8d871afc-c2cc-11de-8d13-0010c6dffd0f', pa.value, null)) as citizenship,
       max(if(pat.uuid='8d871d18-c2cc-11de-8d13-0010c6dffd0f', pa.value, null)) as Mother_name,
       max(if(pat.uuid='b2c38640-2603-4629-aebd-3b54f33f1e3a', pa.value, null)) as phone_number,
       max(if(pat.uuid='342a1d39-c541-4b29-8818-930916f4c2dc', pa.value, null)) as next_of_kin_contact,
       max(if(pat.uuid='d0aa9fd1-2ac5-45d8-9c5e-4317c622c8f5', pa.value, null)) as next_of_kin_relationship,
       max(if(pat.uuid='7cf22bec-d90a-46ad-9f48-035952261294', pa.value, null)) as next_of_kin_address,
       max(if(pat.uuid='830bef6d-b01f-449d-9f8d-ac0fede8dbd3', pa.value, null)) as next_of_kin_name,
       max(if(pat.uuid='b8d0b331-1d2d-4a9a-b741-1816f498bdb6', pa.value, null)) as email_address,
       max(if(pat.uuid='848f5688-41c6-464c-b078-ea6524a3e971', pa.value, null)) as unit,
       max(if(pat.uuid='96a99acd-2f11-45bb-89f7-648dbcac5ddf', pa.value, null)) as cadre,
       max(if(pat.uuid='9f1f8254-20ea-4be4-a14d-19201fe217bf', pa.value, null)) as kdod_rank,
      greatest(ifnull(pa.date_changed,'0000-00-00'),pa.date_created) as latest_date
from amrs.person_attribute pa
       inner join
         (
         select
                pat.person_attribute_type_id,
                pat.name,
                pat.uuid
         from amrs.person_attribute_type pat
         where pat.retired=0
         ) pat on pat.person_attribute_type_id = pa.person_attribute_type_id
                    and pat.uuid in (
        '8d8718c2-c2cc-11de-8d13-0010c6dffd0f', -- birthplace
        '8d871afc-c2cc-11de-8d13-0010c6dffd0f', -- citizenship
        '8d871d18-c2cc-11de-8d13-0010c6dffd0f', -- mother's name
        'b2c38640-2603-4629-aebd-3b54f33f1e3a', -- telephone contact
        '342a1d39-c541-4b29-8818-930916f4c2dc', -- next of kin's contact
        'd0aa9fd1-2ac5-45d8-9c5e-4317c622c8f5', -- next of kin's relationship
        '7cf22bec-d90a-46ad-9f48-035952261294', -- next of kin's address
        '830bef6d-b01f-449d-9f8d-ac0fede8dbd3', -- next of kin's name
        'b8d0b331-1d2d-4a9a-b741-1816f498bdb6', -- email address
        '848f5688-41c6-464c-b078-ea6524a3e971', -- unit
        '96a99acd-2f11-45bb-89f7-648dbcac5ddf', -- cadre
        '9f1f8254-20ea-4be4-a14d-19201fe217bf' -- rank

        )
where pa.voided=0
group by pa.person_id
) att on att.person_id = d.patient_id
set d.phone_number=att.phone_number,
    d.next_of_kin=att.next_of_kin_name,
    d.next_of_kin_relationship=att.next_of_kin_relationship,
    d.next_of_kin_phone=att.next_of_kin_contact,
    d.phone_number=att.phone_number,
    d.birth_place = att.birthplace,
    d.citizenship = att.citizenship,
    d.email_address=att.email_address,
    d.unit=att.unit,
    d.cadre=att.cadre,
    d.kdod_rank=att.kdod_rank,
    d.date_last_modified=if(att.latest_date > ifnull(d.date_last_modified,'0000-00-00'),att.latest_date,d.date_last_modified)
;


update amrsemr_etl.etl_patient_demographics d
join (select pi.patient_id,
             coalesce (max(if(pit.uuid='05ee9cf4-7242-4a17-b4d4-00f707265c8a',pi.identifier,null)),max(if(pit.uuid='b51ffe55-3e76-44f8-89a2-14f5eaf11079',pi.identifier,null))) as upn,
             max(if(pit.uuid='d8ee3b8c-a8fc-4d6b-af6a-9423be5f8906',pi.identifier,null)) district_reg_number,
             max(if(pit.uuid='c4e3caca-2dcc-4dc4-a8d9-513b6e63af91',pi.identifier,null)) Tb_treatment_number,
             max(if(pit.uuid='b4d66522-11fc-45c7-83e3-39a1af21ae0d',pi.identifier,null)) Patient_clinic_number,
             max(if(pit.uuid='49af6cdc-7968-4abb-bf46-de10d7f4859f',pi.identifier,null)) National_id,
             max(if(pit.uuid='6428800b-5a8c-4f77-a285-8d5f6174e5fb',pi.identifier,null)) Huduma_number,
             max(if(pit.uuid='be9beef6-aacc-4e1f-ac4e-5babeaa1e303',pi.identifier,null)) Passport_number,
             max(if(pit.uuid='68449e5a-8829-44dd-bfef-c9c8cf2cb9b2',pi.identifier,null)) Birth_cert_number,
             max(if(pit.uuid='0691f522-dd67-4eeb-92c8-af5083baf338',pi.identifier,null)) Hei_id,
             max(if(pit.uuid='1dc8b419-35f2-4316-8d68-135f0689859b',pi.identifier,null)) cwc_number,
             max(if(pit.uuid='f2b0c94f-7b2b-4ab0-aded-0d970f88c063',pi.identifier,null)) kdod_service_number,
             max(if(pit.uuid='5065ae70-0b61-11ea-8d71-362b9e155667',pi.identifier,null)) CPIMS_unique_identifier,
             max(if(pit.uuid='dfacd928-0370-4315-99d7-6ec1c9f7ae76',pi.identifier,null)) openmrs_id,
             max(if(pit.uuid='ac64e5cb-e3e2-4efa-9060-0dd715a843a1',pi.identifier,null)) unique_prep_number,
             max(if(pit.uuid='1c7d0e5b-2068-4816-a643-8de83ab65fbf',pi.identifier,null)) alien_no,
             max(if(pit.uuid='ca125004-e8af-445d-9436-a43684150f8b',pi.identifier,null)) driving_license_no,
             max(if(pit.uuid='f85081e2-b4be-4e48-b3a4-7994b69bb101',pi.identifier,null)) national_unique_patient_identifier,
             REPLACE(max(if(pit.uuid='fd52829a-75d2-4732-8e43-4bff8e5b4f1a',pi.identifier,null)),'-','') hts_recency_id,
             max(if(pit.uuid='09ebf4f9-b673-4d97-b39b-04f94088ba64',pi.identifier,null)) nhif_number,
             greatest(ifnull(max(pi.date_changed),'0000-00-00'),max(pi.date_created)) as latest_date
      from amrs.patient_identifier pi
             join amrs.patient_identifier_type pit on pi.identifier_type=pit.patient_identifier_type_id
      where voided=0
      group by pi.patient_id) pid on pid.patient_id=d.patient_id
set d.unique_patient_no=pid.upn,
    d.national_id_no=pid.National_id,
    d.huduma_no=pid.Huduma_number,
    d.passport_no=pid.Passport_number,
    d.birth_certificate_no=pid.Birth_cert_number,
    d.patient_clinic_number=pid.Patient_clinic_number,
    d.hei_no=pid.Hei_id,
    d.cwc_number=pid.cwc_number,
    d.Tb_no=pid.Tb_treatment_number,
    d.district_reg_no=pid.district_reg_number,
    d.kdod_service_number=pid.kdod_service_number,
    d.CPIMS_unique_identifier=pid.CPIMS_unique_identifier,
    d.openmrs_id=pid.openmrs_id,
    d.unique_prep_number=pid.unique_prep_number,
    d.alien_no=pid.alien_no,
    d.driving_license_no=pid.driving_license_no,
    d.national_unique_patient_identifier=pid.national_unique_patient_identifier,
    d.hts_recency_id=pid.hts_recency_id,
    d.nhif_number=pid.nhif_number,
    d.date_last_modified=if(pid.latest_date > ifnull(d.date_last_modified,'0000-00-00'),pid.latest_date,d.date_last_modified)
;

update amrsemr_etl.etl_patient_demographics d
join (select o.person_id as patient_id,
             max(if(o.concept_id in(1054),cn.name,null))  as marital_status,
             max(if(o.concept_id in(1712),cn.name,null))  as education_level,
             max(if(o.concept_id in(1542),cn.name,null))  as occupation,
             max(o.date_created) as date_created
                   from amrs.obs o
             join amrs.concept_name cn on cn.concept_id=o.value_coded and cn.concept_name_type='FULLY_SPECIFIED'
                                       and cn.locale='en'
      where o.concept_id in (1054,1712,1542) and o.voided=0
      group by person_id) pstatus on pstatus.patient_id=d.patient_id
set d.marital_status=pstatus.marital_status,
    d.education_level=pstatus.education_level,
    d.occupation=pstatus.occupation,
    d.date_last_modified=if(pstatus.date_created > ifnull(d.date_last_modified,'0000-00-00'),pstatus.date_created,d.date_last_modified)
;

END $$


DROP PROCEDURE IF EXISTS sp_populate_etl_hiv_enrollment $$
CREATE PROCEDURE sp_populate_etl_hiv_enrollment()
BEGIN
-- populate patient_hiv_enrollment table
-- uuid: de78a6be-bfc5-4634-adc3-5f1a280455cc
SELECT "Processing HIV Enrollment data ", CONCAT("Time: ", NOW());
insert into amrsemr_etl.etl_hiv_enrollment (
    patient_id,
    uuid,
    visit_id,
    visit_date,
    location_id,
    encounter_id,
    encounter_provider,
    date_created,
    date_last_modified,
    patient_type,
    date_first_enrolled_in_care,
    entry_point,
    transfer_in_date,
    facility_transferred_from,
    district_transferred_from,
    previous_regimen,
    date_started_art_at_transferring_facility,
    date_confirmed_hiv_positive,
    facility_confirmed_hiv_positive,
    arv_status,
    ever_on_pmtct,
    ever_on_pep,
    ever_on_prep,
    ever_on_haart,
                                             who_stage,
    name_of_treatment_supporter,
    relationship_of_treatment_supporter,
    treatment_supporter_telephone,
    treatment_supporter_address,
    in_school,
    orphan,
    date_of_discontinuation,
    discontinuation_reason,
    voided
    )
select
       e.patient_id,
       e.uuid,
       e.visit_id,
       e.encounter_datetime as visit_date,
       e.location_id,
       e.encounter_id,
       e.creator,
       e.date_created,
       if(max(o.date_created) > min(e.date_created),max(o.date_created),NULL) as date_last_modified,
       max(if(o.concept_id in (164932), o.value_coded, if(o.concept_id=160563 and o.value_coded=1065, 160563, null))) as patient_type ,
       max(if(o.concept_id=160555,o.value_datetime,null)) as date_first_enrolled_in_care ,
       max(if(o.concept_id=160540,o.value_coded,null)) as entry_point,
       max(if(o.concept_id=160534,o.value_datetime,null)) as transfer_in_date,
       max(if(o.concept_id=160535,left(trim(o.value_text),100),null)) as facility_transferred_tcingfrom,
       max(if(o.concept_id=161551,left(trim(o.value_text),100),null)) as district_transferred_from,
       max(if(o.concept_id=164855,o.value_coded,null)) as previous_regimen,
       max(if(o.concept_id=159599,o.value_datetime,null)) as date_started_art_at_transferring_facility,
       max(if(o.concept_id=160554,o.value_datetime,null)) as date_confirmed_hiv_positive,
       max(if(o.concept_id=160632,left(trim(o.value_text),100),null)) as facility_confirmed_hiv_positive,
       max(if(o.concept_id=160533,o.value_coded,null)) as arv_status,
       max(if(o.concept_id=1148,o.value_coded,null)) as ever_on_pmtct,
       max(if(o.concept_id=1691,o.value_coded,null)) as ever_on_pep,
       max(if(o.concept_id=165269,o.value_coded,null)) as ever_on_prep,
       max(if(o.concept_id=1181,o.value_coded,null)) as ever_on_haart,
       max(if(o.concept_id=5356,o.value_coded,null)) as who_stage,
       max(if(o.concept_id=160638,left(trim(o.value_text),100),null)) as name_of_treatment_supporter,
       max(if(o.concept_id=160640,o.value_coded,null)) as relationship_of_treatment_supporter,
       max(if(o.concept_id=160642,left(trim(o.value_text),100),null)) as treatment_supporter_telephone ,
       max(if(o.concept_id=160641,left(trim(o.value_text),100),null)) as treatment_supporter_address,
       max(if(o.concept_id=5629,o.value_coded,null)) as in_school,
       max(if(o.concept_id=1174,o.value_coded,null)) as orphan,
       max(if(o.concept_id=164384, o.value_datetime, null)) as date_of_discontinuation,
       max(if(o.concept_id=161555, o.value_coded, null)) as discontinuation_reason,
       e.voided
from amrs.encounter e
       inner join
         (
         select encounter_type_id, uuid, name from amrs.encounter_type where uuid='de78a6be-bfc5-4634-adc3-5f1a280455cc'
         ) et on et.encounter_type_id=e.encounter_type
       inner join amrs.person p on p.person_id=e.patient_id and p.voided=0
       left outer join amrs.obs o on o.encounter_id=e.encounter_id and o.voided=0
                                  and o.concept_id in (160555,160540,160534,160535,161551,159599,160554,160632,160533,160638,160640,160642,160641,164932,160563,5629,1174,1088,161555,164855,164384,1148,1691,165269,1181,5356)
where e.voided=0
group by e.patient_id, e.encounter_id;
SELECT "Completed processing HIV Enrollment data ", CONCAT("Time: ", NOW());
END $$



-- ------------- populate etl_hiv_followup--------------------------------

DROP PROCEDURE IF EXISTS sp_populate_etl_hiv_followup $$
CREATE PROCEDURE sp_populate_etl_hiv_followup()
BEGIN
SELECT "Processing HIV Followup data ", CONCAT("Time: ", NOW());
INSERT INTO amrsemr_etl.etl_patient_hiv_followup(
uuid,
patient_id,
visit_id,
visit_date,
location_id,
encounter_id,
encounter_provider,
date_created,
date_last_modified,
visit_scheduled,
person_present,
weight,
systolic_pressure,
diastolic_pressure,
height,
temperature,
pulse_rate,
respiratory_rate,
oxygen_saturation,
muac,
z_score_absolute,
z_score,
nutritional_status,
population_type,
key_population_type,
who_stage,
who_stage_associated_oi,
presenting_complaints,
clinical_notes,
on_anti_tb_drugs,
on_ipt,
ever_on_ipt,
cough,
fever,
weight_loss_poor_gain,
night_sweats,
tb_case_contact,
lethargy,
screened_for_tb,
spatum_smear_ordered,
chest_xray_ordered,
genexpert_ordered,
spatum_smear_result,
chest_xray_result,
genexpert_result,
referral,
clinical_tb_diagnosis,
contact_invitation,
evaluated_for_ipt,
has_known_allergies,
has_chronic_illnesses_cormobidities,
has_adverse_drug_reaction,
pregnancy_status,
breastfeeding,
wants_pregnancy,
pregnancy_outcome,
anc_number,
expected_delivery_date,
ever_had_menses,
last_menstrual_period,
menopausal,
gravida,
parity,
full_term_pregnancies,
abortion_miscarriages,
family_planning_status,
family_planning_method,
reason_not_using_family_planning,
tb_status,
started_anti_TB,
tb_rx_date,
tb_treatment_no,
general_examination,
system_examination,
skin_findings,
eyes_findings,
ent_findings,
chest_findings,
cvs_findings,
abdomen_findings,
cns_findings,
genitourinary_findings,
prophylaxis_given,
ctx_adherence,
ctx_dispensed,
dapsone_adherence,
dapsone_dispensed,
inh_dispensed,
arv_adherence,
poor_arv_adherence_reason,
poor_arv_adherence_reason_other,
pwp_disclosure,
pwp_pead_disclosure,
pwp_partner_tested,
condom_provided,
substance_abuse_screening,
screened_for_sti,
cacx_screening,
sti_partner_notification,
at_risk_population,
system_review_finding,
appointment_consent,
next_appointment_reason,
stability,
differentiated_care,
insurance_type,
other_insurance_specify,
insurance_status,
voided
)
select
e.uuid,
e.patient_id,
e.visit_id,
date(e.encounter_datetime) as visit_date,
e.location_id,
e.encounter_id as encounter_id,
e.creator,
e.date_created as date_created,
if(max(o.date_created) > min(e.date_created),max(o.date_created),NULL) as date_last_modified,
max(if(o.concept_id=1246,o.value_coded,null)) as visit_scheduled ,
max(if(o.concept_id=161643,o.value_coded,null)) as person_present,
max(if(o.concept_id=5089,o.value_numeric,null)) as weight,
max(if(o.concept_id=5085,o.value_numeric,null)) as systolic_pressure,
max(if(o.concept_id=5086,o.value_numeric,null)) as diastolic_pressure,
max(if(o.concept_id=5090,o.value_numeric,null)) as height,
max(if(o.concept_id=5088,o.value_numeric,null)) as temperature,
max(if(o.concept_id=5087,o.value_numeric,null)) as pulse_rate,
max(if(o.concept_id=5242,o.value_numeric,null)) as respiratory_rate,
max(if(o.concept_id=5092,o.value_numeric,null)) as oxygen_saturation,
max(if(o.concept_id=1343,o.value_numeric,null)) as muac,
max(if(o.concept_id=162584,o.value_numeric,null)) as z_score_absolute,
max(if(o.concept_id=163515,o.value_coded,null)) as z_score,
max(if(o.concept_id=163515,o.value_coded,null)) as nutritional_status,
max(if(o.concept_id=164930,o.value_coded,null)) as population_type,
max(if(o.concept_id=160581,o.value_coded,null)) as key_population_type,
max(if(o.concept_id=5356,o.value_coded,null)) as who_stage,
  concat_ws(',',nullif(max(if(o.concept_id=167394 and o.value_coded =5006 ,'Asymptomatic','')),''),
            nullif(max(if(o.concept_id=167394 and o.value_coded =130364,'Persistent generalized lymphadenopathy)','')),''),
            nullif(max(if(o.concept_id=167394 and o.value_coded =159214,'Unexplained severe weight loss','')),''),
            nullif(max(if(o.concept_id=167394 and o.value_coded =5330,'Minor mucocutaneous manifestations','')),''),
            nullif(max(if(o.concept_id=167394 and o.value_coded =117543,'Herpes zoster','')),''),
            nullif(max(if(o.concept_id=167394 and o.value_coded =5012,'Recurrent upper respiratory tract infections','')),''),
            nullif(max(if(o.concept_id=167394 and o.value_coded =5018,'Unexplained chronic diarrhoea','')),''),
            nullif(max(if(o.concept_id=167394 and o.value_coded =5027,'Unexplained persistent fever','')),''),
            nullif(max(if(o.concept_id=167394 and o.value_coded =5337,'Oral hairy leukoplakia','')),''),
            nullif(max(if(o.concept_id=167394 and o.value_coded =42,'Pulmonary tuberculosis','')),''),
            nullif(max(if(o.concept_id=167394 and o.value_coded =5333,'Severe bacterial infections such as empyema or pyomyositis or meningitis','')),''),
            nullif(max(if(o.concept_id=167394 and o.value_coded =133440,'Acute necrotizing ulcerative stomatitis or gingivitis or periodontitis','')),''),
            nullif(max(if(o.concept_id=167394 and o.value_coded =148849,'Unexplained anaemia','')),''),
            nullif(max(if(o.concept_id=167394 and o.value_coded =823,'HIV wasting syndrome','')),''),
            nullif(max(if(o.concept_id=167394 and o.value_coded =137375,'Pneumocystis jirovecipneumonia PCP','')),''),
            nullif(max(if(o.concept_id=167394 and o.value_coded =1215,'Recurrent severe bacterial pneumonia','')),''),
            nullif(max(if(o.concept_id=167394 and o.value_coded =1294,'Cryptococcal meningitis','')),''),
            nullif(max(if(o.concept_id=167394 and o.value_coded =990,'Toxoplasmosis of the brain','')),''),
            nullif(max(if(o.concept_id=167394 and o.value_coded =143929,'Chronic orolabial, genital or ano-rectal herpes simplex','')),''),
            nullif(max(if(o.concept_id=167394 and o.value_coded =110915,'Kaposi sarcoma KS','')),''),
            nullif(max(if(o.concept_id=167394 and o.value_coded =160442,'HIV encephalopathy','')),''),
            nullif(max(if(o.concept_id=167394 and o.value_coded =5042,'Extra pulmonary tuberculosis','')),''),
            nullif(max(if(o.concept_id=167394 and o.value_coded =143110,'Cryptosporidiosis with diarrhoea','')),''),
            nullif(max(if(o.concept_id=167394 and o.value_coded =136458,'Isosporiasis','')),''),
            nullif(max(if(o.concept_id=167394 and o.value_coded =5033,'Cryptococcosis extra pulmonary','')),''),
            nullif(max(if(o.concept_id=167394 and o.value_coded =160745,'Disseminated non-tuberculous mycobacterial infection','')),''),
            nullif(max(if(o.concept_id=167394 and o.value_coded =154119,'Cytomegalovirus CMV retinitis','')),''),
            nullif(max(if(o.concept_id=167394 and o.value_coded =5046,'Progressive multifocal leucoencephalopathy','')),''),
            nullif(max(if(o.concept_id=167394 and o.value_coded =131357,'Any disseminated mycosis','')),''),
            nullif(max(if(o.concept_id=167394 and o.value_coded =146513,'Candidiasis of the oesophagus or airways','')),''),
            nullif(max(if(o.concept_id=167394 and o.value_coded =160851,'Non-typhoid salmonella NTS septicaemia','')),''),
            nullif(max(if(o.concept_id=167394 and o.value_coded =155941,'Lymphoma cerebral or B cell Non-Hodgkins Lymphoma','')),''),
            nullif(max(if(o.concept_id=167394 and o.value_coded =116023,'Invasive cervical cancer','')),''),
            nullif(max(if(o.concept_id=167394 and o.value_coded =123084,'Visceral leishmaniasis','')),''),
            nullif(max(if(o.concept_id=167394 and o.value_coded =153701,'Symptomatic HIV-associated nephropathy','')),''),
            nullif(max(if(o.concept_id=167394 and o.value_coded =825,'Unexplained asymptomatic hepatosplenomegaly','')),''),
            nullif(max(if(o.concept_id=167394 and o.value_coded =1249,'Papular pruritic eruptions','')),''),
            nullif(max(if(o.concept_id=167394 and o.value_coded =113116,'Seborrheic dermatitis','')),''),
            nullif(max(if(o.concept_id=167394 and o.value_coded =132387,'Fungal nail infections','')),''),
            nullif(max(if(o.concept_id=167394 and o.value_coded =148762,'Angular cheilitis','')),''),
            nullif(max(if(o.concept_id=167394 and o.value_coded =159344,'Linear gingival erythema','')),''),
            nullif(max(if(o.concept_id=167394 and o.value_coded =1212,'Extensive HPV or molluscum infection','')),''),
            nullif(max(if(o.concept_id=167394 and o.value_coded =159912,'Recurrent oral ulcerations','')),''),
            nullif(max(if(o.concept_id=167394 and o.value_coded =1210,'Parotid enlargement','')),''),
            nullif(max(if(o.concept_id=167394 and o.value_coded =127784,'Recurrent or chronic upper respiratory infection','')),''),
            nullif(max(if(o.concept_id=167394 and o.value_coded =134722,'Unexplained moderate malnutrition','')),''),
            nullif(max(if(o.concept_id=167394 and o.value_coded =163282,'Unexplained persistent fever','')),''),
            nullif(max(if(o.concept_id=167394 and o.value_coded =5334,'Oral candidiasis','')),''),
            nullif(max(if(o.concept_id=167394 and o.value_coded =160515,'Severe recurrent bacterial pneumonia','')),''),
            nullif(max(if(o.concept_id=167394 and o.value_coded =135458,'Lymphoid interstitial pneumonitis (LIP)','')),''),
            nullif(max(if(o.concept_id=167394 and o.value_coded =163712,'HIV-related cardiomyopathy','')),''),
            nullif(max(if(o.concept_id=167394 and o.value_coded =162331,'Unexplained severe wasting or severe malnutrition','')),''),
            nullif(max(if(o.concept_id=167394 and o.value_coded =130021,'Pneumocystis pneumonia','')),''),
            nullif(max(if(o.concept_id=167394 and o.value_coded =146518,'Candida of trachea, bronchi or lungs','')),''),
            nullif(max(if(o.concept_id=167394 and o.value_coded =143744,'Acquired recto-vesicular fistula','')),'')) as who_stage_associated_oi,
max(if(o.concept_id=1154,o.value_coded,null)) as presenting_complaints,
null as clinical_notes, -- max(if(o.concept_id=160430,left(trim(o.value_text),600),null)) as clinical_notes ,
max(if(o.concept_id=164948,o.value_coded,null)) as on_anti_tb_drugs ,
max(if(o.concept_id=164949,o.value_coded,null)) as on_ipt ,
max(if(o.concept_id=164950,o.value_coded,null)) as ever_on_ipt ,
max(if(o.concept_id=1729 and o.value_coded =159799,o.value_coded,null)) as cough,
max(if(o.concept_id=1729 and o.value_coded =1494,o.value_coded,null)) as fever,
max(if(o.concept_id=1729 and o.value_coded =832,o.value_coded,null)) as weight_loss_poor_gain,
max(if(o.concept_id=1729 and o.value_coded =133027,o.value_coded,null)) as night_sweats,
max(if(o.concept_id=1729 and o.value_coded =124068,o.value_coded,null)) as tb_case_contact,
max(if(o.concept_id=1729 and o.value_coded =116334,o.value_coded,null)) as lethargy,
max(if(o.concept_id=1729 and o.value_coded in(159799,1494,832,133027,124068,116334,1066),'Yes','No'))as screened_for_tb,
max(if(o.concept_id=1271 and o.value_coded =307,o.value_coded,null)) as spatum_smear_ordered ,
max(if(o.concept_id=1271 and o.value_coded =12,o.value_coded,null)) as chest_xray_ordered ,
max(if(o.concept_id=1271 and o.value_coded = 162202,o.value_coded,null)) as genexpert_ordered ,
max(if(o.concept_id=307,o.value_coded,null)) as spatum_smear_result ,
max(if(o.concept_id=12,o.value_coded,null)) as chest_xray_result ,
max(if(o.concept_id=162202,o.value_coded,null)) as genexpert_result ,
max(if(o.concept_id=1272,o.value_coded,null)) as referral ,
max(if(o.concept_id=163752,o.value_coded,null)) as clinical_tb_diagnosis ,
max(if(o.concept_id=163414,o.value_coded,null)) as contact_invitation ,
max(if(o.concept_id=162275,o.value_coded,null)) as evaluated_for_ipt ,
max(if(o.concept_id=160557,o.value_coded,null)) as has_known_allergies ,
max(if(o.concept_id=162747,o.value_coded,null)) as has_chronic_illnesses_cormobidities ,
max(if(o.concept_id=121764,o.value_coded,null)) as has_adverse_drug_reaction ,
max(if(o.concept_id=5272,o.value_coded,null)) as pregnancy_status,
max(if(o.concept_id=5632,o.value_coded,null)) as breastfeeding,
max(if(o.concept_id=164933,o.value_coded,null)) as wants_pregnancy,
max(if(o.concept_id=161033,o.value_coded,null)) as pregnancy_outcome,
max(if(o.concept_id=163530,o.value_text,null)) as anc_number,
max(if(o.concept_id=5596,date(o.value_datetime),null)) as expected_delivery_date,
max(if(o.concept_id=162877,o.value_coded,null)) as ever_had_menses,
max(if(o.concept_id=1427,date(o.value_datetime),null)) as last_menstrual_period,
max(if(o.concept_id=160596,o.value_coded,null)) as menopausal,
max(if(o.concept_id=5624,o.value_numeric,null)) as gravida,
max(if(o.concept_id=1053,o.value_numeric,null)) as parity ,
max(if(o.concept_id=160080,o.value_numeric,null)) as full_term_pregnancies,
max(if(o.concept_id=1823,o.value_numeric,null)) as abortion_miscarriages ,
max(if(o.concept_id=160653,o.value_coded,null)) as family_planning_status,
max(if(o.concept_id=374,o.value_coded,null)) as family_planning_method,
max(if(o.concept_id=160575,o.value_coded,null)) as reason_not_using_family_planning ,
max(if(o.concept_id=1659,o.value_coded,null)) as tb_status,
max(if(o.concept_id=162309,o.value_coded,null)) as started_anti_TB,
max(if(o.concept_id=1113,o.value_datetime,null)) as tb_rx_date,
max(if(o.concept_id=161654,trim(o.value_text),null)) as tb_treatment_no,
concat_ws(',',nullif(max(if(o.concept_id=162737 and o.value_coded =1107 ,'None','')),''),
                 nullif(max(if(o.concept_id=162737 and o.value_coded =136443,'Jaundice','')),''),
                 nullif(max(if(o.concept_id=162737 and o.value_coded =460,'Oedema','')),''),
                 nullif(max(if(o.concept_id=162737 and o.value_coded = 5334,'Oral Thrush','')),''),
                 nullif(max(if(o.concept_id=162737 and o.value_coded = 5245,'Pallor','')),''),
                 nullif(max(if(o.concept_id=162737 and o.value_coded = 140125,'Finger Clubbing','')),''),
                 nullif(max(if(o.concept_id=162737 and o.value_coded = 126952,'Lymph Node Axillary','')),''),
                 nullif(max(if(o.concept_id=162737 and o.value_coded = 143050,'Cyanosis','')),''),
                 nullif(max(if(o.concept_id=162737 and o.value_coded = 126939,'Lymph Nodes Inguinal','')),''),
                 nullif(max(if(o.concept_id=162737 and o.value_coded = 823,'Wasting','')),''),
                 nullif(max(if(o.concept_id=162737 and o.value_coded = 142630,'Dehydration','')),''),
                 nullif(max(if(o.concept_id=162737 and o.value_coded = 116334,'Lethargic','')),'')) as general_examination,
max(if(o.concept_id=159615,o.value_coded,null)) as system_examination,
max(if(o.concept_id=1120,o.value_coded,null)) as skin_findings,
max(if(o.concept_id=163309,o.value_coded,null)) as eyes_findings,
max(if(o.concept_id=164936,o.value_coded,null)) as ent_findings,
max(if(o.concept_id=1123,o.value_coded,null)) as chest_findings,
max(if(o.concept_id=1124,o.value_coded,null)) as cvs_findings,
max(if(o.concept_id=1125,o.value_coded,null)) as abdomen_findings,
max(if(o.concept_id=164937,o.value_coded,null)) as cns_findings,
max(if(o.concept_id=1126,o.value_coded,null)) as genitourinary_findings,
max(if(o.concept_id=1109,o.value_coded,null)) as prophylaxis_given,
max(if(o.concept_id=161652,o.value_coded,null)) as ctx_adherence,
max(if(o.concept_id=162229 or (o.concept_id=1282 and o.value_coded = 105281),o.value_coded,null)) as ctx_dispensed,
max(if(o.concept_id=164941,o.value_coded,null)) as dapsone_adherence,
max(if(o.concept_id=164940 or (o.concept_id=1282 and o.value_coded = 74250),o.value_coded,null)) as dapsone_dispensed,
max(if(o.concept_id=162230,o.value_coded,null)) as inh_dispensed,
max(if(o.concept_id=1658,o.value_coded,null)) as arv_adherence,
max(if(o.concept_id=160582,o.value_coded,null)) as poor_arv_adherence_reason,
null as poor_arv_adherence_reason_other, -- max(if(o.concept_id=160632,trim(o.value_text),null)) as poor_arv_adherence_reason_other,
max(if(o.concept_id=159423,o.value_coded,null)) as pwp_disclosure,
max(if(o.concept_id=5616,o.value_coded,null)) as pwp_pead_disclosure,
max(if(o.concept_id=161557,o.value_coded,null)) as pwp_partner_tested,
max(if(o.concept_id=159777,o.value_coded,null)) as condom_provided ,
max(if(o.concept_id=112603,o.value_coded,null)) as substance_abuse_screening ,
max(if(o.concept_id=161558,o.value_coded,null)) as screened_for_sti,
max(if(o.concept_id=164934,o.value_coded,null)) as cacx_screening,
max(if(o.concept_id=164935,o.value_coded,null)) as sti_partner_notification,
max(if(o.concept_id=160581,o.value_coded,null)) as at_risk_population,
max(if(o.concept_id=159615,o.value_coded,null)) as system_review_finding,
max(if(o.concept_id=166607,o.value_coded,null)) as appointment_consent,
max(if(o.concept_id=160288,o.value_coded,null)) as next_appointment_reason,
max(if(o.concept_id=1855,o.value_coded,null)) as stability,
max(if(o.concept_id=164947,o.value_coded,null)) as differentiated_care,
max(if(o.concept_id=159356,o.value_coded,null)) as insurance_type,
max(if(o.concept_id=161011,o.value_text,null)) as other_insurance_specify,
max(if(o.concept_id=165911,o.value_coded,null)) as insurance_status,
e.voided as voided
from amrs.encounter e
	inner join amrs.person p on p.person_id=e.patient_id and p.voided=0
inner join amrs.form f on f.form_id = e.form_id and f.uuid in ('22c68f86-bbf0-49ba-b2d1-23fa7ccf0259','23b4ebbd-29ad-455e-be0e-04aa6bc30798','465a92f2-baf8-42e9-9612-53064be868e8')
left outer join amrs.obs o on o.encounter_id=e.encounter_id and o.voided=0
	and o.concept_id in (1282,1246,161643,5089,5085,5086,5090,5088,5087,5242,5092,1343,162584,163515,5356,167394,5272,5632, 161033,163530,5596,1427,5624,1053,160653,374,160575,1659,161654,161652,162229,162230,1658,160582,160632,159423,5616,161557,159777,112603,161558,160581,5096,163300, 164930, 160581, 1154, 160430,162877, 164948, 164949, 164950, 1271, 307, 12, 162202, 1272, 163752, 163414, 162275, 160557, 162747,
121764, 164933, 160080, 1823, 164940, 164934, 164935, 159615, 160288, 1855, 164947,162549,162877,160596,1109,1113,162309,1729,162737,159615,1120,163309,164936,1123,1124,1125,164937,1126,166607,159356,161011,165911)
where e.voided=0
group by e.patient_id,visit_date;
SELECT "Completed processing HIV Followup data ", CONCAT("Time: ", NOW());
END $$


-- ------------- populate etl_laboratory_extract  uuid:  --------------------------------

DROP PROCEDURE IF EXISTS sp_populate_etl_laboratory_extract $$
CREATE PROCEDURE sp_populate_etl_laboratory_extract()
BEGIN
SELECT "Processing Laboratory data ", CONCAT("Time: ", NOW());
insert into amrsemr_etl.etl_laboratory_extract(
uuid,
encounter_id,
patient_id,
location_id,
visit_date,
visit_id,
order_id,
lab_test,
urgency,
order_reason,
test_result,
date_test_requested,
date_test_result_received,
date_created,
date_last_modified,
created_by
)
select
	e.uuid,
	e.encounter_id,
	e.patient_id,
	e.location_id,
	coalesce(od.date_activated,o.obs_datetime) as visit_date,
	e.visit_id,
	od.order_id,
	od.concept_id,
	od.urgency,
	od.order_reason,
	(CASE when o.concept_id in(5497,730,654,790,856,21,653,5475,887,1015,849,678,676,855,1000443,161469,166395,160912,159644,163594,1006,1007,1009,1008,161153,161481,161482,
							   166018,785,655,717,848,163699,160913,1011,159655,159654,161500,163595,163596,1336,1338,1017,1018,851,729,679,1016,163426,160914) then o.value_numeric
		  when o.concept_id in(1030,1305,1325,159430,161472,1029,1031,1619,1032,162202,307,45,167718,
							   163722,167452,167459,1643,32,1366,1042,299,300,305,306,1618,1875,161470,885,165562,161478,160225,160232,1356,161233,167810,159362,163654,
							   168114,163348,162202) then o.value_coded
		  when o.concept_id in (302,1367,56,1000071,163613,163603,163603,1000451,165552,165402,161156,161155,159648,159649,161467) then o.value_text
		END) AS test_result,
	od.date_activated as date_test_requested,
	e.encounter_datetime as date_test_result_received,
-- test requested by
	e.date_created,
	if(max(o.date_created) > min(e.date_created),max(o.date_created),NULL) as date_last_modified,
	e.creator
from amrs.encounter e
		 inner join amrs.person p on p.person_id=e.patient_id and p.voided=0
		 left join
	 (
		 select encounter_type_id, uuid, name from amrs.encounter_type where uuid in('17a381d1-7e29-406a-b782-aa903b963c28',
																				'a0034eee-1940-4e35-847f-97537a35d05e',
																				'e1406e88-e9a9-11e8-9f32-f2801f1b9fd1',
																				'de78a6be-bfc5-4634-adc3-5f1a280455cc',
																				'bcc6da85-72f2-4291-b206-789b8186a021',
																				'7df67b83-1b84-4fe2-b1b7-794b4e9bfcc3')
	 ) et on et.encounter_type_id=e.encounter_type
		 left join amrs.obs o on e.encounter_id=o.encounter_id and o.voided=0 and o.concept_id in (5497,730,654,790,856,1030,1305,1325,159430,161472,1029,1031,1619,1032,162202,307,45,167718,163722,167452,167459,1643,32,1366,1000612,1019,21,
																							  657,1042,653,5473,5475,299,887,302,1015,300,1367,305,306,1618,1875,849,678,676,1336,855,161470,1000443,
																							  885,56,165562,161469,161478,160225,1000071,166395,160912,159644,163594,1006,1007,1009,1008,161153,161481,161482,166018,159829,785,655,
																							  717,848,163699,1000069,160232,1356,161233,163613,163602,160913,167810,161532,1011,159655,159654,161500,168167,159362,163654,168114,163603,163348,
																							  1000451,165552,165402,161156,161155,159648,159649,161467,163595,163596,1338,1017,1018,851,729,679,1016,163426,160914)
		 left join amrs.orders od on od.order_id = o.order_id and od.order_type_id = 3 and od.voided=0
where e.voided=0
group by o.encounter_id;

/*-- >>>>>>>>>>>>>>> -----------------------------------  Wagners input ------------------------------------------------------------
insert into amrsemr_etl.etl_laboratory_extract(
encounter_id,
patient_id,
visit_date,
visit_id,
lab_test,
test_result,
-- date_test_requested,
-- date_test_result_received,
-- test_requested_by,
date_created,
created_by
)
select
e.encounter_id,
e.patient_id,
e.encounter_datetime as visit_date,
e.visit_id,
o.concept_id,
(CASE when o.concept_id in(5497,730,654,790,856,21) then o.value_numeric
when o.concept_id in(299,1030,302,32) then o.value_coded
END) AS test_result,
-- date requested,
-- date result received
-- test requested by
e.date_created,
e.creator
from amrs.encounter e, obs o, encounter_type et
where e.encounter_id=o.encounter_id and o.voided=0
and o.concept_id in (5497,730,299,654,790,856,1030,21,302,32) and et.encounter_type_id=e.encounter_type
group by e.encounter_id;

-- --------<<<<<<<<<<<<<<<<<<<< ------------------------------------------------------------------------------------------------------
*/
SELECT "Completed processing Laboratory data ", CONCAT("Time: ", NOW());
END $$


-- ------------- populate etl_pharmacy_extract table--------------------------------

DROP PROCEDURE IF EXISTS sp_populate_etl_pharmacy_extract $$
CREATE PROCEDURE sp_populate_etl_pharmacy_extract()
BEGIN
SELECT "Processing Pharmacy data ", CONCAT("Time: ", NOW());
insert into amrsemr_etl.etl_pharmacy_extract(
obs_group_id,
patient_id,
uuid,
visit_date,
visit_id,
encounter_id,
date_created,
date_last_modified,
encounter_name,
location_id,
drug,
drug_name,
is_arv,
is_ctx,
is_dapsone,
frequency,
duration,
duration_units,
voided,
date_voided,
dispensing_provider
)
select
	o.obs_group_id obs_group_id,
	o.person_id,
	max(if(o.concept_id=1282, o.uuid, null)),
	date(o.obs_datetime) as enc_date,
	e.visit_id,
	o.encounter_id,
	e.date_created,
	if(max(o.date_created) > min(e.date_created),max(o.date_created),NULL) as date_last_modified,
	et.name as enc_name,
	e.location_id,
	max(if(o.concept_id = 1282 and o.value_coded is not null,o.value_coded, null)) as drug_dispensed,
	max(if(o.concept_id = 1282, left(cn.name,255), 0)) as drug_name, -- arv:1085
	max(if(o.concept_id = 1282 and cs.concept_set=1085, 1, 0)) as arv_drug, -- arv:1085
	max(if(o.concept_id = 1282 and o.value_coded = 105281,1, 0)) as is_ctx,
	max(if(o.concept_id = 1282 and o.value_coded = 74250,1, 0)) as is_dapsone,
	max(if(o.concept_id = 1443, o.value_numeric, null)) as dose,
	max(if(o.concept_id = 159368, if(o.value_numeric > 10000, 10000, o.value_numeric), null)) as duration, -- catching typos in duration field
	max(if(o.concept_id = 1732 and o.value_coded=1072,'Days',if(o.concept_id=1732 and o.value_coded=1073,'Weeks',if(o.concept_id=1732 and o.value_coded=1074,'Months',null)))) as duration_units,
	o.voided,
	o.date_voided,
	e.creator
from amrs.obs o
	inner join amrs.person p on p.person_id=o.person_id and p.voided=0
	left outer join amrs.encounter e on e.encounter_id = o.encounter_id and e.voided=0
left outer join amrs.encounter_type et on et.encounter_type_id = e.encounter_type
left outer join amrs.concept_name cn on o.value_coded = cn.concept_id and cn.locale='en' and cn.concept_name_type='FULLY_SPECIFIED' -- SHORT'
left outer join amrs.concept_set cs on o.value_coded = cs.concept_id
where o.voided=0 and o.concept_id in(1282,1732,159368,1443,1444)  and e.voided=0
group by o.obs_group_id, o.person_id, encounter_id
having drug_dispensed is not null and obs_group_id is not null;

update amrsemr_etl.etl_pharmacy_extract
	set duration_in_days = if(duration_units= 'Days', duration,if(duration_units='Weeks',duration * 7,if(duration_units='Months',duration * 31,null)))
	where (duration is not null or duration <> "") and (duration_units is not null or duration_units <> "");

SELECT "Completed processing Pharmacy data ", CONCAT("Time: ", NOW());
END $$


-- ------------------------------------ populate hts test table ----------------------------------------


DROP PROCEDURE IF EXISTS sp_populate_hts_test $$
CREATE PROCEDURE sp_populate_hts_test()
BEGIN
SELECT "Processing hts tests";
INSERT INTO amrsemr_etl.etl_hts_test (
patient_id,
visit_id,
encounter_id,
encounter_uuid,
encounter_location,
creator,
date_created,
date_last_modified,
visit_date,
test_type,
population_type,
key_population_type,
priority_population_type,
ever_tested_for_hiv,
months_since_last_test,
patient_disabled,
disability_type,
patient_consented,
client_tested_as,
setting,
approach,
test_strategy,
hts_entry_point,
hts_risk_category,
hts_risk_score,
test_1_kit_name,
test_1_kit_lot_no,
test_1_kit_expiry,
test_1_result,
test_2_kit_name,
test_2_kit_lot_no,
test_2_kit_expiry,
test_2_result,
test_3_kit_name,
test_3_kit_lot_no,
test_3_kit_expiry,
test_3_result,
final_test_result,
syphillis_test_result,
patient_given_result,
couple_discordant,
referred,
referral_for,
referral_facility,
other_referral_facility,
neg_referral_for,
neg_referral_specify,
tb_screening,
patient_had_hiv_self_test ,
remarks,
voided
)
select
e.patient_id,
e.visit_id,
e.encounter_id,
e.uuid,
e.location_id,
e.creator,
e.date_created,
if(max(o.date_created) > min(e.date_created),max(o.date_created),NULL) as date_last_modified,
e.encounter_datetime as visit_date,
max(if((o.concept_id=162084 and o.value_coded=162082 and f.uuid = "402dc5d7-46da-42d4-b2be-f43ea4ad87b0") or (f.uuid = "b08471f6-0892-4bf7-ab2b-bf79797b8ea4"), 2, 1)) as test_type , -- 2 for confirmation, 1 for initial
max(if(o.concept_id=164930,(case o.value_coded when 164928 then "General Population" when 164929 then "Key Population" when 138643 then "Priority Population" else "" end),null)) as population_type,
max(if((o.concept_id=160581 or o.concept_id=165241) and o.value_coded in (105,160666,160578,165084,160579,165100,162277,167691,1142,163488,159674,162198,6096,5622), (case o.value_coded when 105 then 'People who inject drugs' 
	                                                                                                                                                    when 160666 then 'People who use drugs' 
	                                                                                                                                                    when 160578 then 'Men who have sex with men' 
	                                                                                                                                                    when 165084 then 'Male Sex Worker' 
	                                                                                                                                                    when 160579 then 'Female sex worker' 
	                                                                                                                                                    when 165100 then 'Transgender' 
	                                                                                                                                                    when 162277 then 'People in prison and other closed settings' 
	                                                                                                                                                    when 167691 then 'Inmates'  
	                                                                                                                                                    when 1142 then 'Prison Staff' 
	                                                                                                                                                    when 163488 then 'Prison Community'
																																						when 159674 then 'Fisher folk'
																																						when 162198 then 'Truck driver'
																																						when 6096 then 'Discordant'
	                                                                                                                                                    when 5622 then 'Other'  else null end),null)) as key_population_type,
max(if(o.concept_id=160581 and o.value_coded in(159674,162198,160549,162277,1175,165192), (case o.value_coded when 159674 then "Fisher folk" when 162198 then "Truck driver" when 160549 then "Adolescent and young girls" when 162277 then "Prisoner" when 1175 then "Not applicable" when 165192 then "Military and other uniformed services" else null end),null)) as priority_population_type,
max(if(o.concept_id=164401,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end),null)) as ever_tested_for_hiv,
max(if(o.concept_id=159813,o.value_numeric,null)) as months_since_last_test,
max(if(o.concept_id=164951,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end),null)) as patient_disabled,
concat_ws(',',nullif(max(if(o.concept_id=162558 and o.value_coded = 120291,"Hearing impairment",'')),''),
                 nullif(max(if(o.concept_id=162558 and o.value_coded =147215,"Visual impairment",'')),''),
                 nullif(max(if(o.concept_id=162558 and o.value_coded =151342,"Mentally Challenged",'')),''),
                 nullif(max(if(o.concept_id=162558 and o.value_coded = 164538,"Physically Challenged",'')),''),
                 nullif(max(if(o.concept_id=162558 and o.value_coded = 5622,"Other",'')),''),
                 nullif(max(if(o.concept_id=160632,o.value_text,'')),'')) as disability_type,
max(if(o.concept_id=1710,(case o.value_coded when 1 then "Yes" when 0 then "No" else "" end),null)) as patient_consented,
max(if(o.concept_id=164959,(case o.value_coded when 164957 then "Individual" when 164958 then "Couple" else "" end),null)) as client_tested_as,
max(if(o.concept_id=165215,(case o.value_coded when 1537 then "Facility" when 163488 then "Community" else "" end ),null)) as setting,
max(if(o.concept_id=163556,(case o.value_coded when 164163 then "Provider Initiated Testing(PITC)" when 164953 then "Client Initiated Testing (CITC)" else "" end ),null)) as approach,
max(if(o.concept_id=164956,o.value_coded,null)) as test_strategy,
max(if(o.concept_id=160540,o.value_coded,null)) as hts_entry_point,
max(if(o.concept_id=167163,(case o.value_coded when 1407 then "Low" when 1499 then "Moderate" when 1408 then "High" when 167164 then "Very high" else "" end),null)) as hts_risk_category,
max(if(o.concept_id=167162,o.value_numeric,null)) as hts_risk_score,
max(if(t.test_1_result is not null, t.kit_name, null)) as test_1_kit_name,
max(if(t.test_1_result is not null, t.lot_no, null)) as test_1_kit_lot_no,
max(if(t.test_1_result is not null, t.expiry_date, null)) as test_1_kit_expiry,
max(if(t.test_1_result is not null, t.test_1_result, null)) as test_1_result,
max(if(t.test_2_result is not null, t.kit_name, null)) as test_2_kit_name,
max(if(t.test_2_result is not null, t.lot_no, null)) as test_2_kit_lot_no,
max(if(t.test_2_result is not null, t.expiry_date, null)) as test_2_kit_expiry,
max(if(t.test_2_result is not null, t.test_2_result, null)) as test_2_result,
max(if(t.test_3_result is not null, t.kit_name, null)) as test_3_kit_name,
max(if(t.test_3_result is not null, t.lot_no, null)) as test_3_kit_lot_no,
max(if(t.test_3_result is not null, t.expiry_date, null)) as test_3_kit_expiry,
max(if(t.test_3_result is not null, t.test_3_result, null)) as test_3_result,
max(if(o.concept_id=159427,(case o.value_coded when 703 then "Positive" when 664 then "Negative" when 1138 then "Inconclusive" when 163611 then "Invalid" else "" end),null)) as final_test_result,
max(if(o.concept_id=299,(case o.value_coded when 1229 then "Positive" when 1228 then "Negative" else "" end),null)) as syphillis_test_result,
max(if(o.concept_id=164848,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end),null)) as patient_given_result,
max(if(o.concept_id=6096,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end),null)) as couple_discordant,
max(if(o.concept_id=165093,o.value_coded,null)) as referred,
max(if(o.concept_id=1887,(case o.value_coded when 162082 then "Confirmatory test" when 162050 then "Comprehensive care center" when 164461 then "DBS for PCR" else "" end),null)) as referral_for,
max(if(o.concept_id=160481,(case o.value_coded when 163266 then "This health facility" when 164407 then "Other health facility" else "" end),null)) as referral_facility,
max(if(o.concept_id=161550,trim(o.value_text),null)) as other_referral_facility,
concat_ws(',', max(if(o.concept_id = 1272 and o.value_coded = 165276, 'Risk reduction counselling', null)),
            max(if(o.concept_id = 1272 and o.value_coded = 159612, 'Safer sex practices', null)),
            max(if(o.concept_id = 1272 and o.value_coded = 162223, 'VMMC', null)),
            max(if(o.concept_id = 1272 and o.value_coded = 190, 'Condom use counselling', null)),
            max(if(o.concept_id = 1272 and o.value_coded = 1691, 'Post-exposure prophylaxis', null)),
            max(if(o.concept_id = 1272 and o.value_coded = 167125, 'Prevention and treatment of STIs', null)),
            max(if(o.concept_id = 1272 and o.value_coded = 118855, 'Substance abuse and mental health treatment', null)),
            max(if(o.concept_id = 1272 and o.value_coded = 141814, 'Prevention of GBV', null)),
            max(if(o.concept_id = 1272 and o.value_coded = 1370, 'HIV testing and re-testing', null)),
            max(if(o.concept_id = 1272 and o.value_coded = 166536, 'Pre-Exposure Prophylaxis', null)),
            max(if(o.concept_id = 1272 and o.value_coded = 5622, 'Other', null))) as neg_referral_for,
max(if(o.concept_id=164359,trim(o.value_text),null)) as neg_referral_specify,
max(if(o.concept_id=1659,(case o.value_coded when 1660 then "No TB signs" when 142177 then "Presumed TB" when 1662 then "TB Confirmed" when 160737 then "Not done" when 1111 then "On TB Treatment"  else "" end),null)) as tb_screening,
max(if(o.concept_id=164952,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end),null)) as patient_had_hiv_self_test,
max(if(o.concept_id=163042,trim(o.value_text),null)) as remarks,
e.voided
from amrs.encounter e
	inner join amrs.person p on p.person_id=e.patient_id and p.voided=0
	inner join amrs.form f on f.form_id=e.form_id and f.uuid in ("402dc5d7-46da-42d4-b2be-f43ea4ad87b0","b08471f6-0892-4bf7-ab2b-bf79797b8ea4")
inner join amrs.obs o on o.encounter_id = e.encounter_id and o.concept_id in (162084, 164930, 160581, 164401, 164951, 162558,160632, 1710, 164959, 164956,165241,
                                                                                 160540,159427, 164848, 6096, 1659, 164952, 163042, 159813,165215,163556,161550,1887,1272,164359,160481,229,167163,167162,165093,165241)
inner join (
             select
               o.person_id,
               o.encounter_id,
               o.obs_group_id,
               max(if(o.concept_id=1040, (case o.value_coded when 703 then "Positive" when 664 then "Negative" when 163611 then "Invalid"  else "" end),null)) as test_1_result ,
               max(if(o.concept_id=1326, (case o.value_coded when 703 then "Positive" when 664 then "Negative" when 1175 then "N/A"  else "" end),null)) as test_2_result ,
			         max(if(o.concept_id=1000630, (case o.value_coded when 703 then "Positive" when 664 then "Negative" when 1175 then "N/A"  else "" end),null)) as test_3_result ,
               max(if(o.concept_id=164962, (case o.value_coded when 164960 then "Determine" when 164961 then "First Response" when 165351 then "Dual Kit" when 169126 then "One step" when 169127 then "Trinscreen" else "" end),null)) as kit_name ,
               max(if(o.concept_id=164964,trim(o.value_text),null)) as lot_no,
               max(if(o.concept_id=162502,date(o.value_datetime),null)) as expiry_date
             from amrs.obs o
             inner join amrs.encounter e on e.encounter_id = o.encounter_id
             inner join amrs.form f on f.form_id=e.form_id and f.uuid in ("402dc5d7-46da-42d4-b2be-f43ea4ad87b0","b08471f6-0892-4bf7-ab2b-bf79797b8ea4")
             where o.concept_id in (1040, 1326, 1000630, 164962, 164964, 162502) and o.voided=0
             group by e.encounter_id, o.obs_group_id
           ) t on e.encounter_id = t.encounter_id
where e.voided=0
group by e.encounter_id;
SELECT "Completed processing hts tests";
END $$


-- ------------------------------------ POPULATE HTS LINKAGES AND REFERRALS -------------------------------

DROP PROCEDURE IF EXISTS sp_populate_hts_linkage_and_referral $$
CREATE PROCEDURE sp_populate_hts_linkage_and_referral()
BEGIN
SELECT "Processing hts linkages, referrals and tracing";
INSERT INTO amrsemr_etl.etl_hts_referral_and_linkage (
  patient_id,
  visit_id,
  encounter_id,
  encounter_uuid,
  encounter_location,
  creator,
  date_created,
  date_last_modified,
  visit_date,
  tracing_type,
  tracing_status,
  referral_facility,
  facility_linked_to,
	enrollment_date,
	art_start_date,
  ccc_number,
  provider_handed_to,
  cadre,
  remarks,
  voided
)
  select
    e.patient_id,
    e.visit_id,
    e.encounter_id,
    e.uuid,
    e.location_id,
    e.creator,
    e.date_created,
    if(max(o.date_created) > min(e.date_created),max(o.date_created),NULL) as date_last_modified,
    e.encounter_datetime as visit_date,
    max(if(o.concept_id=164966,(case o.value_coded when 1650 then "Phone" when 164965 then "Physical" else "" end),null)) as tracing_type ,
    max(if(o.concept_id=159811,(case o.value_coded when 1065 then "Contacted and linked" when 1066 then "Contacted but not linked" else "" end),null)) as tracing_status,
    max(if(o.concept_id=160481,(case o.value_coded when 163266 then "This health facility" when 164407 then "Other health facility" else "" end),null)) as referral_facility,
    max(if(o.concept_id=162724,trim(o.value_text),null)) as facility_linked_to,
		max(if(o.concept_id=160555,o.value_datetime,null)) as enrollment_date,
		max(if(o.concept_id=159599,o.value_datetime,null)) as art_start_date,
    max(if(o.concept_id=162053,o.value_numeric,null)) as ccc_number,
    max(if(o.concept_id=1473,trim(o.value_text),null)) as provider_handed_to,
    max(if(o.concept_id=162577,(case o.value_coded when 1577 then "Nurse"
                                when 1574 then "Clinical Officer/Doctor"
                                when 1555 then "Community Health Worker"
                                when 1540 then "Employee"
                                when 5488 then "Adherence counsellor"
                                when 5622 then "Other" else "" end),null)) as cadre,
	max(if(o.concept_id=163042,trim(o.value_text),null)) as remarks,
    e.voided
  from amrs.encounter e
		inner join amrs.person p on p.person_id=e.patient_id and p.voided=0
		inner join amrs.form f on f.form_id = e.form_id and f.uuid in ("050a7f12-5c52-4cad-8834-863695af335d","15ed03d2-c972-11e9-a32f-2a2ae2dbcce4")
  left outer join amrs.obs o on o.encounter_id = e.encounter_id and o.concept_id in (164966, 159811, 162724, 160555, 159599, 162053, 1473,162577,160481,163042) and o.voided=0
  where e.voided=0
  group by e.patient_id,e.visit_id;
  SELECT "Completed processing hts linkages";

END $$


-- ------------------------------------ update hts referral table ---------------------------------

DROP PROCEDURE IF EXISTS sp_populate_hts_referral $$
CREATE PROCEDURE sp_populate_hts_referral()
  BEGIN
    SELECT "Processing hts referrals";
    INSERT INTO amrsemr_etl.etl_hts_referral (
      patient_id,
      visit_id,
      encounter_id,
      encounter_uuid,
      encounter_location,
      creator,
      date_created,
      date_last_modified,
      visit_date,
      facility_referred_to,
      date_to_enrol,
      remarks,
      voided
    )
      select
        e.patient_id,
        e.visit_id,
        e.encounter_id,
        e.uuid,
        e.location_id,
        e.creator,
        e.date_created,
        if(max(o.date_created) > min(e.date_created),max(o.date_created),NULL) as date_last_modified,
        e.encounter_datetime as visit_date,
        max(if(o.concept_id=161550,o.value_text,null)) as facility_referred_to ,
        max(if(o.concept_id=161561,o.value_datetime,null)) as date_to_be_enrolled,
        max(if(o.concept_id=163042,o.value_text,null)) as remarks,
        e.voided
      from amrs.encounter e
				inner join amrs.person p on p.person_id=e.patient_id and p.voided=0
				inner join amrs.form f on f.form_id = e.form_id and f.uuid = "9284828e-ce55-11e9-a32f-2a2ae2dbcce4"
        left outer join amrs.obs o on o.encounter_id = e.encounter_id and o.concept_id in (161550, 161561, 163042) and o.voided=0
        where e.voided=0
      group by e.encounter_id;
    SELECT "Completed processing hts referrals";

    END $$


-- ------------- populate etl_patient_triage--------------------------------

DROP PROCEDURE IF EXISTS sp_populate_etl_patient_triage $$
CREATE PROCEDURE sp_populate_etl_patient_triage()
	BEGIN
		SELECT "Processing Patient Triage ", CONCAT("Time: ", NOW());
		INSERT INTO amrsemr_etl.etl_patient_triage(
			uuid,
			patient_id,
			visit_id,
			visit_date,
			location_id,
			encounter_id,
			encounter_provider,
			date_created,
			visit_reason,
            complaint_today,
            complaint_duration,
			weight,
			height,
			systolic_pressure,
			diastolic_pressure,
			temperature,
			pulse_rate,
			respiratory_rate,
			oxygen_saturation,
			muac,
            z_score_absolute,
            z_score,
			nutritional_status,
			nutritional_intervention,
			last_menstrual_period,
            hpv_vaccinated,
            date_last_modified,
			voided
		)
			select
				e.uuid,
				e.patient_id,
				e.visit_id,
				date(e.encounter_datetime) as visit_date,
				e.location_id,
				e.encounter_id as encounter_id,
				e.creator,
				e.date_created as date_created,
				max(if(o.concept_id=160430,trim(o.value_text),null)) as visit_reason,
                max(if(o.concept_id=1154,(case o.value_coded when 1 then "Yes" when 0 then "No" else "" end), "" )) as complaint_today,
                max(if(o.concept_id=159368,o.value_numeric,null)) as complaint_duration,
				max(if(o.concept_id=5089,o.value_numeric,null)) as weight,
				max(if(o.concept_id=5090,o.value_numeric,null)) as height,
				max(if(o.concept_id=5085,o.value_numeric,null)) as systolic_pressure,
				max(if(o.concept_id=5086,o.value_numeric,null)) as diastolic_pressure,
				max(if(o.concept_id=5088,o.value_numeric,null)) as temperature,
				max(if(o.concept_id=5087,o.value_numeric,null)) as pulse_rate,
				max(if(o.concept_id=5242,o.value_numeric,null)) as respiratory_rate,
				max(if(o.concept_id=5092,o.value_numeric,null)) as oxygen_saturation,
				max(if(o.concept_id=1343,o.value_numeric,null)) as muac,
				max(if(o.concept_id=162584,o.value_numeric,null)) as z_score_absolute,
                max(if(o.concept_id=163515,o.value_coded,null)) as z_score,
				max(if(o.concept_id=163515 or o.concept_id=167392,o.value_coded,null)) as nutritional_status,
				max(if(o.concept_id=163304,o.value_coded,null)) as nutritional_intervention,
				max(if(o.concept_id=1427,date(o.value_datetime),null)) as last_menstrual_period,
                max(if(o.concept_id=160325,o.value_coded,null)) as hpv_vaccinated,
				if(max(o.date_created) > min(e.date_created),max(o.date_created),NULL) as date_last_modified,
				e.voided as voided
			from amrs.encounter e
				inner join amrs.person p on p.person_id=e.patient_id and p.voided=0
				inner join
				(
					select encounter_type_id, uuid, name from amrs.encounter_type where uuid in('d1059fb9-a079-4feb-a749-eedd709ae542','a0034eee-1940-4e35-847f-97537a35d05e','465a92f2-baf8-42e9-9612-53064be868e8')
				) et on et.encounter_type_id=e.encounter_type
				left outer join amrs.obs o on o.encounter_id=e.encounter_id and o.voided=0
				and o.concept_id in (160430,1154,159368,5089,5090,5085,5086,5088,5087,5242,5092,1343,163515,167392,1427,160325,162584,163304)
			where e.voided=0
			group by e.patient_id, visit_date
		;
		SELECT "Completed processing Patient Triage data ", CONCAT("Time: ", NOW());
		END $$


-- Populate Allergy and chronic illness----
DROP PROCEDURE IF EXISTS sp_populate_etl_allergy_chronic_illness $$
CREATE PROCEDURE sp_populate_etl_allergy_chronic_illness()
BEGIN
SELECT "Processing alergy and chronic illness", CONCAT("Time: ", NOW());
insert into amrsemr_etl.etl_allergy_chronic_illness(
uuid,
provider,
patient_id,
visit_id,
visit_date,
location_id,
encounter_id,
obs_id,
chronic_illness,
chronic_illness_onset_date,
is_chronic_illness_controlled,
allergy_causative_agent,
allergy_reaction,
allergy_severity,
allergy_onset_date,
complaint,
complaint_date,
complaint_duration,
date_created,
date_last_modified,
voided
)
select
   e.uuid,e.creator,e.patient_id,e.visit_id, date(e.encounter_datetime) as visit_date, e.location_id,e.encounter_id,o1.obs_id,
   max(if(o1.obs_group =159392 and o1.concept_id = 1284,o1.value_coded,null)) as chronic_illness,
   max(if(o1.obs_group =159392 and o1.concept_id = 159948,date(o1.value_datetime),null)) as chronic_illness_onset_date,
   max(if(o1.obs_group =159392 and o1.concept_id = 166937,o1.value_coded,null)) as is_chronic_illness_controlled,
   max(if(o1.obs_group =121689 and o1.concept_id = 160643,o1.value_coded,null)) as allergy_causative_agent,
   max(if(o1.obs_group =121689 and o1.concept_id = 159935,o1.value_coded,null)) as allergy_reaction,
   max(if(o1.obs_group =121689 and o1.concept_id = 162760,o1.value_coded,null)) as allergy_severity,
   max(if(o1.obs_group =121689 and o1.concept_id = 160753,date(o1.value_datetime),null)) as allergy_onset_date,
   max(if(o1.obs_group =160531 and o1.concept_id = 5219,o1.value_coded,null)) as complaint,
   max(if(o1.obs_group =160531 and o1.concept_id = 159948,date(o1.value_datetime),null)) as complaint_date,
   max(if(o1.obs_group =160531 and o1.concept_id = 159368,o1.value_numeric,null)) as complaint_duration,
   e.date_created as date_created,  if(max(o1.date_created) > min(e.date_created),max(o1.date_created),NULL) as date_last_modified,
   e.voided as voided
from amrs.encounter e
   inner join amrs.person p on p.person_id=e.patient_id and p.voided=0
   inner join (
              select encounter_type_id, uuid, name from amrs.encounter_type where uuid in('a0034eee-1940-4e35-847f-97537a35d05e','c6d09e05-1f25-4164-8860-9f32c5a02df0','c4a2be28-6673-4c36-b886-ea89b0a42116','706a8b12-c4ce-40e4-aec3-258b989bf6d3','a2010bf5-2db0-4bf4-819f-8a3cffbcb21b','d1059fb9-a079-4feb-a749-eedd709ae542','465a92f2-baf8-42e9-9612-53064be868e8')
              ) et on et.encounter_type_id=e.encounter_type
   inner join (select o.person_id,o1.encounter_id, o.obs_id,o.concept_id as obs_group,o1.concept_id as concept_id,o1.value_coded, o1.value_datetime,o1.value_numeric,
                      o1.date_created,o1.voided from amrs.obs o join obs o1 on o.obs_id = o1.obs_group_id
                       and o1.concept_id in (1284,159948,160643,159935,162760,160753,166937,5219,159948,159368) and o1.voided=0
                       and o.concept_id in(159392,121689,160531)) o1 on o1.encounter_id = e.encounter_id
where e.voided=0
group by o1.obs_id;

SELECT "Completed processing allergy and chronic illness data ", CONCAT("Time: ", NOW());
END $$



-- ------------------------------------------- running all procedures -----------------------------

DROP PROCEDURE IF EXISTS sp_first_time_setup $$
CREATE PROCEDURE sp_first_time_setup()
BEGIN
DECLARE populate_script_id INT(11);
SELECT "Beginning first time setup", CONCAT("Time: ", NOW());
INSERT INTO amrsemr_etl.etl_script_status(script_name, start_time) VALUES('initial_population_of_tables', NOW());
SET populate_script_id = LAST_INSERT_ID();

CALL sp_populate_etl_patient_demographics();
CALL sp_populate_etl_hiv_enrollment();
CALL sp_populate_etl_hiv_followup();
CALL sp_populate_etl_laboratory_extract();
CALL sp_populate_etl_pharmacy_extract();
CALL sp_populate_etl_program_discontinuation();
CALL sp_populate_etl_mch_enrollment();
CALL sp_populate_etl_mch_antenatal_visit();
CALL sp_populate_etl_mch_postnatal_visit();
CALL sp_populate_etl_tb_enrollment();
CALL sp_populate_etl_tb_follow_up_visit();
CALL sp_populate_etl_tb_screening();
CALL sp_populate_etl_hei_enrolment();
CALL sp_populate_etl_hei_immunization();
CALL sp_populate_etl_hei_follow_up();
CALL sp_populate_etl_mch_delivery();
CALL sp_populate_etl_patient_appointment();
CALL sp_populate_etl_mch_discharge();
CALL sp_drug_event();
CALL sp_populate_hts_test();
CALL sp_populate_etl_generalized_anxiety_disorder();
CALL sp_populate_hts_linkage_and_referral();
CALL sp_populate_hts_referral();
CALL sp_populate_etl_ccc_defaulter_tracing();
CALL sp_populate_etl_ART_preparation();
CALL sp_populate_etl_enhanced_adherence();
CALL sp_populate_etl_patient_triage();
CALL sp_populate_etl_ipt_initiation();
CALL sp_populate_etl_ipt_follow_up();
CALL sp_populate_etl_ipt_outcome();
CALL sp_populate_etl_prep_enrolment();
CALL sp_populate_etl_prep_followup();
CALL sp_populate_etl_prep_behaviour_risk_assessment();
CALL sp_populate_etl_prep_monthly_refill();
CALL sp_populate_etl_progress_note();
CALL sp_populate_etl_prep_discontinuation();
CALL sp_populate_etl_hts_linkage_tracing();
CALL sp_populate_etl_patient_program();
CALL sp_create_default_facility_table();
CALL sp_populate_etl_person_address();
CALL sp_populate_etl_otz_enrollment();
CALL sp_populate_etl_otz_activity();
CALL sp_populate_etl_ovc_enrolment();
CALL sp_populate_etl_cervical_cancer_screening();
CALL sp_populate_etl_patient_contact();
CALL sp_populate_etl_client_trace();
CALL sp_populate_etl_kp_contact();
CALL sp_populate_etl_kp_client_enrollment();
CALL sp_populate_etl_kp_clinical_visit();
CALL sp_populate_etl_kp_sti_treatment();
CALL sp_populate_etl_kp_peer_calendar();
CALL sp_populate_etl_kp_peer_tracking();
CALL sp_populate_etl_kp_treatment_verification();
-- CALL sp_populate_etl_gender_based_violence();
CALL sp_populate_etl_PrEP_verification();
CALL sp_populate_etl_alcohol_drug_abuse_screening();
CALL sp_populate_etl_gbv_screening();
CALL sp_populate_etl_gbv_screening_action();
CALL sp_populate_etl_violence_reporting();
CALL sp_populate_etl_link_facility_tracking();
CALL sp_populate_etl_depression_screening();
CALL sp_populate_etl_adverse_events();
CALL sp_populate_etl_allergy_chronic_illness();
CALL sp_populate_etl_ipt_screening();
CALL sp_populate_etl_pre_hiv_enrollment_art();
CALL sp_populate_etl_covid_19_assessment();
CALL sp_populate_etl_vmmc_enrolment();
CALL sp_populate_etl_vmmc_circumcision_procedure();
CALL sp_populate_etl_vmmc_client_followup();
CALL sp_populate_etl_vmmc_medical_history();
CALL sp_populate_etl_vmmc_post_operation_assessment();
CALL sp_populate_etl_hts_eligibility_screening();
CALL sp_populate_etl_drug_order();
CALL sp_populate_etl_preventive_services();
CALL sp_populate_etl_overdose_reporting();
CALL sp_populate_etl_art_fast_track();
CALL sp_populate_etl_clinical_encounter();
CALL sp_populate_etl_daily_revenue_summary();
CALL sp_update_next_appointment_date();
CALL sp_update_dashboard_table();

UPDATE amrsemr_etl.etl_script_status SET stop_time=NOW() where id= populate_script_id;

SELECT "Completed first time setup", CONCAT("Time: ", NOW());
END $$



