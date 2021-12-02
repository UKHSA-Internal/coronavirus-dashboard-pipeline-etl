#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:

# 3rd party:

# Internal: 

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

PUBLISH_DATE_CALCULATION = """\
SELECT df.partition_id,
       df.area_id,
       df.area_type,
       df.area_code,
       mr.id,
       df.release_id,
       df.date,
       df.payload
FROM (
    SELECT today.partition_id,
           today.area_id,
           today.area_type,
           today.area_code,
           CASE 
             WHEN today.metric = 'newPeopleVaccinatedFirstDoseByVaccinationDate'
               THEN 'newPeopleVaccinatedFirstDoseByPublishDate'
             WHEN today.metric = 'newPeopleVaccinatedSecondDoseByVaccinationDate'
               THEN 'newPeopleVaccinatedSecondDoseByPublishDate'
             ELSE 'newPeopleVaccinatedThirdInjectionByPublishDate'
           END AS metric,
           today.release_id,
           today.date,
           jsonb_build_object(
               'value', 
               CASE 
                   WHEN (today.value - yesterday.value)::INT < 0
                     THEN 0
                   ELSE (today.value - yesterday.value)::INT
               END
           ) AS payload
    FROM (
        SELECT area_id,
               partition_id,
               area_type,
               area_code,
               metric,
               MAX(ts.release_id) AS release_id,
               MAX(date) AS date,
               SUM((payload -> 'value')::NUMERIC) AS value
        FROM covid19.time_series_p{today}_{area_type} AS ts
                 JOIN covid19.metric_reference   AS mr ON mr.id = ts.metric_id
                 JOIN covid19.area_reference     AS ar ON ar.id = ts.area_id
                 JOIN covid19.release_reference  AS rr ON rr.id = ts.release_id
                 JOIN covid19.release_category   AS rc ON rc.release_id = rr.id
        WHERE metric IN (
                'newPeopleVaccinatedFirstDoseByVaccinationDate', 
                'newPeopleVaccinatedSecondDoseByVaccinationDate',
                'newPeopleVaccinatedThirdInjectionByVaccinationDate'
            )
          AND (payload ->> 'value') NOTNULL
          AND process_name = 'VACCINATION'
        GROUP BY partition_id, area_id, area_type, area_code, metric
    ) AS today
    LEFT JOIN (
        SELECT area_id,
               partition_id,
               area_type,
               area_code,
               metric,
               MAX(ts.release_id) AS release_id,
               MAX(date) AS date,
               SUM((payload -> 'value')::NUMERIC) AS value
        FROM covid19.time_series_p{yesterday}_{area_type} AS ts
                 JOIN covid19.metric_reference   AS mr ON mr.id = ts.metric_id
                 JOIN covid19.area_reference     AS ar ON ar.id = ts.area_id
                 JOIN covid19.release_reference  AS rr ON rr.id = ts.release_id
                 JOIN covid19.release_category   AS rc ON rc.release_id = rr.id
        WHERE metric IN (
                'newPeopleVaccinatedFirstDoseByVaccinationDate', 
                'newPeopleVaccinatedSecondDoseByVaccinationDate',
                'newPeopleVaccinatedThirdInjectionByVaccinationDate'
            )
          AND (payload ->> 'value') NOTNULL
          AND process_name = 'VACCINATION'
        GROUP BY partition_id, area_id, area_type, area_code, metric
    ) AS yesterday ON today.area_id = yesterday.area_id AND today.metric = yesterday.metric
) AS df
LEFT JOIN covid19.metric_reference AS mr ON mr.metric = df.metric;\
"""


PERCENTAGE_DATA = """\
SELECT area_id,
       partition_id,
       mr.id AS metric_id,
       mr.metric AS metric,
       area_type,
       area_code,
       release_id,
       date,
       payload
FROM (
    SELECT area_id,
           MAX(partition_id) AS partition_id,
           CASE 
             WHEN MAX(metric) = 'cumVaccinationFirstDoseUptakeByVaccinationDatePercentage'
               THEN 'cumVaccinationFirstDoseUptakeByPublishDatePercentage'
             WHEN MAX(metric) = 'cumVaccinationSecondDoseUptakeByVaccinationDatePercentage'
               THEN 'cumVaccinationSecondDoseUptakeByPublishDatePercentage'
             ELSE 'cumVaccinationThirdInjectionUptakeByPublishDatePercentage'
           END AS metric,
           area_type,
           area_code,
           MAX(ts.release_id) AS release_id,
           MAX(date) AS date,
           jsonb_build_object('value', ROUND(MAX((payload -> 'value')::NUMERIC), 2)) AS payload
    FROM covid19.time_series_p{date}_{area_type} AS ts
             JOIN covid19.metric_reference   AS mr ON mr.id = ts.metric_id
             JOIN covid19.area_reference     AS ar ON ar.id = ts.area_id
             JOIN covid19.release_reference  AS rr ON rr.id = ts.release_id
             JOIN covid19.release_category   AS rc ON rc.release_id = rr.id
    WHERE metric IN (
            'cumVaccinationFirstDoseUptakeByVaccinationDatePercentage',
            'cumVaccinationSecondDoseUptakeByVaccinationDatePercentage',
            'cumVaccinationThirdInjectionUptakeByVaccinationDatePercentage'
        )
      AND (payload ->> 'value') NOTNULL
      AND process_name = 'VACCINATION'
    GROUP BY area_type, area_code, metric_id, area_id
) AS df
LEFT JOIN covid19.metric_reference AS mr ON mr.metric = df.metric;\
"""

