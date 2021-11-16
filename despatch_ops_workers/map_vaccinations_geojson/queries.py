#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:

# 3rd party:

# Internal: 

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


QUERY = """\
SELECT jsonb_build_object(
               'cd', first_dose.area_code,
               'at', first_dose.area_type,
               'f', "first",
               'c', second
           ) AS properties,
       jsonb_build_object(
               'type', first_dose.geometry_type,
               'coordinates', first_dose.coordinates
           ) AS geometry
FROM (
         SELECT *
         FROM (
                  SELECT area_code,
                         area_type,
                         (payload ->> 'value')::FLOAT AS "first",
                         geometry_type,
                         coordinates
                  FROM covid19.time_series_p{date}_utla AS ts
                           LEFT JOIN covid19.area_reference AS ar ON ar.id = ts.area_id
                           LEFT JOIN covid19.metric_reference AS mr ON mr.id = metric_id
                           LEFT JOIN covid19.geo_data AS geo ON ar.id = geo.area_id
                  WHERE mr.metric = 'cumVaccinationFirstDoseUptakeByVaccinationDatePercentage'
                    AND (payload ->> 'value') NOTNULL -- value (JSON attribute)
                    AND date IN (
                      SELECT MAX(date)
                      FROM covid19.time_series_p{date}_msoa
                  )
                  UNION
                  (
                      SELECT area_code,
                             area_type,
                             (payload ->> 'value')::FLOAT AS "first",
                             geometry_type,
                             coordinates
                      FROM covid19.time_series_p{date}_ltla AS ts
                               LEFT JOIN covid19.area_reference AS ar ON ar.id = ts.area_id
                               LEFT JOIN covid19.metric_reference AS mr ON mr.id = metric_id
                               LEFT JOIN covid19.geo_data AS geo ON ar.id = geo.area_id
                      WHERE mr.metric = 'cumVaccinationFirstDoseUptakeByVaccinationDatePercentage'
                        AND (payload ->> 'value') NOTNULL -- value (JSON attribute)
                        AND date IN (
                          SELECT MAX(date)
                          FROM covid19.time_series_p{date}_msoa
                      )
                  )
                  UNION
                  (
                      SELECT area_code,
                             area_type,
                             (payload ->> 'value')::FLOAT AS "first",
                             geometry_type,
                             coordinates
                      FROM covid19.time_series_p{date}_msoa AS ts
                               LEFT JOIN covid19.area_reference AS ar ON ar.id = ts.area_id
                               LEFT JOIN covid19.metric_reference AS mr ON mr.id = metric_id
                               LEFT JOIN covid19.geo_data AS geo ON ar.id = geo.area_id
                      WHERE mr.metric = 'cumVaccinationFirstDoseUptakeByVaccinationDatePercentage'
                        AND (payload ->> 'value') NOTNULL -- value (JSON attribute)
                        AND date IN (
                          SELECT MAX(date)
                          FROM covid19.time_series_p{date}_msoa
                      )
                  )
              ) AS dt
     ) AS first_dose
         JOIN (
    SELECT *
    FROM (
             SELECT area_code,
                    area_type,
                    (payload ->> 'value')::FLOAT AS "second",
                    geometry_type,
                    coordinates
             FROM covid19.time_series_p{date}_utla AS ts
                      LEFT JOIN covid19.area_reference AS ar ON ar.id = ts.area_id
                      LEFT JOIN covid19.metric_reference AS mr ON mr.id = metric_id
                      LEFT JOIN covid19.geo_data AS geo ON ar.id = geo.area_id
             WHERE mr.metric = 'cumVaccinationSecondDoseUptakeByVaccinationDatePercentage'
               AND (payload ->> 'value') NOTNULL -- value (JSON attribute)
               AND date IN (
                 SELECT MAX(date)
                 FROM covid19.time_series_p{date}_msoa
             )
             UNION
             (
                 SELECT area_code,
                        area_type,
                        (payload ->> 'value')::FLOAT AS "second",
                        geometry_type,
                        coordinates
                 FROM covid19.time_series_p{date}_ltla AS ts
                          LEFT JOIN covid19.area_reference AS ar ON ar.id = ts.area_id
                          LEFT JOIN covid19.metric_reference AS mr ON mr.id = metric_id
                          LEFT JOIN covid19.geo_data AS geo ON ar.id = geo.area_id
                 WHERE mr.metric = 'cumVaccinationSecondDoseUptakeByVaccinationDatePercentage'
                   AND (payload ->> 'value') NOTNULL -- value (JSON attribute)
                   AND date IN (
                     SELECT MAX(date)
                     FROM covid19.time_series_p{date}_msoa
                 )
             )
             UNION
             (
                 SELECT area_code,
                        area_type,
                        (payload ->> 'value')::FLOAT AS "second",
                        geometry_type,
                        coordinates
                 FROM covid19.time_series_p{date}_msoa AS ts
                          LEFT JOIN covid19.area_reference AS ar ON ar.id = ts.area_id
                          LEFT JOIN covid19.metric_reference AS mr ON mr.id = metric_id
                          LEFT JOIN covid19.geo_data AS geo ON ar.id = geo.area_id
                 WHERE mr.metric = 'cumVaccinationSecondDoseUptakeByVaccinationDatePercentage'
                   AND (payload ->> 'value') NOTNULL -- value (JSON attribute)
                   AND date IN (
                     SELECT MAX(date)
                     FROM covid19.time_series_p{date}_msoa
                 )
             )
         ) AS dt
) AS second_dose ON 
        second_dose.area_type = first_dose.area_type 
    AND second_dose.area_code = first_dose.area_code;\
"""
