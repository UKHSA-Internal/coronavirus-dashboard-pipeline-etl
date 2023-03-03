METRIC_IDS_QUERY="""\
SELECT metric, id
FROM covid19.metric_reference
WHERE metric = ANY('{metrics_string}');\
"""

# Not used at the moment, remove if obsolete
PREVIOUS_RELEASE_QUERY = """\
SELECT timestamp::DATE as date
FROM covid19.release_category
    JOIN covid19.release_reference AS rr ON rr.id = covid19.release_category.release_id
WHERE process_name = 'AGE-DEMOGRAPHICS: VACCINATION - EVENT DATE'
    AND released = true
ORDER BY timestamp DESC
LIMIT 5;\
"""

VACCINATIONS_QUERY = """\
SELECT partition_id, release_id, area_id, date, payload
FROM (
        SELECT *
        FROM covid19.time_series_p{partition}_other AS tsother
                JOIN covid19.release_reference AS rr ON rr.id = release_id
                JOIN covid19.metric_reference AS mr ON mr.id = metric_id
                JOIN covid19.area_reference AS ar ON ar.id = tsother.area_id
        WHERE metric = 'vaccinationsAgeDemographics'
        AND date > ( DATE('{date}'))
        UNION
        (
            SELECT *
            FROM covid19.time_series_p{partition}_utla AS tsutla
                    JOIN covid19.release_reference AS rr ON rr.id = release_id
                    JOIN covid19.metric_reference AS mr ON mr.id = metric_id
                    JOIN covid19.area_reference AS ar ON ar.id = tsutla.area_id
            WHERE metric = 'vaccinationsAgeDemographics'
            AND date > ( DATE('{date}'))
        )
        UNION
        (
            SELECT *
            FROM covid19.time_series_p{partition}_ltla AS tsltla
                    JOIN covid19.release_reference AS rr ON rr.id = release_id
                    JOIN covid19.metric_reference AS mr ON mr.id = metric_id
                    JOIN covid19.area_reference AS ar ON ar.id = tsltla.area_id
            WHERE metric = 'vaccinationsAgeDemographics'
            AND date > ( DATE('{date}'))
        )
    ) AS tsltla
ORDER BY date DESC;\
"""