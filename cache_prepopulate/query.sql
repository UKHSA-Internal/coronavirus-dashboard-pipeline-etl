SELECT
    'area-' || release_date::TEXT || '-' || area_id::TEXT AS key,
    JSONB_AGG(
        JSONB_BUILD_OBJECT(
            'area_code', area_code,
            'area_type', area_type,
            'area_name', area_name,
            'date', to_char(date::DATE, 'YYYY-MM-DD'),
            'metric', metric,
            'value', value,
            'priority', priority
        )
    )::TEXT AS value
FROM (
    -- Data at UK / NATION / REGION / NHS REGION levels.
    SELECT
           CASE
               WHEN ref.area_type = 'overview' THEN 'UK'
               ELSE ts.area_id::TEXT
           END AS area_id,
           rr.timestamp::DATE AS release_date,
           metric,
           priority,
           area_code,
           ref.area_type,
           area_name,
           ts.date AS date,
           (
               CASE
                   WHEN (payload ->> 'value') = 'UP' THEN 0
                   WHEN (payload ->> 'value') = 'DOWN' THEN 180
                   WHEN (payload ->> 'value') = 'SAME' THEN 90
                   WHEN metric ILIKE ANY('{%percentage%,%rate%,%transmission%}'::VARCHAR[]) THEN round((payload ->> 'value')::NUMERIC, 1)
                   ELSE round((payload ->> 'value')::NUMERIC)::INT
               END
           ) AS value,
           RANK() OVER (
               PARTITION BY (ts.area_id, metric)
               ORDER BY priority, date DESC
           ) AS rank
    FROM covid19.time_series_p${release_date}_other AS ts
        JOIN covid19.release_reference AS rr ON rr.id = release_id
        JOIN covid19.metric_reference  AS mr ON mr.id = metric_id
        JOIN covid19.area_reference    AS ref ON ref.id = area_id
        JOIN covid19.area_priorities   AS ap ON ref.area_type = ap.area_type
    WHERE ts.date > (NOW() - INTERVAL '36 days')
      AND (
        (
            -- UK level data.
            ref.area_type = 'overview'
            AND metric = ANY('{newDeaths28DaysByDeathDate,newDeaths28DaysByDeathDateChange,newDeaths28DaysByDeathDateChangePercentage,newDeaths28DaysByDeathDateRollingSum,newDeaths28DaysByDeathDateDirection,newCasesBySpecimenDate, newCasesBySpecimenDateChange, newCasesBySpecimenDateChangePercentage, newCasesBySpecimenDateRollingSum, newCasesBySpecimenDateDirection,newVirusTestsByPublishDate,newVirusTestsByPublishDateChange,newVirusTestsByPublishDateChangePercentage,newVirusTestsByPublishDateRollingSum,newVirusTestsByPublishDateDirection,newVirusTestsByPublishDate,newVirusTestsByPublishDateChange,newVirusTestsByPublishDateChangePercentage,newVirusTestsByPublishDateRollingSum,newVirusTestsByPublishDateDirection,newAdmissions,newAdmissionsChange,newAdmissionsChangePercentage,newAdmissionsRollingSum,newAdmissionsDirection,newVirusTests,newVirusTestsChange,newVirusTestsChangePercentage,newVirusTestsRollingSum,newVirusTestsDirection,transmissionRateMin,transmissionRateMax,newPeopleVaccinatedFirstDoseByPublishDate,newPeopleVaccinatedSecondDoseByPublishDate,cumPeopleVaccinatedFirstDoseByPublishDate,cumPeopleVaccinatedSecondDoseByPublishDate,cumVaccinationFirstDoseUptakeByPublishDatePercentage,cumVaccinationSecondDoseUptakeByPublishDatePercentage,newDeaths28DaysByPublishDate, newDeaths28DaysByPublishDateChange, newDeaths28DaysByPublishDateChangePercentage, newDeaths28DaysByPublishDateRollingSum, newDeaths28DaysByPublishDateDirection, newDeaths28DaysByDeathDateRollingRate,newCasesBySpecimenDateRollingRate, newCasesByPublishDate, newCasesByPublishDateChange, newCasesByPublishDateChangePercentage, newCasesByPublishDateRollingSum, newCasesByPublishDateDirection, newPeopleVaccinatedThirdInjectionByPublishDate, cumPeopleVaccinatedThirdInjectionByPublishDate, cumVaccinationThirdInjectionUptakeByPublishDatePercentage,newPeopleVaccinatedThirdInjectionByPublishDateRollingSum,newPeopleVaccinatedFirstDoseByPublishDateRollingSum,newPeopleVaccinatedSecondDoseByPublishDateRollingSum,newDailyNsoDeathsByDeathDate,newDailyNsoDeathsByRegistrationDate,newDailyNsoDeathsByDeathDateChangePercentage,newDailyNsoDeathsByDeathDateChange,newDailyNsoDeathsByDeathDateRollingSum,newDailyNsoDeathsByDeathDateDirection,cumPeopleVaccinatedAutumn22ByVaccinationDate50plus,cumVaccinationAutumn22UptakeByVaccinationDatePercentage50plus,newPeopleVaccinatedSpring23ByVaccinationDate75plus,cumPeopleVaccinatedSpring23ByVaccinationDate75plus,cumVaccinationSpring23UptakeByVaccinationDatePercentage75plus}'::VARCHAR[])
        )
        OR (
            -- Anything else that's available at nation, region, or NHS region levels.
            metric = ANY('{newVirusTestsByPublishDate,newVirusTestsByPublishDateChange,newVirusTestsByPublishDateChangePercentage,newVirusTestsByPublishDateRollingSum,newVirusTestsByPublishDateDirection,newAdmissions,newAdmissionsChange,newAdmissionsChangePercentage,newAdmissionsRollingSum,newAdmissionsDirection,transmissionRateMin,transmissionRateMax}'::VARCHAR[])
            OR (
                    LEFT(ref.area_code, 1) = 'W'  -- Welsh deaths only available at nation level.
                AND metric = ANY ('{newDeaths28DaysByDeathDate,newDeaths28DaysByDeathDateChange,newDeaths28DaysByDeathDateChangePercentage,newDeaths28DaysByDeathDateRollingSum,newDeaths28DaysByDeathDateDirection,newDeaths28DaysByPublishDate,newDeaths28DaysByPublishDateChange,newDeaths28DaysByPublishDateChangePercentage,newDeaths28DaysByPublishDateRollingSum,newDeaths28DaysByPublishDateDirection,newDeaths28DaysByDeathDateRollingRate,newDailyNsoDeathsByDeathDate,newDailyNsoDeathsByRegistrationDate,newDailyNsoDeathsByDeathDateChangePercentage,newDailyNsoDeathsByDeathDateChange,newDailyNsoDeathsByDeathDateRollingSum,newDailyNsoDeathsByDeathDateDirection}'::VARCHAR[])
            )
            OR (
                    -- Welsh & NI vax only available at nation level + England & Scotland for fall back.
                    LEFT(ref.area_code, 1) = ANY('{W,N,E,S}'::VARCHAR[])
                AND metric = ANY ('{newDeaths28DaysByDeathDate,newDeaths28DaysByDeathDateChange,newDeaths28DaysByDeathDateChangePercentage,newDeaths28DaysByDeathDateRollingSum,newDeaths28DaysByDeathDateDirection,newPeopleVaccinatedFirstDoseByPublishDate,newPeopleVaccinatedSecondDoseByPublishDate,cumPeopleVaccinatedFirstDoseByPublishDate,cumPeopleVaccinatedSecondDoseByPublishDate,cumVaccinationFirstDoseUptakeByPublishDatePercentage,cumVaccinationSecondDoseUptakeByPublishDatePercentage,newDeaths28DaysByPublishDate, newDeaths28DaysByPublishDateChange, newDeaths28DaysByPublishDateChangePercentage, newDeaths28DaysByPublishDateRollingSum, newDeaths28DaysByPublishDateDirection, newDeaths28DaysByDeathDateRollingRate, newPeopleVaccinatedThirdInjectionByPublishDate, cumPeopleVaccinatedThirdInjectionByPublishDate, cumVaccinationThirdInjectionUptakeByPublishDatePercentage,newPeopleVaccinatedThirdInjectionByPublishDateRollingSum,newPeopleVaccinatedFirstDoseByPublishDateRollingSum,newPeopleVaccinatedSecondDoseByPublishDateRollingSum,newDailyNsoDeathsByDeathDate,newDailyNsoDeathsByRegistrationDate,newDailyNsoDeathsByDeathDateChangePercentage,newDailyNsoDeathsByDeathDateChange,newDailyNsoDeathsByDeathDateRollingSum,newDailyNsoDeathsByDeathDateDirection}'::VARCHAR[])
            )
        )
      )
    UNION (
        SELECT
           CASE
               WHEN ref.area_type = 'nation' THEN UPPER(ref.area_name)
               ELSE ts.area_id::TEXT
           END AS area_id,
           rr.timestamp::DATE AS release_date,
           metric,
           priority,
           area_code,
           ref.area_type,
           area_name,
           ts.date AS date,
           (
               CASE
                   WHEN (payload ->> 'value') = 'UP' THEN 0
                   WHEN (payload ->> 'value') = 'DOWN' THEN 180
                   WHEN (payload ->> 'value') = 'SAME' THEN 90
                   WHEN metric ILIKE ANY('{%percentage%,%rate%,%transmission%}'::VARCHAR[]) THEN round((payload ->> 'value')::NUMERIC, 1)
                   ELSE round((payload ->> 'value')::NUMERIC)::INT
               END
           ) AS value,
           RANK() OVER (
               PARTITION BY (ts.area_id, metric)
               ORDER BY priority, date DESC
           ) AS rank
        FROM covid19.time_series_p${release_date}_other AS ts
            JOIN covid19.release_reference AS rr ON rr.id = release_id
            JOIN covid19.metric_reference  AS mr ON mr.id = metric_id
            JOIN covid19.area_reference    AS ref ON ref.id = area_id
            JOIN covid19.area_priorities   AS ap ON ref.area_type = ap.area_type
        WHERE ts.date > (NOW() - INTERVAL '36 days')
          AND (
            (
                -- UK level data.
                ref.area_type = 'nation'
                AND metric = ANY('{newDeaths28DaysByDeathDate,newDeaths28DaysByDeathDateChange,newDeaths28DaysByDeathDateChangePercentage,newDeaths28DaysByDeathDateRollingSum,newDeaths28DaysByDeathDateDirection,newCasesBySpecimenDate, newCasesBySpecimenDateChange, newCasesBySpecimenDateChangePercentage, newCasesBySpecimenDateRollingSum, newCasesBySpecimenDateDirection,newVirusTestsByPublishDate,newVirusTestsByPublishDateChange,newVirusTestsByPublishDateChangePercentage,newVirusTestsByPublishDateRollingSum,newVirusTestsByPublishDateDirection,newVirusTestsByPublishDate,newVirusTestsByPublishDateChange,newVirusTestsByPublishDateChangePercentage,newVirusTestsByPublishDateRollingSum,newVirusTestsByPublishDateDirection,newAdmissions,newAdmissionsChange,newAdmissionsChangePercentage,newAdmissionsRollingSum,newAdmissionsDirection,newVirusTests,newVirusTestsChange,newVirusTestsChangePercentage,newVirusTestsRollingSum,newVirusTestsDirection,transmissionRateMin,transmissionRateMax,newPeopleVaccinatedFirstDoseByPublishDate,newPeopleVaccinatedSecondDoseByPublishDate,cumPeopleVaccinatedFirstDoseByPublishDate,cumPeopleVaccinatedSecondDoseByPublishDate,cumVaccinationFirstDoseUptakeByPublishDatePercentage,cumVaccinationSecondDoseUptakeByPublishDatePercentage,newDeaths28DaysByPublishDate, newDeaths28DaysByPublishDateChange, newDeaths28DaysByPublishDateChangePercentage, newDeaths28DaysByPublishDateRollingSum, newDeaths28DaysByPublishDateDirection, newDeaths28DaysByDeathDateRollingRate,newCasesBySpecimenDateRollingRate, newCasesByPublishDate, newCasesByPublishDateChange, newCasesByPublishDateChangePercentage, newCasesByPublishDateRollingSum, newCasesByPublishDateDirection, newPeopleVaccinatedThirdInjectionByPublishDate, cumPeopleVaccinatedThirdInjectionByPublishDate, cumVaccinationThirdInjectionUptakeByPublishDatePercentage,newPeopleVaccinatedThirdInjectionByPublishDateRollingSum,newPeopleVaccinatedFirstDoseByPublishDateRollingSum,newPeopleVaccinatedSecondDoseByPublishDateRollingSum,newDailyNsoDeathsByDeathDate,newDailyNsoDeathsByRegistrationDate,newDailyNsoDeathsByDeathDateChangePercentage,newDailyNsoDeathsByDeathDateChange,newDailyNsoDeathsByDeathDateRollingSum,newDailyNsoDeathsByDeathDateDirection,cumPeopleVaccinatedAutumn22ByVaccinationDate50plus,cumVaccinationAutumn22UptakeByVaccinationDatePercentage50plus,newPeopleVaccinatedSpring23ByVaccinationDate75plus,cumPeopleVaccinatedSpring23ByVaccinationDate75plus,cumVaccinationSpring23UptakeByVaccinationDatePercentage75plus}'::VARCHAR[])
            )
            OR (
                -- Anything else that's available at nation, region, or NHS region levels.
                metric = ANY('{newDeaths28DaysByDeathDate,newDeaths28DaysByDeathDateChange,newDeaths28DaysByDeathDateChangePercentage,newDeaths28DaysByDeathDateRollingSum,newDeaths28DaysByDeathDateDirection,newVirusTestsByPublishDate,newVirusTestsByPublishDateChange,newVirusTestsByPublishDateChangePercentage,newVirusTestsByPublishDateRollingSum,newVirusTestsByPublishDateDirection,newAdmissions,newAdmissionsChange,newAdmissionsChangePercentage,newAdmissionsRollingSum,newAdmissionsDirection,transmissionRateMin,transmissionRateMax,newDailyNsoDeathsByDeathDate,newDailyNsoDeathsByRegistrationDate,newDailyNsoDeathsByDeathDateChangePercentage,newDailyNsoDeathsByDeathDateChange,newDailyNsoDeathsByDeathDateRollingSum,newDailyNsoDeathsByDeathDateDirection}'::VARCHAR[])
                OR (
                        LEFT(ref.area_code, 1) = 'W'  -- Welsh deaths only available at nation level.
                    AND metric = ANY ('{newDeaths28DaysByDeathDate,newDeaths28DaysByDeathDateChange,newDeaths28DaysByDeathDateChangePercentage,newDeaths28DaysByDeathDateRollingSum,newDeaths28DaysByDeathDateDirection,newDeaths28DaysByPublishDate,newDeaths28DaysByPublishDateChange,newDeaths28DaysByPublishDateChangePercentage,newDeaths28DaysByPublishDateRollingSum,newDeaths28DaysByPublishDateDirection,newDeaths28DaysByDeathDateRollingRate,newDailyNsoDeathsByDeathDate,newDailyNsoDeathsByRegistrationDate,newDailyNsoDeathsByDeathDateChangePercentage,newDailyNsoDeathsByDeathDateChange,newDailyNsoDeathsByDeathDateRollingSum,newDailyNsoDeathsByDeathDateDirection}'::VARCHAR[])
                )
                OR (
                        -- Welsh & NI vax only available at nation level + England & Scotland for fall back.
                        LEFT(ref.area_code, 1) = ANY('{W,N,E,S}'::VARCHAR[])
                    AND metric = ANY ('{newDeaths28DaysByDeathDate,newDeaths28DaysByDeathDateChange,newDeaths28DaysByDeathDateChangePercentage,newDeaths28DaysByDeathDateRollingSum,newDeaths28DaysByDeathDateDirection,newPeopleVaccinatedFirstDoseByPublishDate,newPeopleVaccinatedSecondDoseByPublishDate,cumPeopleVaccinatedFirstDoseByPublishDate,cumPeopleVaccinatedSecondDoseByPublishDate,cumVaccinationFirstDoseUptakeByPublishDatePercentage,cumVaccinationSecondDoseUptakeByPublishDatePercentage,newDeaths28DaysByPublishDate, newDeaths28DaysByPublishDateChange, newDeaths28DaysByPublishDateChangePercentage, newDeaths28DaysByPublishDateRollingSum, newDeaths28DaysByPublishDateDirection, newDeaths28DaysByDeathDateRollingRate, newPeopleVaccinatedThirdInjectionByPublishDate, cumPeopleVaccinatedThirdInjectionByPublishDate, cumVaccinationThirdInjectionUptakeByPublishDatePercentage,newPeopleVaccinatedThirdInjectionByPublishDateRollingSum,newPeopleVaccinatedFirstDoseByPublishDateRollingSum,newPeopleVaccinatedSecondDoseByPublishDateRollingSum,newDailyNsoDeathsByDeathDate,newDailyNsoDeathsByRegistrationDate,newDailyNsoDeathsByDeathDateChangePercentage,newDailyNsoDeathsByDeathDateChange,newDailyNsoDeathsByDeathDateRollingSum,newDailyNsoDeathsByDeathDateDirection}'::VARCHAR[])
                )
            )
          )
    )
    UNION
    (
        -- Data at UTLA / LTLA REGION levels.
        SELECT area_id,
               release_date,
               metric,
               priority,
               area_code,
               area_type,
               area_name,
               date,
               value,
               RANK() OVER (
                   PARTITION BY (area_id, metric)
                   ORDER BY priority, date DESC
               ) AS rank
        FROM (
            -- Data at UTLA level
            SELECT ts.area_id::TEXT,
                   rr.timestamp::DATE AS release_date,
                   metric,
                   priority,
                   area_code,
                   ref.area_type,
                   area_name,
                   date,
                   (
                       CASE
                           WHEN (payload ->> 'value') = 'UP' THEN 0
                           WHEN (payload ->> 'value') = 'DOWN' THEN 180
                           WHEN (payload ->> 'value') = 'SAME' THEN 90
                           WHEN metric ILIKE ANY('{%percentage%,%rate%,%transmission%}'::VARCHAR[]) THEN round((payload ->> 'value')::NUMERIC, 2)
                           ELSE round((payload ->> 'value')::NUMERIC)::INT
                       END
                   ) AS value
            FROM covid19.time_series_p${release_date}_utla AS ts
                JOIN covid19.release_reference AS rr ON rr.id = release_id
                JOIN covid19.metric_reference AS mr ON mr.id = metric_id
                JOIN covid19.area_reference AS ref ON ref.id = area_id
                JOIN covid19.area_priorities AS ap ON ref.area_type = ap.area_type
            WHERE ts.date > (NOW() - INTERVAL '36 days')
              AND metric = ANY ('{newDeaths28DaysByDeathDate,newDeaths28DaysByDeathDateChange,newDeaths28DaysByDeathDateChangePercentage,newDeaths28DaysByDeathDateRollingSum,newDeaths28DaysByDeathDateDirection,newCasesBySpecimenDate, newCasesBySpecimenDateChange, newCasesBySpecimenDateChangePercentage, newCasesBySpecimenDateRollingSum, newCasesBySpecimenDateDirection,newVirusTestsByPublishDate,newVirusTestsByPublishDateChange,newVirusTestsByPublishDateChangePercentage,newVirusTestsByPublishDateRollingSum,newVirusTestsByPublishDateDirection,newPeopleVaccinatedFirstDoseByPublishDate,newPeopleVaccinatedSecondDoseByPublishDate,cumPeopleVaccinatedFirstDoseByPublishDate,cumPeopleVaccinatedSecondDoseByPublishDate,cumVaccinationFirstDoseUptakeByPublishDatePercentage,cumVaccinationSecondDoseUptakeByPublishDatePercentage,newDeaths28DaysByPublishDate,newDeaths28DaysByPublishDateChange,newDeaths28DaysByPublishDateChangePercentage,newDeaths28DaysByPublishDateRollingSum,newDeaths28DaysByPublishDateDirection,newDeaths28DaysByDeathDateRollingRate,newCasesBySpecimenDateRollingSum,newCasesBySpecimenDateRollingRate,newCasesBySpecimenDateChange,newCasesBySpecimenDateChangePercentage,newCasesBySpecimenDateDirection,newCasesBySpecimenDate,newCasesByPublishDate,newCasesByPublishDateChange,newCasesByPublishDateChangePercentage,newCasesByPublishDateRollingSum,newCasesByPublishDateDirection,newPeopleVaccinatedThirdInjectionByPublishDate, cumPeopleVaccinatedThirdInjectionByPublishDate, cumVaccinationThirdInjectionUptakeByPublishDatePercentage,newPeopleVaccinatedThirdInjectionByPublishDateRollingSum,newPeopleVaccinatedFirstDoseByPublishDateRollingSum,newPeopleVaccinatedSecondDoseByPublishDateRollingSum,newDailyNsoDeathsByDeathDate,newDailyNsoDeathsByRegistrationDate,newDailyNsoDeathsByDeathDateChangePercentage,newDailyNsoDeathsByDeathDateChange,newDailyNsoDeathsByDeathDateRollingSum,newDailyNsoDeathsByDeathDateDirection}'::VARCHAR[])
            UNION
            (
                -- Data at LTLA level.
                SELECT ts.area_id::TEXT,
                       rr.timestamp::DATE AS release_date,
                       metric,
                       priority,
                       area_code,
                       ref.area_type,
                       area_name,
                       date,
                       (
                           CASE
                               WHEN (payload ->> 'value') = 'UP' THEN 0
                               WHEN (payload ->> 'value') = 'DOWN' THEN 180
                               WHEN (payload ->> 'value') = 'SAME' THEN 90
                               WHEN metric ILIKE ANY('{%percentage%,%rate%,%transmission%}'::VARCHAR[]) THEN round((payload ->> 'value')::NUMERIC, 2)
                               ELSE round((payload ->> 'value')::NUMERIC)::INT
                           END
                       ) AS value
                FROM covid19.time_series_p${release_date}_ltla AS ts
                    JOIN covid19.release_reference AS rr ON rr.id = release_id
                    JOIN covid19.metric_reference AS mr ON mr.id = metric_id
                    JOIN covid19.area_reference AS ref ON ref.id = area_id
                    JOIN covid19.area_priorities AS ap ON ref.area_type = ap.area_type
                WHERE ts.date > (NOW() - INTERVAL '36 days')
                  AND metric = ANY ('{newDeaths28DaysByDeathDate,newDeaths28DaysByDeathDateChange,newDeaths28DaysByDeathDateChangePercentage,newDeaths28DaysByDeathDateRollingSum,newDeaths28DaysByDeathDateDirection,newCasesBySpecimenDate, newCasesBySpecimenDateChange, newCasesBySpecimenDateChangePercentage, newCasesBySpecimenDateRollingSum, newCasesBySpecimenDateDirection,newVirusTestsByPublishDate,newVirusTestsByPublishDateChange,newVirusTestsByPublishDateChangePercentage,newVirusTestsByPublishDateRollingSum,newVirusTestsByPublishDateDirection,newPeopleVaccinatedFirstDoseByPublishDate,newPeopleVaccinatedSecondDoseByPublishDate,cumPeopleVaccinatedFirstDoseByPublishDate,cumPeopleVaccinatedSecondDoseByPublishDate,cumVaccinationFirstDoseUptakeByPublishDatePercentage,cumVaccinationSecondDoseUptakeByPublishDatePercentage,newDeaths28DaysByPublishDate,newDeaths28DaysByPublishDateChange,newDeaths28DaysByPublishDateChangePercentage,newDeaths28DaysByPublishDateRollingSum,newDeaths28DaysByPublishDateDirection,newDeaths28DaysByDeathDateRollingRate,newCasesBySpecimenDateRollingSum,newCasesBySpecimenDateRollingRate,newCasesBySpecimenDateChange,newCasesBySpecimenDateChangePercentage,newCasesBySpecimenDateDirection,newCasesBySpecimenDate,newCasesByPublishDate,newCasesByPublishDateChange,newCasesByPublishDateChangePercentage,newCasesByPublishDateRollingSum,newCasesByPublishDateDirection,newPeopleVaccinatedThirdInjectionByPublishDate, cumPeopleVaccinatedThirdInjectionByPublishDate, cumVaccinationThirdInjectionUptakeByPublishDatePercentage,newPeopleVaccinatedThirdInjectionByPublishDateRollingSum,newPeopleVaccinatedFirstDoseByPublishDateRollingSum,newPeopleVaccinatedSecondDoseByPublishDateRollingSum,newDailyNsoDeathsByDeathDate,newDailyNsoDeathsByRegistrationDate,newDailyNsoDeathsByDeathDateChangePercentage,newDailyNsoDeathsByDeathDateChange,newDailyNsoDeathsByDeathDateRollingSum,newDailyNsoDeathsByDeathDateDirection,cumPeopleVaccinatedAutumn22ByVaccinationDate50plus,cumVaccinationAutumn22UptakeByVaccinationDatePercentage50plus,newPeopleVaccinatedSpring23ByVaccinationDate75plus,cumPeopleVaccinatedSpring23ByVaccinationDate75plus,cumVaccinationSpring23UptakeByVaccinationDatePercentage75plus}'::VARCHAR[])
            )
        ) AS ts2
    )
    UNION
    (
        -- Data at NHS TRUST level.
        SELECT ts.area_id::TEXT,
               rr.timestamp::DATE AS release_date,
               metric,
               priority,
               area_code,
               ref.area_type,
               area_name,
               date,
               (
                   CASE
                       WHEN (payload ->> 'value') = 'UP' THEN 0
                       WHEN (payload ->> 'value') = 'DOWN' THEN 180
                       WHEN (payload ->> 'value') = 'SAME' THEN 90
                       WHEN metric ILIKE ANY('{%percentage%,%rate%,%transmission%}'::VARCHAR[]) THEN round((payload ->> 'value')::NUMERIC, 2)
                       ELSE round((payload ->> 'value')::NUMERIC)::INT
                   END
               ) AS value,
               RANK() OVER (
                   PARTITION BY (metric)
                   ORDER BY priority, date DESC
               ) AS rank
        FROM covid19.time_series_p${release_date}_nhstrust AS ts
            JOIN covid19.release_reference AS rr ON rr.id = release_id
            JOIN covid19.metric_reference AS mr ON mr.id = metric_id
            JOIN covid19.area_reference AS ref ON ref.id = area_id
            JOIN covid19.area_priorities AS ap ON ref.area_type = ap.area_type
        WHERE ts.date > (NOW() - INTERVAL '16 days')
          AND metric = ANY ('{newAdmissions,newAdmissionsChange,newAdmissionsChangePercentage,newAdmissionsRollingSum,newAdmissionsDirection}'::VARCHAR[])
          AND (payload ->> 'value') NOTNULL
    )
    UNION
    (
        -- Cases data at MSOA level.
        SELECT ts.area_id::TEXT,
               release_date,
               metric,
               1 AS priority,
               area_code,
               area_type,
               area_name,
               date,
               (
                   CASE
                       WHEN value::TEXT = 'UP' THEN 0
                       WHEN value::TEXT = 'DOWN' THEN 180
                       WHEN value::TEXT = 'SAME' THEN 90
                       WHEN metric LIKE 'newCasesBySpecimenDate%' THEN value::NUMERIC
                       ELSE round(value::NUMERIC)::INT
                   END
               ) AS value,
               rank
        FROM (
            SELECT area_id,
                   (metric || UPPER(LEFT(key, 1)) || RIGHT(key, -1)) AS metric,
                   rr.timestamp::DATE AS release_date,
                   ref.area_code,
                   ref.area_type ,
                   area_name,
                   date,
                   (
                       CASE
                           WHEN value::TEXT <> 'null' THEN TRIM(BOTH '\"' FROM value::TEXT)
                           ELSE '-999999'
                       END
                   ) AS value,
                   RANK() OVER ( PARTITION BY (metric) ORDER BY date DESC ) AS rank
            FROM covid19.time_series_p${release_date}_msoa AS ts
                JOIN covid19.release_reference AS rr ON rr.id = release_id
                JOIN covid19.metric_reference AS mr ON mr.id = metric_id
                JOIN covid19.area_reference AS ref ON ref.id = area_id,
                    jsonb_each(payload) AS pa
            WHERE metric = 'newCasesBySpecimenDate'
              AND ts.date > (NOW() - INTERVAL '10 days')
        ) AS ts
    )
    UNION
    (
        -- Vaccinations data at MSOA level.
        SELECT ts.area_id::TEXT,
               rr.timestamp::DATE AS release_date,
               mr.metric,
               1 AS priority,
               area_code,
               ref.area_type,
               area_name,
               date,
               ROUND((payload ->> 'value')::NUMERIC, 2) AS value,
               RANK() OVER ( PARTITION BY (metric) ORDER BY date DESC ) AS rank
        FROM covid19.time_series_p${release_date}_msoa AS ts
            JOIN covid19.release_reference AS rr ON rr.id = release_id
            JOIN covid19.metric_reference AS mr ON mr.id = metric_id
            JOIN covid19.area_reference AS ref ON ref.id = area_id
        WHERE ts.date > (NOW() - INTERVAL '10 days')
          AND mr.metric = ANY('{cumVaccinationFirstDoseUptakeByVaccinationDatePercentage,cumVaccinationSecondDoseUptakeByVaccinationDatePercentage,cumVaccinationThirdInjectionUptakeByVaccinationDatePercentage}'::VARCHAR[])
    )
) AS ts
WHERE rank = 1
GROUP BY release_date, area_id;
