-- while we did not use age, satus, or how the record was obtained, these could be interesting features
-- for further exploration. do the migration habits of older birds differ younger birds
-- how have collection method changes impacted birds, etc.
SELECT A.*,SIGHTINGS
-- This subquery filters all the data down to just the species and features of interest with some minimal quality checks
FROM (SELECT ECORD_ID, ORIGINAL_BAND,SPECIES_ID,LAT_DD,LON_DD,EVENT_DATE,MIN_AGE_AT_ENC,BIRD_STATUS,HOW_OBTAINED,ISO_COUNTRY
    FROM [dbo].[AllBirdData]
    WHERE SPECIES_ID IN ('1720','2040','3490','1940')
    AND ISDATE(EVENT_DATE) = 1 
    ) A
LEFT JOIN 
-- This subquery coutns how many times an individual bird was seen
-- This is helpful because for routing puroposes a single sighting is not helpful
    (SELECT ORIGINAL_BAND, COUNT(RECORD_ID) AS SIGHTINGS
    FROM [dbo].[AllBirdData]
    WHERE SPECIES_ID IN ('1720','2040','3490','1940')
    GROUP BY ORIGINAL_BAND
    ) B
-- by joining these two subqueries we are able to pull out all the data for birds that have at least one route segment we can map
ON A.ORIGINAL_BAND = B.ORIGINAL_BAND
WHERE SIGHTINGS > 1
-- ording the data assists processing later in the pipeline
ORDER BY SPECIES_ID,ORIGINAL_BAND,TRY_CONVERT(DATETIME, EVENT_DATE, 101)

-- there was a slightly modified version of this query that was run for 
-- ORIGINAL_BAND in ('B99369134836','B69466742264')
-- these were hand picked examples as the dataprocessing code struggled
-- with the golden eagle and the canada goose
