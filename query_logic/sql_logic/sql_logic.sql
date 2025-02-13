WITH RECURSIVE total_entities AS (
    SELECT c.id AS c_id
    FROM CorporateEntities AS c 
    WHERE c.id = 'C_000091'

    UNION ALL

    -- Recursively select related entities
    SELECT DISTINCT r.entity_from_id
    FROM Relationship r
    JOIN total_entities t ON r.entity_to_id = t.c_id
    WHERE r.entity_from_id IS NOT NULL 
    AND r.entity_from_id NOT LIKE 'P%' -- exclude people (P%)
),

-- Grab people related to the entities found in total_entities
tot_people AS (
    SELECT DISTINCT p.entity_from_id as person_id
    FROM Relationship p
    JOIN total_entities t ON p.entity_to_id = t.c_id
    WHERE p.entity_from_id LIKE 'P%' -- only select people (P%)
),

nationalities AS (
    SELECT p.nationality AS nationality, COUNT(p.nationality) AS counts
    FROM People p
    JOIN tot_people b ON b.person_id = p.id 
    WHERE p.nationality IS NOT NULL
    GROUP BY p.nationality
),

nationalities_null AS (
    SELECT p.nationality
    FROM People p
    JOIN tot_people b ON b.person_id = p.id 
    WHERE p.nationality IS NULL
),

ast_events AS (
    SELECT DISTINCT r.entity_to_id 
    FROM Relationship r
    JOIN tot_people p ON r.entity_from_id = p.person_id
    WHERE r.entity_to_id LIKE 'E%' -- only events (E%)
),

gender_counts AS (
    SELECT p.gender AS genders, COUNT(*) AS count
    FROM People p
    JOIN tot_people g ON g.person_id = p.id
    GROUP BY p.gender
),

corpos AS (
    SELECT DISTINCT r.entity_from_id
    FROM Relationship r
    JOIN total_entities t ON r.entity_from_id = t.c_id
    WHERE r.entity_from_id LIKE 'C%' -- only select corporate entities (C%)
),

christian_tradition AS (
    SELECT p.christian_tradition
    FROM People p
    JOIN tot_people t ON t.person_id = p.id
),


religious AS (
    SELECT p.religious_family
    FROM People p
    JOIN tot_people t ON t.person_id = p.id
),

ast_inst AS(
    SELECT c_id
    FROM total_entities t 
    WHERE c_id LIKE 'N%' OR c_id LIKE 'C%'
),

--religious family vs trad
-- soecifics on denominations

inst_types AS(
    SELECT  i.institution_category AS i_type, COUNT(i.institution_category) AS counts
    FROM Institutions i 
    JOIN ast_inst a ON i.id = c_id
    GROUP BY i.institution_category
),

people_by_year AS (
    -- Count people by gender per year
    SELECT year, 
           SUM(CASE WHEN gender = 'Male' THEN 1 ELSE 0 END) as male_count,
           SUM(CASE WHEN gender = 'Female' THEN 1 ELSE 0 END) as female_count,
           SUM(CASE WHEN gender IS NULL OR gender NOT IN ('Male', 'Female') THEN 1 ELSE 0 END) as unknown_count
    FROM (
        SELECT DISTINCT p.id as person_id,
               p.gender,
               generate_series(r.start_year, r.end_year) as year
        FROM People p
        JOIN tot_people t ON t.person_id = p.id
        JOIN Relationship r ON (t.person_id = r.entity_from_id and r.entity_to_id IN (SELECT * FROM total_entities))
        WHERE p.id IS NOT NULL
            AND r.start_year IS NOT NULL 
            AND r.end_year IS NOT NULL
    ) yearly_presence
    GROUP BY year
    ORDER BY year
),

insts_by_year AS (
    -- Count institutions present per year
    SELECT year, COUNT(inst_id) as insts_count
    FROM (
        SELECT DISTINCT i.c_id as inst_id,
               generate_series(r.start_year, r.end_year) as year
        FROM Relationship r
        JOIN ast_inst i ON (i.c_id = r.entity_from_id and r.entity_to_id IN (SELECT * FROM total_entities))  
	-- making sure that the corp is on the lower level and that higher level ent_2_id in curated few
	-- if get insts from curated, insts therefore also curated. getting all from select few. works (?)
        WHERE r.start_year IS NOT NULL 
            AND r.end_year IS NOT NULL
    ) yearly_presence
    GROUP BY year
    ORDER BY year
),

G_ALL_ENTS AS (
    SELECT DISTINCT r.entity_from_id AS all_g_insts
    FROM Relationship r
    JOIN geographynodes g ON r.entity_to_id = g.id
    WHERE g.province_id = 'O_2'
),

G_INSTS AS (
    SELECT COUNT(*) AS total_g_institutions
    FROM (
        SELECT DISTINCT r.entity_from_id AS all_g_insts
        FROM Relationship r
        JOIN geographynodes g ON r.entity_to_id = g.id
        WHERE g.province_id = 'O_2'
        AND r.entity_from_id LIKE 'N%' 
    ) AS sbqry
),
--what is g?
--general areas
G_EVENTS AS (
    SELECT COUNT(*) AS total_g_events
    FROM (
        SELECT DISTINCT r.entity_from_id AS all_g_insts
        FROM Relationship r
        JOIN geographynodes g ON r.entity_to_id = g.id
        WHERE g.province_id = 'O_2'
        AND r.entity_from_id LIKE 'E%' 
    ) AS sbqry
),
G_CORPS AS (
    SELECT COUNT(*) AS total_g_corps
    FROM (
        SELECT DISTINCT r.entity_from_id AS all_g_insts
        FROM Relationship r
        JOIN geographynodes g ON r.entity_to_id = g.id
        WHERE g.province_id = 'O_2'
        AND r.entity_from_id LIKE 'C%' 
    ) AS sbqry
),--i forgot corpos dont have geo data


G_totpeople AS (
    SELECT DISTINCT p.entity_from_id as person_id
    FROM Relationship p
    JOIN G_ALL_ENTS t ON p.entity_to_id = t.all_g_insts
    WHERE p.entity_from_id LIKE 'P%' -- only select people (P%)
),



--EXAMPLES/EXPLANATIONS

--------------------------------------------------------------------------------------------------------------------------------------------------------

-- Example on how to get total people from a corp. in JS

-- WITH RECURSIVE total_entities AS (
--     -- Select the initial corporate entity (C_001098)
--     SELECT c.id AS c_id
--     FROM CorporateEntities AS c 
--     WHERE c.id = 'C_001098'

--     UNION ALL

--     -- Recursively select related entities
--     SELECT r.entity_from_id
--     FROM Relationship r
--     JOIN total_entities t ON r.entity_to_id = t.c_id
--     WHERE r.entity_from_id IS NOT NULL 
--     AND r.entity_from_id NOT LIKE 'P%' -- exclude people (P%)
-- 	--by excluding people we can prevent relationships from people --> something
-- 	--greta did say this was impossible because of the structure of the database but i am being safe.
-- ),

-- -- Grab people related to the entities found in total_entities
-- tot_people AS (
--     SELECT DISTINCT p.entity_from_id
--     FROM Relationship p
--     JOIN total_entities t ON p.entity_to_id = t.c_id
--     WHERE p.entity_from_id LIKE 'P%' -- only select people (P%)
-- )

-- SELECT COUNT(*) FROM tot_people


--------------------------------------------------------------------------------------------------------------------------------------------------------

--If you wanted to get something like g_nationalities, nationalities of people in an area,
--Simple use the nationalities logic and swap in the G_(geography) equivelant.

g_nationalities AS (
    SELECT p.nationality AS nationality, COUNT(p.nationality) AS counts
    FROM People p
    JOIN G_totpeople b ON b.person_id = p.id 
    WHERE p.nationality IS NOT NULL
    GROUP BY p.nationality
)

SELECT * from g_nationalities






