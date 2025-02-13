const pool = require('../db');

// Fetch Network Results
exports.fetchNetworkResults = async (req, res) => {
    try {
        const results = await pool.query('SELECT * FROM network_results');
        res.json(results.rows);
    } catch (error) {
        console.error('Error fetching network results:', error);
        res.status(500).json({ error: 'Internal Server Error' });
    }
};

// Fetch Network Confines
exports.fetchNetworkConfines = async (req, res) => {
    try {
        const results = await pool.query('SELECT * FROM network_confines');
        res.json(results.rows);
    } catch (error) {
        console.error('Error fetching network confines:', error);
        res.status(500).json({ error: 'Internal Server Error' });
    }
};

// Fetch Map Indexes
exports.fetchMapIndexes = async (req, res) => {
    try {
        const results = await pool.query('SELECT * FROM map_indexes');
        res.json(results.rows);
    } catch (error) {
        console.error('Error fetching map indexes:', error);
        res.status(500).json({ error: 'Internal Server Error' });
    }
};

// Fetch Results
exports.fetchResults = async (req, res) => {
    try {
        const results = await pool.query('SELECT * FROM results');
        res.json(results.rows);
    } catch (error) {
        console.error('Error fetching results:', error);
        res.status(500).json({ error: 'Internal Server Error' });
    }
};

// Fetch Search
exports.fetchSearch = async (req, res) => {
    try {
        const searchProp = req.query.search;
        const results = await pool.query(`
            SELECT DISTINCT *
            FROM (
                SELECT 'Person' AS label, p.id, pgroonga_score(tableoid, ctid) AS score
                FROM People p
                WHERE ARRAY [p.full_text_name_content, p.full_text_search_content] &@~
                      ($1, ARRAY [5, 1], ARRAY [NULL, 'scorer_tf_at_most($index, 0.5)'], 'pgroonga_memos_index')::pgroonga_full_text_search_condition_with_scorers
                UNION ALL
                SELECT 'Institution' AS label, i.id, pgroonga_score(tableoid, ctid) AS score
                FROM Institutions i
                WHERE ARRAY [i.full_text_name_content, i.full_text_search_content] &@~
                      ($1, ARRAY [5, 1], ARRAY [NULL, 'scorer_tf_at_most($index, 0.5)'], 'pgroonga_memos_index')::pgroonga_full_text_search_condition_with_scorers
                UNION ALL
                SELECT 'CorporateEntity' AS label, c.id, pgroonga_score(tableoid, ctid) AS score
                FROM CorporateEntities c
                WHERE ARRAY [c.full_text_name_content, c.full_text_search_content] &@~
                      ($1, ARRAY [5, 1], ARRAY [NULL, 'scorer_tf_at_most($index, 0.5)'], 'pgroonga_memos_index')::pgroonga_full_text_search_condition_with_scorers
                UNION ALL
                SELECT 'Event' AS label, e.id, pgroonga_score(tableoid, ctid) AS score
                FROM Events e
                WHERE ARRAY [e.full_text_name_content, e.full_text_search_content] &@~
                      ($1, ARRAY [5, 1], ARRAY [NULL, 'scorer_tf_at_most($index, 0.5)'], 'pgroonga_memos_index')::pgroonga_full_text_search_condition_with_scorers
                UNION ALL
                SELECT 'Publication' AS label, p.id, pgroonga_score(tableoid, ctid) AS score
                FROM Publications p
                WHERE ARRAY [p.full_text_name_content, p.full_text_search_content] &@~
                      ($1, ARRAY [5, 1], ARRAY [NULL, 'scorer_tf_at_most($index, 0.5)'], 'pgroonga_memos_index')::pgroonga_full_text_search_condition_with_scorers
            ) as unioned
            ORDER BY score DESC
            LIMIT 1000;
        `, [searchProp]);
        res.json(results.rows);
    } catch (error) {
        console.error('Error fetching search results:', error);
        res.status(500).json({ error: 'Internal Server Error' });
    }
};

// Fetch DB Wide
exports.fetchDBWide = async (req, res) => {
    try {
        const results = await pool.query('SELECT * FROM db_wide');
        res.json(results.rows);
    } catch (error) {
        console.error('Error fetching DB wide results:', error);
        res.status(500).json({ error: 'Internal Server Error' });
    }
};

// Fetch Corporate Entities Data
exports.fetchCorporateEntitiesData = async (req, res) => {
    try {
        const results = await pool.query('SELECT * FROM corporate_entities_data');
        res.json(results.rows);
    } catch (error) {
        console.error('Error fetching corporate entities data:', error);
        res.status(500).json({ error: 'Internal Server Error' });
    }
};

// Fetch Institutions Data
exports.fetchInstitutionsData = async (req, res) => {
    try {
        const results = await pool.query('SELECT * FROM institutions_data');
        res.json(results.rows);
    } catch (error) {
        console.error('Error fetching institutions data:', error);
        res.status(500).json({ error: 'Internal Server Error' });
    }
};

// Fetch Geography Data
exports.fetchGeographyData = async (req, res) => {
    try {
        const results = await pool.query('SELECT * FROM geography_data');
        res.json(results.rows);
    } catch (error) {
        console.error('Error fetching geography data:', error);
        res.status(500).json({ error: 'Internal Server Error' });
    }
};

// Fetch Corp Options
exports.fetchCorpOptions = async (req, res) => {
    try {
        const results = await pool.query('SELECT * FROM corp_options');
        res.json(results.rows);
    } catch (error) {
        console.error('Error fetching corp options:', error);
        res.status(500).json({ error: 'Internal Server Error' });
    }
};

// Fetch Inst Options
exports.fetchInstOptions = async (req, res) => {
    try {
        const results = await pool.query('SELECT * FROM inst_options');
        res.json(results.rows);
    } catch (error) {
        console.error('Error fetching inst options:', error);
        res.status(500).json({ error: 'Internal Server Error' });
    }
};

// Fetch Geo Options
exports.fetchGeoOptions = async (req, res) => {
    try {
        const results = await pool.query('SELECT * FROM geo_options');
        res.json(results.rows);
    } catch (error) {
        console.error('Error fetching geo options:', error);
        res.status(500).json({ error: 'Internal Server Error' });
    }
};

// Fetch Total People
exports.fetchTotalPeople = async (req, res) => {
    try {
        const results = await pool.query('SELECT COUNT(*) FROM People');
        res.json(results.rows);
    } catch (error) {
        console.error('Error fetching total people:', error);
        res.status(500).json({ error: 'Internal Server Error' });
    }
};

// Fetch Total Institutions
exports.fetchTotalInstitutions = async (req, res) => {
    try {
        const results = await pool.query('SELECT COUNT(*) FROM Institutions');
        res.json(results.rows);
    } catch (error) {
        console.error('Error fetching total institutions:', error);
        res.status(500).json({ error: 'Internal Server Error' });
    }
};

// Fetch Total Relationships
exports.fetchTotalRelationships = async (req, res) => {
    try {
        const results = await pool.query('SELECT COUNT(*) FROM Relationships');
        res.json(results.rows);
    } catch (error) {
        console.error('Error fetching total relationships:', error);
        res.status(500).json({ error: 'Internal Server Error' });
    }
};

// Fetch Total Events
exports.fetchTotalEvents = async (req, res) => {
    try {
        const results = await pool.query('SELECT COUNT(*) FROM Events');
        res.json(results.rows);
    } catch (error) {
        console.error('Error fetching total events:', error);
        res.status(500).json({ error: 'Internal Server Error' });
    }
};

// Fetch Total Publications
exports.fetchTotalPublications = async (req, res) => {
    try {
        const results = await pool.query('SELECT COUNT(*) FROM Publications');
        res.json(results.rows);
    } catch (error) {
        console.error('Error fetching total publications:', error);
        res.status(500).json({ error: 'Internal Server Error' });
    }
};

// Fetch Total Nodes
exports.fetchTotalNodes = async (req, res) => {
    try {
        const results = await pool.query('SELECT COUNT(*) FROM Nodes');
        res.json(results.rows);
    } catch (error) {
        console.error('Error fetching total nodes:', error);
        res.status(500).json({ error: 'Internal Server Error' });
    }
};

// Fetch Total Corporate Entities
exports.fetchTotalCorporateEntities = async (req, res) => {
    try {
        const results = await pool.query('SELECT COUNT(*) FROM CorporateEntities');
        res.json(results.rows);
    } catch (error) {
        console.error('Error fetching total corporate entities:', error);
        res.status(500).json({ error: 'Internal Server Error' });
    }
};

// Fetch Genders
exports.fetchGenders = async (req, res) => {
    try {
        const results = await pool.query('SELECT DISTINCT gender FROM People');
        res.json(results.rows);
    } catch (error) {
        console.error('Error fetching genders:', error);
        res.status(500).json({ error: 'Internal Server Error' });
    }
};

// Fetch Provinces
exports.fetchProvinces = async (req, res) => {
    try {
        const results = await pool.query('SELECT DISTINCT province FROM Locations');
        res.json(results.rows);
    } catch (error) {
        console.error('Error fetching provinces:', error);
        res.status(500).json({ error: 'Internal Server Error' });
    }
};

// Fetch Prefectures
exports.fetchPrefectures = async (req, res) => {
    try {
        const results = await pool.query('SELECT DISTINCT prefecture FROM Locations');
        res.json(results.rows);
    } catch (error) {
        console.error('Error fetching prefectures:', error);
        res.status(500).json({ error: 'Internal Server Error' });
    }
};

// Fetch Counties
exports.fetchCounties = async (req, res) => {
    try {
        const results = await pool.query('SELECT DISTINCT county FROM Locations');
        res.json(results.rows);
    } catch (error) {
        console.error('Error fetching counties:', error);
        res.status(500).json({ error: 'Internal Server Error' });
    }
};

// Fetch Nationality
exports.fetchNationality = async (req, res) => {
    try {
        const results = await pool.query('SELECT DISTINCT nationality FROM People');
        res.json(results.rows);
    } catch (error) {
        console.error('Error fetching nationality:', error);
        res.status(500).json({ error: 'Internal Server Error' });
    }
};

// Fetch Nationality Null
exports.fetchNationalityNull = async (req, res) => {
    try {
        const results = await pool.query('SELECT COUNT(*) FROM People WHERE nationality IS NULL');
        res.json(results.rows);
    } catch (error) {
        console.error('Error fetching nationality null:', error);
        res.status(500).json({ error: 'Internal Server Error' });
    }
};

// Fetch Christian Tradition
exports.fetchChristianTradition = async (req, res) => {
    try {
        const results = await pool.query('SELECT DISTINCT christian_tradition FROM Nodes');
        res.json(results.rows);
    } catch (error) {
        console.error('Error fetching christian tradition:', error);
        res.status(500).json({ error: 'Internal Server Error' });
    }
};

// Fetch Religious Families
exports.fetchReligiousFamilies = async (req, res) => {
    try {
        const results = await pool.query('SELECT DISTINCT religious_family FROM Nodes');
        res.json(results.rows);
    } catch (error) {
        console.error('Error fetching religious families:', error);
        res.status(500).json({ error: 'Internal Server Error' });
    }
};

// Fetch Christian Tradition Null Values
exports.fetchChristianTraditionNullValues = async (req, res) => {
    try {
        const results = await pool.query('SELECT COUNT(*) FROM Nodes WHERE christian_tradition IS NULL');
        res.json(results.rows);
    } catch (error) {
        console.error('Error fetching christian tradition null values:', error);
        res.status(500).json({ error: 'Internal Server Error' });
    }
};

// Fetch Religious Family Null Values
exports.fetchReligiousFamilyNullValues = async (req, res) => {
    try {
        const results = await pool.query('SELECT COUNT(*) FROM Nodes WHERE religious_family IS NULL');
        res.json(results.rows);
    } catch (error) {
        console.error('Error fetching religious family null values:', error);
        res.status(500).json({ error: 'Internal Server Error' });
    }
};

// Fetch PAff Index
exports.fetchPAffIndex = async (req, res) => {
    try {
        const inputValuePAff = req.query.inputValuePAff;
        const results = await pool.query(`
            SELECT DISTINCT 'affiliation' AS type, name_western AS value, name_western AS label, chinese_name_hanzi AS zh
            FROM CorporateEntities ce
            WHERE corporate_entity_category = 'Religious Body'
            AND ARRAY [ce.full_text_name_content, ce.full_text_search_content] &@~
                ($1, ARRAY [5, 1], ARRAY [NULL, 'scorer_tf_at_most($index, 0.5)'], 'pgroonga_memos_index')::pgroonga_full_text_search_condition_with_scorers
        `, [inputValuePAff]);
        res.json(results.rows);
    } catch (error) {
        console.error('Error fetching PAff index:', error);
        res.status(500).json({ error: 'Internal Server Error' });
    }
};

// Fetch Nat Index
exports.fetchNatIndex = async (req, res) => {
    try {
        const inputValueNat = req.query.inputValueNat;
        const results = await pool.query(`
            SELECT DISTINCT 'affiliation' AS type, nationality AS value, nationality AS label
            FROM People p
            WHERE ARRAY [p.full_text_name_content, p.full_text_search_content] &@~
                ($1, ARRAY [5, 1], ARRAY [NULL, 'scorer_tf_at_most($index, 0.5)'], 'pgroonga_memos_index')::pgroonga_full_text_search_condition_with_scorers
        `, [inputValueNat]);
        res.json(results.rows);
    } catch (error) {
        console.error('Error fetching Nat index:', error);
        res.status(500).json({ error: 'Internal Server Error' });
    }
};

//http://localhost:8888/api/total-people/C_001098
exports.total_people = async (req, res) => {
    const node = req.params.node;

    try {
        const sql = `WITH RECURSIVE total_entities AS (
            -- Select the initial corporate entity
            SELECT c.id AS c_id
            FROM CorporateEntities AS c 
            WHERE c.id = $1
        
            UNION ALL
        
            -- Recursively select related entities
            SELECT r.entity_from_id
            FROM Relationship r
            JOIN total_entities t ON r.entity_to_id = t.c_id
            WHERE r.entity_from_id IS NOT NULL 
            AND r.entity_from_id NOT LIKE 'P%' -- exclude people (P%)
		
        ),
        
        -- Grab people related to the entities found in total_entities
        tot_people AS (
            SELECT DISTINCT p.entity_from_id
            FROM Relationship p
            JOIN total_entities t ON p.entity_to_id = t.c_id
            WHERE p.entity_from_id LIKE 'P%' -- only select people (P%)
        )
        
        SELECT COUNT(*) FROM tot_people`;

        const results = await pool.query(sql, [node]); 
        res.json(results.rows);
    } catch (error) {
        console.error('Error:', error);
        res.status(500).json({ error: 'Internal Server Error' });
    }
};
