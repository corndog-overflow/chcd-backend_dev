const pool = require('../db');

exports.fetchResults = async (req, res) => {
    try {
        const results = await pool.query('SELECT * FROM events');
        res.json(results.rows);
    } catch (error) {
        console.error('Error fetching results:', error);
        res.status(500).json({ error: 'Internal Server Error' });
    }
};