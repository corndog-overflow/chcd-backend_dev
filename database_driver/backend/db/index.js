const { Pool } = require('pg');

const pool = new Pool({
    user: "dev",
    host: "98.81.238.63",
    database: "chcd",
    password: "riccimorrison",
    port: 5432,
});

module.exports = pool;