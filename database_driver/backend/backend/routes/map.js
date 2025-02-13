const express = require('express');
const router = express.Router();
const mapController = require('../controllers/mapController');

router.get('/fetchResults', mapController.fetchResults);

module.exports = router;