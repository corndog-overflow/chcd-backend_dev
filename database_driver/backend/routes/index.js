const express = require('express');
const router = express.Router();
const apiController = require('../controllers/apiController');

router.get('/network-results', apiController.fetchNetworkResults);
router.get('/network-confines', apiController.fetchNetworkConfines);
router.get('/map-indexes', apiController.fetchMapIndexes);
router.get('/results', apiController.fetchResults);
router.get('/search', apiController.fetchSearch);
router.get('/db-wide', apiController.fetchDBWide);
router.get('/corporate-entities-data', apiController.fetchCorporateEntitiesData);
router.get('/institutions-data', apiController.fetchInstitutionsData);
router.get('/geography-data', apiController.fetchGeographyData);
router.get('/corp-options', apiController.fetchCorpOptions);
router.get('/inst-options', apiController.fetchInstOptions);
router.get('/geo-options', apiController.fetchGeoOptions);
router.get('/total-people/:node', apiController.total_people);

module.exports = router;