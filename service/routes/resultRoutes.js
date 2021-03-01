const express = require('express');
const router = express.Router();

const resultControllers = require('../controllers/resultControllers');

router.post('/data', (req, res) => {
    const sessionID = req.body.sessionID;
    
    const data = resultControllers.getResult(sessionID);
    res.send(data);
});

module.exports = router;