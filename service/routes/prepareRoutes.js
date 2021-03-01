const express = require('express');
const router = express.Router();

const prepareControllers = require('../controllers/prepareControllers');
const {generateID, saveData} = require('../data');

router.post('/ctr', (req, res) => {
    const count = req.body.count;
    const offset = req.body.offset;
    const sessionID = generateID();

    const data = {
        statusCode: 200,
        STATUS: "PENDING",
        data: {},
        sessionID,
    };
    saveData(sessionID, data);

    res.send(data);
    prepareControllers.calculateCTR(count, offset, sessionID);
});

router.post('/thisctr/:database', async (req, res) => {
    const database = req.params.database;
    const limit = req.body.limit;
    const offset = req.body.offset;
    const c1 = req.body.c1;
    const sessionID = generateID();

    const data = {
        statusCode: 200,
        STATUS: "PENDING",
        data: {},
        sessionID,
    };
    saveData(sessionID, data);

    res.send(data);
    prepareControllers.thisCTR(database, limit, offset, c1, sessionID);
});

router.post('/otherctr/:database', async (req, res) => {
    const database = req.params.database;
    const version = req.body.version;
    const limit = req.body.limit;
    const offset = req.body.offset;
    const sessionID = generateID();

    const data = {
        statusCode: 200,
        STATUS: "PENDING",
        data: {},
        sessionID,
    };
    saveData(sessionID, data);

    res.send(data);
    prepareControllers.otherCTR(database, version, limit, offset, sessionID);
});

router.post('/versions/:database', async (req, res) => {
    const database = req.params.database;
    const limit = req.body.limit;
    const offset = req.body.offset;
    const sessionID = generateID();

    const data = {
        statusCode: 200,
        STATUS: "PENDING",
        data: {},
        sessionID,
    };
    saveData(sessionID, data);

    res.send(data);
    prepareControllers.versionsDatabase(database, limit, offset, sessionID);
});

router.post('/adCompletion/:database', (req, res) => {
    const database = req.params.database;
    const sessionID = generateID();

    const data = {
        statusCode: 200,
        STATUS: "PENDING",
        data: {},
        sessionID,
    };
    saveData(sessionID, data);

    res.send(data);
    prepareControllers.adCompletionDatabase(database, sessionID);
});

router.get('/ctrwrtsrc/:database', (req, res) => {
    const database = req.params.database;
    const sessionID = generateID();

    const data = {
        statusCode: 200,
        STATUS: "PENDING",
        data: {},
        sessionID,
    };
    saveData(sessionID, data);

    res.send(data);
    prepareControllers.ctrsrcDatabase(database, sessionID);
})

router.post('/sourceSink/bucksStatus', (req, res) => {
    const database = req.body.database;
    const upperLimit = req.body.upperLimit;
    const lowerLimit = req.body.lowerLimit;
    const minHoursBefore = req.body.minTimeSpan;
    const maxHoursBefore = req.body.maxTimeSpan;
    const sessionID = generateID();

    const data = {
        statusCode: 200,
        STATUS: "PENDING",
        data: {},
        sessionID,
    };
    saveData(sessionID, data);

    res.send(data);
    prepareControllers.bucksStatus(database, upperLimit, lowerLimit, minHoursBefore, maxHoursBefore, sessionID);
})

router.post('/sourceSink/averageBucksSpendAndEarning/:database', (req, res) => {
    const database = req.params.database;
    const upperLimit = req.body.upperLimit;
    const lowerLimit = req.body.lowerLimit;
    const minHoursBefore = req.body.minTimeSpan;
    const maxHoursBefore = req.body.maxTimeSpan;
    const sessionID = generateID();

    const data = {
        statusCode: 200,
        STATUS: "PENDING",
        data: {},
        sessionID,
    };
    saveData(sessionID, data);

    res.send(data);
    prepareControllers.averageBucksSpendAndEarning(database, upperLimit, lowerLimit, minHoursBefore, maxHoursBefore, sessionID);
})

router.post('/sourceSink/bucksStatus/bucksSpendAndEarning', (req, res) => {
    const database = req.body.database;
    const lowerLimit = req.body.lowerLimit;
    const upperLimit = req.body.upperLimit;
    const minHoursBefore = req.body.minTimeSpan;
    const maxHoursBefore = req.body.maxTimeSpan;
    const sessionID = generateID();

    const data = {
        statusCode: 200,
        STATUS: "PENDING",
        data: {},
        sessionID,
    };
    saveData(sessionID, data);

    res.send(data);
    prepareControllers.bucksSpendAndEarningStatus(database, upperLimit, lowerLimit, minHoursBefore, maxHoursBefore, sessionID);
})


router.post('/sourceSink/averageAdShowPerSource/:database', (req, res) => {
    const database = req.params.database;
    const reqType = req.body.reqType;
    const hoursMin = req.body.hoursMin;
    const hoursMax = req.body.hoursMax;
    const sessionID = generateID();

    const data = {
        statusCode: 200,
        STATUS: "PENDING",
        data: {},
        sessionID,
    };
    saveData(sessionID, data);

    res.send(data);
    prepareControllers.averageAdShowPerSource(database, reqType, hoursMin, hoursMax, sessionID);
});

router.post('/sourceSink/averageAdRejectionPerSource/:database', (req, res) => {
    const database = req.params.database;
    const reqType = req.body.reqType;
    const hoursMin = req.body.hoursMin;
    const hoursMax = req.body.hoursMax;
    const sessionID = generateID();

    const data = {
        statusCode: 200,
        STATUS: "PENDING",
        data: {},
        sessionID,
    };
    saveData(sessionID, data);

    res.send(data);
    prepareControllers.averageAdRejectPerSource(database, reqType, hoursMin, hoursMax, sessionID);
});

router.post('/sourceSink/bucksStatus/totalSpendAndEarning', (req, res) => {
    const database = req.body.database;
    const upperLimit = req.body.upperLimit;
    const lowerLimit = req.body.lowerLimit;
    const minHoursBefore = req.body.minTimeSpan;
    const maxHoursBefore = req.body.maxTimeSpan;
    const sessionID = generateID();

    const data = {
        statusCode: 200,
        STATUS: "PENDING",
        data: {},
        sessionID,
    };
    saveData(sessionID, data);

    res.send(data);
    prepareControllers.totalSpendAndEarning(database, upperLimit, lowerLimit, minHoursBefore, maxHoursBefore, sessionID);
})

router.get('/sourceSink/bucksStatus/getVersions/:database', (req, res) => {
    const database = req.params.database;
    const sessionID = generateID();

    const data = {
        statusCode: 200,
        STATUS: "PENDING",
        data: {},
        sessionID,
    };
    saveData(sessionID, data);

    res.send(data);
    prepareControllers.getVersions(database);
})

module.exports = router;