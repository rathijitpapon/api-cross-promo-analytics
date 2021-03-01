const {getData} = require('../data');

const error = {
    statusCode: 404,
    STATUS: 'NOT Found',
    data: {}
}

const getResult = (sessionID) => {
    const data = getData(sessionID);
    if (!data) 
        return error;
    return data;
}

module.exports = {
    getResult,
}