const { v4: uuidv4 } = require('uuid');

const sessionData = {};

const generateID = () => {
    const id = uuidv4();
    return id.toString();
}

const saveData = (id, data) => {
    sessionData[id] = data;
}

const getData = (id) => {
    if (!sessionData[id])
        return false;
    return sessionData[id];
}

const deleteData = (id) => {
    delete sessionData[id];
}

module.exports = {
    generateID,
    saveData,
    getData,
    deleteData,
}