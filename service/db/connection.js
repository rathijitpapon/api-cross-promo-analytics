const mysql = require('mysql');
require('dotenv').config()

const config = mysql.createConnection({
    host: process.env.HOST,
    user: process.env.USER_DB,
    password: process.env.PASSWORD
});


config.connect((err) => {
    if (err) {
        console.log(err);
    } else {
        console.log("Connected!");
    }
});

module.exports = config;
