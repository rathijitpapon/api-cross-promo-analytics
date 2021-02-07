const mysql = require('mysql');

const config = mysql.createConnection({
    host: process.env.HOST,
    user: process.env.USER_DB,
    password: process.env.PASSWORD,
    database: process.env.DATABASE
});

config.connect(function(err) {
    if (err) 
        console.log(err);
    else
        console.log("Connected!");
});

module.exports = config;