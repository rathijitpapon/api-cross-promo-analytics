const mysql = require('mysql');
require('dotenv').config()

const config = mysql.createConnection({
    host: process.env.HOST,
    user: process.env.USER_DB,
    password: process.env.PASSWORD
});


config.connect((err) => {
    if (err) {
        console.log("abcd");
        console.log(err);
    } else {
        console.log("Connected!");
    }
});

config.changeUser({
    database: process.env.DATABASE
}, (err) => {
    if (err) {
        console.log('abcde');
        return console.log(err);
    }
    console.log('here');
    const query = `select c1, sum(case when install_clicked > 0 then 1 else 0 end) as total_install_clicked, sum(ad_start) as total_ad_start
 from (select c1,install_clicked,ad_start,appversion from cross_promo_ad_status limit 20000 offset 0) as v1
 inner join (select max(appversion) as appver from (select appversion from cross_promo_ad_status limit 20000 offset 0) as t) as v2
  on v1.appversion = v2.appver group by c1`;
    config.query(query, (err, res) => {
        if (err) return console.log(err);
        console.log(res);
        const query2 = `select max(appversion) as max_appversion from (select appversion from cross_promo_ad_status limit 20000 offset 0) as t`;
        config.query(query2,(err,result) => {
            if(err) return console.log(err)
            console.log(result);
        })
    });

});


module.exports = config;
