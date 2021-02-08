const express = require('express');
const router = express.Router();
const db = require('../db/connection');


router.post('/game', (req, res) => {
    const data = req.body;
    db.changeUser({
        database: data.database
    }, (err) => {
        if(err)
            return console.log(err);
        const query = `select c1, sum(case when install_clicked > 0 then 1 else 0 end) as total_install_clicked, sum(ad_start) as total_ad_start
        from (select c1,install_clicked,ad_start,appversion from cross_promo_ad_status limit ${data.limit} offset ${data.offset}) as v1
        inner join (select max(appversion) as max_appversion from (select appversion from cross_promo_ad_status limit ${data.limit} offset ${data.offset}) as t) as v2
        on v1.appversion = v2.max_appversion group by c1`;
        db.query(query,(err, result) =>{
            if(err) return console.log(err);

            const ctr = {};
            result.forEach((x) =>{
                if(x.c1.length > 3)
                    ctr[x.c1] = x.total_install_clicked * 100.0 / x.total_ad_start;
            });
            let version = 0;
            query2 = `select max(appversion) as max_appversion from (select appversion from cross_promo_ad_status limit ${data.limit} offset ${data.offset}) as t`;
            db.query(query2,(err,result) => {
                if(err) return console.log(err);
                version = result[0].max_appversion;
                console.log(version);
                res.send({
                    version: version,
                    ctr: ctr
                });
            });
        })
    });
})


router.post('/ctr', (req, res) => {
    const data = req.body;

    const query = "select appversion,c1, sum(case when install_clicked > 0 then 1 else 0 end) as total_install,sum(case when ad_start > 0 then 1 else 0 end) as total_ad from (select c1,install_clicked,ad_start,appversion from cross_promo_ad_status limit " + data.count + " offset " + data.offset + ") as subquery group by appversion,c1";

    db.query(query, function (err, results) {
        if (err) return res.send({error: err.message});

        const result = [], data1 = [], data2 = [];
        results.map(r => {
            if (r.c1.length > 2)
                result.push(r)
        })

        result.map((entry, ind) => {
            if (ind === 0) {
                data1.push({
                    app_version: entry.appversion,
                    cross_promos: []
                })
            }

            ind = data1.length
            for (let i = 0; i < data1.length; i++) {
                if (data1[i].app_version === entry.appversion) {
                    ind = i
                    break
                }

                if (i === data1.length - 1) {
                    data1.push({
                        app_version: entry.appversion,
                        cross_promos: []
                    })
                }
            }

            data1[ind]['cross_promos'].push({
                c1: entry.c1,
                total_ad_start: entry.total_ad,
                total_install_clicked: entry.total_install
            })
        })

        result.map((entry, ind) => {
            if (ind === 0) {
                data2.push({
                    c1: entry.c1,
                    app_versions: []
                })
            }

            ind = data2.length
            for (let i = 0; i < data2.length; i++) {
                if (data2[i].c1 === entry.c1) {
                    ind = i
                    break
                }
                if (i === data2.length - 1) {
                    data2.push({
                        c1: entry.c1,
                        app_versions: []
                    })
                }
            }

            data2[ind]['app_versions'].push({
                v: entry.appversion,
                total_ad_start: entry.total_ad,
                total_install_clicked: entry.total_install
            })
        })

        res.send({
            v: data1,
            c1: data2,
        })
    })
})


module.exports = router;
