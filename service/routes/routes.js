const express = require('express');
const router = express.Router();
const db = require('../db/connection');


router.post('/game', async (req, res) => {
    const data = req.body;
    db.changeUser({
        database: data.database
    }, (err) => {
        if (err) return res.send({error: err.message});

        let query = `
            select max(appversion) as max_appversion 
            from cross_promo_ad_status
        `;

        db.query(query, (err, result) => {
            if(err) return res.send({error: err.message});

            let max_appversion = 0;
            result.forEach((x) => {
                max_appversion = x.max_appversion;
            });

            query = `
                select
                    c1,
                    sum(case when install_clicked > 0 then 1 else 0 end) as total_install_clicked, 
                    sum(ad_start) as total_ad_start
                from 
                    (select c1, install_clicked, ad_start, appversion 
                    from cross_promo_ad_status limit ${data.limit} offset ${data.offset}) as v
                where
                    v.appversion = ${max_appversion}
                group by
                    c1
            `;

            db.query(query,(err, result) =>{
                if (err) return res.send({error: err.message});
        
                const data = [];
                result.forEach((x) =>{
                    const ctr = {
                        c1: x.c1,
                        ctr: x.total_install_clicked * 100.0 / x.total_ad_start
                    };
                    data.push(ctr);
                });
    
                res.send({
                    latest_version: max_appversion,
                    data: data
                });
            });
        });
    });
})


router.post('/ctr', (req, res) => {
    const data = req.body;
    db.changeUser({
        database: 'dinobattlegp2012'
    }, (err) => {
        if (err) return res.send({error: err.message});

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
        });
    });
})


module.exports = router;
