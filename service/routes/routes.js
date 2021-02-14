const express = require('express');
const router = express.Router();
const db = require('../db/connection');

router.post('/sourceSink/bucksStatus', (req, res) => {
    const dbName = req.body.db;
    const upperLimit = req.body.upperLimit;
    const lowerLimit = req.body.lowerLimit;
    db.changeUser({
        database: dbName
    }, (err) => {
        if(err) return res.status(400).send({error: err.message});
        const query = `
            select
                userLevel,
                count(udid) as total_users,
                 sum(userLatestBucks) as total_bucks
            from
                (select userLevel, udid, userLatestBucks from paid_user_allBuckSpendEvents_battle
                 where userLevel <= 40 and userLevel > 0 
                 and userLatestBucks between ${lowerLimit} and ${upperLimit} limit 20000) as t
            group by userLevel
        `;
        db.query(query, (err, result) =>{
            if(err) {
                return res.status(400).send({error:err.message});
            }
            console.log(result);
            const userLevel = [], averageBucks = [];
            result.forEach((item) => {
                userLevel.push(item.userLevel);
                averageBucks.push(item.total_bucks/item.total_users);
            })
            console.log({
                userLevels: userLevel,
                averageBucks: averageBucks
            });
            return res.send({
                userLevels: userLevel,
                averageBucks: averageBucks
            });
        })
    })
})

router.get('/ctrwrtsrc/:database', (req, res) => {
    db.changeUser({
        database: req.params.database
    }, (err) => {
        if(err) return res.status(400).send({error: err.message});
        const query = `
            select 
                c1,
                sum(case when c2 = 'QuitPromoPanel' then ad_start else 0 end) as total_ad_start_in_quit_panel,
                sum(case when c2 = 'MoreGamesPanel' then ad_start else 0 end) as total_ad_start_in_more_games,
                sum(case when c2 = 'CrossPromo' then ad_start else 0 end) as total_ad_start_in_cross_promo,
                sum(case when c2 = 'QuitPromoPanel' and install_clicked > 0 then 1 else 0 end) as total_install_clicked_in_quit_panel,
                sum(case when c2 = 'MoreGamesPanel' and install_clicked > 0 then 1 else 0 end) as total_install_clicked_in_more_games,
                sum(case when c2 = 'CrossPromo' and install_clicked > 0 then 1 else 0 end) as total_install_clicked_in_cross_promo
            from 
                (select c1, c2, ad_start, install_clicked
                from cross_promo_ad_status limit 20000 offset 0) as v
            group by c1
            `;
        db.query(query, (err, result) =>{
            if(err) {
                return res.status(400).send({error: err.message});
            }
            let c1_array =[], quit_panel_array = [], more_games_array = [], cross_promo_array = [];
            result.forEach((item) =>{
                if(item.c1.length > 2){
                    c1_array.push(item.c1);
                    if(item.total_ad_start_in_quit_panel === 0) quit_panel_array.push(0);
                    else quit_panel_array.push(item.total_install_clicked_in_quit_panel * 100.0 / item.total_ad_start_in_quit_panel);
                    if(item.total_ad_start_in_more_games === 0) more_games_array.push(0);
                    else more_games_array.push(item.total_install_clicked_in_more_games * 100.0 / item.total_ad_start_in_more_games);
                    if(item.total_ad_start_in_cross_promo === 0) cross_promo_array.push(0);
                    cross_promo_array.push(item.total_install_clicked_in_cross_promo * 100.0 / item.total_ad_start_in_cross_promo);
                }
            });
            return res.send({
                c1: c1_array,
                ctr_in_quit_panel: quit_panel_array,
                ctr_in_more_games : more_games_array,
                ctr_in_cross_promo : cross_promo_array
            });
        })
    })
})


router.post('/adCompletion/:database', (req, res) => {
    const data = req.body;
    const database = req.params.database;

    db.changeUser({
        database: database
    }, (err) => {
        if (err) return res.status(400).send(err);
        const query = `
            select 
                c1,
                sum(case when c2 = 'QuitPromoPanel' and adshow_complete = 1 then 1 else 0 end) as total_ad_show_complete_in_quit_panel,
                sum(case when c2 = 'MoreGamesPanel' and adshow_complete = 1 then 1 else 0 end) as total_ad_show_complete_in_more_games,
                sum(case when c2 = 'CrossPromo' and adshow_complete = 1 then 1 else 0 end) as total_ad_show_complete_in_cross_promo
            from 
                (select c1, c2, adshow_complete 
                from cross_promo_ad_status limit 20000 offset 0) as v
            group by c1
            `;
        db.query(query, (err, result) => {
            if (err) {
                return res.status(500).send({error: err.message});
            }
            let c1_array = [], quit_panel_array = [], more_games_array = [], cross_promo_array = [];
            result.forEach((item) => {
                if (item.c1.length > 2) {
                    c1_array.push(item.c1);
                    quit_panel_array.push(item.total_ad_show_complete_in_quit_panel);
                    more_games_array.push(item.total_ad_show_complete_in_more_games);
                    cross_promo_array.push(item.total_ad_show_complete_in_cross_promo);
                }
            });
            return res.status(200).send({
                c1: c1_array,
                total_ad_show_complete_in_quit_panel: quit_panel_array,
                total_ad_show_complete_in_more_games: more_games_array,
                total_ad_show_complete_in_cross_promo: cross_promo_array
            });
        })
    })

})

router.post('/versions/:database', async (req, res) => {
    const database = req.params.database;
    const limit = req.body.limit;
    const offset = req.body.offset;

    db.changeUser({
        database: database
    }, (err) => {
        if (err) return res.status(400).send({error: err.message});

        let query = `
            select 
                appversion, count(distinct udid) as user_count 
            from 
                (select 
                    appversion, udid
                from 
                    cross_promo_ad_status 
                limit ${limit} offset ${offset}
                ) as v
            group by appversion
            order by appversion
        `;

        db.query(query, (err, result) => {
            if (err) return res.status(500).send({error: err.message});

            const data = [];
            result.forEach((x) => {
                const versionData = {
                    version: x.appversion,
                    user_count: x.user_count
                }
                data.push(versionData);
            });

            res.status(200).send({
                data
            });
        });
    });
});

router.post('/otherctr/:database', async (req, res) => {
    const database = req.params.database;
    const version = req.body.version;
    const limit = req.body.limit;
    const offset = req.body.offset;

    db.changeUser({
        database: database
    }, (err) => {
        if (err) return res.status(400).send({error: err.message});

        query = `
            select
                c1,
                sum(case when install_clicked > 0 then 1 else 0 end) as total_install_clicked, 
                sum(ad_start) as total_ad_start
            from 
                (select c1, install_clicked, ad_start, appversion 
                from cross_promo_ad_status limit ${limit} offset ${offset}) as v
            where
                v.appversion = ${version}
            group by
                c1
        `;

        db.query(query, (err, result) => {
            if (err) return res.status(500).send({error: err.message});

            const data = [];
            result.forEach((x) => {
                const ctr = {
                    c1: x.c1,
                    ctr: x.total_install_clicked * 100.0 / x.total_ad_start
                };
                data.push(ctr);
            });

            res.status(200).send({
                data: data
            });
        });
    });
});

router.post('/thisctr/:database', async (req, res) => {
    const database = req.params.database;
    const limit = req.body.limit;
    const offset = req.body.offset;
    const c1 = req.body.c1;

    db.changeUser({
        database: database
    }, (err) => {
        if (err) return res.status(400).send({error: err.message});

        query = `
            select
                sum(case when install_clicked > 0 then 1 else 0 end) as total_install_clicked, 
                sum(ad_start) as total_ad_start
            from 
                (select c1, install_clicked, ad_start, appversion 
                from cross_promo_ad_status limit ${limit} offset ${offset}) as v
            where
                v.c1 = '${c1}'
        `;

        db.query(query,(err, result) =>{
            if (err) return res.status(500).send({error: err.message});

            let ctr = 0;
            result.forEach((x) =>{
                ctr = x.total_install_clicked * 100.0 / x.total_ad_start
            });

            res.status(200).send({
                ctr: ctr
            });
        });
    });
});


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
