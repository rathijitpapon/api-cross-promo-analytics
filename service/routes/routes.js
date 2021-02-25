const express = require('express');
const router = express.Router();
const db = require('../db/connection');

const limit = 20000;
const highestUserLevel = 30;
const lowestUserLevel = 0;

router.get('/sourceSink/bucksStatus/getVersions/:database', (req, res) => {
    const dbName = req.params.database;
    db.changeUser({
        database: dbName
    }, (err) => {
        if(err) return res.status(400).send({error: err.message});
        const query = `
            select
                distinct(appversion) as app_version
            from
                inapp
        `;
        db.query(query, (err, result) => {
            if (err) {
                return res.status(400).send({error: err.message});
            }
            const arr = [];
            result.forEach(item => {
                arr.push(Number(item.app_version));
            });
            arr.sort((a,b) => b - a);
            return res.send(arr);
        })
    })
})

router.post('/sourceSink/bucksStatus/totalSpendAndEarning', (req, res) => {
    const dbName = req.body.db;
    const upperLimit = req.body.upperLimit;
    const lowerLimit = req.body.lowerLimit;
    const minHoursBefore = req.body.minTimeSpan;
    const maxHoursBefore = req.body.maxTimeSpan;
    const appVersion = req.body.appVersion;

    let timestamp = new Date();
    timestamp = new Date(timestamp.getTime() - timestamp.getTimezoneOffset() * 60000);
    timestamp = new Date(timestamp.getTime() - minHoursBefore * 60 * 60 * 1000);
    const min_timestamp = new Date(timestamp).toISOString().replace(/T/, ' ').replace(/\..+/, '');

    timestamp = new Date();
    timestamp = new Date(timestamp.getTime() - timestamp.getTimezoneOffset() * 60000);
    timestamp = new Date(timestamp.getTime() - maxHoursBefore * 60 * 60 * 1000);
    const max_timestamp = new Date(timestamp).toISOString().replace(/T/, ' ').replace(/\..+/, '');


    db.changeUser({
        database: dbName
    }, (err) => {
        if (err) return res.status(400).send({error: err.message});

        let versionQuery;
        if(appVersion === 0) {
            versionQuery = 'currentAppVersion >= 0';
        } else {
            let str = '';
            str += appVersion;
            versionQuery = 'currentAppVersion in (' + str + ')';
        }
        const query = `
            select 
                userLevel,
                sum(BucksTotalSpend) as total_spend,
                sum(BucksTotalEarn) as total_earn
            from
                (select 
                    userLevel,BucksTotalSpend,BucksTotalEarn,time_stamp
                from 
                    paid_user_allBuckSpendEvents_battle
                where userLevel <= ${highestUserLevel} and userLevel > ${lowestUserLevel} 
                and ${versionQuery}
                and userLatestBucks between ${lowerLimit} and ${upperLimit}
                limit ${limit}) as t
                where 
                    t.time_stamp >= '${max_timestamp}' and t.time_stamp  <= '${min_timestamp}'
            group by userLevel
        `;
        db.query(query, (err, result) => {
            if (err) return res.status(400).send({error: err.message});
            const userLevel = [], totalBucksSpend = [], totalBucksEarn = [];
            result.forEach((item) => {
                userLevel.push(item.userLevel);
                totalBucksSpend.push(item.total_spend);
                totalBucksEarn.push(item.total_earn);
            });

            return res.send({
                userLevel: userLevel,
                totalBucksEarn: totalBucksEarn,
                totalBucksSpend: totalBucksSpend
            });
        })
    })
})

router.post('/sourceSink/averageAdShowPerSource/:database', (req, res) => {
    const database = req.params.database;
    const reqType = req.body.reqType;
    const hoursBefore = req.body.hoursBefore;

    let left = 0, right = 1000000000000000;
    if (reqType === 'all')
        left = 0, right = 1000000000000000;
    else if (reqType === 'reward')
        left = 0, right = 99999
    else if (reqType === 'int')
        left = 100000, right = 1000000000000000;

    let timestamp = new Date();
    timestamp = new Date(timestamp.getTime() - timestamp.getTimezoneOffset() * 60000);
    timestamp = new Date(timestamp.getTime() - hoursBefore * 60 * 60 * 1000);
    timestamp = new Date(timestamp).toISOString().replace(/T/, ' ').replace(/\..+/, '');

    db.changeUser({
        database
    }, (err) => {
        if (err) return res.status(400).send({error: err.message});

        const query1 = `
            select
                user_level,
                adshow_source,
                sum(ad_show) as total_adShow
            from (
                select 
                    user_level, 
                    adshow_source, 
                    ad_show, 
                    time_stamp, 
                    adid
                from rewarded_ad_status
                where 
                    user_level <= 30 and 
                    user_level > 0 
                limit 20000
            ) as v
            where 
                v.time_stamp >= '${timestamp}' and
                v.adid >= ${left} and v.adid <= ${right}
            group by user_level, adshow_source 
            order by user_level
        `;

        db.query(query1, (err, result1) => {
            if (err) {
                return res.status(500).send({error: err.message});
            }

            const query2 = `
                select
                    user_level,
                    count(distinct udid) as user_count
                from (
                    select 
                        user_level, 
                        udid, 
                        time_stamp, 
                        adid
                    from rewarded_ad_status
                    where 
                        user_level <= 30 and 
                        user_level > 0 
                    limit 20000
                ) as v
                where 
                    v.time_stamp >= '${timestamp}' and
                    v.adid >= ${left} and v.adid <= ${right}
                group by user_level
                order by user_level
            `;

            db.query(query2, (err, result2) => {
                if (err) {
                    return res.status(500).send({error: err.message});
                }

                const userCountInLevel = {};
                const totalAdShowInLevel = {};

                const averageAdShowPerSource = [];
                const userLevel = [];

                result2.forEach((x, ind) => {
                    if (!userLevel.includes(x.user_level)) {
                        userLevel.push(x.user_level);
                        userCountInLevel[x.user_level] = 0;
                        totalAdShowInLevel[x.user_level] = 0;
                    }

                    userCountInLevel[x.user_level] += x.user_count;
                });

                result1.forEach((x, ind) => {
                    totalAdShowInLevel[x.user_level] += x.total_adShow;
                });

                result1.forEach((x, ind) => {
                    const averageAdShow = totalAdShowInLevel[x.user_level] / userCountInLevel[x.user_level];
                    const value = (x.total_adShow / totalAdShowInLevel[x.user_level]) * averageAdShow;

                    averageAdShowPerSource.push({
                        value: +(value).toFixed(2),
                        source: x.adshow_source,
                        level: x.user_level,
                    });
                });

                res.status(200).send({
                    averageAdShowPerSource,
                    userLevel,
                });
            });
        });
    })
});

router.post('/sourceSink/bucksStatus/bucksSpendAndEarning', (req, res) => {
    const database = req.body.db;
    const lowerLimit = req.body.lowerLimit;
    const upperLimit = req.body.upperLimit;
    const minHoursBefore = req.body.minTimeSpan;
    const maxHoursBefore = req.body.maxTimeSpan;
    const appVersion = req.body.appVersion;

    let timestamp = new Date();
    timestamp = new Date(timestamp.getTime() - timestamp.getTimezoneOffset() * 60000);
    timestamp = new Date(timestamp.getTime() - minHoursBefore * 60 * 60 * 1000);
    const min_timestamp = new Date(timestamp).toISOString().replace(/T/, ' ').replace(/\..+/, '');

    timestamp = new Date();
    timestamp = new Date(timestamp.getTime() - timestamp.getTimezoneOffset() * 60000);
    timestamp = new Date(timestamp.getTime() - maxHoursBefore * 60 * 60 * 1000);
    const max_timestamp = new Date(timestamp).toISOString().replace(/T/, ' ').replace(/\..+/, '');

    db.changeUser({
        database: database
    }, (err) => {
        if (err) return res.status(400).send({error: err.message});

        const query = 'SHOW COLUMNS FROM paid_user_allBuckSpendEvents_battle';
        db.query(query, (err, result) => {
            if (err) {
                return res.status(500).send({error: err.message});
            }

            let versionQuery;
            if(appVersion === 0) {
                versionQuery = 'currentAppVersion >= 0';
            } else {
                let str = '';
                str += appVersion;
                versionQuery = 'currentAppVersion in (' + str + ')';
            }

            let spendCol = [];
            let earnCol = [];
            result.forEach((x) => {
                if (x.Field.includes("gae") && x.Field.includes("Spend"))
                    spendCol.push(x.Field)
                else if (x.Field.includes("gae") && x.Field.includes("Earn"))
                    earnCol.push(x.Field);
            });

            let total_earns = [], total_spends = [], average_earns_strings = [], average_spends_strings = [];
            let valuesOf_average_earns = [], valuesOf_average_spends = [];
            let average_earns = {}, average_spends = {};
            let query2 = `select userLevel,count(udid) as total_users`;

            let query1 = `select userLevel,time_stamp, udid`;
            for (let eCol of earnCol) {
                query1 += ', ' + eCol;
                let splitString = eCol.split('Earn');
                total_earns.push(`total_earn_${splitString[1]}`);
                average_earns_strings.push(`averageEarn${splitString[1]}`);
                query2 += `, sum(${eCol}) as total_earn_${splitString[1]}`;
                valuesOf_average_earns.push([]);
            }
            for (let sCol of spendCol) {
                query1 += ', ' + sCol;
                let splitString = sCol.split('Spend');
                total_spends.push(`total_spend_${splitString[1]}`);
                average_spends_strings.push(`averageSpend${splitString[1]}`);
                query2 += `, sum(${sCol}) as total_spend_${splitString[1]}`;
                valuesOf_average_spends.push([]);
            }

            query1 += ` 
            from 
                paid_user_allBuckSpendEvents_battle
            where 
                (${versionQuery}) and
                userLevel <= ${highestUserLevel} and userLevel > ${lowestUserLevel} 
                and userLatestBucks between ${lowerLimit} and ${upperLimit} 
            limit ${limit}`;

            query2 += `
                from (${query1}) as t
                where 
                    t.time_stamp >= '${max_timestamp}' and t.time_stamp  <= '${min_timestamp}'
                group by userLevel`;

            db.query(query2, (err, result) => {
                if (err) return res.status(400).send({error: err.message});
                let userLevel = []
                result.forEach((item, index) => {
                    userLevel.push(item.userLevel);
                    total_earns.forEach((x, i) => {
                        valuesOf_average_earns[i].push((item[x]/item.total_users).toFixed(2));
                    });
                    total_spends.forEach((x, i) => {
                        valuesOf_average_spends[i].push((-item[x]/item.total_users).toFixed(2));
                    });
                });
                average_earns_strings.forEach((x, i) => {
                    average_earns[x] = valuesOf_average_earns[i];
                });
                average_spends_strings.forEach((x, i) => {
                    average_spends[x] = valuesOf_average_spends[i];
                });
                return res.send({
                    userLevels: userLevel,
                    averageEarns: average_earns,
                    averageSpends: average_spends
                });
            });
        });
    })
})

router.post('/sourceSink/averageBucksSpendAndEarning/:database', (req, res) => {
    const database = req.params.database;
    const upperLimit = req.body.upperLimit;
    const lowerLimit = req.body.lowerLimit;
    const minHoursBefore = req.body.minTimeSpan;
    const maxHoursBefore = req.body.maxTimeSpan;
    const appVersion = req.body.appVersion;

    let timestamp = new Date();
    timestamp = new Date(timestamp.getTime() - timestamp.getTimezoneOffset() * 60000);
    timestamp = new Date(timestamp.getTime() - minHoursBefore * 60 * 60 * 1000);
    const min_timestamp = new Date(timestamp).toISOString().replace(/T/, ' ').replace(/\..+/, '');

    timestamp = new Date();
    timestamp = new Date(timestamp.getTime() - timestamp.getTimezoneOffset() * 60000);
    timestamp = new Date(timestamp.getTime() - maxHoursBefore * 60 * 60 * 1000);
    const max_timestamp = new Date(timestamp).toISOString().replace(/T/, ' ').replace(/\..+/, '');


    db.changeUser({
        database
    }, (err) => {
        if (err) return res.status(400).send({error: err.message});

        let versionQuery;
        if(appVersion === 0) {
            versionQuery = 'currentAppVersion >= 0';
        } else {
            let str = '';
            str += appVersion;
            versionQuery = 'currentAppVersion in (' + str + ')';
        }
        const query = `
            select
                userLevel, 
                count(distinct udid) as user_count,
                sum(BucksTotalSpend) as bucksTotalSpend,
                sum(BucksTotalEarn) as bucksTotalEarn
            from (
                select userLevel, udid, BucksTotalSpend, BucksTotalEarn, time_stamp
                from paid_user_allBuckSpendEvents_battle
                where 
                    ${versionQuery} and
                    userLevel <= 30 and 
                    userLevel > 0 and 
                    userLatestBucks between ${lowerLimit} and ${upperLimit} 
                    limit 20000
            ) as v
            where 
                v.time_stamp >= '${max_timestamp}' and v.time_stamp <= '${min_timestamp}'
            group by userLevel
        `;

        db.query(query, (err, result) => {
            if (err) {
                return res.status(500).send({error: err.message});
            }

            const averageBucksSpend = [];
            const averageBucksEarn = [];
            const userLevel = [];
            result.forEach((x, ind) => {
                averageBucksSpend.push(+((x.bucksTotalSpend / x.user_count).toFixed(2)));
                averageBucksEarn.push(+((x.bucksTotalEarn / x.user_count).toFixed(2)));
                userLevel.push(x.userLevel);
            });

            for (let i = 1; i < averageBucksSpend.length; i++) {
                averageBucksSpend[i] += averageBucksSpend[i - 1];
            }
            for (let i = 1; i < averageBucksEarn.length; i++) {
                averageBucksEarn[i] += averageBucksEarn[i - 1];
            }

            res.status(200).send({
                averageBucksSpend,
                averageBucksEarn,
                userLevel,
            });
        });
    })
})

router.post('/sourceSink/bucksStatus', (req, res) => {
    const dbName = req.body.db;
    const upperLimit = req.body.upperLimit;
    const lowerLimit = req.body.lowerLimit;
    const minHoursBefore = req.body.minTimeSpan;
    const maxHoursBefore = req.body.maxTimeSpan;
    const appVersion = req.body.appVersion;

    let timestamp = new Date();
    timestamp = new Date(timestamp.getTime() - timestamp.getTimezoneOffset() * 60000);
    timestamp = new Date(timestamp.getTime() - minHoursBefore * 60 * 60 * 1000);
    const min_timestamp = new Date(timestamp).toISOString().replace(/T/, ' ').replace(/\..+/, '');

    timestamp = new Date();
    timestamp = new Date(timestamp.getTime() - timestamp.getTimezoneOffset() * 60000);
    timestamp = new Date(timestamp.getTime() - maxHoursBefore * 60 * 60 * 1000);
    const max_timestamp = new Date(timestamp).toISOString().replace(/T/, ' ').replace(/\..+/, '');
    db.changeUser({
        database: dbName
    }, (err) => {
        if (err) return res.status(400).send({error: err.message});
        let versionQuery;
        if(appVersion === 0) {
            versionQuery = 'currentAppVersion >= 0';
        } else {
            let str = '';
            str += appVersion;
            versionQuery = 'currentAppVersion in (' + str + ')';
        }
        const query = `
            select
                userLevel,
                count(udid) as total_users,
                 sum(userLatestBucks) as total_bucks
            from
                (select 
                    userLevel, udid, userLatestBucks,time_stamp
                from 
                    paid_user_allBuckSpendEvents_battle
                where 
                    userLevel <= ${highestUserLevel} and userLevel > ${lowestUserLevel} 
                    and (${versionQuery})
                    and userLatestBucks between ${lowerLimit} and ${upperLimit} limit ${limit}) as t
            where
                t.time_stamp >= '${max_timestamp}' and t.time_stamp <= '${min_timestamp}'
            group by userLevel
        `;
        db.query(query, (err, result) => {
            if (err) {
                return res.status(400).send({error: err.message});
            }
            const userLevel = [], averageBucks = [];
            result.forEach((item) => {
                userLevel.push(item.userLevel);
                averageBucks.push((item.total_bucks / item.total_users).toFixed(2));
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
        if (err) return res.status(400).send({error: err.message});
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
                from cross_promo_ad_status limit ${limit} offset 0) as v
            group by c1
            `;
        db.query(query, (err, result) => {
            if (err) {
                return res.status(400).send({error: err.message});
            }
            let c1_array = [], quit_panel_array = [], more_games_array = [], cross_promo_array = [];
            result.forEach((item) => {
                if (item.c1.length > 2) {
                    c1_array.push(item.c1);
                    if (item.total_ad_start_in_quit_panel === 0) quit_panel_array.push(0);
                    else quit_panel_array.push(item.total_install_clicked_in_quit_panel * 100.0 / item.total_ad_start_in_quit_panel);
                    if (item.total_ad_start_in_more_games === 0) more_games_array.push(0);
                    else more_games_array.push(item.total_install_clicked_in_more_games * 100.0 / item.total_ad_start_in_more_games);
                    if (item.total_ad_start_in_cross_promo === 0) cross_promo_array.push(0);
                    cross_promo_array.push(item.total_install_clicked_in_cross_promo * 100.0 / item.total_ad_start_in_cross_promo);
                }
            });
            return res.send({
                c1: c1_array,
                ctr_in_quit_panel: quit_panel_array,
                ctr_in_more_games: more_games_array,
                ctr_in_cross_promo: cross_promo_array
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
                from cross_promo_ad_status limit ${limit} offset 0) as v
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

        db.query(query, (err, result) => {
            if (err) return res.status(500).send({error: err.message});

            let ctr = 0;
            result.forEach((x) => {
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
