const db = require('../db/connection');

const {saveData, deleteData} = require('../data');

const calculateCTR = (count, offset, sessionID) => {
    db.changeUser({
        database: 'dinobattlegp2012'
    }, (err) => {
        if (err) {
            const data = {
                statusCode: 400,
                STATUS: "ERROR",
                data: err.message,
                sessionID,
            };
            saveData(sessionID, data);
            return;
        };

        const query = "select appversion,c1, sum(case when install_clicked > 0 then 1 else 0 end) as total_install,sum(case when ad_start > 0 then 1 else 0 end) as total_ad from (select c1,install_clicked,ad_start,appversion from cross_promo_ad_status limit " + count + " offset " + offset + ") as subquery group by appversion,c1";

        db.query(query, function (err, results) {
            if (err) {
                const data = {
                    statusCode: 500,
                    STATUS: "ERROR",
                    data: err.message,
                    sessionID,
                };
                saveData(sessionID, data);
                return;
            };

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

            
            const data = {
                statusCode: 200,
                STATUS: "SUCCESS",
                data: {
                    v: data1,
                    c1: data2,
                },
                sessionID,
            };
            saveData(sessionID, data);
            
            setTimeout(() => {
                deleteData(sessionID);
            }, 600000);
        });
    });
}

const thisCTR = (database, limit, offset, c1, sessionID) => {
    db.changeUser({
        database: database
    }, (err) => {
        if (err) {
            const data = {
                statusCode: 400,
                STATUS: "ERROR",
                data: err.message,
                sessionID,
            };
            saveData(sessionID, data);
            return;
        };

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
            if (err) {
                const data = {
                    statusCode: 500,
                    STATUS: "ERROR",
                    data: err.message,
                    sessionID,
                };
                saveData(sessionID, data);
                return;
            };

            let ctr = 0;
            result.forEach((x) => {
                ctr = x.total_install_clicked * 100.0 / x.total_ad_start
            });

            const data = {
                statusCode: 200,
                STATUS: "SUCCESS",
                data: {
                    ctr: ctr
                },
                sessionID,
            };
            saveData(sessionID, data);
            
            setTimeout(() => {
                deleteData(sessionID);
            }, 600000);
        });
    });
};

const otherCTR = (database, version, limit, offset, sessionID) => {
    db.changeUser({
        database: database
    }, (err) => {
        if (err) {
            const data = {
                statusCode: 400,
                STATUS: "ERROR",
                data: err.message,
                sessionID,
            };
            saveData(sessionID, data);
            return;
        };

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
            if (err) {
                const data = {
                    statusCode: 500,
                    STATUS: "ERROR",
                    data: err.message,
                    sessionID,
                };
                saveData(sessionID, data);
                return;
            };

            const output = [];
            result.forEach((x) => {
                const ctr = {
                    c1: x.c1,
                    ctr: x.total_install_clicked * 100.0 / x.total_ad_start
                };
                output.push(ctr);
            });

            const data = {
                statusCode: 200,
                STATUS: "SUCCESS",
                data: {
                    data: output,
                },
                sessionID,
            };
            saveData(sessionID, data);
            
            setTimeout(() => {
                deleteData(sessionID);
            }, 600000);
        });
    });
};

const versionsDatabase = (database, limit, offset, sessionID) => {
    db.changeUser({
        database: database
    }, (err) => {
        if (err) {
            const data = {
                statusCode: 400,
                STATUS: "ERROR",
                data: err.message,
                sessionID,
            };
            saveData(sessionID, data);
            return;
        };

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
            if (err) {
                const data = {
                    statusCode: 500,
                    STATUS: "ERROR",
                    data: err.message,
                    sessionID,
                };
                saveData(sessionID, data);
                return;
            };

            const output = [];
            result.forEach((x) => {
                const versionData = {
                    version: x.appversion,
                    user_count: x.user_count
                }
                output.push(versionData);
            });

            const data = {
                statusCode: 200,
                STATUS: "SUCCESS",
                data: {
                    data: output
                },
                sessionID,
            };
            saveData(sessionID, data);
            
            setTimeout(() => {
                deleteData(sessionID);
            }, 600000);
        });
    });
};

const adCompletionDatabase = (database, sessionID) => {
    db.changeUser({
        database: database
    }, (err) => {
        if (err) {
            const data = {
                statusCode: 400,
                STATUS: "ERROR",
                data: err.message,
                sessionID,
            };
            saveData(sessionID, data);
            return;
        };

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
                const data = {
                    statusCode: 500,
                    STATUS: "ERROR",
                    data: err.message,
                    sessionID,
                };
                saveData(sessionID, data);
                return;
            };

            let c1_array = [], quit_panel_array = [], more_games_array = [], cross_promo_array = [];
            result.forEach((item) => {
                if (item.c1.length > 2) {
                    c1_array.push(item.c1);
                    quit_panel_array.push(item.total_ad_show_complete_in_quit_panel);
                    more_games_array.push(item.total_ad_show_complete_in_more_games);
                    cross_promo_array.push(item.total_ad_show_complete_in_cross_promo);
                }
            });

            const data = {
                statusCode: 200,
                STATUS: "SUCCESS",
                data: {
                    c1: c1_array,
                    total_ad_show_complete_in_quit_panel: quit_panel_array,
                    total_ad_show_complete_in_more_games: more_games_array,
                    total_ad_show_complete_in_cross_promo: cross_promo_array
                },
                sessionID,
            };
            saveData(sessionID, data);
            
            setTimeout(() => {
                deleteData(sessionID);
            }, 600000);
        })
    })
}

const ctrsrcDatabase = (database, sessionID) => {
    db.changeUser({
        database: database
    }, (err) => {
        if (err) {
            const data = {
                statusCode: 400,
                STATUS: "ERROR",
                data: err.message,
                sessionID,
            };
            saveData(sessionID, data);
            return;
        };

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
        db.query(query, (err, result) => {
            if (err) {
                const data = {
                    statusCode: 500,
                    STATUS: "ERROR",
                    data: err.message,
                    sessionID,
                };
                saveData(sessionID, data);
                return;
            };

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

            const data = {
                statusCode: 200,
                STATUS: "SUCCESS",
                data: {
                    c1: c1_array,
                    ctr_in_quit_panel: quit_panel_array,
                    ctr_in_more_games: more_games_array,
                    ctr_in_cross_promo: cross_promo_array
                },
                sessionID,
            };
            saveData(sessionID, data);
            
            setTimeout(() => {
                deleteData(sessionID);
            }, 600000);
        })
    })
}

const bucksStatus = (database, upperLimit, lowerLimit, minHoursBefore, maxHoursBefore, sessionID) => {
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
        if (err) {
            const data = {
                statusCode: 400,
                STATUS: "ERROR",
                data: err.message,
                sessionID,
            };
            saveData(sessionID, data);
            return;
        };

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
                and userLatestBucks between ${lowerLimit} and ${upperLimit} limit ${limit}) as t
            where
                t.time_stamp >= '${max_timestamp}' and t.time_stamp <= '${min_timestamp}'
            group by userLevel
        `;
        db.query(query, (err, result) => {
            if (err) {
                const data = {
                    statusCode: 500,
                    STATUS: "ERROR",
                    data: err.message,
                    sessionID,
                };
                saveData(sessionID, data);
                return;
            };
            
            const userLevel = [], averageBucks = [];
            result.forEach((item) => {
                userLevel.push(item.userLevel);
                averageBucks.push((item.total_bucks / item.total_users).toFixed(2));
            });

            const data = {
                statusCode: 200,
                STATUS: "SUCCESS",
                data: {
                    userLevels: userLevel,
                    averageBucks: averageBucks
                },
                sessionID,
            };
            saveData(sessionID, data);
            
            setTimeout(() => {
                deleteData(sessionID);
            }, 600000);
        })
    })
}

const averageBucksSpendAndEarning = (database, upperLimit, lowerLimit, minHoursBefore, maxHoursBefore, sessionID) => {
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
        if (err) {
            const data = {
                statusCode: 400,
                STATUS: "ERROR",
                data: err.message,
                sessionID,
            };
            saveData(sessionID, data);
            return;
        };

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
                const data = {
                    statusCode: 500,
                    STATUS: "ERROR",
                    data: err.message,
                    sessionID,
                };
                saveData(sessionID, data);
                return;
            };

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

            const data = {
                statusCode: 200,
                STATUS: "SUCCESS",
                data: {
                    averageBucksSpend,
                    averageBucksEarn,
                    userLevel,
                },
                sessionID,
            };
            saveData(sessionID, data);
            
            setTimeout(() => {
                deleteData(sessionID);
            }, 600000);
        });
    })
}

const bucksSpendAndEarningStatus = (database, lowerLimit, upperLimit, minHoursBefore, maxHoursBefore, sessionID) => {
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
        if (err) {
            const data = {
                statusCode: 400,
                STATUS: "ERROR",
                data: err.message,
                sessionID,
            };
            saveData(sessionID, data);
            return;
        };

        const query = 'SHOW COLUMNS FROM paid_user_allBuckSpendEvents_battle';
        db.query(query, (err, result) => {
            if (err) {
                const data = {
                    statusCode: 500,
                    STATUS: "ERROR",
                    data: err.message,
                    sessionID,
                };
                saveData(sessionID, data);
                return;
            };

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
            from paid_user_allBuckSpendEvents_battle
                 where userLevel <= ${highestUserLevel} and userLevel > ${lowestUserLevel} 
                 and userLatestBucks between ${lowerLimit} and ${upperLimit} limit ${limit}`;

            query2 += `
                from (${query1}) as t
                where 
                    t.time_stamp >= '${max_timestamp}' and t.time_stamp  <= '${min_timestamp}'
                group by userLevel`;

            db.query(query2, (err, result) => {
                if (err) {
                    const data = {
                        statusCode: 500,
                        STATUS: "ERROR",
                        data: err.message,
                        sessionID,
                    };
                    saveData(sessionID, data);
                    return;
                };

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

                const data = {
                    statusCode: 200,
                    STATUS: "SUCCESS",
                    data: {
                        userLevels: userLevel,
                        averageEarns: average_earns,
                        averageSpends: average_spends
                    },
                    sessionID,
                };
                saveData(sessionID, data);
                
                setTimeout(() => {
                    deleteData(sessionID);
                }, 600000);
            });
        });
    })
}

const averageAdShowPerSource = (database, reqType, hoursMin, hoursMax, sessionID) => {
    let left = 0, right = 1000000000000000;
    if (reqType === 'all')
        left = 0, right = 1000000000000000;
    else if (reqType === 'reward')
        left = 0, right = 99999
    else if (reqType === 'int')
        left = 100000, right = 1000000000000000;

    let timestampMin = new Date();
    timestampMin = new Date(timestampMin.getTime() - timestampMin.getTimezoneOffset() * 60000);
    timestampMin = new Date(timestampMin.getTime() - hoursMin * 60 * 60 * 1000);
    timestampMin = new Date(timestampMin).toISOString().replace(/T/, ' ').replace(/\..+/, '');

    let timestampMax = new Date();
    timestampMax = new Date(timestampMax.getTime() - timestampMax.getTimezoneOffset() * 60000);
    timestampMax = new Date(timestampMax.getTime() - hoursMax * 60 * 60 * 1000);
    timestampMax = new Date(timestampMax).toISOString().replace(/T/, ' ').replace(/\..+/, '');

    db.changeUser({
        database
    }, (err) => {
        if (err) {
            const data = {
                statusCode: 400,
                STATUS: "ERROR",
                data: err.message,
                sessionID,
            };
            saveData(sessionID, data);
            return;
        };

        const query1 = `
            select
                user_level,
                adshow_source,
                sum(ad_show) as total_adShow
            from rewarded_ad_status
            where 
                user_level <= 30 and 
                user_level > 0 and
                time_stamp <= '${timestampMin}' and
                time_stamp >= '${timestampMax}' and
                adid >= ${left} and adid <= ${right}
            group by user_level, adshow_source 
            order by user_level
        `;

        db.query(query1, (err, result1) => {
            if (err) {
                const data = {
                    statusCode: 500,
                    STATUS: "ERROR",
                    data: err.message,
                    sessionID,
                };
                saveData(sessionID, data);
                return;
            };

            const query2 = `
                select
                    user_level,
                    count(distinct udid) as user_count
                from rewarded_ad_status
                where 
                    user_level <= 30 and 
                    user_level > 0 and
                    time_stamp <= '${timestampMin}' and
                    time_stamp >= '${timestampMax}' and
                    adid >= ${left} and adid <= ${right}
                group by user_level
                order by user_level
            `;

            db.query(query2, (err, result2) => {
                if (err) {
                    const data = {
                        statusCode: 500,
                        STATUS: "ERROR",
                        data: err.message,
                        sessionID,
                    };
                    saveData(sessionID, data);
                    return;
                };

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

                const data = {
                    statusCode: 200,
                    STATUS: "SUCCESS",
                    data: {
                        averageAdShowPerSource,
                        userLevel,
                    },
                    sessionID,
                };
                saveData(sessionID, data);
                
                setTimeout(() => {
                    deleteData(sessionID);
                }, 600000);
            });
        });
    })
}

const averageAdRejectPerSource = (database, reqType, hoursMin, hoursMax, sessionID) => {
    let left = 0, right = 1000000000000000;
    if (reqType === 'all')
        left = 0, right = 1000000000000000;
    else if (reqType === 'reward')
        left = 0, right = 99999
    else if (reqType === 'int')
        left = 100000, right = 1000000000000000;

    let timestampMin = new Date();
    timestampMin = new Date(timestampMin.getTime() - timestampMin.getTimezoneOffset() * 60000);
    timestampMin = new Date(timestampMin.getTime() - hoursMin * 60 * 60 * 1000);
    timestampMin = new Date(timestampMin).toISOString().replace(/T/, ' ').replace(/\..+/, '');

    let timestampMax = new Date();
    timestampMax = new Date(timestampMax.getTime() - timestampMax.getTimezoneOffset() * 60000);
    timestampMax = new Date(timestampMax.getTime() - hoursMax * 60 * 60 * 1000);
    timestampMax = new Date(timestampMax).toISOString().replace(/T/, ' ').replace(/\..+/, '');

    db.changeUser({
        database
    }, (err) => {
        if (err) {
            const data = {
                statusCode: 400,
                STATUS: "ERROR",
                data: err.message,
                sessionID,
            };
            saveData(sessionID, data);
            return;
        };

        const query1 = `
            select
                user_level,
                adshow_source,
                (sum(ad_show) - sum(adshow_complete)) as total_adShow
            from rewarded_ad_status
            where 
                user_level <= 30 and 
                user_level > 0 and
                time_stamp <= '${timestampMin}' and
                time_stamp >= '${timestampMax}' and
                adid >= ${left} and adid <= ${right}
            group by user_level, adshow_source 
            order by user_level
        `;

        db.query(query1, (err, result1) => {
            if (err) {
                const data = {
                    statusCode: 500,
                    STATUS: "ERROR",
                    data: err.message,
                    sessionID,
                };
                saveData(sessionID, data);
                return;
            };

            const query2 = `
                select
                    user_level,
                    count(distinct udid) as user_count
                from rewarded_ad_status
                where 
                    user_level <= 30 and 
                    user_level > 0 and
                    time_stamp <= '${timestampMin}' and
                    time_stamp >= '${timestampMax}' and
                    adid >= ${left} and adid <= ${right}
                group by user_level
                order by user_level
            `;

            db.query(query2, (err, result2) => {
                if (err) {
                    const data = {
                        statusCode: 500,
                        STATUS: "ERROR",
                        data: err.message,
                        sessionID,
                    };
                    saveData(sessionID, data);
                    return;
                };

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

                const data = {
                    statusCode: 200,
                    STATUS: "SUCCESS",
                    data: {
                        averageAdShowPerSource,
                        userLevel,
                    },
                    sessionID,
                };
                saveData(sessionID, data);
                
                setTimeout(() => {
                    deleteData(sessionID);
                }, 600000);
            });
        });
    })
}

const totalSpendAndEarning = (database, upperLimit, lowerLimit, minHoursBefore, maxHoursBefore, sessionID) => {
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
        if (err) {
            const data = {
                statusCode: 400,
                STATUS: "ERROR",
                data: err.message,
                sessionID,
            };
            saveData(sessionID, data);
            return;
        };

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
                and userLatestBucks between ${lowerLimit} and ${upperLimit}
                limit ${limit}) as t
                where 
                    t.time_stamp >= '${max_timestamp}' and t.time_stamp  <= '${min_timestamp}'
            group by userLevel
        `;
        db.query(query, (err, result) => {
            if (err) {
                const data = {
                    statusCode: 500,
                    STATUS: "ERROR",
                    data: err.message,
                    sessionID,
                };
                saveData(sessionID, data);
                return;
            };

            const userLevel = [], totalBucksSpend = [], totalBucksEarn = [];
            result.forEach((item) => {
                userLevel.push(item.userLevel);
                totalBucksSpend.push(item.total_spend);
                totalBucksEarn.push(item.total_earn);
            });

            const data = {
                statusCode: 200,
                STATUS: "SUCCESS",
                data: {
                    userLevel: userLevel,
                    totalBucksEarn: totalBucksEarn,
                    totalBucksSpend: totalBucksSpend
                },
                sessionID,
            };
            saveData(sessionID, data);
            
            setTimeout(() => {
                deleteData(sessionID);
            }, 600000);
        })
    })
}

const getVersions = (database) => {
    db.changeUser({
        database: database
    }, (err) => {
        if (err) {
            const data = {
                statusCode: 400,
                STATUS: "ERROR",
                data: err.message,
                sessionID,
            };
            saveData(sessionID, data);
            return;
        };

        const query = `
            select
                distinct(appversion) as app_version
            from
                inapp
        `;
        db.query(query, (err, result) => {
            if (err) {
                const data = {
                    statusCode: 500,
                    STATUS: "ERROR",
                    data: err.message,
                    sessionID,
                };
                saveData(sessionID, data);
                return;
            };

            const arr = [];
            result.forEach(item => {
                arr.push(Number(item.app_version));
            });
            arr.sort((a,b) => b - a);

            const data = {
                statusCode: 200,
                STATUS: "SUCCESS",
                data: arr,
                sessionID,
            };
            saveData(sessionID, data);
            
            setTimeout(() => {
                deleteData(sessionID);
            }, 600000);
        })
    })
}

module.exports = {
    calculateCTR,
    thisCTR,
    otherCTR,
    versionsDatabase,
    adCompletionDatabase,
    ctrsrcDatabase,
    bucksStatus,
    averageBucksSpendAndEarning,
    bucksSpendAndEarningStatus,
    averageAdShowPerSource,
    averageAdRejectPerSource,
    totalSpendAndEarning,
    getVersions,
}