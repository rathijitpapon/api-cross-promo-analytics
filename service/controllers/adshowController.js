const db = require('../db/connection');

const averageAdShowPerSource = (data, io) => {
    const database = data.database;
    const reqType = data.reqType;
    const hoursMin = data.hoursMin;
    const hoursMax = data.hoursMax;
    const dataId = data.id;

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
        if (err) return {error: err.message};

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
                    user_level > 0 and
                    time_stamp <= '${timestampMin}' and
                    time_stamp >= '${timestampMax}'
                limit 20000
            ) as v
            where 
                v.adid >= ${left} and v.adid <= ${right}
            group by user_level, adshow_source 
            order by user_level
        `;

        db.query(query1, (err, result1) => {
            if (err) {
                return {error: err.message};
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
                        user_level > 0 and
                        time_stamp <= '${timestampMin}' and
                        time_stamp >= '${timestampMax}'
                    limit 20000
                ) as v
                where 
                    v.adid >= ${left} and v.adid <= ${right}
                group by user_level
                order by user_level
            `;

            db.query(query2, (err, result2) => {
                if (err) {
                    return {error: err.message};
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

                result = {
                    averageAdShowPerSource,
                    userLevel,
                };

                io.to(dataId).emit(dataId, result);
            });
        });
    })
}

module.exports = {
    averageAdShowPerSource,
}