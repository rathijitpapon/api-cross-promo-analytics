const express = require('express');
const router = express.Router();
const db = require('../db/connection');

router.post('/sourceSink/bucksSpendAndEarning',(req,res) =>{
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
                sum(gaeBuckEarnAfterFight) as total_earn_AfterFight,
                sum(gaeBuckEarnGoal) as total_earn_inGoal,
                sum(gaeBuckEarnInAppPackChest) as total_earn_InAppPackChest,
                sum(gaeBuckEarnInAppUSD) as total_earn_InAppUSD,
                sum(gaeBuckEarnDailyBonus) as total_earn_DailyBonus,
                sum(gaeBuckEarnInAppPanel) as total_earn_InAppPanel,
                sum(gaeBuckEarninitialbuck) as total_earn_initialbuck,
                sum(gaeBuckEarnInAppSpecialOffer) as total_earn_InAppSpecialOffer,
                sum(gaeBuckEarndailyBonusProduct) as total_earn_dailyBonusProduct,
                sum(gaeBuckEarnAdReward) as total_earn_AdReward,
                sum(gaeBuckEarnSpinPanel) as total_earn_SpinPanel,
                sum(gaeBuckEarnInApp) as total_earn_InApp,
                sum(gaeBuckEarnOwnAdReward) as total_earn_OwnAdReward,
                sum(gaeBuckEarnVideoAdReward) as total_earn_VideoAdReward,
                sum(gaeBuckEarninAppMessage) as total_earn_inAppMessage,
                sum(gaeBuckEarnVIPBenefits) as total_earn_VIPBenefits,
                sum(gaeBuckEarnJump3Times) as total_earn_Jump3Times,
                sum(gaeBuckEarnBestReward) as total_earn_BestReward,
                sum(gaeBuckEarnJump3Times2x) as total_earn_Jump3Times2x,
                sum(gaeBuckEarnJ3TGOBestReward) as total_earn_J3TGOBestReward,
--                 sum(gaeBuckEarnFightWinLooseBR) as total_earn_FightWinLooseBR,
                sum(gaeBuckEarnOwnAdVIPReward) as total_earn_OwnAdVIPReward,
                
                sum(gaeBucksSpendClaimWithGemsPopup) as total_spend_ClaimWithGemsPopup,
                sum(gaeBuckSpendNursery) as total_spend_Nursery,
                sum(gaeBuckSpendProduct) as total_spend_Product,
                sum(gaeBuckSpendProductLand) as total_spend_ProductLand,
                sum(gaeBucksSpendRefillEnergy) as total_spend_RefillEnergy,
                sum(gaeBuckSpendOuter) as total_spend_Outer,
                sum(gaeBuckSpendFarm) as total_spend_Farm,
                sum(gaeBuckSpendSummonCard) as total_spend_SummonCard,
                sum(gaeBucksSpendClaimWithGems) as total_spend_ClaimWithGems,
                sum(gaeBuckSpendChallenge9Summon) as total_spend_Challenge9Summon,
                sum(gaeBuckSpendProductEvolve) as total_spend_ProductEvolve,
                sum(gaeBuckSpendBreedLab) as total_spend_BreedLab,
                sum(gaeBucksSpendTodaysOffer_BuyFood) as total_spend_TodaysOffer_BuyFood,
                sum(gaeBucksSpendTodaysOffer_BuyProduct) as total_spend_TodaysOffer_BuyProduct,
                sum(gaeBucksSpendTodaysOffer_BuyAll) as total_spend_TodaysOffer_BuyAll,
                sum(gaeBucksSpendDailyBonusProduct) as total_spend_DailyBonusProduct,
                sum(gaeBucksSpendJ3TBallShop) as total_spend_J3TBallShop,
                sum(gaeBucksSpendDailyTaskSkip) as total_spend_DailyTaskSkip,
                sum(gaeBucksSpendLuckyGiftSpin) as total_spend_LuckyGiftSpin
            from
                (select 
                    userLevel, 
                    udid, 
                    gaeBuckEarnAfterFight,
                    gaeBuckEarnGoal, 
                    gaeBuckEarnInAppPackChest,
                    gaeBuckEarnDailyBonus,
                    gaeBuckEarnInAppUSD,
                    gaeBuckEarnInAppPanel,
                    gaeBuckEarninitialbuck,
                    gaeBuckEarnInAppSpecialOffer,
                    gaeBuckEarndailyBonusProduct,
                    gaeBuckEarnAdReward,
                    gaeBuckEarnSpinPanel,
                    gaeBuckEarnInApp,
                    gaeBuckEarnOwnAdReward,
                    gaeBuckEarnVideoAdReward,
                    gaeBuckEarninAppMessage,
                    gaeBuckEarnVIPBenefits,
                    gaeBuckEarnJump3Times,
                    gaeBuckEarnBestReward,
                    gaeBuckEarnJump3Times2x,
                    gaeBuckEarnJ3TGOBestReward,
--                     gaeBuckEarnFightWinLooseBR,
                    gaeBuckEarnOwnAdVIPReward,
                    
                    gaeBucksSpendClaimWithGemsPopup,
                    gaeBuckSpendNursery,
                    gaeBuckSpendProduct,
                    gaeBuckSpendProductLand,
                    gaeBucksSpendRefillEnergy,
                    gaeBuckSpendOuter,
                    gaeBuckSpendFarm,
                    gaeBuckSpendSummonCard,
                    gaeBucksSpendClaimWithGems,
                    gaeBuckSpendChallenge9Summon,
                    gaeBuckSpendProductEvolve,
                    gaeBuckSpendBreedLab,
                    gaeBucksSpendTodaysOffer_BuyFood,
                    gaeBucksSpendTodaysOffer_BuyProduct,
                    gaeBucksSpendTodaysOffer_BuyAll,
                    gaeBucksSpendDailyBonusProduct,
                    gaeBucksSpendJ3TBallShop,
                    gaeBucksSpendDailyTaskSkip,
                    gaeBucksSpendLuckyGiftSpin
                from paid_user_allBuckSpendEvents_battle
                 where userLevel <= 30 and userLevel > 0 
                 and userLatestBucks between ${lowerLimit} and ${upperLimit} limit 20000) as t
            group by userLevel
        `;
        db.query(query, (err, result) => {
            if(err) {
                return res.status(400).send({error: err.message});
            }

            const userLevel = [];
            const averageEarnAfterFight = [];
            const averageEarninGoal = [];
            const averageEarnInAppPackChest = [];
            const averageEarnInAppUSD = [];
            const averageEarnDailyBonus = [];
            const averageEarnInAppPanel = [];
            const averageEarninitialbuck = [];
            const averageEarnInAppSpecialOffer = [];
            const averageEarndailyBonusProduct = [];
            const averageEarnAdReward = [];
            const averageEarnSpinPanel = [];
            const averageEarnInApp = [];
            const averageEarnOwnAdReward = [];
            const averageEarnVideoAdReward = [];
            const averageEarninAppMessage = [];
            const averageEarnVIPBenefits = [];
            const averageEarnJump3Times = [];
            const averageEarnBestReward = [];
            const averageEarnJump3Times2x = [];
            const averageEarnJ3TGOBestReward = [];
            const averageEarnFightWinLooseBR = [];
            const averageEarnOwnAdVIPReward = [];

            const averageSpendClaimWithGemsPopup = [];
            const averageSpendNursery = [];
            const averageSpendProduct = [];
            const averageSpendProductLand = [];
            const averageSpendRefillEnergy = [];
            const averageSpendOuter = [];
            const averageSpendFarm = [];
            const averageSpendSummonCard = [];
            const averageSpendClaimWithGems = [];
            const averageSpendChallenge9Summon = [];
            const averageSpendProductEvolve = [];
            const averageSpendBreedLab = [];
            const averageSpendTodaysOffer_BuyFood = [];
            const averageSpendTodaysOffer_BuyProduct = [];
            const averageSpendTodaysOffer_BuyAll = [];
            const averageSpendDailyBonusProduct = [];
            const averageSpendJ3TBallShop = [];
            const averageSpendDailyTaskSkip = [];
            const averageSpendLuckyGiftSpin = [];


            result.forEach((item) => {
                userLevel.push(item.userLevel);
                averageEarnAfterFight.push((item.total_earn_AfterFight/item.total_users).toFixed(2));
                averageEarninGoal.push((item.total_earn_inGoal/item.total_users).toFixed(2));
                averageEarnInAppPackChest.push((item.total_earn_InAppPackChest/item.total_users).toFixed(2));
                averageEarnInAppUSD.push((item.total_earn_InAppUSD/item.total_users).toFixed(2));
                averageEarnDailyBonus.push((item.total_earn_DailyBonus/item.total_users).toFixed(2));
                averageEarnInAppPanel.push((item.total_earn_InAppPanel/item.total_users).toFixed(2));
                averageEarninitialbuck.push((item.total_earn_initialbuck/item.total_users).toFixed(2));
                averageEarnInAppSpecialOffer.push((item.total_earn_InAppSpecialOffer/item.total_users).toFixed(2));
                averageEarndailyBonusProduct.push((item.total_earn_dailyBonusProduct/item.total_users).toFixed(2));
                averageEarnAdReward.push((item.total_earn_AdReward/item.total_users).toFixed(2));
                averageEarnSpinPanel.push((item.total_earn_SpinPanel/item.total_users).toFixed(2));
                averageEarnInApp.push((item.total_earn_InApp/item.total_users).toFixed(2));
                averageEarnOwnAdReward.push((item.total_earn_OwnAdReward/item.total_users).toFixed(2));
                averageEarnVideoAdReward.push((item.total_earn_VideoAdReward/item.total_users).toFixed(2));
                averageEarninAppMessage.push((item.total_earn_inAppMessage/item.total_users).toFixed(2));
                averageEarnVIPBenefits.push((item.total_earn_VIPBenefits/item.total_users).toFixed(2));
                averageEarnJump3Times.push((item.total_earn_Jump3Times/item.total_users).toFixed(2));
                averageEarnBestReward.push((item.total_earn_BestReward/item.total_users).toFixed(2));
                averageEarnJump3Times2x.push((item.total_earn_Jump3Times2x/item.total_users).toFixed(2));
                averageEarnJ3TGOBestReward.push((item.total_earn_J3TGOBestReward/item.total_users).toFixed(2));
                averageEarnFightWinLooseBR.push((0/item.total_users).toFixed(2));
                averageEarnOwnAdVIPReward.push((item.total_earn_OwnAdVIPReward/item.total_users).toFixed(2));

                averageSpendClaimWithGemsPopup.push((-item.total_spend_ClaimWithGemsPopup/item.total_users).toFixed(2));
                averageSpendNursery.push((-item.total_spend_Nursery/item.total_users).toFixed(2));
                averageSpendProduct.push((-item.total_spend_Product/item.total_users).toFixed(2));
                averageSpendProductLand.push((-item.total_spend_ProductLand/item.total_users).toFixed(2));
                averageSpendRefillEnergy.push((-item.total_spend_RefillEnergy/item.total_users).toFixed(2));
                averageSpendOuter.push((-item.total_spend_Outer/item.total_users).toFixed(2));
                averageSpendFarm.push((-item.total_spend_Farm/item.total_users).toFixed(2));
                averageSpendSummonCard.push((-item.total_spend_SummonCard/item.total_users).toFixed(2));
                averageSpendClaimWithGems.push((-item.total_spend_ClaimWithGems/item.total_users).toFixed(2));
                averageSpendChallenge9Summon.push((-item.total_spend_Challenge9Summon/item.total_users).toFixed(2));
                averageSpendProductEvolve.push((-item.total_spend_ProductEvolve/item.total_users).toFixed(2));
                averageSpendBreedLab.push((-item.total_spend_BreedLab/item.total_users).toFixed(2));
                averageSpendTodaysOffer_BuyFood.push((-item.total_spend_TodaysOffer_BuyFood/item.total_users).toFixed(2));
                averageSpendTodaysOffer_BuyProduct.push((-item.total_spend_TodaysOffer_BuyProduct/item.total_users).toFixed(2));
                averageSpendTodaysOffer_BuyAll.push((-item.total_spend_TodaysOffer_BuyAll/item.total_users).toFixed(2));
                averageSpendDailyBonusProduct.push((-item.total_spend_DailyBonusProduct/item.total_users).toFixed(2));
                averageSpendJ3TBallShop.push((-item.total_spend_J3TBallShop/item.total_users).toFixed(2));
                averageSpendDailyTaskSkip.push((-item.total_spend_DailyTaskSkip/item.total_users).toFixed(2));
                averageSpendLuckyGiftSpin.push((-item.total_spend_LuckyGiftSpin/item.total_users).toFixed(2));
            });
            return res.send({
                userLevels: userLevel,
                averageEarnAfterFight: averageEarnAfterFight,
                averageEarninGoal: averageEarninGoal,
                averageEarnInAppPackChest: averageEarnInAppPackChest,
                averageEarnInAppUSD: averageEarnInAppUSD,
                averageEarnDailyBonus: averageEarnDailyBonus,
                averageEarnInAppPanel: averageEarnInAppPanel,
                averageEarninitialbuck: averageEarninitialbuck,
                averageEarnInAppSpecialOffer: averageEarnInAppSpecialOffer,
                averageEarndailyBonusProduct: averageEarndailyBonusProduct,
                averageEarnAdReward: averageEarnAdReward,
                averageEarnSpinPanel: averageEarnSpinPanel,
                averageEarnInApp: averageEarnInApp,
                averageEarnOwnAdReward: averageEarnOwnAdReward,
                averageEarnVideoAdReward: averageEarnVideoAdReward,
                averageEarninAppMessage: averageEarninAppMessage,
                averageEarnVIPBenefits: averageEarnVIPBenefits,
                averageEarnJump3Times: averageEarnJump3Times,
                averageEarnBestReward: averageEarnBestReward,
                averageEarnJump3Times2x: averageEarnJump3Times2x,
                averageEarnJ3TGOBestReward: averageEarnJ3TGOBestReward,
                averageEarnFightWinLooseBR: averageEarnFightWinLooseBR,
                averageEarnOwnAdVIPReward: averageEarnOwnAdVIPReward,

                averageSpendClaimWithGemsPopup: averageSpendClaimWithGemsPopup,
                averageSpendNursery: averageSpendNursery,
                averageSpendProduct: averageSpendProduct,
                averageSpendProductLand: averageSpendProductLand,
                averageSpendRefillEnergy: averageSpendRefillEnergy,
                averageSpendOuter: averageSpendOuter,
                averageSpendFarm: averageSpendFarm,
                averageSpendSummonCard: averageSpendSummonCard,
                averageSpendClaimWithGems: averageSpendClaimWithGems,
                averageSpendChallenge9Summon: averageSpendChallenge9Summon,
                averageSpendProductEvolve: averageSpendProductEvolve,
                averageSpendBreedLab: averageSpendBreedLab,
                averageSpendTodaysOffer_BuyFood: averageSpendTodaysOffer_BuyFood,
                averageSpendTodaysOffer_BuyProduct: averageSpendTodaysOffer_BuyProduct,
                averageSpendTodaysOffer_BuyAll: averageSpendTodaysOffer_BuyAll,
                averageSpendDailyBonusProduct: averageSpendDailyBonusProduct,
                averageSpendJ3TBallShop: averageSpendJ3TBallShop,
                averageSpendDailyTaskSkip: averageSpendDailyTaskSkip,
                averageSpendLuckyGiftSpin: averageSpendLuckyGiftSpin
            });
        })
    })
})

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
                 where userLevel <= ${highestUserLevel} and userLevel > ${lowestUserLevel} 
                 and userLatestBucks between ${lowerLimit} and ${upperLimit} limit ${limit}) as t
            group by userLevel
        `;
        db.query(query, (err, result) =>{
            if(err) {
                return res.status(400).send({error:err.message});
            }
            const userLevel = [], averageBucks = [];
            result.forEach((item) => {
                userLevel.push(item.userLevel);
                averageBucks.push((item.total_bucks/item.total_users).toFixed(2));
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
                from cross_promo_ad_status limit ${limit} offset 0) as v
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
