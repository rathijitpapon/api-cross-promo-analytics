require('dotenv').config()
const db = require('./service/db/connection');

const express = require('express');
const cors=require('cors');

var corsOptions = {
    origin: "*",
};

const app = express();
app.use((req, res, next) => {
    res.header('Access-Control-Allow-Origin', '*');
    next();
});

app.use(express.json());
app.use(express.urlencoded({ extended: true }));  
app.use(express.json({limit: '100mb'}));
app.use(express.urlencoded({limit: '100mb', extended: true}));
app.use(cors(corsOptions));

app.post('/ctr', (req, res) => {
    const data = req.body;

    const query = "select appversion,c1, sum(case when install_clicked > 0 then 1 else 0 end) as total_install,sum(case when ad_start > 0 then 1 else 0 end) as total_ad from (select c1,install_clicked,ad_start,appversion from cross_promo_ad_status limit "+data.count+" offset "+data.offset+") as subquery group by appversion,c1";

    db.query(query, function(err, results) {
        if (err) return res.send({ error: err.message});

        const result = [], data1 = [], data2 = [];
        results.map(r => {
            if(r.c1.length > 2)
                result.push(r)
        })

        result.map((entry,ind)=>{
            if(ind === 0){
                data1.push({
                    app_version: entry.appversion,
                    cross_promos: []
                })
            }

            ind = data1.length
            for(let i = 0; i < data1.length; i++){
                if(data1[i].app_version === entry.appversion) {
                    ind = i
                    break
                }

                if(i === data1.length - 1){
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

        result.map((entry,ind)=>{
            if(ind === 0){
                data2.push({
                    c1: entry.c1,
                    app_versions: []
                })
            }

            ind = data2.length
            for(let i = 0; i < data2.length; i++){
                if(data2[i].c1 === entry.c1) {
                    ind = i
                    break
                }
                if(i===data2.length-1){
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

const port = process.env.PORT || 5000;

app.listen(port, () => {
    console.log(`Server is running at port ${port}`);
})


