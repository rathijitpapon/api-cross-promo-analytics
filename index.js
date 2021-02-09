require('dotenv').config()

const routes = require('./service/routes/routes');


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

app.use(routes);

const port = process.env.PORT || 5000;

app.listen(port, () => {
    console.log(`Server is running at port ${port}`);
})


