require('dotenv').config()
const socketio = require('socket.io');
const http = require('http');

const routes = require('./service/routes/routes');
const adshowController = require('./service/controllers/adshowController');


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

const server = http.createServer(app);
const io = socketio(server);

io.on("connect", socket => {
    socket.on("join", async (data, cb) => {
        socket.join(data.id);
    });

    socket.on("sendData", async (data, cb) => {
        adshowController.averageAdShowPerSource(data, io);       
    });

    socket.on("sendCompletedAdData", async (data, cb) => {
        adshowController.averageAdRejectPerSource(data, io);       
    });
});


const port = process.env.PORT || 5000;
server.listen(port, () => {
    console.log(`Server is running at port ${port}`);
});
