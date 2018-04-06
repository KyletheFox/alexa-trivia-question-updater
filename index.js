const mysql = require('mysql');
const events = require('events');
const redis = require("redis");
const EventEmitter = events.EventEmitter;

const DB_HOST = process.env.DB_HOST;
const DB_USER = process.env.DB_USER;
const DB_PASS = process.env.DB_PASS;
const DB_TABLE = process.env.DB_TABLE;
const DB_DATABASE = process.env.DB_DATABASE;
const DAYS_TIL_AVAILABLE = process.env.DAYS_TIL_AVAILABLE;
const NUMBER_OF_ACTIVE_QUESTIONS = process.env.NUMBER_OF_ACTIVE_QUESTIONS;
const REDIS_HOST = process.env.REDIS_HOST;


exports.handler = function(event, context, callback) {
    //console.log('Received event:', JSON.stringify(event, null, 2));
    var flowController = new EventEmitter();
    var client = redis.createClient(REDIS_HOST);

    context.callbackWaitsForEmptyEventLoop = false; 

    client.on("error", function (err) {
        console.log("Error " + err);
    });

    const conn = mysql.createConnection({
        host: DB_HOST,
        user: DB_USER,
        password: DB_PASS,
        database: DB_DATABASE
    });

    // Test for caching
    // client.get("something", (err,reply) => {
    //     if (reply === null) {
    //         console.log('cache is empty');
    //         client.set('something', 'something cached', redis.print);
    //     } else {
    //         console.log('cache has something');
    //     }
    // })

    conn.connect((err) => {
        if (err) {
            conn.end();
            throw err;
        }
        
        flowController.on('resetTable', function (lang) {
            conn.query("UPDATE " + DB_TABLE + " SET available_count = available_count - 1 where available_count > 0", (err, results) => {
                conn.query("UPDATE " + DB_TABLE + " SET available_count = '" + DAYS_TIL_AVAILABLE + "', active_ind = 'N' WHERE active_ind = 'Y'", (err, results) => {
                    flowController.emit('getLanguages');
                })
            })
            
        });
        
        flowController.on('getLanguages', function () {
            conn.query("SELECT Distinct(language) FROM " + DB_TABLE , (err, languages) => {
                if(err) throw err;
    
                languages.forEach(language => {
                    console.log('Languange Found: ' + language.language);
                    flowController.emit('updateQuestions', language.language);
                });
                
                conn.end();

                flowController.emit('clearCache');
            });
        });
        
        flowController.on('updateQuestions', function (lang) {
            conn.query("UPDATE " + DB_TABLE + 
                        " SET \
                        active_ind = 'Y'\
                    WHERE\
                        active_ind = 'N' AND language = '" + lang + "' AND available_count = '0' ORDER BY RAND() LIMIT " + NUMBER_OF_ACTIVE_QUESTIONS, (err, rows) => {
                if(err) throw err;
                else console.log("Sucessful DB Update");
            })
        });

        flowController.on('clearCache', function() {
            client.flushall();
            client.quit();
        });

        flowController.emit('resetTable');
    });
    
};