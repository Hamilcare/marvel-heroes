var mongodb = require("mongodb");
var csv = require("csv-parser");
var fs = require("fs");

var MongoClient = mongodb.MongoClient;
var mongoUrl = "mongodb://localhost:27017";
const dbName = "marvel";
const collectionName = "heroes";
//const bulkSize = 1000;


const insertHeroes = (db, callback) => {
    const collection = db.collection(collectionName)
    let heroes = []
    fs.createReadStream('./all-heroes.csv')
        .pipe(csv())
        .on('data', data => {
            heroes.push(data)
        })
        .on('end', () => {
            collection.insertMany(heroes, (err, result) => {
                callback(result)
            })
        })
}

MongoClient.connect(mongoUrl, function (err, client) {
    if (err) {
        console.error(err);
        throw err;
    }
    const db = client.db(dbName);
    insertHeroes(db, (result) => {
        console.log('Insertion completed : ', result)
        client.close();
    })
});