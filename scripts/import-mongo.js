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
            heroes.push({
                id: data.id,
                name: data.name,
                description: data.description,
                imageUrl: data.imageUrl,
                backgroundImageUrl: data.backgroundImageUrl,
                externalLink: data.externalLink,
                identity: {
                    secretIdentities: data.secretIdentities.split(','),
                    birthPlace: data.birthPlace,
                    occupation: data.occupation,
                    aliases: data.aliases.split(','),
                    alignment: data.alignment,
                    firstAppearance: data.firstAppearance,
                    yearAppearance: parseInt(data.yearAppearance),
                    universe: data.universe
                },
                appearance: {
                    gender: data.gender,
                    race: data.race,
                    height: parseInt(data.height),
                    weight: parseInt(data.weight),
                    eyeColor: data.eyeColor,
                    hairColor: data.hairColor
                },
                teams: data.teams.split(','),
                powers: data.powers.split(','),
                partners: data.partners.split(','),
                skills: {
                    intelligence: parseInt(data.intelligence),
                    strength: parseInt(data.strength),
                    speed: parseInt(data.speed),
                    durability: parseInt(data.durability),
                    power: parseInt(data.power),
                    combat: parseInt(data.combat)
                },
                creators: data.creators.split(',')
            })
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