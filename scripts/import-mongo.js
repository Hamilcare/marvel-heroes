var mongodb = require("mongodb");
var csv = require("csv-parser");
var fs = require("fs");

var MongoClient = mongodb.MongoClient;
var mongoUrl = "mongodb://localhost:27017";
const dbName = "marvel";
const collectionName = "heroes";
//const bulkSize = 1000;

const safeSplit = string => !string ? [] : string.split(',')
const safeParseInt = string => {
    const number = Number.parseInt(string)
    if (Number.isNaN(number))
        return undefined
    return number
}

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
                    secretIdentities: safeSplit(data.secretIdentities),
                    birthPlace: data.birthPlace,
                    occupation: data.occupation,
                    aliases: safeSplit(data.aliases),
                    alignment: data.alignment,
                    firstAppearance: data.firstAppearance,
                    yearAppearance: safeParseInt(data.yearAppearance),
                    universe: data.universe
                },
                appearance: {
                    gender: data.gender,
                    race: data.race,
                    height: safeParseInt(data.height),
                    weight: safeParseInt(data.weight),
                    eyeColor: data.eyeColor,
                    hairColor: data.hairColor
                },
                teams: safeSplit(data.teams),
                powers: safeSplit(data.powers),
                partners: safeSplit(data.partners),
                skills: {
                    intelligence: safeParseInt(data.intelligence),
                    strength: safeParseInt(data.strength),
                    speed: safeParseInt(data.speed),
                    durability: safeParseInt(data.durability),
                    power: safeParseInt(data.power),
                    combat: safeParseInt(data.combat)
                },
                creators: safeSplit(data.creators)
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