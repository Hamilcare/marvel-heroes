const csv = require('csv-parser');
const fs = require('fs');
const {Client} = require('@elastic/elasticsearch')
const esClient = new Client({node: 'http://localhost:9200'})
const heroesIndexName = 'heroes'
const chunkSize = 1000

async function run() {

    //Creation de l'index
    esClient.indices.create({index: heroesIndexName}, (err, resp) => {
        if (err) console.trace(err.message);
        else console.log(heroesIndexName + "created")
    })

    let heroes = [];

    fs.createReadStream('./all-heroes.csv')
        .pipe(
            csv(
                {separator: ';'}
            )
        ).on('data', (data) => {
            heroes.push({
                hero_id: data.id,
                name: data.name,
                description: data.description,
                /*imageUrl: data.imageUrl,
                backgroundImageUrl: data.backgroundImageUrl,
                externalLink: data.externalLink,*/
                secretIdentities: data.secretIdentities,
                aliases: data.aliases,
                partners: data.partners
            })
        }
    ).on('end', async() => {
        while(heroes.length){
            try{
                let resp = await esClient.bulk(createBulkInsertQuery(heroes.splice(0,chunkSize)));
                console.log(`Inserted ${resp.body.items.length} heroes`);
            } catch (error) {
                console.trace(error)
            }
        }
    })


}

function createBulkInsertQuery(actors) {

    const body = actors.reduce((acc, actor) => {

        const { object_id, ...params } = actor
        acc.push({ index: { _index: heroesIndexName, _type: '_doc', _id: actor.hero_id} })
        acc.push(params)
        return acc
    }, []);

    return { body };
}

run().catch(console.error);
