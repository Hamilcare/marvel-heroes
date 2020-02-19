const csv = require('csv-parser');
const fs = require('fs');
const { Client } = require('@elastic/elasticsearch')
const esClient = new Client({ node: 'http://localhost:9200' })
const heroesIndexName = 'heroes'
const chunkSize = 2000

async function run() {

    let body = {
        mappings: {
            properties: {
                suggest: { type: "completion" }
            }
        }
    }

    //Creation de l'index
    esClient.indices.create({ index: heroesIndexName, body: body }, (err, resp) => {
        if (err) console.trace(err.message);
        else console.log(heroesIndexName + "created")
    });

    let heroes = [];

    fs.createReadStream('./all-heroes.csv')
        .pipe(
            csv(
                { separator: ',' }
            )
        ).on('data', (data) => {
            heroes.push({
                id: data.id,
                name: data.name,
                aliases: data.aliases,
                secretIdentities: data.secretIdentities,
                description: data.description,
                imageUrl: data.imageUrl,
                universe: data.universe,
                gender: data.gender,
                partners: data.partners,
                "suggest":
                    [
                        {
                            "input": data["name"],
                            "weight": 10
                        },
                        {
                            "input": data["aliases"],
                            "weight": 7
                        },
                        {
                            "input": data["secretIdentities"],
                            "weight": 5
                        }
                    ]
            })
        }
        ).on('end', async () => {
            while (heroes.length) {
                try {
                    let resp = await esClient.bulk(createBulkInsertQuery(heroes.splice(0, chunkSize)));
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
        acc.push({ index: { _index: heroesIndexName, _type: '_doc', _id: actor.id } })
        acc.push(params)
        return acc
    }, []);

    return { body };
}

run().catch(console.error);