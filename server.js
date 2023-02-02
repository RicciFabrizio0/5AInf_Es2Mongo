//Installato e richiesto il modulo di mongodb
let mongo = require("mongodb");
//Prelevo la parte del modulo per la gestione del client mongo
let mongoClient = mongo.MongoClient;
let  urlServerMongoDb = "mongodb://127.0.0.1:27017";

let http = require("http");
let url = require("url");

let database = "banche";

//DEFINISCO IL SERVER
let json, op;
let server = http.createServer(function(req, res){
    //Avverto il browser che ritorno un oggetto JSON
    res.setHeader('Content-Type', 'application/json');

    //Decodifico la richiesta ed eseguo la query interessata
    let scelta = (url.parse(req.url)).pathname;
    switch(scelta){
        
        case "/q1":
            find(res, "utenti", {residenza:"Fossano"});
            break;
        
        case "/q2":
            find(res, "utenti", {nome:/^[CL]/, anni:{$gt:50}});
            break;

        case "/q3":
            find(res, "utenti", {nome:/o$/}, {nome:1, cognome:1, _id:0}, 2);
            break;
        
        case "/q4":
            op = [
                {$group: {_id:"$residenza", etaMedia: {$avg: "$anni"}}},
            ]
            aggregate(res, "utenti", op)
            break;

        case "/q5":
            find2(res, "utenti", {nome:"Rosanna", cognome:"Gelso"}, {_id:1}, function(ris){
                console.log(ris[0]._id);
                find(res, "transazioni", {mittente:ris[0]._id}, {mittente:1, destinatario:1, somma:1});
            })
            break;

        case "/q6":
            cont(res, "transazioni", {somma:{$gt:20}});
            break;

        case "/q7":
            find2(res, "utenti", {nome:"Mattia", cognome:"Manzo"}, {}, function(ris){
                console.log(ris)
                op = [
                    {$match: {destinatario:ris[0]._id}},
                    {
                        $lookup: {
                           from: "utenti",
                           localField: "destinatario",
                           foreignField: "_id",
                           as: "region_docs"
                        }
                    },
                    {
                        $unwind: "$region_docs"
                    },
                    {$group: {_id:"$region_docs.nome", sommatoria: {$sum: "$somma"}}},
                ]
                aggregate(res, "transazioni", op)
                
            })

            break;

        case "/q8":
            op = [
                {
                    $lookup: {
                       from: "utenti",
                       localField: "destinatario",
                       foreignField: "_id",
                       as: "region_docs"
                    }
                },
                {
                    $unwind: "$region_docs"
                },
                {$group: {_id:"$region_docs.nome", sommatoria: {$sum: "$somma"}}},
            ]
            aggregate(res, "transazioni", op)
            break;

        case "/q9":
            let data = new Date("2021-01-01");
            find(res, "transazioni", {data:{$gt:data}}, {_id:0})
            break;
        
        default:
            json = {cod:-1, desc:"Nessuna query trovata con quel nome"};
            res.end(JSON.stringify(json));
    }
});

server.listen(8888, "127.0.0.1");
console.log("Il server è in ascolto sulla porta 8888");

function creaConnessione(nomeDb, response, callback){
    console.log(mongoClient);
    let promise = mongoClient.connect(urlServerMongoDb);
    promise.then(function(connessione){
        callback(connessione, connessione.db(nomeDb))
    });
    promise.catch(function(err){
        json = {cod:-1, desc:"Errore nella connessione"};
        response.end(JSON.stringify(json));
    });
}

function find2(res, col, obj, select, callback){
    creaConnessione(database, res, function(conn, db){
        let promise = db.collection(col).find(obj).project(select).toArray();
        promise.then(function(ris){
            conn.close();
            callback(ris);
        });

        promise.catch(function(error){
            obj = { cod:-2, desc:"Errore nella ricerca"}
            res.end(JSON.stringify(obj));
            conn.close();
        });
    });
}

function find(res, col, obj, select){
    creaConnessione(database, res, function(conn, db){
        let promise = db.collection(col).find(obj).project(select).toArray();
        promise.then(function(ris){
            //console.log(ris);
            obj = { cod:0, desc:"Dati trovati con successo", ris};
            res.end(JSON.stringify(obj));
            conn.close();
        });

        promise.catch(function(error){
            obj = { cod:-2, desc:"Errore nella ricerca"}
            res.end(JSON.stringify(obj));
            conn.close();
        });
    });
}
/*
    aggregate -> aggregazione di funzioni di ricerca

    opzioni -> array di oggetti dove ogni oggetto è un 
            filtro che vogliamo applicare alla collezione

*/
function aggregate(res, col, opzioni){
    creaConnessione(database, res, function(conn, db){
        let promise = db.collection(col).aggregate(opzioni).toArray();
        promise.then(function(ris){
            //console.log(ris);
            obj = { cod:0, desc:"Dati trovati con successo", ris};
            res.end(JSON.stringify(obj));
            conn.close();
        });

        promise.catch(function(error){
            obj = { cod:-2, desc:"Errore nella ricerca"}
            res.end(JSON.stringify(obj));
            conn.close();
        });
    });
}

function limit(res, col, obj, select, n){
    creaConnessione(database, res, function(conn, db){
        let promise = db.collection(col).find(obj).project(select).limit(n).toArray();
        promise.then(function(ris){
            //console.log(ris);
            obj = { cod:0, desc:"Dati trovati con successo", ris};
            res.end(JSON.stringify(obj));
            conn.close();
        });

        promise.catch(function(error){
            obj = { cod:-2, desc:"Errore nella ricerca"}
            res.end(JSON.stringify(obj));
            conn.close();
        });
    });
}

function sort(res, col, obj, select, orderby){
    creaConnessione(database, res, function(conn, db){
        let promise = db.collection(col).find(obj).project(select).sort(orderby).toArray();
        promise.then(function(ris){
            //console.log(ris);
            obj = { cod:0, desc:"Dati trovati con successo", ris};
            res.end(JSON.stringify(obj));
            conn.close();
        });

        promise.catch(function(error){
            obj = { cod:-2, desc:"Errore nella ricerca"}
            res.end(JSON.stringify(obj));
            conn.close();
        });
    });
}

function cont(res, col, query){
    creaConnessione(database, res, function(conn, db){
        let promise = db.collection(col).countDocuments(query);
        promise.then(function(ris){
            //console.log(ris);
            obj = { cod:0, desc:"Dati trovati con successo", ris};
            res.end(JSON.stringify(obj));
            conn.close();
        });

        promise.catch(function(error){
            obj = { cod:-2, desc:"Errore nella ricerca"}
            res.end(JSON.stringify(obj));
            conn.close();
        });
    });
}

function cont2(res, col, query){
    creaConnessione(database, res, function(conn, db){
        let promise = db.collection(col).count(query);
        promise.then(function(ris){
            //console.log(ris);
            obj = { cod:0, desc:"Dati trovati con successo", ris};
            res.end(JSON.stringify(obj));
            conn.close();
        });

        promise.catch(function(error){
            obj = { cod:-2, desc:"Errore nella ricerca"}
            res.end(JSON.stringify(obj));
            conn.close();
        });
    });
}

function insertOne(res, col, obj){
    creaConnessione(database, res, function(conn, db){
        let promise = db.collection(col).insertOne(obj); 
        promise.then(function(ris){
            json = { cod:1, desc:"Insert in esecuzione", ris };
            res.end(JSON.stringify(json));
            conn.close();
        });
        promise.catch(function(err){
            obj = { cod:-2, desc:"Errore nell'inserimento"}
            res.end(JSON.stringify(obj));
            conn.close();
        });
    });
}

function insertMany(res, col, array){
    creaConnessione(database, res, function(conn, db){
        let promise = db.collection(col).insertMany(array); 
        promise.then(function(ris){
            json = { cod:1, desc:"Insert in esecuzione", ris };
            res.end(JSON.stringify(json));
            conn.close();
        });
        promise.catch(function(err){
            obj = { cod:-2, desc:"Errore nell'inserimento"}
            res.end(JSON.stringify(obj));
            conn.close();
        });
    });
}

function remove(res, col, where){
    creaConnessione(database, res, function(conn, db){
        let promise = db.collection(col).deleteMany(where); 
        promise.then(function(ris){
            json = { cod:1, desc:"Remove in esecuzione", ris };
            res.end(JSON.stringify(json));
            conn.close();
        });
        promise.catch(function(err){
            obj = { cod:-2, desc:"Errore nella cancellazione"}
            res.end(JSON.stringify(obj));
            conn.close();
        });
    });
}