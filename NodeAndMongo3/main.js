const express = require("express")
const mongoose = require('mongoose')
require('express-async-errors') // this is so I don't need to worry about catching errors in an asynchronous function
// it will just automatically get caught by my normal error handler

const app = express()

const port = 3000;

// design the mongo schema
const subscriberSchema = new mongoose.Schema({
    name: {
        type: String,
        required: true
    },
    email: {
        type: String,
        required: true
    },
    zipCode: {
        type: String,
        required: true
    },
    age: Number
});

function badParams(res, name, value, requirement) {
    let message = '<h1>400 Error</h1>'
    message += `For parameter ${name}, you submitted "${value}", but we require ${requirement}.<BR>`
    message += 'Silly user, you should learn how to use the website.'
    res.status(400).send(message)
}

async function main() {

    //console.log(__dirname)
    // mongoose stuff
    const connection = await mongoose.createConnection('mongodb://127.0.0.1:27017/NodeAndMongo3_db')

    const Subscriber = connection.model('Subscriber', subscriberSchema)
    // according to documents, Mongo automatically names the collection the lowercase plural version of the string here

    // server
    app.use(express.urlencoded({ extended: false }))

// This is the homepage
    app.get('/', (req, res) => {
        res.sendFile(__dirname + '/pages/home.html')
    })

    app.get('/contact', (req, res) => {
        res.sendFile(__dirname + '/pages/contact.html')
    })

    app.get('/subscribers', async (req, res) => {
        // find all subscribers
        let message = `<h1>Subscribers</h1>`
        message += `Name        | Email       | Zip Code    | Age<BR>`
        let subscribers = await Subscriber.find()

        for (let subscriber of subscribers) {
            // I don't know why my IDE is yelling at me that the subscriber needs to be awaited
            // I already awaited the entire collection
            message += `${(subscriber).name} | ${subscriber.email} | ${subscriber.zipCode} | `
            if (subscriber.age) message += `${subscriber.age}`
            message +=`<BR>`
        }

        message += `<BR><form action="/" method="get">
                        <input type="submit" value="Home">
                    </form>`

        res.send(message)
    })

    app.get('/age-filter', (req, res) => {
        res.sendFile(__dirname + '/pages/age-filter.html')
    })

    app.post('/subscribe', async (req, res) => {
        async function addToDatabase(name, email, zipCode, age) {
            const subscriber = age? new Subscriber({name: name, email: email, zipCode: zipCode, age: age})
                : new Subscriber({name: name, email: email, zipCode: zipCode})
            return await subscriber.save()
        }

        if (req.body.name.length < 1) {
            badParams(res, 'name', req.body.name, 'your input to have at least one letter')
            return
        }
        if (req.body.email.length < 1) {
            badParams(res, 'email', req.body.email, 'your input to have at least one letter')
            return
        }
        if (req.body.zipCode.length < 1) {
            badParams(res, 'zip code', req.body.zipCode, 'your input to have at least one letter')
            return
        }
        const ageNum = parseInt(req.body.age)
        if (req.body.age && req.body.age !== '' && !(ageNum > 0 && ageNum <= 100)) {
            badParams(res, 'age', req.body.age, 'must be between 1 and 100')
            return
        }
        let message = `Congratulations! Successfully subscribed ${req.body.name} with email of ${req.body.email}`
        if (!req.body.age || req.body.age === '') {
            message += ` and zip code of ${req.body.zipCode}.`
            await addToDatabase(req.body.name, req.body.email, req.body.zipCode)
        } else {
            message += `, zip code of ${req.body.zipCode}, and age of ${ageNum}.`
            await addToDatabase(req.body.name, req.body.email, req.body.zipCode, ageNum)
        }
        message += `<BR><BR><form action="/" method="get">
                        <input type="submit" value="Home">
                    </form>`
        res.send(message)
    })

    app.post('/process-age-filter', async (req, res) => {
        const age = parseInt(req.body.age)
        if (isNaN(age)) {
            badParams(res, 'age', req.body.age, 'your input to be a number between 1 and 100')
            return
        }
        const numRemovedObj = await Subscriber.deleteMany().where('age').gte(age)
        const numRemoved = numRemovedObj.deletedCount
        let message = `Congratulations. You filtered all users older than ${req.body.age}, removing ${numRemoved}.`
        message += `<BR><BR><form action="/" method="get">
                        <input type="submit" value="Home">
                    </form>`
        res.send(message)
    })

    /*app.get('/error-test', async (req, res) => {
        //res.send('Hello World!')
        async function errorThrow() {
            throw new Error(`Aaaaaah! We're on fire! God save our souls!`)
        }
        await errorThrow()
    })//*/

    // borrowed code from https://stackoverflow.com/questions/6528876/how-to-redirect-404-errors-to-a-page-in-expressjs
    // The 404 Route (ALWAYS Keep this as the last route)
    app.get('/*', function(req, res){
        let message = '<h1>404 Error</h1>';
        message += `I'm sorry, but "` + req.get('host') + req.originalUrl + '" is not a valid url.<BR>';
        message += 'Valid URLs are "localhost:3000/", "localhost:3000/contact", "localhost:3000/subscribers", and "localhost:3000/age-filter".';
        res.status(404).send(message);
    });

    // error handling
    // whenever an exception is thrown, this will catch it and send a 500 error instead
    app.use((err, req, res, next) => {
        if (res.headersSent) {
            return next(err)
        }

        console.log(err)

        let message = '<h1>500 Error</h1>'
        message += 'We apologize for technical difficulties.<BR>'
        message += 'If this is your server, please check the console to see where you messed up.'

        res.status(500).send(message)
    })

    app.listen(port, () => {
        console.log(`Subscribe app listening on port ${port}`)
    })

}

main().catch(err => console.log(err));
