const express = require('express');
const fetch = require('node-fetch');
require('dotenv').config();

const app = express();
const port = process.env.PORT || 3000;

app.listen(port, () => {
  console.log(`starting server at ${port}`);
});
app.use(express.static('public'));
app.use(express.json({ limit: '1mb' }));

const db = require('./queries')


// app.get('/api', (request, response) => {
//   database.find({}, (err, data) => {
//     if (err) {
//       response.end();
//       return;
//     }
//     response.json(data);
//   });
// });




// app.get('/predictions/:id', db.getPrediction)


// app.listen(port, () => {
//   console.log(`App running on port ${port}.`)
// })

// const express = require('express')
// const bodyParser = require('body-parser')
// const app = express()
// const port = 3000


// app.use(bodyParser.json())
// app.use(
//   bodyParser.urlencoded({
//     extended: true,
//   })
// )

// app.get('/', (request, response) => {
//   response.json({ info: 'Node.js, Express, and Postgres API' })
// })


