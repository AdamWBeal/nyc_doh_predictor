const Pool = require('pg').Pool
const pool = new Pool({
  user: 'postgres',
  host: 'localhost',
  database: 'new_doh',
  password: 'makers',
  port: 5432,
})


  const getPrediction = (request, response) => {
    const id = parseInt(request.params.id)
  
    pool.query('SELECT * FROM public.predictions WHERE camis = $1', [id], (error, results) => {
      if (error) {
        throw error
      }
      response.status(200).json(results.rows)
    })
  }

module.exports = {getPrediction}