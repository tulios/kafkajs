const flatten = require('./flatten')

const promiseAllInBatches = async ({ batchSize }, arrayOfFunctions) => {
  const size = batchSize || arrayOfFunctions.length
  const numberOfBatches = Math.ceil(arrayOfFunctions.length / size)
  const results = Array(numberOfBatches)
  let i = 0

  for (let batchNumber = 0; batchNumber < numberOfBatches; batchNumber++) {
    const batch = arrayOfFunctions.slice(i, i + size)
    const result = await Promise.all(batch.map(fn => fn()))
    results[batchNumber] = result
    i += size
  }

  return flatten(results)
}

module.exports = promiseAllInBatches
