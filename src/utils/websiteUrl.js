const BASE_URL = 'https://kafka.js.org'

module.exports = (path, hash) => `${BASE_URL}/${path}${hash ? '#' + hash : ''}`
