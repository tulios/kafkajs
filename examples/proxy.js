#!/usr/bin/env node

const http = require('http')
const setup = require('proxy')

const server = setup(http.createServer())
server.listen(3128, function() {
  const port = server.address().port
  console.log('HTTP(s) proxy server listening on port %d', port)
})
