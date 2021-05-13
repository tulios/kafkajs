const AuthenticationMechanisms = {
  PLAIN: () => require('./plain'),
  'SCRAM-SHA-256': () => require('./scram256'),
  'SCRAM-SHA-512': () => require('./scram512'),
  AWS: () => require('./awsIam'),
  OAUTHBEARER: () => require('./oauthBearer'),
}

module.exports = {
  AuthenticationMechanisms,
}
