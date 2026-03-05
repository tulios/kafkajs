/**
 * Example: SASL OAUTHBEARER Authentication
 *
 * Uses OAuth 2.0 bearer tokens for authentication. The client provides
 * an async function (oauthBearerProvider) that returns the token.
 *
 * This mechanism is ideal for integrating with OAuth 2.0 / OIDC identity
 * providers like Keycloak, Okta, Azure AD, Auth0, etc.
 *
 * Broker configuration (server.properties):
 *   sasl.enabled.mechanisms=OAUTHBEARER
 *   # For production, configure OIDC validation:
 *   sasl.oauthbearer.jwks.endpoint.url=https://idp.example.com/oauth2/jwks
 *   sasl.oauthbearer.expected.audience=kafka
 *
 * Usage:
 *   OAUTH_TOKEN_URL=https://idp.example.com/oauth2/token \
 *   OAUTH_CLIENT_ID=my-client \
 *   OAUTH_CLIENT_SECRET=my-secret \
 *   KAFKA_BROKERS=kafka1:9092,kafka2:9092 \
 *   node examples/sasl-oauthbearer.js
 */

const { Kafka, logLevel } = require('../index')

// Simple token provider using client_credentials grant
// In production, use a library like `simple-oauth2` or `openid-client`
async function fetchOAuthToken() {
  const tokenUrl = process.env.OAUTH_TOKEN_URL
  const clientId = process.env.OAUTH_CLIENT_ID
  const clientSecret = process.env.OAUTH_CLIENT_SECRET

  if (!tokenUrl || !clientId || !clientSecret) {
    // For development/testing, return a dummy unsecured token
    console.warn('OAuth credentials not set, using unsecured token (development only)')
    return {
      value: Buffer.from(JSON.stringify({ sub: 'test', iat: Date.now() })).toString('base64'),
    }
  }

  const response = await fetch(tokenUrl, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({
      grant_type: 'client_credentials',
      client_id: clientId,
      client_secret: clientSecret,
      scope: process.env.OAUTH_SCOPE || '',
    }),
  })

  if (!response.ok) {
    throw new Error(`OAuth token request failed: ${response.status} ${response.statusText}`)
  }

  const data = await response.json()
  return { value: data.access_token }
}

// Token caching provider - reuses tokens and refreshes when needed
function createOAuthBearerProvider() {
  let cachedToken = null
  let tokenExpiry = 0

  return async () => {
    const now = Date.now()
    // Refresh token 30 seconds before expiry
    if (!cachedToken || now >= tokenExpiry - 30000) {
      cachedToken = await fetchOAuthToken()
      // Default 1 hour expiry if not specified
      tokenExpiry = now + 3600 * 1000
    }
    return cachedToken
  }
}

const kafka = new Kafka({
  clientId: 'sasl-oauthbearer-example',
  brokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(','),
  logLevel: logLevel.INFO,
  ssl: process.env.SSL === 'true' || undefined,
  sasl: {
    mechanism: 'oauthbearer',
    oauthBearerProvider: createOAuthBearerProvider(),
  },
})

const run = async () => {
  const admin = kafka.admin()
  await admin.connect()
  console.log('Connected with SASL OAUTHBEARER authentication')

  const topics = await admin.listTopics()
  console.log('Topics:', topics)

  await admin.disconnect()
}

run().catch(e => {
  console.error(`Error: ${e.message}`, e)
  process.exit(1)
})
