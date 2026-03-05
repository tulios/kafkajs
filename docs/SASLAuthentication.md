---
id: sasl-authentication
title: SASL Authentication
---

KafkaJS supports all SASL authentication mechanisms that Apache Kafka supports. This guide covers each mechanism, when to use it, and how to configure it.

## Supported Mechanisms

| Mechanism | `sasl.mechanism` | Use Case | Security Level |
|---|---|---|---|
| [PLAIN](#plain) | `'plain'` | Simple username/password | Low (requires TLS) |
| [SCRAM-SHA-256](#scram-sha-256--scram-sha-512) | `'scram-sha-256'` | Secure password auth | Good |
| [SCRAM-SHA-512](#scram-sha-256--scram-sha-512) | `'scram-sha-512'` | Secure password auth | Good |
| [OAUTHBEARER](#oauthbearer) | `'oauthbearer'` | OAuth 2.0 / OIDC | Strong |
| [AWS IAM](#aws-iam) | `'aws'` | Amazon MSK | Strong |
| [GSSAPI (Kerberos)](#gssapi-kerberos) | `'gssapi'` | Enterprise Kerberos | Strong |

## Common Options

All SASL mechanisms share these connection-level options:

```javascript
const kafka = new Kafka({
  brokers: ['kafka1:9092'],
  ssl: true, // Recommended for all SASL mechanisms
  authenticationTimeout: 10000, // Timeout in ms (default: 10000)
  reauthenticationThreshold: 10000, // Re-auth threshold in ms (default: 10000)
  sasl: {
    mechanism: '...', // One of the supported mechanisms
    // ... mechanism-specific options
  },
})
```

| Option | Description | Default |
|---|---|---|
| `authenticationTimeout` | Timeout in ms for authentication requests | `10000` |
| `reauthenticationThreshold` | When periodic reauthentication (`connections.max.reauth.ms`) is configured on the broker, reauthenticate when this many milliseconds remain of session lifetime | `10000` |

## PLAIN

The simplest mechanism. Sends username and password to the broker.

**Always use with SSL/TLS** — credentials are sent in cleartext.

```javascript
const kafka = new Kafka({
  brokers: ['kafka1:9092'],
  ssl: true,
  sasl: {
    mechanism: 'plain',
    username: 'my-username',
    password: 'my-password',
  },
})
```

**Broker config:**
```properties
sasl.enabled.mechanisms=PLAIN
```

## SCRAM-SHA-256 / SCRAM-SHA-512

Salted Challenge Response Authentication Mechanism ([RFC 5802](https://tools.ietf.org/html/rfc5802)). Uses PBKDF2 key derivation with a multi-step challenge-response handshake. Passwords are never sent in cleartext, even without TLS.

```javascript
const kafka = new Kafka({
  brokers: ['kafka1:9092'],
  ssl: true,
  sasl: {
    mechanism: 'scram-sha-256', // or 'scram-sha-512'
    username: 'my-username',
    password: 'my-password',
  },
})
```

**Create credentials on the broker:**
```bash
kafka-configs.sh --bootstrap-server localhost:9092 \
  --alter --add-config 'SCRAM-SHA-256=[password=secret],SCRAM-SHA-512=[password=secret]' \
  --entity-type users --entity-name my-username
```

**Broker config:**
```properties
sasl.enabled.mechanisms=SCRAM-SHA-256,SCRAM-SHA-512
```

### Delegation Tokens

Kafka delegation tokens ([KIP-48](https://cwiki.apache.org/confluence/display/KAFKA/KIP-48+Delegation+token+support+for+Kafka)) work over SCRAM mechanisms. Use the token ID as the username and the token HMAC as the password:

```javascript
const kafka = new Kafka({
  brokers: ['kafka1:9092'],
  ssl: true,
  sasl: {
    mechanism: 'scram-sha-256',
    username: tokenId, // Delegation token ID
    password: tokenHmac, // Delegation token HMAC (base64-encoded)
  },
})
```

## OAUTHBEARER

Uses OAuth 2.0 bearer tokens ([RFC 7628](https://tools.ietf.org/html/rfc7628)). Ideal for integrating with OAuth 2.0 / OIDC identity providers (Keycloak, Okta, Azure AD, Auth0, etc.).

```javascript
const kafka = new Kafka({
  brokers: ['kafka1:9092'],
  ssl: true,
  sasl: {
    mechanism: 'oauthbearer',
    oauthBearerProvider: async () => {
      // Fetch token from your OAuth provider
      const token = await fetchTokenFromIdP()
      return {
        value: token,
        extensions: { // Optional SASL extensions
          logicalCluster: 'my-cluster',
        },
      }
    },
  },
})
```

The `oauthBearerProvider` must be an async function that returns an object with:
- `value` (required): The OAuth bearer token string
- `extensions` (optional): Key-value pairs sent as SASL extensions

### Token Caching

Your `oauthBearerProvider` implementation should cache and refresh tokens:

```javascript
function createOAuthProvider(clientId, clientSecret, tokenUrl) {
  let cachedToken = null
  let expiresAt = 0

  return async () => {
    if (Date.now() < expiresAt - 30000) {
      return { value: cachedToken }
    }

    const response = await fetch(tokenUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: new URLSearchParams({
        grant_type: 'client_credentials',
        client_id: clientId,
        client_secret: clientSecret,
      }),
    })

    const data = await response.json()
    cachedToken = data.access_token
    expiresAt = Date.now() + data.expires_in * 1000
    return { value: cachedToken }
  }
}
```

**Broker config (production with OIDC):**
```properties
sasl.enabled.mechanisms=OAUTHBEARER
sasl.oauthbearer.jwks.endpoint.url=https://idp.example.com/oauth2/jwks
sasl.oauthbearer.expected.audience=kafka
sasl.oauthbearer.expected.issuer=https://idp.example.com
```

## AWS IAM

Authenticates using AWS IAM credentials. Used with Amazon MSK or brokers running [STACK's Kafka AWS IAM LoginModule](https://github.com/STACK-Fintech/kafka-auth-aws-iam).

```javascript
const kafka = new Kafka({
  brokers: ['kafka1:9092'],
  ssl: true,
  sasl: {
    mechanism: 'aws',
    authorizationIdentity: 'AIDAIOSFODNN7EXAMPLE', // UserId or RoleId
    accessKeyId: 'AKIAIOSFODNN7EXAMPLE',
    secretAccessKey: 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
    sessionToken: 'optional-session-token', // Required for temporary credentials
  },
})
```

| Option | Description | Required |
|---|---|---|
| `authorizationIdentity` | The `aws:userid` (UserId or RoleId) | Yes |
| `accessKeyId` | AWS access key ID | Yes |
| `secretAccessKey` | AWS secret access key | Yes |
| `sessionToken` | AWS session token (for STS temporary credentials) | No |

Get the `authorizationIdentity` using the AWS CLI:
```bash
aws iam get-user  # Returns UserId
aws iam get-role --role-name MyRole  # Returns RoleId
```

## GSSAPI (Kerberos)

Uses Kerberos tickets for authentication ([RFC 4752](https://tools.ietf.org/html/rfc4752)). Provides strong mutual authentication without transmitting passwords. Common in enterprise environments with existing Kerberos infrastructure.

### Prerequisites

1. Install the `kerberos` npm package:
   ```bash
   npm install kerberos
   ```
2. A Kerberos KDC must be running and accessible
3. Configure `/etc/krb5.conf` with your realm settings
4. Obtain a keytab file or kinit a TGT before running the client

### Configuration

```javascript
const kafka = new Kafka({
  brokers: ['kafka1:9092'],
  sasl: {
    mechanism: 'gssapi',
    serviceName: 'kafka',
    principal: 'kafka-client@EXAMPLE.COM',
    keytab: '/etc/security/keytabs/client.keytab',
  },
})
```

| Option | Description | Default |
|---|---|---|
| `serviceName` | Kerberos service name (must match broker's `sasl.kerberos.service.name`) | `'kafka'` |
| `principal` | Client Kerberos principal (e.g., `user@REALM`) | *default from krb5.conf* |
| `keytab` | Path to keytab file for non-interactive authentication | *uses default TGT* |
| `kerberosServicePrincipal` | Full service principal override (e.g., `kafka/host@REALM`) | *built from serviceName/host* |

**Broker config:**
```properties
sasl.enabled.mechanisms=GSSAPI
sasl.kerberos.service.name=kafka
```

### Using with kinit

If you don't have a keytab, you can authenticate interactively first:

```bash
kinit kafka-client@EXAMPLE.COM
node my-kafka-app.js
```

Then omit the `keytab` and `principal` options — the default TGT from the credential cache will be used.

## Custom Authentication Mechanisms

If you need a mechanism not built into KafkaJS, you can implement a custom authentication provider. See [Custom Authentication Mechanisms](CustomAuthenticationMechanism.md) for details.

```javascript
const kafka = new Kafka({
  brokers: ['kafka1:9092'],
  sasl: {
    mechanism: 'my-custom-mechanism',
    authenticationProvider: ({ host, port, logger, saslAuthenticate }) => ({
      authenticate: async () => {
        // Your custom authentication logic
      },
    }),
  },
})
```

## Security Best Practices

1. **Always use TLS**: Configure `ssl: true` or provide SSL options. This is critical for `PLAIN` and `AWS` mechanisms where credentials are sent in cleartext.

2. **Use SCRAM over PLAIN**: When using password-based auth, prefer `SCRAM-SHA-256` or `SCRAM-SHA-512` over `PLAIN`. SCRAM never sends passwords in cleartext.

3. **Rotate credentials**: Use short-lived tokens with `OAUTHBEARER`, temporary credentials with `AWS`, or delegation tokens with `SCRAM`.

4. **Use reauthentication**: Configure `connections.max.reauth.ms` on brokers and `reauthenticationThreshold` on the client to periodically re-authenticate connections.

5. **Principle of least privilege**: Grant only the minimum ACL permissions needed for your application.
