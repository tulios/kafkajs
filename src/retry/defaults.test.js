module.exports = {
  maxRetryTime: 1000,
  initialRetryTime: 10,
  factor: 0.002, // randomization factor
  multiplier: 1.2, // exponential factor
  retries: 15, // max retries
}
