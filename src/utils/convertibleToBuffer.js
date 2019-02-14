module.exports = function convertibleToBuffer(value) {
  try {
    Buffer.from(value)
    return true
  } catch (e) {
    if (e.name === 'TypeError') {
      return false
    }

    throw e
  }
}
