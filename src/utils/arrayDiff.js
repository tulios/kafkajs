module.exports = (a, b) => {
  const result = []
  const length = a.length
  let i = 0
  const bSet = new Set(b)

  while (i < length) {
    if (!bSet.has(a[i])) {
      result.push(a[i])
    }
    i += 1
  }

  return result
}
