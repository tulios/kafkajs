const shuffle = array => array.sort(() => Math.random() - 0.5)

module.exports = array => {
  if (!Array.isArray(array)) {
    throw new TypeError("'array' is not an array")
  }

  if (array.length < 2) {
    return array
  }

  let shuffled = [...array]

  while (!shuffled.some((value, index) => value !== array[index])) {
    shuffled = shuffle(shuffled)
  }

  return shuffled
}
