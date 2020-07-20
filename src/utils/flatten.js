/**
 * Flatten the given arrays into a new array
 *
 * @param {Array<Array<T>>} arrays
 * @returns {Array<T>}
 * @template T
 */
function flatten(arrays) {
  return [].concat.apply([], arrays)
}

module.exports = flatten
