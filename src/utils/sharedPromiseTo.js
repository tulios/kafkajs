/**
 * @template T
 * @param { () => Promise<T> } [defaultFunc]
 * @returns { (func?: () => Promise<T>) => Promise<T> }
 */
module.exports = defaultFunc => {
  let promise = null
  return func => {
    if (promise == null)
      promise = (defaultFunc ? defaultFunc() : func()).finally(() => (promise = null))
    return promise
  }
}
