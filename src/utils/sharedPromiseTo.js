/**
 * @param { () => Promise<void> } [func]
 * @returns { (() => Promise<void>?) => Promise<void> }
 */
module.exports = defaultFunc => {
  let promise = null
  return func => {
    if (promise == null)
      promise = (defaultFunc ? defaultFunc() : func()).finally(() => (promise = null))
    return promise
  }
}
