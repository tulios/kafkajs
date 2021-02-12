/**
 * @param { () => Promise<void> } [func]
 * @returns { (() => Promise<void>?) => Promise<void> }
 */
module.exports = defaultFunc => {
  let promise = null
  return async func => {
    if (promise) {
      return await promise
    }
    promise = defaultFunc ? defaultFunc() : func()
    return await promise.finally(() => (promise = null))
  }
}
