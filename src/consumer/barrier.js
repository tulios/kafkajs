/**
 * @template T
 * @return {{lock: Promise<T>, unlock: (v?: T) => void, setError: (e: Error) => void, error: Error}}
 */
module.exports = () => {
  let _resolve
  let _reject
  let error = null
  const lock = new Promise((resolve, reject) => {
    _resolve = resolve
    _reject = reject
  })

  return {
    lock,
    unlock: v => (error ? _reject(error) : _resolve(v)),
    setError: e => (error = error || e),
    error,
  }
}
