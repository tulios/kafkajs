/**
 * @template T
 * @return {{lock: Promise<T>, unlock: (v?: T) => void, unlockWithError: (e: Error) => void}}
 */
module.exports = () => {
  let unlock
  let unlockWithError
  const lock = new Promise(resolve => {
    unlock = resolve
    unlockWithError = resolve
  })

  return { lock, unlock, unlockWithError }
}
