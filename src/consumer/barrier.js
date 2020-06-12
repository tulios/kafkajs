module.exports = () => {
  let unlock
  let unlockWithError
  const lock = new Promise(resolve => {
    unlock = resolve
    unlockWithError = resolve
  })

  return { lock, unlock, unlockWithError }
}
