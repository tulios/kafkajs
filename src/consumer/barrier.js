module.exports = () => {
  let unlock
  let unlockWithError
  const lock = new Promise((resolve, reject) => {
    unlock = resolve
    unlockWithError = reject
  })

  return { lock, unlock, unlockWithError }
}
