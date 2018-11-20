const { EventEmitter } = require('events')
const { KafkaJSNonRetriableError } = require('../../errors')
const STATES = require('./transactionStates')

const VALID_STATE_TRANSITIONS = {
  [STATES.UNINITIALIZED]: [STATES.READY],
  [STATES.READY]: [STATES.READY, STATES.TRANSACTING],
  [STATES.TRANSACTING]: [STATES.COMMITTING, STATES.ABORTING],
  [STATES.COMMITTING]: [STATES.READY],
  [STATES.ABORTING]: [STATES.READY],
}

module.exports = ({ logger, initialState = STATES.UNINITIALIZED }) => {
  let currentState = initialState

  const stateMachine = Object.assign(new EventEmitter(), {
    /**
     * Ensure state machine is in the correct state before calling method
     */
    guard(object, method, eligibleStates) {
      const fn = object.method

      object.method = (...args) => {
        if (!eligibleStates.includes(currentState)) {
          throw new KafkaJSNonRetriableError(
            `Transaction state exception: Cannot call "${method}" in state "${currentState}"`
          )
        }

        return fn.apply(object, args)
      }
    },
    /**
     * Transition safely to a new state
     */
    transitionTo(state) {
      logger.debug(`Transaction state transition ${currentState} --> ${state}`)

      if (!VALID_STATE_TRANSITIONS[currentState].includes(state)) {
        throw new KafkaJSNonRetriableError(
          `Transaction state exception: Invalid transition ${currentState} --> ${state}`
        )
      }

      stateMachine.emit('transition', { to: state, from: currentState })
      currentState = state
    },

    state() {
      return currentState
    },
  })

  return stateMachine
}
