module.exports = class SubscriptionState {
  constructor() {
    this.pausedTopics = new Set()
    this.resumedTopics = new Set()
  }

  /**
   * @param {Array<string>} topics
   */
  pause(topics = []) {
    topics.forEach(topic => this.pausedTopics.add(topic))
  }

  /**
   * @param {Array<string>} topics
   */
  resume(topics = []) {
    topics.forEach(topic => this.resumedTopics.add(topic))
    topics.forEach(topic => this.pausedTopics.delete(topic))
  }

  /**
   * @returns {Array<string>} paused topics
   */
  paused() {
    return Array.from(this.pausedTopics.values())
  }

  /**
   * @returns {Array<string>} resumed topics
   */
  resumed() {
    return Array.from(this.resumedTopics.values())
  }

  ackResumed() {
    this.resumedTopics.clear()
  }
}
