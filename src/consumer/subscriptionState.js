module.exports = class SubscriptionState {
  constructor() {
    this.pausedTopics = new Set()
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
    topics.forEach(topic => this.pausedTopics.delete(topic))
  }

  /**
   * @returns {Array<string>} paused topics
   */
  paused() {
    return Array.from(this.pausedTopics.values())
  }
}
