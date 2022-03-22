/**
 * A cheap way to deterministically convert a number to a positive value. When the input is
 * positive, the original value is returned. When the input number is negative, the returned
 * positive value is the original value bit AND against 0x7fffffff which is not its absolutely
 * value.
 */
module.exports = x => x & 0x7fffffff
