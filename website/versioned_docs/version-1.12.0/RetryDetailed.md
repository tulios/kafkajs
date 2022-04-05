---
id: version-1.12.0-retry-detailed
title: Retry Mechanism Explained
original_id: retry-detailed
---

The retry mechanism uses a randomization function that grows exponentially. This formula and how the default values affect it is best described by the example below:

- 1st retry:
  - Always a flat `initialRetryTime` ms
  - Default: `300ms`
- Nth retry:
  - Formula: `Random(previousRetryTime * (1 - factor), previousRetryTime * (1 + factor)) * multiplier`
  - N = 1:
    - Since `previousRetryTime == initialRetryTime` just plug the values in the formula:
    - Random(300 * (1 - 0.2), 300 * (1 + 0.2)) * 2 => Random(240, 360) * 2 => (480, 720) ms
    - Hence, somewhere between `480ms` to `720ms`
  - N = 2:
    - Since `previousRetryTime` from N = 1 was in a range between 480ms and 720ms, the retry for this step will be in the range of:
    - `previousRetryTime = 480ms` => Random(480 * (1 - 0.2), 480 * (1 + 0.2)) * 2 => Random(384, 576) * 2 => (768, 1152) ms
    - `previousRetryTime = 720ms` => Random(720 * (1 - 0.2), 720 * (1 + 0.2)) * 2 => Random(576, 864) * 2 => (1152, 1728) ms
    - Hence, somewhere between `768ms` to `1728ms`
  - And so on...

![Plot with default configuration](assets/retry-plot.svg)