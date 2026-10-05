# Changelog

## V9.2 development — qualified event interpretation

- Decode supported native option statuses and actual leg executions against
  exact retained contract qualifications, preserving full execution IDs.
- Separate parent and child states; ignore cumulative/net prices as financial
  evidence. Reject unknown, ambiguous, fractional or lossy event fields.
- Keep corrections, fees, lifecycle, REST backfill and certification pending.

## V9.2 development — raw event ingress

- Preserve exact native trading frames and capture the verified account sink
  before queue delivery; retain full IDs and propagate storage failures.
- Route option/unclassified records away from legacy stock fill callbacks and
  expose SDK transport failure to adapter recovery without hidden reconnects.
- Add synthetic pinned-SDK and threaded delivery coverage. Option capability
  declaration, native normalization/backfill and certification remain pending.

## 0.1.0 - unreleased

- Initial Alpaca Paper adapter for whole-share US equities and ETFs.
