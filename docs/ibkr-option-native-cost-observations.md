# IB option native cost observations

`OrderPreview.native_costs` is optional and omitted from old payloads. It preserves
native what-if facts; it does not grant entry permission, certify a budget, or
change PAPER/LIVE policy.

## Evidence contract

- `scope` and `request_hash` bind the complete exact `OptionExecutionRequest`,
  including all legs/bindings, OPEN/CLOSE intent, limit, quantities, account alias,
  generation, expiry, quote and authorization references.
- `option_request_hash` uses UTF-8 JSON of `dataclass_to_payload(request)`, sorted
  keys, compact separators, `ensure_ascii=False`, and SHA-256.
- `observed_at` matches the enclosing preview. `api_version` records the actual
  ib_async client version and negotiated TWS server protocol version. This is
  not a claim that the IB official API package has that same release number.
- `raw_evidence_json` is canonical UTF-8 JSON containing exactly `source`
  (`IB_OPTION_WHAT_IF_COSTS`), `scope`, `request_hash`, `observed_at`, `api_version`,
  and `native_fields`. The last member retains the actual fee field names and
  decoded values. Native doubles retain their string representations.
- `native_evidence_sha256` hashes that fee-only projection. It detects accidental
  or inconsistent edits, but is neither a broker signature nor proof of durable
  retention. Provenance depends on the authenticated Runner/adapter read path.
  It does not include the native account identifier.
- `estimate_cash`, `min_cash`, `max_cash` are replayed from the raw fields. Missing,
  unset, non-finite, negative, non-USD or unrepresentable amounts remain `None`.
  The estimate is never substituted for a missing maximum. Explicit native zero
  is distinguishable from unknown.
- `field_basis` describes which original field family was present. If both
  legacy and renamed families are present, conflicting/unknown aliases cannot
  supply a usable amount. Inconsistent ranges cannot supply a usable maximum.
- `coverage=EXACT_WHAT_IF_ORDER_ONLY` applies only to this current hypothetical
  order. A reverse CLOSE requires its own exact request and native observation;
  rejection/timeout is not replaced with a hypothetical owned position.

## Vendor semantics and limits

IB's [API 10.33 changelog](https://www.interactivebrokers.com/docs/tws-api/changelog)
(17 December 2024) documents the commission-to-commissionAndFees rename. Therefore
legacy `maxCommission` is **not** labeled commission-only or assumed to exclude
all external fees. Conversely, its old name is not evidence of a guaranteed
future all-in fee cap.

The current [OrderState reference](https://www.interactivebrokers.com/docs/tws-api/ref/order-state)
describes `commissionAndFees`, `minCommissionAndFees`, `maxCommissionAndFees` and
`commissionAndFeesCurrency` for the order. The supported ib_async 2.x decoder
still exposes the legacy `commission`, `minCommission`, `maxCommission` and
`commissionCurrency` names. Both representations are retained without rewriting
native evidence or silently selecting a lower conflicting maximum.

[What-if](https://www.interactivebrokers.com/docs/tws-api/doc/orders/whatif) is a
current-account hypothetical credit check, not execution or an assignment/
exercise funding estimate. Future order prices, modifications, fees and broker
acceptance can differ. An absent native maximum remains a real data dependency.
No fixed per-contract fee or research assumption is used by this evidence layer.

Tests use native ib_async callback objects with socket bytes captured locally.
They do not connect to a gateway, submit an order, activate an adapter or deploy.
