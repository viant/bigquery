## Aug 26 2026

- `credURL=op://...` loads credentials via scy/1Password without a local credentials file.
- DSN and `SetOptions` credentials are detected via explicit client options; application default credentials (`GOOGLE_APPLICATION_CREDENTIALS`) remain supported when no DSN credentials are configured.
- Requires `afsc` with `op://` support — the driver blank-imports [`github.com/viant/afsc/op`](https://github.com/viant/afsc/tree/master/op) directly; no new `scy` release is needed.

## Aug 17 2022 0.2.0
 * Integration with SCY secret manager
## Dec 25 2021 0.1.0
 * Initial Release

