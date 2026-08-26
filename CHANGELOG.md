## Aug 26 2026

- `credURL=op://...` loads credentials via scy/1Password without a local credentials file.
- DSN and `SetOptions` credentials take precedence over Application Default Credentials; `isAuth` no longer probes `GOOGLE_APPLICATION_CREDENTIALS` when deciding whether explicit credentials were configured.
- Requires scy with `op://` support (see [scy](https://github.com/viant/scy) / [afsc/op](https://github.com/viant/afsc/tree/master/op)).

## Aug 17 2022 0.2.0
 * Integration with SCY secret manager
## Dec 25 2021 0.1.0
 * Initial Release

