// Package openobserve is the Grow Codes SDK for OpenObserve.
//
// Create a client from the environment (GROW_OPENOBSERVE_URL,
// GROW_OPENOBSERVE_TOKEN, ...) or from a Config literal:
//
//	client, err := openobserve.NewClientFromEnv()
//	if err != nil {
//		return err
//	}
//	defer client.Close()
//	status, err := client.Health(ctx)
//
// Every error is an *Error; compare codes with errors.Is against the Err*
// sentinels (errors.Is(err, openobserve.ErrNotFound)). Credentials are wrapped in
// Secret and never appear in errors, hooks or logs.
//
// OpenObserve is a trademark of ZincLabs, Inc.. This package is not affiliated
// with or endorsed by ZincLabs, Inc..
package openobserve
