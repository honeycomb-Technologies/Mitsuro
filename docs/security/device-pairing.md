# Device pairing

Mitsuro retains its HTTP API, SSE updates, and WebSocket terminal and browser streams. QR pairing enrolls a device into that existing transport.

## Pair a device

Enable remote access on the server first. On a trusted owner connection, open Settings → Connection → Pair a device. Enter the server's reachable HTTPS origin and create a QR code. On the phone, choose **Pair using a QR code**, scan it, review the server address, and request pairing. Compare the code shown on both devices before allowing access on the owner screen.

Invitations expire after two minutes and accept one claim. A scanned invitation does not authorize API access. The owner approves the specific pending device claim; a phone cannot approve itself. Device credentials expire after 90 days. Use the owner screen to revoke individual devices or create a fresh invitation to pair again.

The camera opens only when requested. Pasting a pairing link is also supported. Existing connection links and owner credentials remain compatible; they do not gain the security properties of the new invitation flow.

## Security boundaries

Each invitation and device credential contains 256 bits of random secret material. SQLite stores their SHA-256 hashes, not the reusable secrets. Approval and one-time claiming use database transactions. Pairing responses disable caching. The QR contains a versioned `mitsuro://pair` fragment, with a destination origin and a one-use invitation; it never contains the owner credential. HTTP origins are accepted only for loopback development.

New pairing requests omit ambient cookies, refuse redirects, and never reuse the currently connected server's credentials. Native clients use Expo fetch for those guarantees. After approval, native clients use the existing secure credential storage and connection lifecycle. Web clients use the existing browser credential storage, which is accessible to JavaScript: preventing XSS remains essential.

New device WebSocket connections exchange the credential in an Authorization header for a one-use, path-bound ticket valid for 30 seconds. The reusable device credential is rejected in WebSocket query parameters. Existing owner query-token compatibility remains available for older clients and retains its URL-logging exposure.

Revocation rejects subsequent requests. Active device SSE and WebSocket attachments check revocation and expiry every five seconds. Disabling remote access also disconnects those attachments. Foreground paired clients check authenticated device access every 30 seconds and show connection recovery when access is lost; public server health alone does not establish device authorization. Revocation does not undo completed actions, kill every durable background job, or erase data already read by a device.

A paired device can run code and access the workspace as the server's operating-system user. It is a trusted operator, not an untrusted sandbox tenant. Administrative API restrictions prevent accidental credential and server configuration changes, but cannot contain a malicious operator who already has unrestricted code execution as that user. Revoke an unwanted device promptly; suspected host compromise requires host and credential recovery beyond device revocation.

A photographed or relayed QR code is not proof of proximity. Check the destination and comparison code on the trusted host. TLS must terminate at a trusted endpoint; pairing does not add end-to-end encryption through an untrusted relay. Keep the server behind the intended private network or correctly configured HTTPS gateway.

## Review references

The review uses [OWASP ASVS](https://owasp.org/www-project-application-security-verification-standard/), [OWASP MASVS](https://mas.owasp.org/MASVS/), and the [WebSocket Security Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/WebSocket_Security_Cheat_Sheet.html) as verification guidance. The confirmation and expiry design draws on [RFC 8628](https://www.rfc-editor.org/rfc/rfc8628.html) and the cross-device threats in [RFC 10027](https://www.rfc-editor.org/rfc/rfc10027.html). This is a Mitsuro enrollment protocol, not an OAuth implementation or a claim of NIST assurance-level certification.
