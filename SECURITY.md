# Security

This library handles authorization tokens on the client. It never logs them: the built-in `logger`
receives status, error codes and close info, not headers. If you find a path where a token could leak
(logs, error messages, `cause` objects), please report it.

Report vulnerabilities privately via GitHub's **Report a vulnerability** on the Security tab of the
repository rather than in a public issue. You will get an acknowledgement within a few days and a fix
or a mitigation plan before any public disclosure.

Scope: the code in this repository. Vulnerabilities in transports (AWS AppSync Events and others)
should go to their vendors.
