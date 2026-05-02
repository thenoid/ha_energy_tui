# Security Policy

## Supported Versions

This project is currently alpha. Security fixes target the latest version on `main`.

## Reporting a Vulnerability

Please do not open public issues for vulnerabilities involving token handling,
credential exposure, or unsafe Home Assistant writes.

Report privately by contacting the maintainer, or open a GitHub security advisory
if the repository has advisories enabled.

## Token Handling

HA Energy TUI uses a Home Assistant long-lived access token supplied by environment
variable or CLI argument. Treat that token like a password.

Do not paste tokens into issues, logs, screenshots, or shell history.
