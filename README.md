Calendar Group Import Automation (PowerShell)

Overview
This PowerShell script automates the import, preview, and safe update/creation of calendar events in a Microsoft 365 Group calendar using Microsoft Graph. It validates a source CSV, detects duplicates, and supports dry-run/preview mode for safe and idempotent operation.

Features
CSV Schema Checks: Verifies existence and required headers of calendar CSV input.
Duplicate Detection: Finds duplicate events within the CSV by subject and date.
Preview Decisions: Calculates per-row import decisions: WouldCreate, WouldUpdate, WouldSkip, DuplicateInCsv, or Invalid.
Interactive Microsoft Graph Connection: Handles delegated authentication for required permissions (Group.ReadWrite.All).
Group & Calendar Resolution: Looks up the target group and its primary calendar (sample: calendar@domain.com).
Safe Event Creation & Update: Only creates or updates events as decided by the preview logic; supports -WhatIf mode.
Logging: Exports detailed preview and run summaries to timestamped CSV logs.
Verbose Option: Additional runtime diagnostics for troubleshooting.

How It Works
Validate CSV using Test-CalendarCsvSchema
Detect Duplicates and preview import actions with Get-CalendarMatchPreview
Connect to Microsoft Graph using Connect-CalendarGraph
Resolve Target Group and Calendar with Resolve-GroupCalendar
Dry-run or Live Import with Invoke-CalendarImport (-WhatIf recommended initially)

Example Usage
powershell
# Dry run: preview import actions, no changes made
Invoke-CalendarImport -WhatIf -Verbose

# Live run: creates/updates group calendar events
Invoke-CalendarImport -Verbose

Prerequisites
PowerShell 7.x recommended
Microsoft Graph PowerShell SDK (install with Install-Module Microsoft.Graph -Scope CurrentUser)
Delegated access with Group.ReadWrite.All consent

Configuration
All configuration (CSV path, target SMTP, calendar settings, encoding, etc.) is hardcoded for project use and documented in comments within functions. Adjust values as needed for your environment.

Supported Platforms
Windows, Linux, or macOS with PowerShell 7.x
Microsoft 365 cloud tenant with Group calendar

Security & Privacy
No secrets, API keys, credentials, or sensitive data are stored in the script.
Always run with generic placeholders (calendar@domain.com).
Logs do not include private data—review outputs before sharing externally.
