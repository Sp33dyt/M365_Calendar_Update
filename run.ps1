# Script to import events from a CSV file into a M365 group calendar

function Test-CalendarCsvSchema {
<#
.SYNOPSIS
Validates the presence and header schema of the calendar CSV file using hardcoded configuration.

.DESCRIPTION
Checks that the CSV file exists, is readable, and contains the required headers with exact names.
No parameters are accepted; configuration is defined inside the function for this project’s needs.

.CONFIGURATION
- CsvPath:        C:\script\events.csv
- ExpectedHeaders: Title, Day, Description
- Delimiter:      comma (,)
- Encoding:       utf8
- IncludeHash:    true (adds SHA256 for run metadata)

.EXAMPLE
Test-CalendarCsvSchema -Verbose
Returns a structured pass/fail result without requiring any arguments.

.OUTPUTS
pscustomobject
Properties:
- Path, Exists, Readable, SizeBytes, DelimiterUsed, EncodingUsed
- HeadersFound (string[]), ExpectedHeaders (string[])
- MissingHeaders (string[]), ExtraHeaders (string[])
- HeaderMatch (bool), Status ('Pass'|'Fail'|'Error'), Reason
- FileHashSha256 (when IncludeHash=true)

.NOTES
- Compatible with 7+.
- This function ONLY validates the header/schema; it does not parse data rows or dates.
- TODO: If project paths change later, update the CONFIGURATION block.
#>
    [CmdletBinding()]
    param()

    begin {
        Set-StrictMode -Version Latest

        # --- Hardcoded configuration (per project decision: no external parameters) ---
        $CsvPath         = 'C:\CalendarODP\events.csv'
        $ExpectedHeaders = @('Title','Day','Description')
        [char]$Delimiter = ','
        $Encoding        = 'utf8'   # one of: utf8, unicode, utf7, utf32, ascii, bigendianunicode, oem, default
        $IncludeHash     = $true
        # ------------------------------------------------------------------------------

        Write-Verbose ("Config: Path='{0}', Delimiter='{1}', Encoding='{2}', IncludeHash={3}" -f $CsvPath,$Delimiter,$Encoding,$IncludeHash)
    }
    process {
        $result = [pscustomobject]@{
            Path            = $CsvPath
            Exists          = $false
            Readable        = $false
            SizeBytes       = $null
            DelimiterUsed   = [string]$Delimiter
            EncodingUsed    = $Encoding
            HeadersFound    = @()
            ExpectedHeaders = $ExpectedHeaders
            MissingHeaders  = @()
            ExtraHeaders    = @()
            HeaderMatch     = $false
            Status          = 'Fail'
            Reason          = $null
            FileHashSha256  = $null
        }

        try {
            if (-not (Test-Path -LiteralPath $CsvPath -PathType Leaf)) {
                $result.Reason = 'FileNotFound'  # per requirement: if folder/file missing, treat as no input
                return $result
            }

            $file = Get-Item -LiteralPath $CsvPath -ErrorAction Stop
            $result.Exists    = $true
            $result.SizeBytes = $file.Length

            $headerLine = Get-Content -LiteralPath $CsvPath -TotalCount 1 -Encoding $Encoding -ErrorAction Stop
            if (-not $headerLine) {
                $result.Reason = 'EmptyFile'
                return $result
            }
            $result.Readable = $true

            $rawHeader = ($headerLine | Select-Object -First 1).Trim()
            $headers = ($rawHeader -split [regex]::Escape([string]$Delimiter)) | ForEach-Object { $_.Trim().Trim('"') }
            $result.HeadersFound = $headers

            $missing = @($ExpectedHeaders | Where-Object { $_ -notin $headers })
            $extra   = @($headers | Where-Object { $_ -notin $ExpectedHeaders })
            $result.MissingHeaders = $missing
            $result.ExtraHeaders   = $extra
            $result.HeaderMatch    = ($missing.Count -eq 0)

            if ($IncludeHash) {
                $result.FileHashSha256 = (Get-FileHash -LiteralPath $CsvPath -Algorithm SHA256 -ErrorAction Stop).Hash
            }

            if (-not $result.HeaderMatch) {
                $result.Status = 'Fail'
                $result.Reason = if ($missing.Count -gt 0) { "MissingHeaders: $($missing -join ', ')" } else { 'HeaderMismatch' }
            } else {
                $result.Status = 'Pass'
                $result.Reason = $null
            }

            Write-Verbose ("CSV header check: Status={0}; Missing=[{1}]; Extra=[{2}]" -f $result.Status, ($missing -join ', '), ($extra -join ', '))
            return $result
        }
        catch {
            $msg = $_.Exception.Message
            Write-Error -Message "Failed to validate CSV schema: $msg" -ErrorAction Continue
            $result.Status = 'Error'
            $result.Reason = $msg
            return $result
        }
    }
}

function Read-CalendarCsv {
<#
.SYNOPSIS
Reads raw rows from the calendar CSV using hardcoded configuration.

.DESCRIPTION
Loads the CSV from disk and emits raw row objects for downstream processing.
This function is read-only and assumes the header schema was already validated
by Test-CalendarCsvSchema. It defensively checks for required headers and
returns objects augmented with a RowNumber for traceability.

.CONFIGURATION
- CsvPath:   C:\CalendarODP\events.csv
- Delimiter: comma (,)
- Encoding:  utf8
- Required headers: Title, Day, Description

.EXAMPLE
Read-CalendarCsv -Verbose | Format-Table -AutoSize

.OUTPUTS
pscustomobject
Properties per row:
- RowNumber   [int]    : 1-based row index (excluding header)
- Title       [string] : Event subject (raw, untrimmed here)
- Day         [string] : Date in MM/DD/YYYY (raw string here)
- Description [string] : Body text (raw; may contain newlines)

.NOTES
- Compatible with PowerShell 5.1+ and 7+ (tested target: 7+).
- No parameters are accepted (project decision).
- This function does NOT normalize or validate dates/text; that is the next step.
- TODO: If project paths change later, update the CONFIGURATION block.
#>
    [CmdletBinding()]
    param()

    begin {
        Set-StrictMode -Version Latest

        # --- Hardcoded configuration (per project decision: no external parameters) ---
        $CsvPath       = 'C:\CalendarODP\events.csv'
        [char]$Delimiter = ','
        $Encoding      = 'utf8' # valid values: utf8, unicode, utf7, utf32, ascii, bigendianunicode, oem, default
        $RequiredHeaders = @('Title','Day','Description')
        # ------------------------------------------------------------------------------

        Write-Verbose ("Config: Path='{0}', Delimiter='{1}', Encoding='{2}'" -f $CsvPath,$Delimiter,$Encoding)
    }
    process {
        try {
            if (-not (Test-Path -LiteralPath $CsvPath -PathType Leaf)) {
                $ex = New-Object System.IO.FileNotFoundException("CSV file not found at '$CsvPath'")
                $err = New-Object System.Management.Automation.ErrorRecord($ex, 'FileNotFound', [System.Management.Automation.ErrorCategory]::ObjectNotFound, $CsvPath)
                $PSCmdlet.ThrowTerminatingError($err)
            }

            $importParams = @{
                LiteralPath = $CsvPath
                Delimiter   = $Delimiter
                Encoding    = $Encoding
                ErrorAction = 'Stop'
            }
            $rows = Import-Csv @importParams

            if (-not $rows -or $rows.Count -eq 0) {
                Write-Information "Read-CalendarCsv: File has a header but no data rows: '$CsvPath'."
                return
            }

            # Defensive header check (should already be validated upstream)
            $firstProps = $rows[0].psobject.Properties.Name
            $missing = @($RequiredHeaders | Where-Object { $_ -notin $firstProps })
            if ($missing.Count -gt 0) {
                $ex = New-Object System.InvalidOperationException("Missing required header(s): $($missing -join ', ')")
                $err = New-Object System.Management.Automation.ErrorRecord($ex, 'HeaderMismatch', [System.Management.Automation.ErrorCategory]::InvalidData, $CsvPath)
                $PSCmdlet.ThrowTerminatingError($err)
            }

            $i = 0
            foreach ($r in $rows) {
                $i++
                # Emit raw values only; normalization happens in the next function
                [pscustomobject]@{
                    RowNumber   = [int]$i
                    Title       = [string]$r.Title
                    Day         = [string]$r.Day
                    Description = [string]$r.Description
                }
            }

            Write-Information ("Read-CalendarCsv: Imported {0} row(s) from '{1}'." -f $i, $CsvPath)
        }
        catch {
            $msg = $_.Exception.Message
            Write-Error -Message ("Failed to read CSV: {0}" -f $msg) -ErrorAction Continue
            # Terminating errors are thrown above with ThrowTerminatingError; here we just surface the message.
        }
    }
}

function ConvertTo-CanonicalEvent {
<#
.SYNOPSIS
Normalizes raw CSV rows into canonical event objects (all-day, London TZ) and validates fields.

.DESCRIPTION
Enhancements:
- Accepts single- or double-digit month/day (e.g., 11/6/2025, 11/16/2025).
- Strips non-digit and non-slash characters from the Day field before parsing
  (handles stray quotes, NBSP, zero-width chars).
- Tries exact formats first, then falls back to culture-based TryParse (en-US).

.CONFIGURATION
- Culture: en-US
- Accepted formats: MM/dd/yyyy, M/d/yyyy, M/dd/yyyy, MM/d/yyyy
- Time zone: Europe/London (fallback: GMT Standard Time)
#>
    [CmdletBinding()]
    param()

    begin {
        Set-StrictMode -Version Latest

        # --- Hardcoded configuration ---
        $CultureName     = 'en-US'
        $AcceptedFormats = @('MM/dd/yyyy','M/d/yyyy','M/dd/yyyy','MM/d/yyyy')
        $PreferredTzIds  = @('Europe/London','GMT Standard Time')
        $NormalizeDescription = $true
        # --------------------------------

        try { $culture = [System.Globalization.CultureInfo]::GetCultureInfo($CultureName) }
        catch { throw "Culture '$CultureName' not available." }

        $tz = $null
        foreach ($id in $PreferredTzIds) { try { $tz = [System.TimeZoneInfo]::FindSystemTimeZoneById($id); break } catch { } }
        if (-not $tz) { throw "Unable to resolve a London timezone. Tried: $($PreferredTzIds -join ', ')" }

        Write-Verbose ("Using Culture='{0}', TimeZoneId='{1}'" -f $culture.Name, $tz.Id)
    }
    process {
        $raw = @( Read-CalendarCsv )
        if (-not $raw -or $raw.Count -eq 0) {
            Write-Information "ConvertTo-CanonicalEvent: No input rows available."
            return
        }

        foreach ($r in $raw) {
            $errReason = $null
            $isValid   = $true

            # Subject
            $subject = [string]($r.Title ?? '')
            $subject = $subject.Trim()
            if ([string]::IsNullOrWhiteSpace($subject)) {
                $errReason = 'EmptyTitle'
                $isValid   = $false
            }

            # Day parsing (tolerant): remove any non [0-9 or /], then parse
            $dayRaw   = [string]($r.Day ?? '')
            $dayTrim  = $dayRaw.Trim()
            # Remove ZERO-WIDTH & other odd chars; keep digits and slashes
            $dayClean = ($dayTrim -replace '[^\d/]', '')
            # Also trim accidental leading/trailing slashes
            $dayClean = $dayClean.Trim('/')

            $dayDate = $null
            if ($isValid) {
                if ([string]::IsNullOrWhiteSpace($dayClean)) {
                    $errReason = "InvalidDate('$dayRaw')"
                    $isValid   = $false
                } else {
                    $tmp = [datetime]::MinValue
                    $ok = [datetime]::TryParseExact($dayClean, $AcceptedFormats, $culture,
                        [System.Globalization.DateTimeStyles]::None, [ref]$tmp)
                    if (-not $ok) {
                        # Fallback: culture-aware parse to catch edge cases like single-digit variations
                        $ok = [datetime]::TryParse($dayClean, $culture,
                            [System.Globalization.DateTimeStyles]::AllowWhiteSpaces, [ref]$tmp)
                    }
                    if ($ok) { $dayDate = $tmp }
                    else {
                        $errReason = "InvalidDate('$dayRaw')"
                        $isValid   = $false
                    }
                }
            }

            # Description normalization
            $body = [string]($r.Description ?? '')
            if ($NormalizeDescription -and $body) {
                $body = ($body -replace '(\r\n|\r|\n)+',' ') -replace '\s{2,}',' '
                $body = $body.Trim()
            }

            # All-day local boundaries (wall-clock)
            $startLocal = $null
            $endLocal   = $null
            if ($isValid) {
                $startLocal = [datetime]::SpecifyKind($dayDate.Date, [System.DateTimeKind]::Unspecified)
                $endLocal   = $startLocal.AddDays(1)
            }

            [pscustomobject]@{
                RowNumber  = [int]$r.RowNumber
                Subject    = $subject
                Day        = if ($dayDate) { $dayDate.ToString('yyyy-MM-dd') } else { $null }
                StartLocal = $startLocal
                EndLocal   = $endLocal
                TimeZoneId = $tz.Id
                IsAllDay   = $true
                Body       = $body
                IsValid    = $isValid
                Error      = $errReason
                Key        = if ($isValid) { '{0}|{1}' -f $subject, $dayDate.ToString('yyyy-MM-dd') } else { $null }
            }
        }
    }
}

function Find-CsvDuplicateEvent {
<#
.SYNOPSIS
Flags duplicate events in the CSV based on Title+Day (canonical Key), keeping the first occurrence.

.DESCRIPTION
Consumes canonical event objects produced by ConvertTo-CanonicalEvent (called internally, no parameters).
Determines duplicates using the Key = "Subject|yyyy-MM-dd". The first occurrence of a given Key is marked
as unique; subsequent occurrences are flagged as DuplicateInCsv. Invalid rows (IsValid=$false) are passed
through unchanged and never marked as duplicates.

ASSUMPTIONS
- Duplicate comparison is case-insensitive on the Key (Subject|Date).  # TODO: Change if strict case is desired.

.EXAMPLE
Find-CsvDuplicateEvent | Format-Table RowNumber,Subject,Day,IsValid,IsDuplicateInCsv,DuplicateIndex,DuplicateCount

.OUTPUTS
pscustomobject
Original canonical fields from ConvertTo-CanonicalEvent plus:
- IsDuplicateInCsv [bool]  : $true for 2nd+ occurrence of the same Key; $false otherwise.
- DuplicateIndex    [int?] : 0 for first occurrence; 1 for second, etc. $null when invalid/no key.
- DuplicateCount    [int]  : Total occurrences for that Key (0 when invalid/no key).
- Reason            [string]: 'DuplicateInCsv' when IsDuplicateInCsv=$true, otherwise $null.

.NOTES
- Compatible with PowerShell 5.1+ and 7+.
- No external parameters; uses hardcoded configuration and upstream functions.
#>
    [CmdletBinding()]
    param()

    begin {
        Set-StrictMode -Version Latest
        Write-Verbose "Find-CsvDuplicateEvent: Loading canonical events..."
    }
    process {
        $events = @( ConvertTo-CanonicalEvent )
        if (-not $events -or $events.Count -eq 0) {
            Write-Information "Find-CsvDuplicateEvent: No canonical events available."
            return
        }

        # Build total counts per (case-insensitive) Key for valid rows only
        $groupCounts = @{}
        foreach ($e in $events) {
            if ($e.IsValid -and $e.Key) {
                $k = $e.Key.ToLowerInvariant()
                if ($groupCounts.ContainsKey($k)) { $groupCounts[$k]++ } else { $groupCounts[$k] = 1 }
            }
        }

        # Track seen occurrences to compute DuplicateIndex in original row order
        $seen = @{}
        foreach ($e in $events) {
            if (-not $e.IsValid -or -not $e.Key) {
                # Pass-through for invalid rows (never marked as duplicates)
                [pscustomobject]@{
                    RowNumber        = $e.RowNumber
                    Subject          = $e.Subject
                    Day              = $e.Day
                    StartLocal       = $e.StartLocal
                    EndLocal         = $e.EndLocal
                    TimeZoneId       = $e.TimeZoneId
                    IsAllDay         = $e.IsAllDay
                    Body             = $e.Body
                    IsValid          = $e.IsValid
                    Error            = $e.Error
                    Key              = $e.Key
                    IsDuplicateInCsv = $false
                    DuplicateIndex   = $null
                    DuplicateCount   = 0
                    Reason           = $null
                }
                continue
            }

            $k = $e.Key.ToLowerInvariant()
            if ($seen.ContainsKey($k)) { $seen[$k]++ } else { $seen[$k] = 1 }
            $dupIndex = $seen[$k] - 1
            $isDup    = ($dupIndex -gt 0)
            $count    = $groupCounts[$k]

            [pscustomobject]@{
                RowNumber        = $e.RowNumber
                Subject          = $e.Subject
                Day              = $e.Day
                StartLocal       = $e.StartLocal
                EndLocal         = $e.EndLocal
                TimeZoneId       = $e.TimeZoneId
                IsAllDay         = $e.IsAllDay
                Body             = $e.Body
                IsValid          = $e.IsValid
                Error            = $e.Error
                Key              = $e.Key
                IsDuplicateInCsv = $isDup
                DuplicateIndex   = if ($isDup) { [int]$dupIndex } else { 0 }
                DuplicateCount   = [int]$count
                Reason           = if ($isDup) { 'DuplicateInCsv' } else { $null }
            }
        }
    }
}

function Get-CalendarMatchPreview {
<#
.SYNOPSIS
Produces a preview action per row (WouldCreate / Invalid / DuplicateInCsv) without modifying any calendar.

.DESCRIPTION
Consumes the output of Find-CsvDuplicateEvent (internally invoked; no parameters).
Since online calendar matching is not implemented yet, valid & unique rows are
provisionally marked as **WouldCreate**. Invalid rows and CSV duplicates are surfaced
with their reasons. This function is read-only and intended for Phase 1 preview.

NOTE: In Phase 2, this preview will be upgraded to query the target calendar to
differentiate WouldCreate vs WouldUpdate vs WouldSkip.

.CONFIGURATION
- No parameters; uses upstream hardcoded settings and functions.

.EXAMPLE
Get-CalendarMatchPreview | Format-Table RowNumber,Subject,Day,Decision,Reason

.OUTPUTS
pscustomobject
Properties:
- RowNumber           [int]
- Subject             [string]
- Day                 [string]   (yyyy-MM-dd)
- Key                 [string]   (Subject|yyyy-MM-dd)
- IsValid             [bool]
- IsDuplicateInCsv    [bool]
- Decision            [string]   ('WouldCreate'|'Invalid'|'DuplicateInCsv')
- Reason              [string]   (error/duplicate reason)
- Notes               [string]   (e.g., 'Calendar lookup not implemented')
#>
    [CmdletBinding()]
    param()

    begin {
        Set-StrictMode -Version Latest
        Write-Verbose "Get-CalendarMatchPreview: Gathering canonical rows with duplicate flags..."
    }
    process {
        try {
            $rows = @( Find-CsvDuplicateEvent )
            if (-not $rows -or $rows.Count -eq 0) {
                Write-Information "Get-CalendarMatchPreview: No rows to preview."
                return
            }

            foreach ($e in $rows) {
                # Determine decision based on validity and CSV-duplicate status only.
                if (-not $e.IsValid) {
                    [pscustomobject]@{
                        RowNumber        = [int]$e.RowNumber
                        Subject          = [string]$e.Subject
                        Day              = [string]$e.Day
                        Key              = [string]$e.Key
                        IsValid          = $false
                        IsDuplicateInCsv = $false
                        Decision         = 'Invalid'
                        Reason           = [string]$e.Error
                        Notes            = 'Calendar lookup not implemented'
                    }
                    continue
                }

                if ($e.IsDuplicateInCsv) {
                    [pscustomobject]@{
                        RowNumber        = [int]$e.RowNumber
                        Subject          = [string]$e.Subject
                        Day              = [string]$e.Day
                        Key              = [string]$e.Key
                        IsValid          = $true
                        IsDuplicateInCsv = $true
                        Decision         = 'DuplicateInCsv'
                        Reason           = 'DuplicateInCsv'
                        Notes            = 'Calendar lookup not implemented'
                    }
                    continue
                }

                # Valid & unique → provisional WouldCreate (until calendar match is added in Phase 2)
                [pscustomobject]@{
                    RowNumber        = [int]$e.RowNumber
                    Subject          = [string]$e.Subject
                    Day              = [string]$e.Day
                    Key              = [string]$e.Key
                    IsValid          = $true
                    IsDuplicateInCsv = $false
                    Decision         = 'WouldCreate'  # TODO(Phase 2): Could be WouldUpdate/WouldSkip after calendar query
                    Reason           = $null
                    Notes            = 'Calendar lookup not implemented'
                }
            }
        }
        catch {
            Write-Error -Message ("Get-CalendarMatchPreview failed: {0}" -f $_.Exception.Message) -ErrorAction Continue
        }
    }
}

function Connect-CalendarGraph {
<#
.SYNOPSIS
Establishes a delegated Microsoft Graph connection for calendar work (hardcoded config).

.DESCRIPTION
- Uses interactive sign-in with the Microsoft Graph PowerShell SDK.
- Requests the delegated scope **Group.ReadWrite.All** (sufficient for Microsoft 365 Group calendars).
- Idempotent: if an existing Graph context already has the required scope, it is reused.
- No parameters by design (project decision). Adjust config inside the function if needed.

.CONFIGURATION
- Required scope: Group.ReadWrite.All
- Environment: Default Graph cloud (Global)
- PS: 7+ recommended
- Module: Microsoft.Graph (specifically, Microsoft.Graph.Authentication)

.EXAMPLE
Connect-CalendarGraph -Verbose
Returns a context object describing the active Graph connection.

.OUTPUTS
pscustomobject
Properties:
- Connected        [bool]
- Account          [string]
- TenantId         [string]
- Environment      [string]
- ScopesGranted    [string[]]
- RequiredScope    [string]
- Status           [string]   ('Connected' | 'AlreadyConnected' | 'Error')
- Message          [string]

.NOTES
- Does not write secrets to logs.
- If the Microsoft.Graph SDK is missing, emits a terminating error with install guidance.
- This function does not modify calendar data; it only establishes auth context.
#>
    [CmdletBinding()]
    param()

    begin {
        Set-StrictMode -Version Latest

        # --- Hardcoded configuration ---
        $RequiredScope = 'Group.ReadWrite.All'
        $GraphEnvironment = 'Global'  # default cloud
        # --------------------------------

        Write-Verbose ("Auth config: Scope='{0}', Environment='{1}'" -f $RequiredScope, $GraphEnvironment)

        # Ensure Microsoft Graph SDK is available (need Connect-MgGraph)
        if (-not (Get-Command -Name Connect-MgGraph -ErrorAction SilentlyContinue)) {
            $ex = New-Object System.InvalidOperationException("Microsoft Graph PowerShell SDK not found. Install with: Install-Module Microsoft.Graph -Scope CurrentUser")
            $err = New-Object System.Management.Automation.ErrorRecord($ex, 'GraphSdkMissing', [System.Management.Automation.ErrorCategory]::ResourceUnavailable, 'Microsoft.Graph')
            $PSCmdlet.ThrowTerminatingError($err)
        }
    }
    process {
        try {
            # Idempotency: if already connected with required scope, reuse
            $ctx = $null
            try { $ctx = Get-MgContext -ErrorAction Stop } catch { $ctx = $null }

            if ($ctx -and $ctx.Account -and $ctx.Scopes -and ($ctx.Scopes -contains $RequiredScope)) {
                Write-Verbose ("Existing Graph context found for '{0}' with required scope." -f $ctx.Account)
                return [pscustomobject]@{
                    Connected     = $true
                    Account       = [string]$ctx.Account
                    TenantId      = [string]$ctx.TenantId
                    Environment   = [string]$ctx.Environment
                    ScopesGranted = [string[]]$ctx.Scopes
                    RequiredScope = $RequiredScope
                    Status        = 'AlreadyConnected'
                    Message       = 'Reused existing Microsoft Graph connection.'
                }
            }

            # Connect interactively for delegated scope
            Write-Verbose "Connecting to Microsoft Graph (interactive sign-in)..."
            Connect-MgGraph -Scopes $RequiredScope -NoWelcome -ContextScope Process -ErrorAction Stop | Out-Null

            # Validate context & scope
            $ctx = Get-MgContext -ErrorAction Stop
            if (-not $ctx -or -not $ctx.Account) {
                $ex = New-Object System.InvalidOperationException("Graph context not established after Connect-MgGraph.")
                $err = New-Object System.Management.Automation.ErrorRecord($ex, 'GraphContextMissing', [System.Management.Automation.ErrorCategory]::AuthenticationError, $null)
                $PSCmdlet.ThrowTerminatingError($err)
            }
            if (-not ($ctx.Scopes -contains $RequiredScope)) {
                # Clean up to avoid partial state
                try { Disconnect-MgGraph -ErrorAction SilentlyContinue | Out-Null } catch { }
                $ex = New-Object System.UnauthorizedAccessException("Required scope '$RequiredScope' not granted. Please consent and retry.")
                $err = New-Object System.Management.Automation.ErrorRecord($ex, 'GraphScopeInsufficient', [System.Management.Automation.ErrorCategory]::PermissionDenied, $RequiredScope)
                $PSCmdlet.ThrowTerminatingError($err)
            }

            Write-Verbose ("Connected. Account='{0}', Tenant='{1}', Scopes=[{2}]" -f $ctx.Account, $ctx.TenantId, ($ctx.Scopes -join ', '))
            return [pscustomobject]@{
                Connected     = $true
                Account       = [string]$ctx.Account
                TenantId      = [string]$ctx.TenantId
                Environment   = [string]$ctx.Environment
                ScopesGranted = [string[]]$ctx.Scopes
                RequiredScope = $RequiredScope
                Status        = 'Connected'
                Message       = 'Microsoft Graph connection established.'
            }
        }
        catch {
            $msg = $_.Exception.Message
            Write-Error -Message ("Connect-CalendarGraph failed: {0}" -f $msg) -ErrorAction Continue
            return [pscustomobject]@{
                Connected     = $false
                Account       = $null
                TenantId      = $null
                Environment   = $GraphEnvironment
                ScopesGranted = @()
                RequiredScope = $RequiredScope
                Status        = 'Error'
                Message       = $msg
            }
        }
    }
}

function Resolve-GroupCalendar {
<#
.SYNOPSIS
Resolves the Microsoft 365 Group (by SMTP) and its default Calendar using Microsoft Graph.

.DESCRIPTION
- Ensures a Microsoft Graph delegated connection (via Connect-CalendarGraph).
- Looks up the target Group by its primary SMTP address (hardcoded).
- Retrieves the Group's default Calendar (Calendar.Id, Name).
- Emits a structured object for downstream use.

.CONFIGURATION
- Target Group SMTP (UAT): test-earnings-calendar@oxford-dp.com
  # TODO: switch to earnings-calendar@oxford-dp.com for production.

.EXAMPLE
Resolve-GroupCalendar -Verbose

.OUTPUTS
pscustomobject
Properties:
- Status            [string]   ('Resolved'|'Error')
- GroupMail         [string]
- GroupId           [string]
- GroupDisplayName  [string]
- CalendarId        [string]
- CalendarName      [string]
- Message           [string]

.NOTES
- Requires Microsoft Graph PowerShell SDK and prior consent for Group.ReadWrite.All.
- Throws a terminating error when the group cannot be found or calendar retrieval fails.
#>
    [CmdletBinding()]
    param()

    begin {
        Set-StrictMode -Version Latest

        # --- Hardcoded configuration (no external parameters) ---
        $TargetGroupSmtp = 'earnings-calendar@oxford-dp.com'
        # --------------------------------------------------------

        Write-Verbose ("Resolve-GroupCalendar: Target SMTP = '{0}'" -f $TargetGroupSmtp)

        # Ensure Graph is connected (idempotent)
        $ctx = Connect-CalendarGraph
        if (-not $ctx.Connected) {
            $ex  = New-Object System.InvalidOperationException("Microsoft Graph not connected: $($ctx.Message)")
            $err = New-Object System.Management.Automation.ErrorRecord($ex, 'GraphNotConnected', [System.Management.Automation.ErrorCategory]::AuthenticationError, $null)
            $PSCmdlet.ThrowTerminatingError($err)
        }
    }
    process {
        try {
            # Primary lookup: exact match by primary SMTP (mail eq '...'), which should be unique
            $smtpEsc = $TargetGroupSmtp.Replace("'", "''")
            $filter  = "mail eq '$smtpEsc'"

            Write-Verbose ("Querying group by filter: {0}" -f $filter)
            $grp = Get-MgGroup -Filter $filter -ErrorAction Stop

            # Fallback: try proxyAddresses/any(...) or search if no exact hit
            if (-not $grp) {
                Write-Verbose "Primary match not found. Trying proxyAddresses filter..."
                $proxy = "proxyAddresses/any(c:c eq 'SMTP:$smtpEsc')"
                try { $grp = Get-MgGroup -Filter $proxy -ErrorAction Stop } catch { }
            }
            if (-not $grp) {
                Write-Verbose "No proxy match. Trying search (ConsistencyLevel eventual)..."
                try {
                    $grp = Get-MgGroup -Search "`"$TargetGroupSmtp`"" -ConsistencyLevel eventual -ErrorAction Stop
                } catch { }
            }

            if (-not $grp) {
                $ex  = New-Object System.Management.Automation.ItemNotFoundException("Microsoft 365 Group not found for SMTP '$TargetGroupSmtp'.")
                $err = New-Object System.Management.Automation.ErrorRecord($ex, 'GroupNotFound', [System.Management.Automation.ErrorCategory]::ObjectNotFound, $TargetGroupSmtp)
                $PSCmdlet.ThrowTerminatingError($err)
            }

            # If multiple are returned (unlikely), pick the one whose Mail matches exactly first
            if ($grp -is [System.Array]) {
                $exact = $grp | Where-Object { $_.Mail -eq $TargetGroupSmtp } | Select-Object -First 1
                $grp = if ($exact) { $exact } else { $grp | Select-Object -First 1 }
            }

            $groupId   = [string]$grp.Id
            $groupName = [string]$grp.DisplayName
            $groupMail = [string]$grp.Mail

            if ([string]::IsNullOrWhiteSpace($groupId)) {
                $ex  = New-Object System.InvalidOperationException("Resolved group has no Id. DisplayName='$groupName', Mail='$groupMail'.")
                $err = New-Object System.Management.Automation.ErrorRecord($ex, 'GroupIdMissing', [System.Management.Automation.ErrorCategory]::InvalidData, $grp)
                $PSCmdlet.ThrowTerminatingError($err)
            }

            Write-Verbose ("Resolved Group: Id='{0}', Name='{1}', Mail='{2}'" -f $groupId, $groupName, $groupMail)

            # Retrieve default calendar for the group (singular calendar resource)
            Write-Verbose "Retrieving group's default Calendar..."
            $cal = Get-MgGroupCalendar -GroupId $groupId -ErrorAction Stop
            if (-not $cal -or -not $cal.Id) {
                $ex  = New-Object System.InvalidOperationException("Group calendar not available for GroupId '$groupId'.")
                $err = New-Object System.Management.Automation.ErrorRecord($ex, 'GroupCalendarNotFound', [System.Management.Automation.ErrorCategory]::ObjectNotFound, $groupId)
                $PSCmdlet.ThrowTerminatingError($err)
            }

            $calendarId   = [string]$cal.Id
            $calendarName = [string]($cal.Name ?? 'Calendar')

            Write-Verbose ("Resolved Calendar: Id='{0}', Name='{1}'" -f $calendarId, $calendarName)

            [pscustomobject]@{
                Status           = 'Resolved'
                GroupMail        = $groupMail
                GroupId          = $groupId
                GroupDisplayName = $groupName
                CalendarId       = $calendarId
                CalendarName     = $calendarName
                Message          = 'Group and default calendar resolved.'
            }
        }
        catch {
            $msg = $_.Exception.Message
            Write-Error -Message ("Resolve-GroupCalendar failed: {0}" -f $msg) -ErrorAction Continue
            [pscustomobject]@{
                Status           = 'Error'
                GroupMail        = $TargetGroupSmtp
                GroupId          = $null
                GroupDisplayName = $null
                CalendarId       = $null
                CalendarName     = $null
                Message          = $msg
            }
        }
    }
}

function Get-CalendarMatchDecision {
<#
.SYNOPSIS
Queries the Microsoft 365 Group calendar to decide per-row: WouldCreate / WouldUpdate / WouldSkip.

.DESCRIPTION
Fixes a logic bug: an empty calendarView (no events that day) now yields **WouldCreate** instead of producing no output.
Also simplifies retry logic and cleanly emits a single Error decision on repeated failures.

.PREREQUISITES
- Connect-CalendarGraph (delegated, Group.ReadWrite.All)
- Resolve-GroupCalendar

.OUTPUTS
pscustomobject with:
RowNumber, Subject, Day, Key, Decision ('WouldCreate'|'WouldUpdate'|'WouldSkip'|'Invalid'|'DuplicateInCsv'|'Error'),
Reason, MatchedItemId, Differences (string[])
#>
    [CmdletBinding()]
    param()

    begin {
        Set-StrictMode -Version Latest

        # --- Hardcoded configuration ---
        $PreferredTzIds = @('Europe/London','GMT Standard Time')
        $PreferHeader   = @{ 'Prefer' = 'outlook.timezone="Europe/London"' }
        $MaxRetries     = 3
        $BackoffSeconds = 2
        # --------------------------------

        function Normalize-PlainText([string]$s) {
            if ([string]::IsNullOrWhiteSpace($s)) { return '' }
            return (($s -replace '(\r\n|\r|\n)+',' ') -replace '\s{2,}',' ').Trim()
        }
        function Strip-Html([string]$html) {
            if ([string]::IsNullOrEmpty($html)) { return '' }
            $t = ($html -replace '<[^>]+>',' ')
            return Normalize-PlainText $t
        }

        # Resolve London TZ for UTC conversions
        $tz = $null
        foreach ($id in $PreferredTzIds) { try { $tz = [System.TimeZoneInfo]::FindSystemTimeZoneById($id); break } catch { } }
        if (-not $tz) { throw "Unable to resolve a London timezone. Tried: $($PreferredTzIds -join ', ')" }

        # Ensure Graph is connected & group calendar is resolved
        $ctx = Connect-CalendarGraph
        if (-not $ctx.Connected) {
            $ex  = New-Object System.InvalidOperationException("Graph not connected: $($ctx.Message)")
            $err = New-Object System.Management.Automation.ErrorRecord($ex, 'GraphNotConnected', [System.Management.Automation.ErrorCategory]::AuthenticationError, $null)
            $PSCmdlet.ThrowTerminatingError($err)
        }

        $res = Resolve-GroupCalendar
        if ($res.Status -ne 'Resolved') {
            $ex  = New-Object System.InvalidOperationException("Group calendar not resolved: $($res.Message)")
            $err = New-Object System.Management.Automation.ErrorRecord($ex, 'CalendarNotResolved', [System.Management.Automation.ErrorCategory]::ObjectNotFound, $null)
            $PSCmdlet.ThrowTerminatingError($err)
        }
        $groupId = $res.GroupId
    }
    process {
        $rows = @( Find-CsvDuplicateEvent )
        if (-not $rows -or $rows.Count -eq 0) {
            Write-Information "Get-CalendarMatchDecision: No input rows available."
            return
        }

        foreach ($e in $rows) {
            # Pass through invalid & CSV-duplicate rows
            if (-not $e.IsValid) {
                [pscustomobject]@{
                    RowNumber     = [int]$e.RowNumber
                    Subject       = [string]$e.Subject
                    Day           = [string]$e.Day
                    Key           = [string]$e.Key
                    Decision      = 'Invalid'
                    Reason        = [string]$e.Error
                    MatchedItemId = $null
                    Differences   = @()
                }
                continue
            }
            if ($e.IsDuplicateInCsv) {
                [pscustomobject]@{
                    RowNumber     = [int]$e.RowNumber
                    Subject       = [string]$e.Subject
                    Day           = [string]$e.Day
                    Key           = [string]$e.Key
                    Decision      = 'DuplicateInCsv'
                    Reason        = 'DuplicateInCsv'
                    MatchedItemId = $null
                    Differences   = @()
                }
                continue
            }

            # Compute UTC window for all-day event (midnight→midnight London)
            $startUtc = [TimeZoneInfo]::ConvertTimeToUtc($e.StartLocal, $tz)
            $endUtc   = [TimeZoneInfo]::ConvertTimeToUtc($e.EndLocal,   $tz)

            # Query group calendarView with retries
            $attempt = 0
            $view    = $null
            $failed  = $false
            while ($attempt -lt $MaxRetries) {
                try {
                    $attempt++
                    $view = Get-MgGroupCalendarView `
                        -GroupId $groupId `
                        -StartDateTime ($startUtc.ToString('o')) `
                        -EndDateTime   ($endUtc.ToString('o')) `
                        -Headers $PreferHeader `
                        -ErrorAction Stop
                    break
                }
                catch {
                    if ($attempt -lt $MaxRetries) {
                        Start-Sleep -Seconds $BackoffSeconds
                    } else {
                        $failed = $true
                        [pscustomobject]@{
                            RowNumber     = [int]$e.RowNumber
                            Subject       = [string]$e.Subject
                            Day           = [string]$e.Day
                            Key           = [string]$e.Key
                            Decision      = 'Error'
                            Reason        = "CalendarView query failed after $MaxRetries attempts: $($_.Exception.Message)"
                            MatchedItemId = $null
                            Differences   = @()
                        }
                    }
                }
            }
            if ($failed) { continue }

            # Treat null as empty for matching (this is the bug fix)
            if ($null -eq $view) { $view = @() }

            # Match by Subject (case-insensitive) and IsAllDay
            $subject = [string]$e.Subject
            $matches = $view | Where-Object {
                ($_.IsAllDay -eq $true) -and
                ($_.Subject -and ($_.Subject.ToString()).Trim()) -and
                ($_.Subject.ToString().Trim().ToLowerInvariant() -eq $subject.ToLowerInvariant())
            }

            if (-not $matches -or $matches.Count -eq 0) {
                # No existing item that day with same subject → WouldCreate
                [pscustomobject]@{
                    RowNumber     = [int]$e.RowNumber
                    Subject       = $subject
                    Day           = [string]$e.Day
                    Key           = [string]$e.Key
                    Decision      = 'WouldCreate'
                    Reason        = $null
                    MatchedItemId = $null
                    Differences   = @()
                }
                continue
            }

            # Choose the first match and compare
            $match = $matches | Select-Object -First 1
            $matchId = [string]$match.Id

            $existingBody = ''
            if ($match.Body -and $match.Body.Content) {
                if ($match.Body.ContentType -eq 'html') {
                    $existingBody = Strip-Html $match.Body.Content
                } else {
                    $existingBody = Normalize-PlainText $match.Body.Content
                }
            }
            $incomingBody = Normalize-PlainText ([string]$e.Body)

            $diffs = @()
            if ($existingBody -ne $incomingBody) { $diffs += 'Body' }

            if ($diffs.Count -gt 0) {
                [pscustomobject]@{
                    RowNumber     = [int]$e.RowNumber
                    Subject       = $subject
                    Day           = [string]$e.Day
                    Key           = [string]$e.Key
                    Decision      = 'WouldUpdate'
                    Reason        = "Differences: $($diffs -join ', ')"
                    MatchedItemId = $matchId
                    Differences   = $diffs
                }
            }
            else {
                [pscustomobject]@{
                    RowNumber     = [int]$e.RowNumber
                    Subject       = $subject
                    Day           = [string]$e.Day
                    Key           = $string = [string]$e.Key
                    Decision      = 'WouldSkip'
                    Reason        = 'No changes detected'
                    MatchedItemId = $matchId
                    Differences   = @()
                }
            }
        }
    }
}


function New-GroupCalendarEventSafe {
<#
.SYNOPSIS
Creates all-day events in the Group calendar for rows decided as **WouldCreate**.

.DESCRIPTION
Reads decisions from Get-CalendarMatchDecision and creates events for those marked `WouldCreate`.
Uses SupportsShouldProcess so you can run with -WhatIf. Fixes a parser error by quoting the
`'end'` key in the Graph payload hashtable.

.EXAMPLE
# Preview (no writes)
New-GroupCalendarEventSafe -WhatIf | Format-Table -AutoSize

.EXAMPLE
# Create new events (writes)
New-GroupCalendarEventSafe | Format-Table -AutoSize

.OUTPUTS
pscustomobject
- RowNumber, Subject, Day
- Action ('Create'), Status ('Success'|'WhatIf'|'Error')
- ItemId, Reason/Error

.NOTES
- Requires Microsoft Graph PowerShell SDK and delegated scope Group.ReadWrite.All.
- Idempotency is handled by the preview phase; this only processes WouldCreate.
#>
    [CmdletBinding(SupportsShouldProcess = $true, ConfirmImpact = 'Medium')]
    param()

    begin {
        Set-StrictMode -Version Latest

        # --- Hardcoded configuration ---
        $PreferHeader   = @{ 'Prefer' = 'outlook.timezone="Europe/London"' }
        $PayloadTz      = 'Europe/London'
        $MaxRetries     = 3
        $BackoffSeconds = 2
        # --------------------------------

        # Ensure Graph is connected and calendar is resolved
        $ctx = Connect-CalendarGraph
        if (-not $ctx.Connected) {
            $ex  = New-Object System.InvalidOperationException("Graph not connected: $($ctx.Message)")
            $err = New-Object System.Management.Automation.ErrorRecord($ex, 'GraphNotConnected', [System.Management.Automation.ErrorCategory]::AuthenticationError, $null)
            $PSCmdlet.ThrowTerminatingError($err)
        }

        $res = Resolve-GroupCalendar
        if ($res.Status -ne 'Resolved') {
            $ex  = New-Object System.InvalidOperationException("Group calendar not resolved: $($res.Message)")
            $err = New-Object System.Management.Automation.ErrorRecord($ex, 'CalendarNotResolved', [System.Management.Automation.ErrorCategory]::ObjectNotFound, $null)
            $PSCmdlet.ThrowTerminatingError($err)
        }
        $groupId = $res.GroupId
        Write-Verbose ("Using GroupId='{0}', CalendarId='{1}'" -f $groupId, $res.CalendarId)
    }
    process {
        $decisions = @( Get-CalendarMatchDecision )
        if (-not $decisions -or $decisions.Count -eq 0) {
            Write-Information "New-GroupCalendarEventSafe: No decision rows available."
            return
        }

        $createQueue = $decisions | Where-Object { $_.Decision -eq 'WouldCreate' }
        if (-not $createQueue -or $createQueue.Count -eq 0) {
            Write-Information "New-GroupCalendarEventSafe: Nothing to create (no 'WouldCreate' rows)."
            return
        }

        foreach ($d in $createQueue) {
            # Parse Day (yyyy-MM-dd) to local wall-clock boundaries: 00:00 → +1 day
            $dayString = [string]$d.Day
            try {
                $dayDate = [datetime]::ParseExact($dayString, 'yyyy-MM-dd', [System.Globalization.CultureInfo]::InvariantCulture)
            } catch {
                [pscustomobject]@{
                    RowNumber = [int]$d.RowNumber
                    Subject   = [string]$d.Subject
                    Day       = $dayString
                    Action    = 'Create'
                    Status    = 'Error'
                    ItemId    = $null
                    Reason    = "Invalid canonical day format: '$dayString'"
                }
                continue
            }

            $startLocal = '{0:yyyy-MM-dd}T00:00:00' -f $dayDate
            $endLocal   = '{0:yyyy-MM-dd}T00:00:00' -f $dayDate.AddDays(1)

            $subject = [string]$d.Subject

            # Prefer the canonical body (normalized) if available
            $canon = $null
            try {
                $canon = @( ConvertTo-CanonicalEvent ) | Where-Object { $_.Key -eq $d.Key } | Select-Object -First 1
            } catch { $canon = $null }
            $bodyTxt = if ($canon -and $canon.Body) { [string]$canon.Body } else { '' }

            # IMPORTANT FIX: quote the 'end' key in the payload hashtable
            $payload = @{
                subject  = $subject
                isAllDay = $true
                start    = @{ dateTime = $startLocal; timeZone = $PayloadTz }
                'end'    = @{ dateTime = $endLocal;   timeZone = $PayloadTz }
                body     = @{ contentType = 'Text'; content = $bodyTxt }
            }

            $targetLabel = "Group:$groupId Day:$dayString Subject:'$subject'"
            if ($PSCmdlet.ShouldProcess($targetLabel, 'Create event')) {
                # Call Graph with retry on transient failures
                $attempt = 0
                $created = $null
                do {
                    try {
                        $attempt++
                        $created = New-MgGroupEvent -GroupId $groupId -BodyParameter $payload -Headers $PreferHeader -ErrorAction Stop
                    } catch {
                        $isTransient = ($_.Exception.Message -match '429|503|504|timeout|throttle')
                        if ($isTransient -and $attempt -lt $MaxRetries) {
                            Start-Sleep -Seconds $BackoffSeconds
                            continue
                        } else {
                            $errMsg = $_.Exception.Message
                            [pscustomobject]@{
                                RowNumber = [int]$d.RowNumber
                                Subject   = $subject
                                Day       = $dayString
                                Action    = 'Create'
                                Status    = 'Error'
                                ItemId    = $null
                                Reason    = "Create failed after $attempt attempt(s): $errMsg"
                            }
                            break
                        }
                    }
                    break
                } while ($true)

                if ($created -and $created.Id) {
                    [pscustomobject]@{
                        RowNumber = [int]$d.RowNumber
                        Subject   = $subject
                        Day       = $dayString
                        Action    = 'Create'
                        Status    = 'Success'
                        ItemId    = [string]$created.Id
                        Reason    = $null
                    }
                }
            }
            else {
                # -WhatIf path
                [pscustomobject]@{
                    RowNumber = [int]$d.RowNumber
                    Subject   = $subject
                    Day       = $dayString
                    Action    = 'Create'
                    Status    = 'WhatIf'
                    ItemId    = $null
                    Reason    = 'WhatIf: creation not executed'
                }
            }
        }
    }
}

function Set-GroupCalendarEventSafe {
<#
.SYNOPSIS
Updates existing Group calendar events for rows decided as **WouldUpdate** (from Get-CalendarMatchDecision).

.DESCRIPTION
- Reads preview decisions and applies updates only to matched items needing changes.
- Safe-by-default via ShouldProcess: run with -WhatIf for a non-mutating preview.
- Updates **Subject** and/or **Body** (text) based on detected Differences.

.PREREQUISITES
- Microsoft Graph PowerShell SDK installed.
- Delegated auth with scope **Group.ReadWrite.All**.
- Connect-CalendarGraph + Resolve-GroupCalendar succeed.

.CONFIGURATION (hardcoded)
- Time zone header: Prefer outlook.timezone="Europe/London"
- Retries on transient failures: 3 (2s backoff)
- Fields updated: Subject, Body (text)

.EXAMPLE
# Preview only (no writes)
Set-GroupCalendarEventSafe -WhatIf | Format-Table -AutoSize

.EXAMPLE
# Apply updates
Set-GroupCalendarEventSafe | Format-Table -AutoSize

.OUTPUTS
pscustomobject
- RowNumber, Subject, Day, Action('Update'), Status('Success'|'WhatIf'|'Error'), ItemId, Changes(string[]), Reason

.NOTES
- Idempotent behavior: does nothing if the server item already matches (because only WouldUpdate rows are processed).
#>
    [CmdletBinding(SupportsShouldProcess = $true, ConfirmImpact = 'Medium')]
    param()

    begin {
        Set-StrictMode -Version Latest

        # --- Hardcoded configuration ---
        $PreferHeader   = @{ 'Prefer' = 'outlook.timezone="Europe/London"' }
        $MaxRetries     = 3
        $BackoffSeconds = 2
        # --------------------------------

        # Ensure Graph connection + calendar resolution
        $ctx = Connect-CalendarGraph
        if (-not $ctx.Connected) {
            $ex  = New-Object System.InvalidOperationException("Graph not connected: $($ctx.Message)")
            $err = New-Object System.Management.Automation.ErrorRecord($ex, 'GraphNotConnected', [System.Management.Automation.ErrorCategory]::AuthenticationError, $null)
            $PSCmdlet.ThrowTerminatingError($err)
        }

        $res = Resolve-GroupCalendar
        if ($res.Status -ne 'Resolved') {
            $ex  = New-Object System.InvalidOperationException("Group calendar not resolved: $($res.Message)")
            $err = New-Object System.Management.Automation.ErrorRecord($ex, 'CalendarNotResolved', [System.Management.Automation.ErrorCategory]::ObjectNotFound, $null)
            $PSCmdlet.ThrowTerminatingError($err)
        }
        $groupId = $res.GroupId
        Write-Verbose ("Set-GroupCalendarEventSafe: Using GroupId='{0}', CalendarId='{1}'" -f $groupId, $res.CalendarId)
    }
    process {
        $decisions = @( Get-CalendarMatchDecision )
        if (-not $decisions -or $decisions.Count -eq 0) {
            Write-Information "Set-GroupCalendarEventSafe: No decision rows available."
            return
        }

        $updateQueue = $decisions | Where-Object { $_.Decision -eq 'WouldUpdate' -and $_.MatchedItemId }
        if (-not $updateQueue -or $updateQueue.Count -eq 0) {
            Write-Information "Set-GroupCalendarEventSafe: Nothing to update (no 'WouldUpdate' rows)."
            return
        }

        foreach ($d in $updateQueue) {
            $eventId  = [string]$d.MatchedItemId
            $subject  = [string]$d.Subject
            $dayValue = [string]$d.Day
            $changes  = @()

            # Pull the canonical body/subject from normalized rows by Key
            $canon = $null
            try { $canon = @( ConvertTo-CanonicalEvent ) | Where-Object { $_.Key -eq $d.Key } | Select-Object -First 1 } catch { $canon = $null }
            $newSubject = if ($canon -and $canon.Subject) { [string]$canon.Subject } else { $subject }
            $newBodyTxt = if ($canon -and $canon.Body)    { [string]$canon.Body    } else { '' }

            if ($newSubject -ne $subject) { $changes += 'Subject' }
            if ($d.Differences -and ($d.Differences -contains 'Body')) { $changes += 'Body' }

            # If Differences didn’t enumerate but we have canonical text, still include Body to be safe
            if (-not $changes -and $newBodyTxt) { $changes += 'Body' }

            # Build minimal PATCH payload
            $payload = @{}
            if ($changes -contains 'Subject') { $payload['subject'] = $newSubject }
            if ($changes -contains 'Body')    { $payload['body']    = @{ contentType = 'Text'; content = $newBodyTxt } }

            # If nothing to change (edge case), emit Skip-equivalent result
            if ($payload.Count -eq 0) {
                [pscustomobject]@{
                    RowNumber = [int]$d.RowNumber
                    Subject   = $subject
                    Day       = $dayValue
                    Action    = 'Update'
                    Status    = 'WhatIf'
                    ItemId    = $eventId
                    Changes   = @()
                    Reason    = 'No effective changes to apply'
                }
                continue
            }

            $targetLabel = "Group:$groupId Event:$eventId Day:$dayValue Subject:'$newSubject'"
            if ($PSCmdlet.ShouldProcess($targetLabel, 'Update event')) {
                $attempt = 0
                $ok      = $false
                do {
                    try {
                        $attempt++
                        Update-MgGroupEvent -GroupId $groupId -EventId $eventId -BodyParameter $payload -Headers $PreferHeader -ErrorAction Stop
                        $ok = $true
                    } catch {
                        $isTransient = ($_.Exception.Message -match '429|503|504|timeout|throttle')
                        if ($isTransient -and $attempt -lt $MaxRetries) {
                            Start-Sleep -Seconds $BackoffSeconds
                            continue
                        } else {
                            $errMsg = $_.Exception.Message
                            [pscustomobject]@{
                                RowNumber = [int]$d.RowNumber
                                Subject   = $newSubject
                                Day       = $dayValue
                                Action    = 'Update'
                                Status    = 'Error'
                                ItemId    = $eventId
                                Changes   = $changes
                                Reason    = "Update failed after $attempt attempt(s): $errMsg"
                            }
                            break
                        }
                    }
                    break
                } while ($true)

                if ($ok) {
                    [pscustomobject]@{
                        RowNumber = [int]$d.RowNumber
                        Subject   = $newSubject
                        Day       = $dayValue
                        Action    = 'Update'
                        Status    = 'Success'
                        ItemId    = $eventId
                        Changes   = $changes
                        Reason    = $null
                    }
                }
            }
            else {
                # -WhatIf path
                [pscustomobject]@{
                    RowNumber = [int]$d.RowNumber
                    Subject   = $newSubject
                    Day       = $dayValue
                    Action    = 'Update'
                    Status    = 'WhatIf'
                    ItemId    = $eventId
                    Changes   = $changes
                    Reason    = 'WhatIf: update not executed'
                }
            }
        }
    }
}

function Export-CalendarImportLog {
<#
.SYNOPSIS
Exports a per-run CSV log (plus a summary section) to C:\CalendarODP\Log_YYYYMMDD.csv.

.DESCRIPTION
Hardened to avoid "Count property" errors:
- Forces arrays with @(...).
- Uses @(...).Count everywhere.
- If there are 0 preview rows, still writes a summary-only log.

.CONFIGURATION (hardcoded)
- Base folder: C:\CalendarODP
- Input CSV :  C:\CalendarODP\events.csv
- Log file  :  C:\CalendarODP\Log_YYYYMMDD.csv
- Encoding  :  UTF-8
#>
    [CmdletBinding()]
    param()

    begin {
        Set-StrictMode -Version Latest

        $BaseDir  = 'C:\CalendarODP'
        $CsvPath  = Join-Path $BaseDir 'events.csv'
        $LogPath  = Join-Path $BaseDir ("Log_{0}.csv" -f (Get-Date -Format 'yyyyMMdd'))
        $Encoding = 'utf8'

        Write-Verbose ("Export-CalendarImportLog: BaseDir='{0}', CsvPath='{1}', LogPath='{2}'" -f $BaseDir,$CsvPath,$LogPath)

        if (-not (Test-Path -LiteralPath $BaseDir -PathType Container)) {
            Write-Information "Export-CalendarImportLog: Folder '$BaseDir' not found. No input/log will be produced."
            return
        }
        if (-not (Test-Path -LiteralPath $CsvPath -PathType Leaf)) {
            Write-Information "Export-CalendarImportLog: Input CSV '$CsvPath' not found. No log will be produced."
            return
        }
    }
    process {
        try {
            $runId  = [Guid]::NewGuid().ToString()
            $nowIso = (Get-Date).ToString('o')

            # Real preview decisions (array-safe)
            $preview = @( Get-CalendarMatchDecision )
            if (-not $preview) { $preview = @() }

            # Build per-row entries (array-safe even for 0/1 rows)
            $entries = @(
                foreach ($p in $preview) {
                    [pscustomobject]@{
                        RunId     = $runId
                        Timestamp = $nowIso
                        RowNumber = [int]$p.RowNumber
                        Subject   = [string]$p.Subject
                        Day       = [string]$p.Day
                        Action    = [string]$p.Decision
                        Status    = if ($p.Decision -eq 'Error') { 'Error' } else { 'Preview' }
                        ItemId    = [string]($p.MatchedItemId ?? '')
                        Reason    = [string]($p.Reason ?? '')
                        Key       = [string]($p.Key ?? '')
                    }
                }
            )

            # Totals (use @(...).Count to handle single objects)
            $totalRows = @($entries).Count
            $totals = @{
                WouldCreate    = @( $entries | Where-Object { $_.Action -eq 'WouldCreate' } ).Count
                WouldUpdate    = @( $entries | Where-Object { $_.Action -eq 'WouldUpdate' } ).Count
                WouldSkip      = @( $entries | Where-Object { $_.Action -eq 'WouldSkip' } ).Count
                Invalid        = @( $entries | Where-Object { $_.Action -eq 'Invalid' } ).Count
                DuplicateInCsv = @( $entries | Where-Object { $_.Action -eq 'DuplicateInCsv' } ).Count
                Error          = @( $entries | Where-Object { $_.Action -eq 'Error' } ).Count
            }

            # Write file: rows if any, then append summary
            if ($totalRows -gt 0) {
                $entries | Export-Csv -LiteralPath $LogPath -NoTypeInformation -Encoding $Encoding
            } else {
                # Create the file and write a header comment if there are no rows
                Set-Content -LiteralPath $LogPath -Value '# No preview rows; summary only' -Encoding $Encoding
            }

            Add-Content -LiteralPath $LogPath -Value ''
            Add-Content -LiteralPath $LogPath -Value '# Summary'
            Add-Content -LiteralPath $LogPath -Value ("# RunId,{0}" -f $runId)
            Add-Content -LiteralPath $LogPath -Value ("# Timestamp,{0}" -f $nowIso)
            Add-Content -LiteralPath $LogPath -Value ("# TotalRows,{0}" -f $totalRows)
            foreach ($k in 'WouldCreate','WouldUpdate','WouldSkip','Invalid','DuplicateInCsv','Error') {
                Add-Content -LiteralPath $LogPath -Value ("# {0},{1}" -f $k, $totals[$k])
            }

            Write-Information ("Export-CalendarImportLog: Wrote {0} row(s) to '{1}'." -f $totalRows, $LogPath)

            [pscustomobject]@{
                LogPath   = $LogPath
                RunId     = $runId
                TotalRows = $totalRows
                Totals    = $totals
                Status    = 'Success'
                Message   = if ($totalRows -gt 0) { 'Preview log and summary written.' } else { 'Summary-only log written (no preview rows).' }
            }
        }
        catch {
            $msg = $_.Exception.Message
            Write-Error -Message ("Export-CalendarImportLog failed: {0}" -f $msg) -ErrorAction Continue
            [pscustomobject]@{
                LogPath   = $LogPath
                RunId     = $runId
                TotalRows = 0
                Totals    = @{}
                Status    = 'Error'
                Message   = $msg
            }
        }
    }
}


function Invoke-CalendarImport {
<#
.SYNOPSIS
Runs the end-to-end calendar import pipeline with preview, (optional) create/update, and logging.

.DESCRIPTION
Sequence (hardcoded configuration; no parameters):
1) Validate CSV schema (presence + headers).
2) Build canonical rows, detect CSV duplicates, and query Microsoft Graph to get **preview decisions**
   (WouldCreate / WouldUpdate / WouldSkip / Invalid / DuplicateInCsv).
3) Export a per-run **preview log** to C:\CalendarODP\Log_YYYYMMDD.csv.
4) If not running with -WhatIf, **create** and **update** events as needed (idempotent by design).
5) Print a summary and return a structured result.

Safety:
- Uses `SupportsShouldProcess` and honors `-WhatIf` to prevent any changes.
- Child functions that mutate also honor `-WhatIf` (propagated).

.EXAMPLE
# Dry-run (no changes), write preview log, show summary
Invoke-CalendarImport -WhatIf -Verbose

.EXAMPLE
# Live run (creates/updates as needed), write preview log, show summary
Invoke-CalendarImport -Verbose

.OUTPUTS
pscustomobject
Properties:
- Mode                ('DryRun'|'Live')
- CsvPath             (string)
- SchemaStatus        ('Pass'|'Fail'|'Error')
- PreviewTotals       (hashtable of decisions)
- Created             (int)
- Updated             (int)
- Errors              (int)
- LogPath             (string; from Export-CalendarImportLog when available)
- Message             (string)
#>
    [CmdletBinding(SupportsShouldProcess = $true, ConfirmImpact = 'Medium')]
    param()

    begin {
        Set-StrictMode -Version Latest

        # --- Hardcoded configuration (keep aligned with earlier functions) ---
        $BaseDir = 'C:\CalendarODP'
        $CsvPath = Join-Path $BaseDir 'events.csv'
        # ---------------------------------------------------------------------

        $mode = if ($WhatIfPreference) { 'DryRun' } else { 'Live' }
        Write-Verbose ("Invoke-CalendarImport: Starting run. Mode={0}" -f $mode)
    }
    process {
        # 1) Schema validation (presence + headers)
        $schema = Test-CalendarCsvSchema
        if (-not $schema -or $schema.Status -ne 'Pass') {
            $reason = if ($schema) { $schema.Reason } else { 'Unknown' }
            Write-Information ("Invoke-CalendarImport: Schema check not passed. Status={0}; Reason={1}" -f ($schema.Status ?? 'N/A'), $reason)
            return [pscustomobject]@{
                Mode          = $mode
                CsvPath       = $CsvPath
                SchemaStatus  = $schema.Status
                PreviewTotals = @{}
                Created       = 0
                Updated       = 0
                Errors        = 0
                LogPath       = $null
                Message       = "Schema check failed or input missing: $reason"
            }
        }

        # 2) Preview decisions (Graph-backed)
        $preview = @( Get-CalendarMatchDecision )
        if (-not $preview) {
            Write-Information "Invoke-CalendarImport: No rows returned from preview; nothing to do."
            return [pscustomobject]@{
                Mode          = $mode
                CsvPath       = $CsvPath
                SchemaStatus  = $schema.Status
                PreviewTotals = @{}
                Created       = 0
                Updated       = 0
                Errors        = 0
                LogPath       = $null
                Message       = 'No preview rows available.'
            }
        }

        # Tally preview
        $totals = @{
            WouldCreate    = ($preview | Where-Object Decision -eq 'WouldCreate').Count
            WouldUpdate    = ($preview | Where-Object Decision -eq 'WouldUpdate').Count
            WouldSkip      = ($preview | Where-Object Decision -eq 'WouldSkip').Count
            Invalid        = ($preview | Where-Object Decision -eq 'Invalid').Count
            DuplicateInCsv = ($preview | Where-Object Decision -eq 'DuplicateInCsv').Count
            Error          = ($preview | Where-Object Decision -eq 'Error').Count
        }
        Write-Verbose ("Preview totals: {0}" -f ( ($totals.GetEnumerator() | Sort-Object Name | ForEach-Object { "$($_.Name)=$($_.Value)" }) -join '; ' ))

        # 3) Export preview log (writes file even in WhatIf mode)
        $log = Export-CalendarImportLog
        $logPath = $log.LogPath

        $created = 0
        $updated = 0
        $errors  = 0

        # 4) Apply changes when not -WhatIf (propagate -WhatIf to child mutators)
        # Create: items marked WouldCreate
        $createRes = @( New-GroupCalendarEventSafe -WhatIf:$WhatIfPreference )
        if ($createRes) {
            $created += ($createRes | Where-Object Status -eq 'Success').Count
            $errors  += ($createRes | Where-Object Status -eq 'Error').Count
        }

        # Update: items marked WouldUpdate
        $updateRes = @( Set-GroupCalendarEventSafe -WhatIf:$WhatIfPreference )
        if ($updateRes) {
            $updated += ($updateRes | Where-Object Status -eq 'Success').Count
            $errors  += ($updateRes | Where-Object Status -eq 'Error').Count
        }

        # 5) Final console summary
        Write-Information ("Invoke-CalendarImport: Mode={0}; WouldCreate={1}; WouldUpdate={2}; WouldSkip={3}; Invalid={4}; DuplicateInCsv={5}; PreviewErrors={6}" -f `
            $mode, $totals.WouldCreate, $totals.WouldUpdate, $totals.WouldSkip, $totals.Invalid, $totals.DuplicateInCsv, $totals.Error)

        if ($mode -eq 'Live') {
            Write-Information ("Invoke-CalendarImport: Applied changes → Created={0}; Updated={1}; Errors={2}" -f $created, $updated, $errors)
        } else {
            Write-Information "Invoke-CalendarImport: Dry-run (-WhatIf) — no changes applied."
        }

        # 6) Return structured result
        [pscustomobject]@{
            Mode          = $mode
            CsvPath       = $CsvPath
            SchemaStatus  = $schema.Status
            PreviewTotals = $totals
            Created       = $created
            Updated       = $updated
            Errors        = $errors
            LogPath       = $logPath
            Message       = if ($mode -eq 'Live') { 'Preview logged; changes applied (see console for counts).' } else { 'Preview logged; no changes applied (WhatIf).' }
        }
    }
}

#MAIN
write-host "<<<Test-CalendarCsvSchema>>>"
Test-CalendarCsvSchema
write-host "<<<Read-CalendarCsv >>>"
Read-CalendarCsv
write-host "<<< ConvertTo-CanonicalEvent -Verbose | Format-Table -AutoSize>>>"
ConvertTo-CanonicalEvent -Verbose | Format-Table -AutoSize
write-host "<<< Find-CsvDuplicateEvent | Format-Table RowNumber,Subject,Day,IsValid,IsDuplicateInCsv,DuplicateIndex,DuplicateCount>>>"
Find-CsvDuplicateEvent | Format-Table RowNumber,Subject,Day,IsValid,IsDuplicateInCsv,DuplicateIndex,DuplicateCount
write-host "<<< Get-CalendarMatchPreview | Format-Table RowNumber,Subject,Day,Decision,Reason>>>"
Get-CalendarMatchPreview | Format-Table RowNumber,Subject,Day,Decision,Reason
write-host "<<< Connect-CalendarGraph -Verbose>>>"
Connect-CalendarGraph -Verbose
write-host "<<< Resolve-GroupCalendar -Verbose>>>"
Resolve-GroupCalendar -Verbose
write-host "<<< Get-CalendarMatchDecision -Verbose | Format-Table RowNumber,Subject,Day,Decision,Reason>>>"
Get-CalendarMatchDecision -Verbose | Format-Table RowNumber,Subject,Day,Decision,Reason
write-host "<<< New-GroupCalendarEventSafe -WhatIf | Format-Table -AutoSize>>>"
New-GroupCalendarEventSafe -WhatIf | Format-Table -AutoSize
write-host "<<< Set-GroupCalendarEventSafe -WhatIf | Format-Table -AutoSize>>>"
Set-GroupCalendarEventSafe -WhatIf | Format-Table -AutoSize
write-host "<<< Export-CalendarImportLog -Verbose>>>"
Export-CalendarImportLog -Verbose
write-host "<<< Invoke-CalendarImport -Verbose>>>"
Invoke-CalendarImport -Verbose
