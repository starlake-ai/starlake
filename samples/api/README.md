# REST API Extraction Samples

Sample configurations for extracting data from popular SaaS APIs using `starlake extract-rest-data`.

## Prerequisites

Set the required environment variables for each API before running.

## Slack

```bash
export SLACK_BOT_TOKEN="xoxb-..."   # Bot User OAuth Token (scopes: channels:read, users:read, channels:history)
starlake extract-rest-schema --config slack
starlake extract-rest-data --config slack --outputDir /tmp/slack-data
```

**Endpoints**: channels, users, messages (per channel with parent-child)

## Jira

```bash
export JIRA_DOMAIN="yourcompany"     # yourcompany.atlassian.net
export JIRA_EMAIL="you@company.com"
export JIRA_API_TOKEN="..."          # https://id.atlassian.com/manage-profile/security/api-tokens
starlake extract-rest-schema --config jira
starlake extract-rest-data --config jira --outputDir /tmp/jira-data
```

**Endpoints**: projects, issues (with JQL), users, worklogs (per issue with parent-child)

## Twitter / X

```bash
export TWITTER_BEARER_TOKEN="..."     # X API v2 Bearer Token
export TWITTER_SEARCH_QUERY="starlake OR dataengineering"
export TWITTER_USERNAME="starlakeai"
export TWITTER_USER_ID="123456789"
starlake extract-rest-schema --config twitter
starlake extract-rest-data --config twitter --outputDir /tmp/twitter-data
```

**Endpoints**: recent tweet search, user profile, user tweets, followers

## Salesforce

```bash
export SF_INSTANCE="yourorg"          # yourorg.salesforce.com
export SF_CLIENT_ID="..."             # Connected App Consumer Key
export SF_CLIENT_SECRET="..."         # Connected App Consumer Secret
starlake extract-rest-schema --config salesforce
starlake extract-rest-data --config salesforce --outputDir /tmp/sf-data --incremental
```

**Endpoints**: accounts, contacts, opportunities, leads, tasks, cases (all with SOQL via REST, incremental via LastModifiedDate)

## HubSpot

```bash
export HUBSPOT_ACCESS_TOKEN="..."    # Private app access token (scopes: crm.objects.contacts.read, crm.objects.companies.read, crm.objects.deals.read)
starlake extract-rest-schema --config hubspot
starlake extract-rest-data --config hubspot --outputDir /tmp/hubspot-data --incremental
```

**Endpoints**: companies, contacts, deals, tickets, owners, deal pipelines (all with cursor pagination, incremental via hs_lastmodifieddate)

## Stripe

```bash
export STRIPE_SECRET_KEY="sk_..."    # Secret key (sk_live_... or sk_test_...)
starlake extract-rest-schema --config stripe
starlake extract-rest-data --config stripe --outputDir /tmp/stripe-data --incremental
```

**Endpoints**: customers, charges, payment intents, subscriptions, invoices, products, prices, refunds, balance transactions (all with auto-pagination via last item id)

## GitHub

```bash
export GITHUB_TOKEN="ghp_..."        # Personal Access Token (scopes: repo, read:org)
export GITHUB_ORG="starlake-ai"
starlake extract-rest-schema --config github
starlake extract-rest-data --config github --outputDir /tmp/github-data --incremental
```

**Endpoints**: repositories, pull requests (per repo), issues (per repo), members, teams (all with Link header pagination, incremental via updated_at)

## Common Options

```bash
# Incremental extraction (only new/updated records)
starlake extract-rest-data --config <api> --outputDir /tmp/data --incremental

# JSON Lines output (preserves nested structures)
starlake extract-rest-data --config <api> --outputDir /tmp/data --outputFormat jsonl

# Resume after failure
starlake extract-rest-data --config <api> --outputDir /tmp/data --resume

# Limit records (for testing)
starlake extract-rest-data --config <api> --outputDir /tmp/data --limit 100
```