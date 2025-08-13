# DShield Top Attackers ETL Connector

**Name:** Nivetha Dhanakoti <br>
**Roll Number:** 3122225001087

## Overview
This ETL pipeline fetches the Top Attackers IP Feed from DShield, transforms it into structured data, and loads it into MongoDB for analysis.

### API Details
- Base URL: https://www.dshield.org
- Endpoint: `/ipsascii.html`
- Format: `text/plain`
- Auth: None required

### Example Data
| ip              | asn    | country_code | attacks | name  |
|-----------------|--------|--------------|---------|-------|
| 45.183.247.179  | 262287 | BR           | 34230   | None  |

## Setup
1. Create a `.env` file:
2. pip install -r requirements.txt
3. Run ETL: `python etl_connector.py`

## MongoDB DATA
```json
{
  "ip": "185.224.128.017",
  "asn": 475183,
  "country_code": "9728",
  "attacks": null,
  "name": "2025-08-09",
  "ingested_at": {
    "$date": "2025-08-11T03:26:22.663Z"
  }
}
```