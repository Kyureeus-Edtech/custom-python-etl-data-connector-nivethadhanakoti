# NewsAPI ETL Connector

**Name:** Nivetha Dhanakoti\\
**Roll Number:** 3122225001087

## Overview
This connector extracts top news headlines from NewsAPI, transforms them, and loads into MongoDB.

## Setup
1. Register at https://newsapi.org/ and get your API key.
2. Create a `.env` file with:
3. pip install -r requirements.txt
4. Run ETL: `python etl_connector.py`

## Data Model

```json
{
  "author": "Herb Scribner",
  "title": "COVID-19 cases boom as 'Stratus' variant breaks out - Axios",
  "description": "The CDC upgraded wastewater activity for COVID-19 from \"low\" to \"moderate\" amid the \"Stratus\" variant surge.",
  "url": "https://www.axios.com/2025/08/08/covid-19-stratus-variant-symptoms-cases-cdc",
  "publishedAt": "2025-08-09T08:22:01Z",
  "content": "<ul><li>The risk of a summertime COVID-19 wave comes after Health Secretary Robert F. Kennedy Jr. unilaterally changed federal COVID vaccine recommendations, causing confusion over who should get the… [+2861 chars]",
  "ingestion_timestamp": {
    "$date": "2025-08-10T09:22:24.052Z"
  }
}
```
