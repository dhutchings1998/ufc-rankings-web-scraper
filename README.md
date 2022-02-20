# UFC Rankings Web Scraper

## Summary
At a high level, this application scrapes the current rankings from www.ufc.com/rankings and tweets any ranking updates. The application runs once a day using Heroku Scheduler.

## Steps
1) Scrape UFC rankings page and send data to pipeline.
2) Clean scraped data.
3) Compare scraped data to BigQuery. If there are any ranking changes, update database and tweet results.

## Files
rankings_spider.py
- This file is responsible for scraping the rankings from the UFC website.

pipelines.py
- This file cleans the scraped data and updates Twitter & BigQuery if there are ranking updates.

## Technology & API's
Language: Python

Database: Google BigQuery

Cloud Scheduler: Heroku

API's/Libraries: 
- Scrapy (web scraping)
- Tweepy (Twitter API)
- Twilio Sendgrid (for error reporting)
- Pandas_gbq (connecting with Google BigQuery API)

## Future Plans
- Twitter bot will reply to mentions with the ranking of a specific fighter. (Coming Spring 2022)

## Note
I've only included the web scraping and pipeline files in this repository for security reasons. The full application is in a private repository.
