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

## Example Tweets from 1/25/22
> Bantamweight rankings update: Pedro Munhoz moves up from 10th to 9th

> Men's Pound-for-Pound rankings update: Stipe Miocic falls from 9th to 10th

## Future Plans
- Twitter bot will reply to mentions with the ranking of a specific fighter. (Coming Spring 2022)

## Areas for improvement
- Move data cleaning functions to separate utils file, making pipelines.py more readable.

## Note
I've only included the web scraping and pipeline files in this repository for security reasons. The full application is in a private repository.
