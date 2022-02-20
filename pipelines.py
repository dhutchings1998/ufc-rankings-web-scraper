from itemadapter import ItemAdapter
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
from multipledispatch import dispatch
import pandas_gbq
from google.oauth2 import service_account
import pandas as pd
from dateutil import parser
from datetime import datetime
import re
import numpy as np
import tweepy
import json


class RankingsPipeline:
    def __init__(self):
        twitter_auth_file = open("./twitter_auth.json")
        twitter_auth = json.load(twitter_auth_file)
        tweepy_auth = tweepy.OAuthHandler(
            twitter_auth["API Key"], twitter_auth["API Key Secret"]
        )
        tweepy_auth.set_access_token(
            twitter_auth["Access Token"], twitter_auth["Access Token Secret"]
        )

        self.tweepy_api = tweepy.API(tweepy_auth)
        self.credentials = service_account.Credentials.from_service_account_file(
            "./bq_auth.json"
        )
        self.project_id = "#############"

        self.sg = SendGridAPIClient("############################################")

    @dispatch(list)
    def clean_strings(self, lst):
        '''Removed extra whitespace from strings and converts empty strings to null values.'''
        clean_lst = [item.strip() for item in lst] 
        clean_lst = [
            " ".join(item.split()) for item in clean_lst
        ] 
        clean_lst = [None if item == "" else item for item in clean_lst]
        return clean_lst

    @dispatch(str)
    def clean_strings(self, s):
        '''Removed extra whitespace from a string.'''
        clean_str = s.strip()  # strip whitespace
        clean_str = " ".join(s.split())  # remove repeated spaces
        return clean_str

    def clean_date(self, date):
        '''
        Converts scraped date into pandas datetime object
        and identifies correct year.

        Parameters
        ----------
        date : str
            The scraped date in string format
        
        Returns
        -------
        pd.datetime
            The date as a pandas datetime object
        '''
        clean_date = re.sub(r"Last updated:", "", date).strip()
        clean_date = parser.parse(clean_date)

        prior_year = clean_date.replace(year=clean_date.year - 1)
        now = datetime.now()

        prior_year_diff = prior_year - now
        current_year_diff = clean_date - now

        if abs(prior_year_diff.days) < abs(current_year_diff.days):
            clean_date = clean_date.replace(year=clean_date.year - 1)

        return pd.to_datetime(
            clean_date.strftime("%m/%d/%Y"), format="%m/%d/%Y", errors="coerce"
        )

    def ord(self, n):
        '''Adds ordinal to end of integer.'''
        return str(n) + (
            "th"
            if 4 <= n % 100 <= 20
            else {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
        )

    def class_to_table_name(self, class_name):
        '''
        Takes in the name of a weight class and returns the proper
        Google BigQuery table name.
        '''
        table_name = re.sub(" ", "_", class_name).lower()
        table_name = "ufc_rankings." + re.sub("'", "", table_name)
        return table_name

    def scraped_data_to_dict(self, classes, ranks, champions, fighter_names, rank_changes, last_updated):
        '''
        Converts scraped data into a dictionary divided by weight classes.

        Parameters
        ----------
        classes : lst
            A list of scraped weight classes
        ranks : lst
            A list of scraped ranks (i.e. 0, 1, 2...)
        champions : lst
            A list of scraped champions
        fighter_names : lst
            A list of scraped fighter names
        rank_changes : lst
            A list of scraped ranking changes
        last_updated : lst
            A list of length = 1 that contains the date of rankings update
        
        Returns
        -------
        cleaned_data : dict
            A dictionary of scraped data broken down by weight class
        '''
        ranks = [None if i is None else int(i) for i in ranks]
        last_updated = self.clean_date(last_updated)

        cleaned_data = {}
        index = 0
        champ_index = 0
        for weight_class in classes:
            if weight_class == "Women's Featherweight":
                continue

            index_start = index
            index_end = None
            if "Pound-for-Pound" in weight_class:
                index_end = index_start + 14
            else:
                index_end = index_start + 15

            class_ranks = ranks[index_start:index_end]
            class_champion = champions[champ_index]
            class_fighters = fighter_names[index_start:index_end]
            class_rank_changes = rank_changes[index_start:index_end]

            class_fighters.insert(0, class_champion)
            class_ranks.insert(0, 0)
            class_rank_changes.insert(0, None)

            cleaned_data[weight_class] = {
                "ranks": class_ranks,
                "fighters": class_fighters,
                "rank_change": class_rank_changes,
                "last_updated": [last_updated for i in range(len(class_ranks))],
            }

            index = index_end
            champ_index = champ_index + 1
        return cleaned_data
        
    def fetch_gbq_rankings(self, classes):
        '''
        Fetches data from BigQuery and returns it as a dictionary.
        '''
        db = {}
        for c in classes:
            table_name = self.class_to_table_name(c)
            class_df = pandas_gbq.read_gbq(
                "SELECT * FROM `{}`".format(table_name),
                project_id=self.project_id,
                credentials=self.credentials,
            )
            class_df.sort_values("ranks", inplace=True)
            class_df["last_updated"] = class_df["last_updated"].dt.date
            db[c] = class_df.to_dict()
        return db

    def check_ranking_updates(self, scraped_dict):
        '''
        Compares scraped data to database and returns a dictionary of updates.

        Parameters
        ----------
        scraped_dict : dict
            Scraped rankings data in dictionary format
        
        Returns
        -------
        updates : dict
            A dictionary of ranking updates broken down by weight class
        '''
        db = self.fetch_gbq_rankings(scraped_dict)

        updates = {}
        for c in scraped_dict:
            scraped_data = pd.DataFrame(scraped_dict[c])
            scraped_data['last_updated'] = scraped_data['last_updated'].dt.date

            db_data = pd.DataFrame(db[c])

            rankings_diff = pd.concat(
                [db_data[["ranks", "fighters"]], scraped_data[["ranks", "fighters"]]]
            ).drop_duplicates(keep=False)

            # Scraped data is more recent and there are differences between rankings
            if (
                pd.Timestamp(scraped_data["last_updated"][0])
                > pd.Timestamp(db_data["last_updated"][0])
                and not rankings_diff.empty
            ):
                rankings_diff = pd.concat(
                    [
                        db_data[["ranks", "fighters"]],
                        scraped_data[["ranks", "fighters"]],
                    ]
                ).drop_duplicates(keep=False)

                def handle_groupby(a):
                    a = np.array(a)
                    _, idx = np.unique(a, return_index=True)
                    lst = list(a[np.sort(idx)])
                    return [int(i) for i in lst]

                rankings_diff = (
                    rankings_diff.groupby("fighters")["ranks"]
                    .apply(handle_groupby)
                    .reset_index()
                )

                moved_ranks = rankings_diff[rankings_diff["ranks"].str.len() > 1]
                in_out_rankings = rankings_diff[rankings_diff["ranks"].str.len() == 1]
                fighters_removed = None
                fighters_added = None

                if not in_out_rankings.empty:
                    in_out_rankings.loc[:, "out_rankings"] = list(
                        in_out_rankings["fighters"].apply(
                            lambda row: row in list(db_data["fighters"])
                        )
                    )
                    fighters_removed = in_out_rankings.loc[
                        in_out_rankings["out_rankings"], ["fighters", "ranks"]
                    ]
                    fighters_added = in_out_rankings.loc[
                        in_out_rankings["out_rankings"] == False, ["fighters", "ranks"]
                    ]
                fighters_added = (
                    fighters_added.to_dict() if fighters_added is not None else None
                )
                fighters_removed = (
                    fighters_removed.to_dict() if fighters_removed is not None else None
                )
                moved_ranks = None if moved_ranks.empty else moved_ranks.to_dict()

                updates[c] = {
                    "fighters_added": fighters_added,
                    "fighters_removed": fighters_removed,
                    "moved_ranks": moved_ranks,
                }

        return updates

    def tweet(self, updates):
        '''
        Takes in list of rankings updates and tweets the results.

        Parameters
        ----------
        updates : dict
            A dictionary of rankings updates

        Returns
        -------
        None
        '''
        for c in updates:
            fighters_added = pd.DataFrame(updates[c]["fighters_added"])
            fighters_removed = pd.DataFrame(updates[c]["fighters_removed"])
            moved_ranks = pd.DataFrame(updates[c]["moved_ranks"])
            if not fighters_added.empty:
                for _, row in fighters_added.iterrows():
                    tweet = (
                        c
                        + " rankings update: "
                        + row["fighters"]
                        + " enters rankings at number "
                        + str(row["ranks"][0])
                    )
                    tweet = re.sub(r"\b0th\b", "Champion", tweet)
                    self.tweepy_api.update_status(tweet)
            if not fighters_removed.empty:
                for _, row in fighters_removed.iterrows():
                    tweet = (
                        c
                        + " rankings update: "
                        + row["fighters"]
                        + " removed from rankings"
                    )
                    self.tweepy_api.update_status(tweet)
            if not moved_ranks.empty:
                for _, row in moved_ranks.iterrows():
                    if row["ranks"][0] < row["ranks"][1]:
                        tweet = (
                            c
                            + " rankings update: "
                            + row["fighters"]
                            + " falls from "
                            + self.ord(row["ranks"][0])
                            + " to "
                            + self.ord(row["ranks"][1])
                        )
                        tweet = (
                            re.sub(r"\b0th\b", "top of P4P list", tweet)
                            if "Pound-for-Pound" in c
                            else re.sub(r"\b0th\b", "Champion", tweet)
                        )
                        self.tweepy_api.update_status(tweet)
                    else:
                        tweet = (
                            c
                            + " rankings update: "
                            + row["fighters"]
                            + " moves up from "
                            + self.ord(row["ranks"][0])
                            + " to "
                            + self.ord(row["ranks"][1])
                        )
                        tweet = (
                            re.sub(r"\b0th\b", "top of P4P list", tweet)
                            if "Pound-for-Pound" in c
                            else re.sub(r"\b0th\b", "Champion", tweet)
                        )
                        self.tweepy_api.update_status(tweet)

    def write_to_gbq(self, scraped_dict):
        '''
        Writes scraped data to BigQuery, replacing its current values.

        Parameters
        ----------
        scraped_dict : dict
            Scraped rankings data
        
        Returns
        -------
        None
        '''
        for c in scraped_dict:
            df = pd.DataFrame(scraped_dict[c])
            df["rank_change"] = pd.to_numeric(df["rank_change"], downcast="integer")

            table_name = self.class_to_table_name(c)
            pandas_gbq.to_gbq(
                df,
                table_name,
                project_id=self.project_id,
                if_exists="replace",
                credentials=self.credentials,
            )

    def process_item(self, item, spider):
        '''Cleans data and updates twitter and BigQuery if new rankings come out.'''
        try:
            adapter = ItemAdapter(item)
            for section in adapter:
                adapter[section] = self.clean_strings(adapter[section])

            scraped_dict = self.scraped_data_to_dict(adapter['classes'], adapter['ranks'], adapter['champions'], adapter['fighter_names'], adapter['rank_changes'], adapter['last_updated'])
            updates = self.check_ranking_updates(scraped_dict)
            
            if bool(updates):
                self.tweet(updates)
                self.write_to_gbq(scraped_dict)

            return updates

        except BaseException as error:
            message = Mail(
                from_email='dhutchings1998@gmail.com',
                to_emails='dhutchings1998@gmail.com',
                subject='Web Scraper Error',
                html_content = '<h1>Error: <h1> <p>{}<p>'.format(error)
            )
            response = self.sg.send(message)
            return response
            
