from scrapy.crawler import CrawlerProcess
import json
import pandas as pd
import os
from google.cloud import bigquery
from google.oauth2 import service_account
import yagmail
from functions import (
    salary_type_func,
    salary_high_func,
    salary_low_func
)
from datetime import datetime

def main_scraper_1():
    # Navigate to markus_amzn_automation directory
    os.chdir(os.getcwd() + "/indeed")

    # Run the first crawler that crawls this link --> https://ca.indeed.com/jobs?l=Greater+Toronto+Area%2C+ON&sc=0kf%3Aocc%286YCJB%29%3B&radius=35&sort=date&vjk=f55ce01235a88065
    from indeed.spiders.indeed_zyte_api_1 import IndeedZyteAPI1Spider
    process = CrawlerProcess(settings={"FEEDS": {"output_indeed_zyte_api_1.json":{"format": "json", "overwrite": True, "encoding": "utf-8"}}})
    process.crawl(IndeedZyteAPI1Spider)
    process.start()

    # Open the JSON file containing the output and format the data
    with open("output_indeed_zyte_api_1.json", mode="r", encoding="utf-8") as f:
        df = json.load(f)
        df = pd.DataFrame(df)
        f.close()
    
    # Apply the salary_type_func
    df["salary_type"] = df["salary"].apply(salary_type_func)

    # Create the columns containing the salary bands
    df["salary_low"] = df[["salary", "salary_type"]].apply(lambda x: salary_low_func(*x), axis=1)
    df["salary_high"] = df.apply(lambda x: salary_high_func(x["salary"], x["salary_type"]), axis=1)

    # Change the data type of crawled_timestamp to datetime
    df["crawled_timestamp"] = df["crawled_timestamp"].apply(lambda x: pd.to_datetime(x))

    # Upload the results to bigquery
     # First, set the credentials
    key_path_local = os.getcwd() + "/bq_credentials.json"
    credentials = service_account.Credentials.from_service_account_file(
        key_path_local, scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )

    # Now, instantiate the client and upload the table to BigQuery
    client = bigquery.Client(project="web-scraping-371310", credentials=credentials)
    job_config = bigquery.LoadJobConfig(
        schema = [
            bigquery.SchemaField("job_title_name", "STRING"), 
            bigquery.SchemaField("job_type", "STRING"), 
            bigquery.SchemaField("company_name", "STRING"), 
            bigquery.SchemaField("company_indeed_url", "STRING"), 
            bigquery.SchemaField("city", "STRING"), 
            bigquery.SchemaField("remote", "STRING"), 
            bigquery.SchemaField("salary", "STRING"), 
            bigquery.SchemaField("crawled_page_rank", "INT64"),  
            bigquery.SchemaField("job_page_url", "STRING"), 
            bigquery.SchemaField("listing_page_url", "STRING"), 
            bigquery.SchemaField("job_description", "STRING"), 
            bigquery.SchemaField("crawled_timestamp", "TIMESTAMP"), 
            bigquery.SchemaField("salary_type", "STRING"), 
            bigquery.SchemaField("salary_low", "FLOAT64"),
            bigquery.SchemaField("salary_high", "FLOAT64")
        ]
    )
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND

    # Upload the table
    client.load_table_from_dataframe(
        dataframe=df,
        destination="web-scraping-371310.crawled_datasets.chris_indeed_workflow",
        job_config=job_config
    ).result()

    # Step 16: Send success E-mail
    yag = yagmail.SMTP("omarmoataz6@gmail.com", oauth2_file=os.getcwd() + "/email_authentication.json")
    contents = [
        f"This is an automatic notification to inform you that the Indeed crawler ran successfully"
    ]
    yag.send(["omarmoataz6@gmail.com"], f"The Indeed crawler ran successfully at {datetime.now()} CET", contents)

if __name__ == '__main__':
    main_scraper_1()