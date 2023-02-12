import re

# HELPER FUNCTIONS
# Format the salary column by creating a function that splits the salary string intro two columns
def salary_type_func(salary):
    if salary is None:
        return None
    else:
        if salary.find("year") != -1:
            return "year"
        elif salary.find("hour") != -1:
            return "hour"
        elif salary.find("month") != -1:
            return "month"
        elif salary.find("week") != -1:
            return "week"
        else:
            return None

# Define a function that return the higher end of the salary
def salary_high_func(salary, salary_type):
    if salary is None:
        return None
    else:
        if salary.find("–") != -1: # Type 1: $55,000–$62,000 a year
            return float(re.findall(pattern=f"(?<=–\$).*(?=\sa\s{salary_type}|\san\s{salary_type})", string=salary)[0].replace(",", ""))
        elif salary.find("From") != -1: # Type 2: From $80,000 a year
            return None
        elif salary.find("Up") != -1: # Type 3: Up to $160,000 a year
            return float(re.findall(pattern=f"(?<=Up\sto\s\$).*(?=\sa\s{salary_type}|\san\s{salary_type})", string=salary)[0].replace(",", ""))
        elif any(x in salary for x in ["-", "From", "Up"]) == False: # Type 4: $150,000 a year
            return float(re.findall(pattern=f"(?<=\$).*(?=\sa\s{salary_type}|\san\s{salary_type})", string=salary)[0].replace(",", ""))
        else:
            return None

# Define a function that returns the lower end of the salary
def salary_low_func(salary, salary_type):
    if salary is None:
        return None
    else:
        if salary.find("–") != -1: # Type 1: $55,000–$62,000 a year
            return float(re.findall(pattern="(?<=\$).*(?=\–\$)", string=salary)[0].replace(",", ""))
        elif salary.find("From") != -1: # Type 2: From $80,000 a year
            return float(re.findall(pattern=f"(?<=From\s\$).*(?=\sa\s{salary_type}|\san\s{salary_type})", string=salary)[0].replace(",", ""))
        elif salary.find("Up") != -1: # Type 3: Up to $160,000 a year
            return None
        elif any(x in salary for x in ["-", "From", "Up"]) is False: # Type 4: $150,000 a year
            return float(re.findall(pattern=f"(?<=\$).*(?=\sa\s{salary_type}|\san\s{salary_type})", string=salary)[0].replace(",", ""))
        else:
            return None

def post_crawling_func(output_json_file, crawler_name):
    import json
    import pandas as pd
    import os
    from google.cloud import bigquery
    from google.oauth2 import service_account
    import yagmail
    from datetime import datetime

    # Open the JSON file containing the output and format the data
    with open(f"{output_json_file}.json", mode="r", encoding="utf-8") as f:
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

    # Add another field to identify the crawler
    df["crawler_name"] = crawler_name

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
            bigquery.SchemaField("salary_high", "FLOAT64"),
            bigquery.SchemaField("crawler_name", "STRING"),
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
        f"This is an automatic notification to inform you that the Indeed {crawler_name} ran successfully"
    ]
    yag.send(["omarmoataz6@gmail.com"], f"The Indeed {crawler_name} ran successfully at {datetime.now()} CET", contents)