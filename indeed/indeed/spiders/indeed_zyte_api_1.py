import scrapy
from inputs import (
    custom_settings_zyte_api_dict,
    num_listings_per_page,
    page_index_step,
    max_page_index
)
import re
from math import ceil
import logging
from datetime import datetime


class IndeedZyteAPI1Spider(scrapy.Spider):
    name = 'indeed_zyte_api_1'
    base_url = "https://ca.indeed.com/jobs?l=Greater+Toronto+Area%2C+ON&sc=0kf%3Aocc%286YCJB%29%3B&radius=35&sort=date&vjk=f55ce01235a88065"
    custom_settings=custom_settings_zyte_api_dict
    
    def start_requests(self):
        yield scrapy.Request(
            url=IndeedZyteAPI1Spider.base_url,
            callback=self.parse,
        )

    def parse(self, response):
        # Extract the total number of jobs as a string
        num_jobs = response.xpath("//div[@class='jobsearch-JobCountAndSortPane-jobCount']/span/text()").get()
        logging.info(f"The total number of jobs: {num_jobs}")

        # Extract the number of jobs as an integer
        num_jobs = int(''.join(re.findall(pattern="\d+", string=num_jobs)))

        # Calculate the number of pages to loop over
        num_page_loop = (ceil(num_jobs / num_listings_per_page) - 1) * page_index_step # 15 is the number of listings per page. The index = (number of pages - 1) x 10. For example, 14 pages means that the index would end at (14 - 1) x 10 = 130
        if num_page_loop <= max_page_index:
            for_loop_end_range = num_page_loop
        else:
            for_loop_end_range = max_page_index
        
        logging.info(f"The num_page_loop is: {num_page_loop}. The for_loop_end_range index is: {for_loop_end_range}")

        # Loop over every page until all job listings are crawled
        # The maximum number of pages shown by indeed is 55 pages. This corresponds to index 540. Take the max of num_page_loop + 10 (because the step = 10) or 540 as the for loop's range end
        page_counter = 1
        for i in range(0, for_loop_end_range + page_index_step, page_index_step):
            logging.info(f"Crawling page number {page_counter} with index {i}")
            listing_page_url = IndeedZyteAPI1Spider.base_url + f"&start={i}"
            yield scrapy.Request(
                url=listing_page_url,
                callback=self.parse_listing_page,
                meta={"crawled_page_rank": page_counter, "listing_page_url": listing_page_url}
            )
            page_counter += 1

    def parse_listing_page(self, response):
        # Number of Listings
        listings = response.xpath("//ul[@class='jobsearch-ResultsList css-0']/li")
        for li in listings:
            job_title_name = li.xpath(".//h2[contains(@class, 'jobTitle')]/a/span/text()").get()
            if job_title_name is None: # Sometimes, the the HTML content under "li" is NULL. If this is the case, don't add anything to the "output_dict"
                continue
            else:
                # Clean the crawled fields
                # company_indeed_url
                try:
                    company_indeed_url = "https://ca.indeed.com" + li.xpath(".//span[@class='companyName']/a/@href").get()
                except TypeError: # If the XPATH yields a NULL value, it cannot be concatenated to a string
                    company_indeed_url = None

                # job_indeed_url
                try:
                    job_indeed_url = "https://ca.indeed.com" + li.xpath(".//h2[contains(@class, 'jobTitle')]/a/span/../@href").get()
                except TypeError:
                    job_indeed_url = None
                
                # company_name
                company_name_with_url = li.xpath(".//span[@class='companyName']/a/text()").get()
                company_name_wout_url = li.xpath(".//span[@class='companyName']/text()").get()
                company_name = company_name_with_url if company_name_with_url is not None else company_name_wout_url

                # City
                city = li.xpath(".//div[@class='companyLocation']/text()").get()
                unwanted_words = ["Temporarily Remote in ", "Remote in ", "Hybrid remote in "]
                if bool([wo for wo in unwanted_words if(wo in city)]): # If TRUE (i.e., if an unwanted sub-string exists in city, remove it from the main string, which is city) 
                    # Set the remote variable to the value of the unwanted word
                    remote = re.findall(pattern=".*(?=\sin\s)", string=city)[0]

                    # Remove the unwanted word from city
                    for wo in unwanted_words:
                        city = re.sub(wo, "", city)
                else:
                    remote = None
                
                # Salary
                salary = li.xpath(".//div[@class='metadata salary-snippet-container']/div/text()").get()
                
                # Yield the data
                output_dict_listing_page = {
                    "listing_page_url": response.meta["listing_page_url"],
                    "job_title_name": job_title_name,
                    "job_indeed_url": job_indeed_url,
                    "company_name": company_name,
                    "company_indeed_url": company_indeed_url,
                    "city": city,
                    "remote": remote,
                    "salary": salary,
                    "crawled_page_rank": response.meta["crawled_page_rank"]
                }

                logging.info("Proceeding to crawl data from the job page itself...")
                yield scrapy.Request(
                    url=job_indeed_url,
                    callback=self.parse_job_page,
                    meta=output_dict_listing_page
                )
        
    def parse_job_page(self, response):
        job_type = response.xpath("//div[text()='Job type']//following-sibling::div/text()").getall()
        if job_type is not None:
            # Remove unwanted keywords from the job_type list
            wanted_job_types = ["Full-time", "Permanent", "Contract", "Part-time", "Temporary", "Apprenticeship", "Internship", "Internship / Co-op", "Casual", "Freelance", "Fixed term contract"]
            job_type = [job for job in job_type if(job in wanted_job_types)] # If TRUE (i.e., if an unwanted sub-string exists in city, remove it from the main string, which is city) 

            # Join the elements of the list to form a string and separate them with a comma
            job_type = ', '.join(job_type)

        # Job description
        job_description = response.xpath("//div[@id='jobDescriptionText']//text()").getall()
        if job_description is not None:
            job_description = [job.strip() for job in job_description]
            job_description = ''.join(job_description)

        yield {
            # Job page fields
            "job_title_name": response.meta["job_title_name"],
            "job_type": job_type,
            "company_name": response.meta["company_name"],
            "company_indeed_url": response.meta["company_indeed_url"],
            "city": response.meta["city"],
            "remote": response.meta["remote"],
            "salary": response.meta["salary"],
            "crawled_page_rank": response.meta["crawled_page_rank"],
            "job_page_url": response.meta["job_indeed_url"],
            "listing_page_url": response.meta["listing_page_url"],
            "job_description": job_description,
            "crawled_timestamp": datetime.now()
        }
