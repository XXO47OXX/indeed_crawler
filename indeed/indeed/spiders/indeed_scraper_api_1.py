import scrapy
from inputs import (
    custom_settings_dict,
    prefix,
    suffix,
    num_listings_per_page,
    page_index_step,
    max_page_index
)
from w3lib.html import remove_tags
import re
from math import ceil
import logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s  - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    encoding="utf-8",
    filename="scraper_api_logs.log"
)


class IndeedScraperAPI1Spider(scrapy.Spider):
    name = 'indeed_scraper_api_1'
    # base_url = "https://ca.indeed.com/jobs?l=Greater+Toronto+Area%2C+ON&sc=0kf%3Aocc%286YCJB%29%3B&radius=35&sort=date&vjk=f55ce01235a88065"
    base_url = "https://ca.indeed.com/jobs?l=Greater+Toronto+Area%2C+ON&sc=0kf%3Ajt%28apprenticeship%29occ%286YCJB%29%3B&radius=35&sort=date&vjk=a976e74a57a75f5d"
    custom_settings=custom_settings_dict
    
    def start_requests(self):
        yield scrapy.Request(
            url=prefix + IndeedScraperAPI1Spider.base_url + suffix,
            callback=self.parse
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
            listing_page_url_to_crawl = prefix + IndeedScraperAPI1Spider.base_url + f"&start={i}" + suffix
            yield scrapy.Request(
                url=listing_page_url_to_crawl,
                callback=self.parse_listing_page,
                meta={"crawled_page_rank": page_counter}
            )
            page_counter += 1

    def parse_listing_page(self, response):
        # Number of Listings
        listings = response.xpath("//ul[@class='jobsearch-ResultsList css-0']/li")
        for li in listings:
            # job_indeed_name and job_indeed_url
            job_title_name = li.xpath(".//h2[contains(@class, 'jobTitle')]/a/span/text()").get()
            job_indeed_url = "https://ca.indeed.com" + li.xpath(".//h2[contains(@class, 'jobTitle')]/a/span/../@href").get()
            if job_title_name is None: # Sometimes, the the HTML content under "li" is NULL. If this is the case, don't add anything to the "output_dict_listing_page"
                continue
            else:
                # Clean the crawled fields
                # company_indeed_url
                try:
                    company_indeed_url = "https://ca.indeed.com" + li.xpath(".//span[@class='companyName']/a/@href").get()
                except TypeError: # If the XPATH yields a NULL value, it cannot be concatenated to a string
                    company_indeed_url = None
                
                # company_name
                company_name_with_url = li.xpath(".//span[@class='companyName']/a/text()").get()
                company_name_wout_url = li.xpath(".//span[@class='companyName']/text()").get()
                company_name = company_name_with_url if company_name_with_url is not None else company_name_wout_url

                # City
                city = li.xpath(".//div[@class='companyLocation']/text()").get()
                unwanted_words = ["Temporarily Remote in ", "Remote in ", "Hybrid remote in "]
                if bool([wo for wo in unwanted_words if(wo in city)]): # If TRUE (i.e., if an unwanted sub-string exists in city, remove it from the main string, which is city) 
                    for wo in unwanted_words:
                        city = re.sub(wo, "", city)
                
                # Yield the data
                output_dict_listing_page = {
                    "url_to_crawl": remove_tags(response.headers["Sa-Final-Url"]),
                    "job_title_name": job_title_name,
                    "job_indeed_url": job_indeed_url,
                    "company_name": company_name,
                    "company_indeed_url": company_indeed_url,
                    "city": city,
                    "crawled_page_rank": response.meta["crawled_page_rank"]
                }

                logging.info("Proceeding to crawl data from the job page itself...")
                yield scrapy.Request(
                    url=job_indeed_url,
                    callback=self.parse_job_page,
                    meta=output_dict_listing_page
                )
        
    def parse_job_page(self, response):
        yield {
            # Job page fields
            "job_page_url_to_crawl": response.meta["job_indeed_url"],
            "salary": response.xpath("//div[text()='Salary']//following-sibling::div/span/text()").get(),
            "job_type": response.xpath("//div[text()='Job type']//following-sibling::div[1]/text()").get(),
            "remote": response.xpath("//div[text()='Job type']//following-sibling::div[2]/text()").get(),
            "job_description": response.xpath("//div[@id='jobDescriptionText']/text()").get(),
            # Listing page fields
            "listing_page_url_to_crawl": response.meta["listing_page_url_to_crawl"],
            "job_title_name": response.meta["job_title_name"],
            "company_name": response.meta["company_name"],
            "company_indeed_url": response.meta["company_indeed_url"],
            "city": response.meta["city"],
            "crawled_page_rank": response.meta["crawled_page_rank"],
        }
