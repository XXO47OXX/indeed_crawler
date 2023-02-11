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
        num_jobs = response.xpath("//div[@class='jobsearch-JobCountAndSortPane-jobCount']/span").get()
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
            url_to_crawl = IndeedZyteAPI1Spider.base_url + f"&start={i}"
            yield scrapy.Request(
                url=url_to_crawl,
                callback=self.parse_listing_page,
                meta={"crawled_page_rank": page_counter, "url_to_crawl": url_to_crawl}
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
                    company_indeed_url = "ca.indeed.com" + li.xpath(".//span[@class='companyName']/a/@href").get()
                except TypeError: # If the XPATH yields a NULL value, it cannot be concatenated to a string
                    company_indeed_url = None
                
                # job_indeed_url
                try:
                    # job_indeed_url = "ca.indeed.com" + li.xpath(".//h2[@class='jobTitle css-1h4a4n5 eu4oa1w0']/a/@href").get()
                    job_indeed_url = "ca.indeed.com" + li.xpath("..//h2[contains(@class, 'jobTitle')]/a/span/../@href").get()
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
                    for wo in unwanted_words:
                        city = re.sub(wo, "", city)
                
                # Yield the data
                output_dict_listing_page = {
                    "url_to_crawl": response.meta["url_to_crawl"],
                    "job_title_name": job_title_name,
                    "job_indeed_url": job_indeed_url,
                    "company_name": company_name,
                    "company_indeed_url": company_indeed_url,
                    "city": city,
                    "crawled_page_rank": response.meta["crawled_page_rank"]
                }

                yield output_dict_listing_page
