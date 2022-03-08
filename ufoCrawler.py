import scrapy
import re
import logging
import json

# If we want to use multi threading or run multi twister, have to import CrawlRunner
from scrapy.crawler import CrawlerProcess


class UFOSpider(scrapy.Spider):
    # def __init__(self):
    # Name is a required member and star_urls is list of inital webpges to start crawl from
    name = "UFO"
    #default start URL = "https://www.nuforc.org/webreports/ndxloc.html"
    start_urls = ["https://www.nuforc.org/webreports/ndxloc.html"]
    all_reports = []

    # logging.basicConfig(level=logging.INFO, filename="crawler.log", filemode="w")
    # logger = logging.getLogger("crawler")

    def parse(self, response):
        '''
        Yields links from https://www.nuforc.org/webreports/ndxloc.html which is list of records indexed location.
        Each link links to records of a specific states
        The same recursive loop will yield individual report links from state report links
        because both individual and state record list have same formate of stating next links
        '''
        for next_link in response.xpath("//td/font/a/@href").getall():
            # self.logger.debug(f'aquired {next_link} to crawl')
            yield response.follow(next_link, callback=self.parse)

        # when no next link is yeilded, it means we reached individual record page and we can scrape the data
        individual_report_link_pattern = re.compile(".*://www.nuforc.org/webreports/reports/.*.html")
        if (individual_report_link_pattern.match(response.url)):
            # self.logger.info(f'Scraping individual report at {response.url}')
            # scrape data into a dictionary
            report = {}
            attribute_value_pattern = re.compile(
                "(Occurred|Reported|Posted|Location|Shape|Duration|Characteristics)\s?:",
                re.IGNORECASE)
            try:
                for record in response.xpath("//tbody/tr/td/font/text()").getall():
                    record = record.strip()
                    if (attribute_value_pattern.match(record)):
                        key, value = record.split(":", maxsplit=1)
                        report[key.strip()] = value
                    else:
                        if ("Characteristics" in report):
                            report['Characteristics'] = f'{report["Characteristics"]}\n{record}'
                        else:
                            report['Characteristics'] = record
                # self.logger.info(f'Finshed scraping individual report at {response.url}')
                # FIXME: Not a fan of using class variable like this so, figure out a better way (lookout as scrapy requires class instead of object to strart crawl process)
                UFOSpider.all_reports.append(report)
                yield report
            except Exception as e:
                report = {
                    "URL": response.url,
                    "Exception": e
                }
                UFOSpider.all_reports.append(report)
                yield report


def main():
    process = CrawlerProcess()
    process.crawl(UFOSpider)
    process.start()
    with open ("ufoReports.json", "w") as outFile:
        json.dump(UFOSpider.all_reports,outFile, indent="")


if __name__ == "__main__":
    main()
