import json
from typing import Optional

import scrapy
import scrapy.http
from attr import dataclass
from scrapy.crawler import CrawlerProcess
from scrapy.http.response.text import TextResponse
from scrapy.settings import Settings
from scrapy.utils.log import configure_logging

from prohka import config


class ProHKASpider(scrapy.Spider):  # type: ignore # Class cannot subclass "Spider" (has type "Any")  # pylint: disable=abstract-method

    def __init__(self, base_url: str, email: str, code: str):
        super().__init__(name="ProHKA", start_urls=[])
        self._base_url = base_url
        self._email = email
        self._code = code

    def start_requests(self):
        yield scrapy.Request(url=f"{self._base_url}/login", callback=self.parse_login)

    def parse_login(self, response: TextResponse):
        return scrapy.FormRequest.from_response(
            response,
            formdata={"form[input]": self._email},
            callback=self.after_email,
        )

    def after_email(self, response: TextResponse):
        return scrapy.FormRequest.from_response(
            response,
            formdata={"code": self._code},
            callback=self.main_page,
        )

    def main_page(self, response: TextResponse):
        data = ConsumptionData(heating=self.chart(response, "Heizung"), warm_water=self.chart(response, "Warmwasser"))
        print(data)
        return data

    def chart(self, response: TextResponse, label: str):
        attr_name = "data-consumption-chart-data-value"
        json_content = response.xpath(f'//div[./div/div/span[contains(text(), "{label}")]]/div[@{attr_name}]').attrib[
            attr_name
        ]
        chart = json.loads(json_content)
        return ConsumptionChart(
            labels=chart["labels"],
            datasets=[ConsumptionDataset(label=ds["label"], data=ds["data"]) for ds in chart["datasets"]],
        )


@dataclass
class ConsumptionDataset:
    label: str
    data: list[Optional[float]]


@dataclass
class ConsumptionChart:
    labels: list[str]
    datasets: list[ConsumptionDataset]


@dataclass
class ConsumptionData:
    heating: ConsumptionChart
    warm_water: ConsumptionChart


def main():
    configure_logging({"LOG_FORMAT": "%(levelname)s: %(message)s"})
    process = CrawlerProcess(settings=Settings(values={"DOWNLOAD_DELAY": 0.1}), install_root_handler=True)
    process.crawl(ProHKASpider, base_url=config.BASE_URL, email=config.EMAIL, code=config.CODE)
    process.start()


if __name__ == "__main__":
    main()
