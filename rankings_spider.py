import scrapy

class RankingsSpider(scrapy.Spider):
    name = 'rankings'
    start_urls = ['https://www.ufc.com/rankings']

    def parse(self, response):
        yield {
            'classes': response.css(' div.view-grouping-content > table > caption > div > div.info > h4::text').extract(),
            'champions': response.css('#block-mainpagecontent > div > div.l-container > div > div > div > div.view-content > div > div.view-grouping-content > table > caption > div > div.info > h5 > div > div > div > a::text').extract(),
            'ranks': response.css('td.views-field.views-field-weight-class-rank::text').extract(),
            'rank_changes': response.css('td.views-field.views-field-weight-class-rank-change::text').extract(),
            'fighter_names': response.css('td.views-field.views-field-title > div > div > div > a::text').extract(),
            'last_updated': response.css('div.list-denotions > p:nth-child(1)::text').get()
        }