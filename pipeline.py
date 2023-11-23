import requests
from bs4 import BeautifulSoup
from loguru import logger
import time
import pandas as pd

url_review = "https://www.booking.com/reviewlist.en-gb.html?aid=304142&label=gen173nr-1FCAEoggI46AdIM1gEaLACiAEBmAExuAEYyAEM2AEB6AEB-AECiAIBqAIEuAKd9f2qBsACAdICJGRhZTE4M2YxLThkOTctNGE2My1hZDhkLTYxNDMyYWZlNzI2M9gCBeACAQ&sid=8ca968abfb7a6c67280fba58227dba59&srpvid=68a6725495cf000e&;cc1=us&pagename=hilton-atlanta&r_lang=&review_topic_category_id=&type=total&score=&sort=f_recent_desc&time_of_year=&dist=1&rows=25&rurl=&text=&translate=&_=1700757341034"

# Set up the headers
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
}

# Set up the payload template
payload_template = {
    "aid": "304142",
    "label": "gen173nr-1FCAEoggI46AdIM1gEaLACiAEBmAExuAEYyAEM2AEB6AEB-AECiAIBqAIEuAKd9f2qBsACAdICJGRhZTE4M2YxLThkOTctNGE2My1hZDhkLTYxNDMyYWZlNzI2M9gCBeACAQ",
    "sid": "8ca968abfb7a6c67280fba58227dba59",
    "srpvid": "68a6725495cf000e",
    "cc1": "us",
    "pagename": "hilton-atlanta",
    "r_lang": "",
    "review_topic_category_id": "",
    "type": "total",
    "score": "",
    "sort": "f_recent_desc",
    "time_of_year": "",
    "dist": "1",
    "rows": "10",
    "rurl": "",
    "text": "",
    "translate": "",
    "_": "1700757341034",
}

# Initialize parameters list
params_list = []

# Set up logging configuration
logger.add("scraping_log.log", rotation="500 MB", level="INFO")

# Initialize a list to store data
data_list = []

# Make requests and scrape data
for page in range(1, 26):  # 25 pages
    payload = payload_template.copy()
    payload["offset"] = page  # Adjust offset for each page

    # Make the request
    response = requests.get(url_review, headers=headers, params=payload)

    # Wait for 2 seconds
    # time.sleep(2)

    # Save parameters to the list
    params_list.append(payload)

    # Parse the HTML content
    soup = BeautifulSoup(response.text, "html.parser")
    parsed = []

    # Extract reviews
    for review_box in soup.select(".review_list_new_item_block"):
        get_css = lambda css: review_box.select_one(css).text.strip() if review_box.select_one(css) else ""
        parsed.append(
            {
                "id": review_box.get("data-review-url"),
                "score": get_css(".bui-review-score__badge"),
                "title": get_css(".c-review-block__title"),
                "date": get_css(".c-review-block__date"),
                "user_name": get_css(".bui-avatar-block__title"),
                "user_country": get_css(".bui-avatar-block__subtitle"),
                "text": "".join(review_box.select_one(".c-review__body").stripped_strings)
                if review_box.select_one(".c-review__body")
                else "",
                "lang": review_box.select_one(".c-review__body").get("lang")
                if review_box.select_one(".c-review__body")
                else "",
            }
        )

    # Add parsed data to the list
    data_list.extend(parsed)

    # Process parsed data as needed
    logger.info(f"Page {page} processed. Total reviews: {len(parsed)}")

# Convert data list to a pandas DataFrame
df = pd.DataFrame(data_list)

# Print the DataFrame
logger.info("Scraping process complete.")
logger.info(df)
