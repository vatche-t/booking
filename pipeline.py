import requests
from bs4 import BeautifulSoup
import pandas as pd
from loguru import logger


def scrape_hotel_info(hotel_name, location):
    url_overall = f"https://www.booking.com/hotel/{location}/{hotel_name}.en-gb.html?label=gen173nr-1BCAso3QFCKWZvdXItc2Vhc29ucy1iYW5na29rLWF0LWNoYW8tcGhyYXlhLXJpdmVySDNYBGhNiAEBmAENuAEYyAEM2AEB6AEBiAIBqAIEuAKtsIKrBsACAdICJDVkNTk4NDQ1LTYwYjAtNDRiYy05NzE3LTA2NDA3NzdjZTkzY9gCBeACAQ&sid=8ca968abfb7a6c67280fba58227dba59&dist=0&group_adults=2&group_children=0&keep_landing=1&no_rooms=1&sb_price_type=total&type=total&#tab-main"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
    }

    response = requests.get(url_overall, headers=headers)
    soup = BeautifulSoup(response.text, "html.parser")

    # Extract overall review score
    review_score_overall = soup.select_one(".d86cee9b25").text.strip() if soup.select_one(".d86cee9b25") else ""
    total_reviews = soup.select_one(".d935416c47").text.strip() if soup.select_one(".d935416c47") else ""
    overall_rate_text = soup.select_one(".cb2cbb3ccb").text.strip() if soup.select_one(".cb2cbb3ccb") else ""

    # Extract individual category ratings
    categories = ["Staff", "Facilities", "Cleanliness", "Comfort", "Value for money", "Location"]
    category_ratings = {}

    for category in categories:
        element = soup.find("span", class_="c-score-bar__title", text=category)
        if element:
            rating_span = element.find_next("span", class_="c-score-bar__score")
            # Check for unwanted span and skip if found
            if rating_span and "fcd9eec8fb" not in rating_span["class"]:
                rating = rating_span.text.strip()
                category_ratings[category.lower().replace(" ", "_")] = rating

    # Create a list with the parsed data
    hotel_info_list = [
        {
            "hotel_name": hotel_name,
            "review_score_overall": review_score_overall,
            "total_review_count": total_reviews,
            "overall_rate_text": overall_rate_text,
            **category_ratings,
        }
    ]

    # Convert list to a pandas DataFrame
    overal_review_df = pd.DataFrame(hotel_info_list)

    return overal_review_df


def scrape_reviews(hotel_name):
    url_review = f"https://www.booking.com/reviewlist.en-gb.html?aid=304142&label=gen173nr-1FCAEoggI46AdIM1gEaLACiAEBmAExuAEYyAEM2AEB6AEB-AECiAIBqAIEuAKd9f2qBsACAdICJGRhZTE4M2YxLThkOTctNGE2My1hZDhkLTYxNDMyYWZlNzI2M9gCBeACAQ&sid=8ca968abfb7a6c67280fba58227dba59&srpvid=68a6725495cf000e&;cc1=us&pagename={hotel_name}&r_lang=&review_topic_category_id=&type=total&score=&sort=f_recent_desc&time_of_year=&dist=1&rows=10&rurl=&text=&translate=&_=1700757341034"

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
        "pagename": "passalacqua-moltrasio",
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
                    "review_id": review_box.get("data-review-url"),
                    "review_score": get_css(".bui-review-score__badge"),
                    "review_title": get_css(".c-review-block__title"),
                    "review_date": get_css(".c-review-block__date"),
                    "user_name": get_css(".bui-avatar-block__title"),
                    "user_country": get_css(".bui-avatar-block__subtitle"),
                    "room_type:": get_css(".c-review-block__room-link .bui-list__body"),
                    "stay_date": get_css(".c-review-block__stay-date .c-review-block__date"),
                    "travel_type": get_css(".review-panel-wide__traveller_type .bui-list__body"),
                    "stay_night": get_css(".c-review-block__stay-date .bui-list__body"),
                    "review_text": "".join(review_box.select_one(".c-review__body").stripped_strings)
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
    review_details_df = pd.DataFrame(data_list)

    # Return the DataFrame
    return review_details_df


def extract_names_and_locations_from_file(file_path):
    """
    Extracts names and locations from a text file with a list of names and two-character words separated by commas.

    Parameters:
    - file_path (str): Path to the input text file.

    Returns:
    - list of dict: A list of dictionaries with 'hotel_name' and 'location' keys.
    """
    data_list = []

    # Open the file and process each line
    with open(file_path, "r") as file:
        for line in file:
            # Split the line by comma
            parts = line.split(",")

            # Extract hotel_name and location
            if len(parts) == 2:
                hotel_name = parts[0].strip()
                location = parts[1].strip()

                # Add to the data list
                data_list.append({"hotel_name": hotel_name, "location": location})

    return data_list


file_path = "hotels.txt"  # Replace with the actual path to your file
hotel_data = extract_names_and_locations_from_file(file_path)

for hotel in hotel_data:
    hotel_name = hotel["hotel_name"]
    location = hotel["location"]

    hotel_info = scrape_hotel_info(hotel_name, location)
    reviews_df = scrape_reviews(hotel_name)

    # Print or use the data as needed
    print(hotel_info)
    print(reviews_df)
