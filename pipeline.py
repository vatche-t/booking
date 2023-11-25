import os
import glob
import time

import requests
from bs4 import BeautifulSoup
import pandas as pd
from loguru import logger

import config


def scrape_hotel_info(hotel_name, location):
    # Set up the loguru logger
    log_format = "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <cyan>{level: <8}</cyan> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"
    logger.add("scraping_log.log", rotation="500 MB", level="INFO", format=log_format, colorize=True, serialize=True)

    # Log start of the function
    logger.info(f"Scraping hotel info for {hotel_name} in {location}")

    url_overall = f"https://www.booking.com/hotel/{location}/{hotel_name}.en-gb.html"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
    }

    # Log the URL being scraped
    logger.info(f"Fetching data from URL: {url_overall}")

    response = requests.get(url_overall, headers=headers)
    soup = BeautifulSoup(response.text, "html.parser")

    # Extract overall review score
    review_score_overall = soup.select_one(".d86cee9b25").text.strip() if soup.select_one(".d86cee9b25") else ""
    total_reviews = soup.select_one(".d935416c47").text.strip() if soup.select_one(".d935416c47") else ""
    overall_rate_text = soup.select_one(".cb2cbb3ccb").text.strip() if soup.select_one(".cb2cbb3ccb") else ""

    # Log the extracted data
    logger.info(f"Overall review score: {review_score_overall}")
    logger.info(f"Total reviews: {total_reviews}")
    logger.info(f"Overall rate text: {overall_rate_text}")

    # Extract individual category ratings
    categories = ["Staff", "Facilities", "Cleanliness", "Comfort", "Value for money", "Location"]
    category_ratings = {}

    for category in categories:
        element = soup.find("span", class_="c-score-bar__title", string=category)
        if element:
            rating_span = element.find_next("span", class_="c-score-bar__score")
            logger.info(f"Category: {category}, Element: {element}, Rating Span: {rating_span}")

            # Check for unwanted span and skip if found
            if rating_span and "fcd9eec8fb" not in rating_span["class"]:
                rating = rating_span.text.strip()
                category_ratings[category.lower().replace(" ", "_")] = rating

    # Log the category ratings
    logger.info(f"Category Ratings: {category_ratings}")

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
    overall_review_df = pd.DataFrame(hotel_info_list)

    # Log the end of the function
    logger.info(f"Scraping completed for {hotel_name} in {location}")

    return overall_review_df


def scrape_reviews(hotel_name, location):
    url_review = "https://www.booking.com/reviewlist.html"

    # Set up the headers
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
    }

    # Set up the payload template
    payload_template = {
        "cc1": f"{location}",
        "pagename": f"{hotel_name}",
        "type": "total",
        "sort": "f_recent_desc",
        "time_of_year": "",
        "dist": "1",
        "rows": "25",
    }

    # Initialize parameters list
    params_list = []

    # Set up logging configuration
    logger.add("scraping_log.log", rotation="500 MB", level="INFO")

    # Initialize a list to store data
    data_list = []

    # Make requests and scrape data
    page = 1
    while True:
        payload = payload_template.copy()
        payload["offset"] = (page - 1) * int(payload["rows"])  # Adjust offset for each page

        # Make the request
        response = requests.get(url_review, headers=headers, params=payload)

        # Save parameters to the list
        params_list.append(payload)

        # Parse the HTML content
        soup = BeautifulSoup(response.text, "html.parser")
        parsed = []

        # Extract reviews
        review_boxes = soup.select(".review_list_new_item_block")
        if not review_boxes:
            break  # Break out of the loop if no reviews are found

        for review_box in review_boxes:
            get_css = lambda css: review_box.select_one(css).text.strip() if review_box.select_one(css) else ""
            parsed.append(
                {
                    "hotel_name": hotel_name,
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

        page += 1
        time.sleep(1.5)

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


def merge_hotel_info_and_reviews(hotel_name, location):
    # Scrape hotel info with retries
    for _ in range(config.MAX_RETRIES):
        try:
            hotel_info_df = scrape_hotel_info(hotel_name, location)

            # Check if hotel_info_df is empty
            if not hotel_info_df.empty:
                break  # Break out of the retry loop if successful
            else:
                # Log the error and retry after a delay
                logger.warning(f"Retrying hotel info scraping for {hotel_name} in {location}")
                time.sleep(config.RETRY_DELAY)
        except Exception as e:
            # Log the exception and retry after a delay
            logger.warning(f"Exception during hotel info scraping for {hotel_name} in {location}: {str(e)}")
            time.sleep(config.RETRY_DELAY)
    else:
        # If the loop completes without a successful scrape, log an error and return an empty DataFrame
        logger.error(f"Error scraping hotel info for {hotel_name} in {location}")
        return pd.DataFrame()

    # Scrape reviews with retries
    for _ in range(config.MAX_RETRIES):
        try:
            reviews_df = scrape_reviews(hotel_name, location)

            # Check if reviews_df is empty
            if not reviews_df.empty:
                break  # Break out of the retry loop if successful
            else:
                # Log the error and retry after a delay
                logger.warning(f"Retrying reviews scraping for {hotel_name} in {location}")
                time.sleep(config.RETRY_DELAY)
        except Exception as e:
            # Log the exception and retry after a delay
            logger.warning(f"Exception during reviews scraping for {hotel_name} in {location}: {str(e)}")
            time.sleep(config.RETRY_DELAY)
    else:
        # If the loop completes without a successful scrape, log an error and return an empty DataFrame
        logger.error(f"Error scraping reviews for {hotel_name} in {location}")
        return pd.DataFrame()

    # Replace empty values with space in both DataFrames
    hotel_info_df.replace("", " ", inplace=True)
    reviews_df.replace("", " ", inplace=True)

    # Merge DataFrames on 'hotel_name'
    merged_hotels_df = pd.merge(reviews_df, hotel_info_df, on="hotel_name", how="left")

    return merged_hotels_df


file_path = "hotels.txt"  # Replace with the actual path to your file
hotel_data = extract_names_and_locations_from_file(file_path)


def save_to_feather(df, file_name):
    folder_path = "hotel_data_feather"
    os.makedirs(folder_path, exist_ok=True)

    file_path = os.path.join(folder_path, f"{file_name}.feather")

    try:
        df.to_feather(file_path)
        print(f"File saved successfully: {file_path}")
    except Exception as e:
        print(f"Error saving file {file_path}: {e}")
        # Log the error or take any necessary logging action
        pass  # Continue execution even if there's an error


# Inside your loop
for hotel in hotel_data:
    hotel_name = hotel["hotel_name"]
    location = hotel["location"]

    merged_hotel_data = merge_hotel_info_and_reviews(hotel_name, location)

    # Save the merged_data as a feather file
    save_to_feather(merged_hotel_data, hotel_name)


def merge_feather_files_and_save_csv_and_excel():
    folder_path = "hotel_data_feather"
    feather_files = glob.glob(os.path.join(folder_path, "*.feather"))

    # Initialize an empty list to store DataFrames
    reviews_df = []

    # Read each feather file into a DataFrame and append to the list
    for feather_file in feather_files:
        df = pd.read_feather(feather_file)
        reviews_df.append(df)

    # Merge all DataFrames on 'hotel_name'
    final_hotels_reviews_df = pd.concat(reviews_df, ignore_index=True)

    # Save the merged DataFrame as a CSV file with utf-8 encoding
    csv_file_path = os.path.join(folder_path, "../final_hotel_data.csv")
    final_hotels_reviews_df.to_csv(
        csv_file_path,
        index=False,
    )

    # Save the merged DataFrame as an Excel file with utf-8 encoding
    excel_file_path = os.path.join(folder_path, "../final_hotel_data.xlsx")
    final_hotels_reviews_df.to_excel(excel_file_path, index=False)

    return final_hotels_reviews_df


# Call the function to merge feather files and save as CSV and Excel
merge_feather_files_and_save_csv_and_excel()
