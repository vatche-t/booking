

## Prerequisites

- **Python 3.10:** Make sure you have Python 3.10 installed. You can download it from the [official Python website](https://www.python.org/downloads/).

- **Virtual Environment (venv):** Create a virtual environment to manage project dependencies.
  ```bash
  python3 -m venv venv
  source venv/bin/activate  # On Windows, use venv\Scripts\activate
  ```

- **Install Dependencies:** Install project dependencies using `requirements.txt`.
  ```bash
  pip install -r requirements.txt
  ```

## Project Structure

- **`config.py`:** Configuration file containing constants and settings for the application.

- **`pipeline.py`:** Python script that handles scraping and processing

- **`main.py`:** Main Python script that orchestrates the scraping process.

## How to Use

1. **Activate Virtual Environment:**
   ```bash
   source venv/bin/activate  # On Windows, use venv\Scripts\activate
   ```

2. **Run the Scraper:**
   ```bash
   python main.py
   ```## Additional Information

- **Python Version:** 3.10


- **Dependencies:** Check `requirements.txt` for the list of Python packages used in this project.


#
# booking.com Data Scraper

This Python script scrapes hotel information and reviews from Booking.com and organizes the data in a structured format. The data is saved in Feather files and later merged into a CSV file for further analysis.

## Project Description

The project consists of two main functions:

### `scrape_hotel_info`

This function takes a hotel name and location, performs web scraping on Booking.com, and extracts information such as the overall review score, total reviews, and category ratings. The data is logged, and the results are returned as a Pandas DataFrame.

### `scrape_reviews`

This function scrapes reviews for a given hotel name and location. It supports pagination to retrieve multiple pages of reviews. The scraped data includes review details such as score, title, date, user information, and more. The results are returned as a Pandas DataFrame.

### Additional Functions

- `extract_names_and_locations_from_file`: Reads a text file containing hotel names and locations separated by commas and returns a list of dictionaries.

- `merge_hotel_info_and_reviews`: Combines the scraped hotel information and reviews into a single Pandas DataFrame. It checks for errors during scraping and logs them.

- `save_to_feather`: Saves a DataFrame to a Feather file, creating a folder named "hotel_data_feather" if it doesn't exist.

- `merge_hotel_feather_files_and_save_csv`: Merges all Feather files in the "hotel_data_feather" folder and saves the result as a CSV file named "hotel_data_booking.csv" in the "hotel_data" folder.

## Usage

1. Ensure you have the required libraries installed: `requests`, `beautifulsoup4`, `pandas`, `loguru`.

2. Create a text file named `hotels.txt` with a list of hotel names and locations.

3. Run the script. It will scrape data for each hotel, save it as Feather files, and then merge and save as a CSV file.

```bash
python main.py


Last updated on: 2024-02-14

Last updated on: 2024-02-16

Last updated on: 2024-02-16

Last updated on: 2024-02-19