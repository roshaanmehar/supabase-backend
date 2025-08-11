import time
import json
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

def scrape_all_districts():
    """
    Scrapes all UK postcode districts from doogal.co.uk.

    This function automates navigating the website, handling cookie consent,
    looping through each postcode area (e.g., AB, AL, B), and collecting
    the list of district names for each. The final data is saved to a JSON file.
    """
    # Set up Chrome options to run in non-headless mode
    chrome_options = Options()
    chrome_options.headless = False  # This ensures the browser window is visible

    # Set up the Chrome driver
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)

    # Initialize a dictionary to store all the postcode data
    postcode_data = {}

    try:
        # Navigate to the initial page
        driver.get("https://www.doogal.co.uk/UKPostcodes")
        print("Successfully navigated to the website.")
        
        # Wait for the cookie consent banner and click "Accept"
        try:
            wait = WebDriverWait(driver, 10)
            accept_button = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, 'button.mol-ads-cmp--btn-primary')))
            accept_button.click()
            print("Accepted cookie consent.")
        except TimeoutException:
            print("Cookie consent banner not found or already accepted.")

        # Find all postcode area rows in the main table
        area_rows = WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'table.postcodeAreasTable tbody tr'))
        )
        
        # Create a list to hold area information (name and URL)
        areas_to_scrape = []
        for row in area_rows:
            area_name = row.find_element(By.CSS_SELECTOR, 'td:nth-child(2)').text
            area_link_element = row.find_element(By.CSS_SELECTOR, 'td:nth-child(1) a')
            area_url = area_link_element.get_attribute('href')
            areas_to_scrape.append({'name': area_name, 'url': area_url})
        
        print(f"Found {len(areas_to_scrape)} postcode areas to scrape.")

        # Loop through each postcode area
        for area in areas_to_scrape:
            driver.get(area['url'])
            area_name = area['name']
            print(f"\n--- Scraping districts for: {area_name} ---")

            try:
                # Get all the district link elements on the current page
                district_links_elements = WebDriverWait(driver, 10).until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div.realContent h4 ~ a"))
                )
                
                # Extract the text from each link (which is the district name)
                district_names = [link.text for link in district_links_elements]
                print(f"Found {len(district_names)} districts for {area_name}.")

                # Store the list of districts under the area's name key
                postcode_data[area_name] = district_names
            
            except TimeoutException:
                print(f"No districts found for {area_name}. Skipping.")
                continue
        
        # Save the collected data to a JSON file
        with open('postcodes.json', 'w', encoding='utf-8') as f:
            json.dump(postcode_data, f, indent=4, ensure_ascii=False)
        print("\nSuccessfully saved all district data to postcodes.json")


    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        # Close the browser window
        driver.quit()
        print("\nScraping complete. Browser closed.")

if __name__ == "__main__":
    scrape_all_districts()
