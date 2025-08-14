import requests
import pandas as pd
import time
import re
import random
import logging

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import Select
from urllib.parse import urlparse

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class ScopusCrawler:
    def __init__(self, start_keyword_index=0, start_page=1):
        self.keywords = [
            "LLM embodied", 
            "LLM AND IoT",
            "LLM wireless communications",
            "embodied AI AND IoT",
            "embodied AI internet of things",
            "LLM spectrum management",
            "embodied AI wireless communication",
            "LLM OR large language model"
        ]

        self.base_url = "https://www-scopus-com-ssl.oca.korea.ac.kr"
        self.library_url = "https://libs.korea.ac.kr/"
        self.driver = None
        self.results_data = {}

        self.start_keyword_index = start_keyword_index
        self.start_page = start_page

    def setup_driver(self):
        chrome_options = Options()

        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option("useAutomationExtension", False)

        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-plugins")
        chrome_options.add_argument("--disable-images")
        chrome_options.add_argument("--disable-javascript")

        chrome_options.add_argument("--log-level=3")
        chrome_options.add_argument("--disable-logging")
        chrome_options.add_argument("--disable-background-networking")

        chrome_options.add_argument(
            "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )

        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--start-maximized")

        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=chrome_options)

        self.driver.execute_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )

        self.driver.execute_cdp_cmd(
            "Page.addScriptToEvaluateOnNewDocument",
            {
                "source": """
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                });
                
                Object.defineProperty(navigator, 'plugins', {
                    get: () => [1, 2, 3, 4, 5]
                });
                
                Object.defineProperty(navigator, 'languages', {
                    get: () => ['en-US', 'en', 'ko-KR', 'ko']
                });
                
                window.chrome = {
                    runtime: {}
                };
            """
            },
        )

        logger.info("Chrome driver setup completed")

    def extract_author_affiliation_mapping(self):
        author_affiliation_map = {}
        affiliation_dict = {}

        try:
            affiliation_elements = self.driver.find_elements(
                By.CSS_SELECTOR,
                "section[data-testid='detailed-information-affiliations'] ul.DetailedInformationFlyout_list__76Ipn li",
            )

            for aff in affiliation_elements:
                try:
                    try:
                        sup = aff.find_element(By.TAG_NAME, "sup").text.strip()
                        text = aff.find_element(By.TAG_NAME, "span").text.strip()
                        affiliation_dict[sup] = text
                    except NoSuchElementException:
                        text = aff.text.strip()
                        if text:
                            default_key = "default"
                            counter = 1
                            while default_key in affiliation_dict:
                                default_key = f"default{counter}"
                                counter += 1
                            affiliation_dict[default_key] = text
                except Exception:
                    continue

            author_elements = self.driver.find_elements(
                By.CSS_SELECTOR,
                "ul.DetailedInformationFlyout_list__76Ipn li[data-testid='authorItem-button']",
            )

            num_affiliations = len(affiliation_dict)

            for i, author_el in enumerate(author_elements, 1):
                try:
                    name = author_el.find_element(
                        By.CSS_SELECTOR, "span.Button_text__0dddp"
                    ).text.strip()

                    if not name or name.lower() in ["authors"] or name.startswith("+"):
                        continue

                    superscripts = []
                    sup_elements = author_el.find_elements(
                        By.CSS_SELECTOR, "sup.AuthorList_affiliation__bTM3u"
                    )
                    for sup in sup_elements:
                        val = sup.text.strip()
                        for s in re.split(r"[,\s]+", val):
                            if s:
                                superscripts.append(s)

                    if num_affiliations == 1:
                        superscripts = list(affiliation_dict.keys())
                    elif num_affiliations > 1:
                        if not superscripts:
                            first_affiliation = list(affiliation_dict.keys())[0] if affiliation_dict else ""
                            if first_affiliation:
                                superscripts = [first_affiliation]
                            else:
                                superscripts = []
                    elif num_affiliations == 0:
                        superscripts = []
                    else:
                        superscripts = []

                    author_affiliation_map[name] = superscripts

                except Exception:
                    continue

        except Exception:
            pass

        return author_affiliation_map, affiliation_dict

    def contains_llm(self, text):
        text_lower = text.lower()
        
        if re.search(r'\bllms?\b', text_lower):
            return True
        
        if "large language model" in text_lower:
            return True
        
        return False

    def extract_llm_sentences(self, text):        
        sentences = re.split(r'[.!?]+', text)
        llm_sentences = []
        
        for sentence in sentences:
            sentence = sentence.strip()
            if sentence and self.contains_llm(sentence):
                llm_sentences.append(sentence)
        
        return ' | '.join(llm_sentences)

    def human_like_delay(self, min_seconds=1, max_seconds=3):
        delay = random.uniform(min_seconds, max_seconds)
        time.sleep(delay)

    def login_and_access_scopus(self):
        try:
            self.driver.get(self.library_url)
            time.sleep(3)

            try:
                academic_db_button = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable(
                        (By.CSS_SELECTOR, "a[data-target='.home-service-link-group-2']")
                    )
                )
                academic_db_button.click()
                time.sleep(2)
            except TimeoutException:
                input("Academic DB button not found. Please click manually and press Enter: ")

            try:
                scopus_link = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable(
                        (By.XPATH, "//a[contains(@href, 'scopus.com') and text()='Scopus']")
                    )
                )

                scopus_url = scopus_link.get_attribute("href")
                self.driver.get(scopus_url)
                time.sleep(5)
                
            except TimeoutException:
                input("Scopus link not found. Please click manually and press Enter: ")

            input("Press Enter when ready to proceed: ")

            return True

        except Exception as e:
            logger.error(f"Error accessing Scopus: {str(e)}")
            input("Please access Scopus manually and press Enter: ")
            return True

    def set_results_per_page(self, count=10):
        try:
            display_select = WebDriverWait(self.driver, 5).until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, ".Select-module__vDMww")
                )
            )

            select = Select(display_select)
            try:
                select.select_by_value("10")
            except:
                try:
                    select.select_by_value("20")
                except:
                    select.select_by_value("50")

            time.sleep(3)
            logger.info("Results per page setting completed")

        except Exception as e:
            logger.warning(f"Failed to set results per page: {str(e)}")

    def search_keyword(self, keyword):
        try:
            current_url = self.driver.current_url

            try:
                base_url = "https://www-scopus-com-ssl.oca.korea.ac.kr"

            except Exception:
                input("Please navigate to Scopus search page manually and press Enter: ")
                base_url = "https://www-scopus-com-ssl.oca.korea.ac.kr"

            try:
                search_input = WebDriverWait(self.driver, 5).until(
                    EC.presence_of_element_located(
                        (
                            By.CSS_SELECTOR,
                            "input[placeholder=' '][class*='styleguide-input_input']",
                        )
                    )
                )

            except TimeoutException:
                search_paths = [
                    "/search/form.uri?display=basic",
                    "/search/form.uri",
                    "/document/search.uri",
                ]

                for search_path in search_paths:
                    try:
                        search_url = base_url + search_path
                        self.driver.get(search_url)
                        time.sleep(3)

                        search_input = WebDriverWait(self.driver, 5).until(
                            EC.presence_of_element_located(
                                (
                                    By.CSS_SELECTOR,
                                    "input[placeholder=' '][class*='styleguide-input_input']",
                                )
                            )
                        )
                        break
                    except Exception:
                        continue
                else:
                    input("Please navigate to Scopus search page manually and press Enter: ")

                    try:
                        search_input = WebDriverWait(self.driver, 10).until(
                            EC.presence_of_element_located(
                                (
                                    By.CSS_SELECTOR,
                                    "input[placeholder=' '][class*='styleguide-input_input']",
                                )
                            )
                        )
                    except:
                        input(f"Please search for '{keyword}' manually and press Enter: ")
                        return True

            search_input.clear()
            self.human_like_delay(0.5, 1)
            search_input.send_keys(keyword)

            try:
                search_within_dropdown = WebDriverWait(self.driver, 5).until(
                    EC.element_to_be_clickable(
                        (By.CSS_SELECTOR, "select[data-testid='select-search-within']")
                    )
                )

                select = Select(search_within_dropdown)
                select.select_by_value("KEY")
            except:
                pass

            self.human_like_delay(0.5, 1)
            search_button = WebDriverWait(self.driver, 5).until(
                EC.element_to_be_clickable(
                    (By.CSS_SELECTOR, "button[type='submit'][class*='Button_button']")
                )
            )
            search_button.click()

            result_selectors = [
                "tbody tr.TableItems-module__A6xTk",
                ".result-item",
                "[data-testid='search-results']",
                ".document-result",
                ".search-results-content",
            ]

            for selector in result_selectors:
                try:
                    WebDriverWait(self.driver, 15).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                    )
                    break
                except:
                    continue
            else:
                input("Please confirm search results are displayed and press Enter: ")

            self.set_results_per_page(10)

            logger.info(f"Search for keyword '{keyword}' completed")
            return True

        except Exception as e:
            logger.error(f"Error searching keyword '{keyword}': {str(e)}")
            input(f"Please search for '{keyword}' manually and press Enter: ")
            return True

    def extract_paper_links(self, paper_elements):
        paper_links = []

        for i, paper_element in enumerate(paper_elements, 1):
            try:
                title_link = paper_element.find_element(By.CSS_SELECTOR, "h3 a")
                href = title_link.get_attribute("href")

                if href.startswith("http"):
                    paper_link = href
                else:
                    if "oca.korea.ac.kr" in self.driver.current_url:
                        base_url = "https://www-scopus-com-ssl.oca.korea.ac.kr"
                    else:
                        base_url = "https://www.scopus.com"
                    paper_link = base_url + href

                paper_links.append(paper_link)

            except Exception as e:
                logger.warning(f"Error extracting paper link: {str(e)}")
                continue

        return paper_links

    def get_detailed_author_info(self, paper_link):
        detailed_info = {
            "authors": [],
            "emails": [],
            "detailed_affiliations": [],
            "raw_affiliations": [],
            "universities": [],
            "countries": [],
            "detected_sentences": "",
            "link": paper_link,
        }

        try:
            self.driver.execute_script(f"window.open('{paper_link}', '_blank');")
            self.driver.switch_to.window(self.driver.window_handles[-1])

            self.human_like_delay(5, 7)

            title_text = ""
            try:
                title_element = WebDriverWait(self.driver, 15).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "h2[data-testid='publication-titles']"))
                )
                title_text = title_element.text.strip()
            except:
                pass

            abstract_text = ""
            try:
                abstract_element = WebDriverWait(self.driver, 15).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "div[id='document-details-abstract']"))
                )
                abstract_text = abstract_element.text.strip()
            except:
                pass

            title_has_llm = self.contains_llm(title_text) if title_text else False
            abstract_has_llm = self.contains_llm(abstract_text) if abstract_text else False
            
            if not (title_has_llm or abstract_has_llm):
                detailed_info["detected_sentences"] = "No LLM keywords found - skipped"
                
                self.driver.close()
                self.driver.switch_to.window(self.driver.window_handles[0])
                return detailed_info

            detected_sentences = []
            if title_has_llm:
                title_sentences = self.extract_llm_sentences(title_text)
                if title_sentences:
                    detected_sentences.append(f"Title: {title_sentences}")
            
            if abstract_has_llm:
                abstract_sentences = self.extract_llm_sentences(abstract_text)
                if abstract_sentences:
                    detected_sentences.append(f"Abstract: {abstract_sentences}")
            
            detailed_info["detected_sentences"] = " | ".join(detected_sentences)

            try:
                show_all_button = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable(
                        (By.XPATH, "//button[.//span[text()='Show all information']]")
                    )
                )
                self.driver.execute_script("arguments[0].click();", show_all_button)
                time.sleep(5)
            except:
                pass

            author_affiliation_map, affiliation_dict = (
                self.extract_author_affiliation_mapping()
            )

            for name, superscripts in author_affiliation_map.items():
                detailed_info["authors"].append(name)

                try:
                    email_element = self.driver.find_element(
                        By.XPATH,
                        f"//span[text()='{name}']/ancestor::li//a[starts-with(@href, 'mailto:')]",
                    )
                    email = email_element.get_attribute("href").replace("mailto:", "")
                    detailed_info["emails"].append(email)
                except:
                    detailed_info["emails"].append("")

                affs, affs_raw, univs, countries = [], [], [], []
                for sup in superscripts:
                    if sup in affiliation_dict:
                        aff_text = affiliation_dict[sup]
                        affs.append(aff_text)
                        
                        if sup.startswith("default"):
                            affs_raw.append(f"[No superscript] {aff_text}")
                        else:
                            affs_raw.append(f"[{sup}] {aff_text}")
                            
                        parsed = self.parse_affiliation(aff_text)
                        univs.append(parsed["university"])
                        countries.append(parsed["country"])

                if not affs:
                    affs = [""]
                    affs_raw = [""]
                    univs = [""]
                    countries = [""]

                detailed_info["detailed_affiliations"].append(" | ".join(affs))
                detailed_info["raw_affiliations"].append(" | ".join(affs_raw))
                detailed_info["universities"].append(" | ".join(univs))
                detailed_info["countries"].append(" | ".join(countries))

            self.driver.close()
            self.driver.switch_to.window(self.driver.window_handles[0])

        except Exception as e:
            logger.error(f"Error extracting detailed information: {str(e)}")
            try:
                if len(self.driver.window_handles) > 1:
                    self.driver.close()
                self.driver.switch_to.window(self.driver.window_handles[0])
            except:
                pass

        return detailed_info

    def parse_affiliation(self, affiliation_text):
        parsed = {"department": "", "university": "", "country": ""}

        if not affiliation_text:
            return parsed

        parts = [part.strip() for part in affiliation_text.split(",")]

        if len(parts) >= 3:
            parsed["department"] = parts[0]
            parsed["university"] = parts[1]
            parsed["country"] = parts[-1]

        elif len(parts) == 2:
            parsed["university"] = parts[0]
            parsed["country"] = parts[1]

        elif len(parts) == 1:
            parsed["university"] = parts[0]

        if (
            len(parts) >= 2
            and "university" not in parts[0].lower()
            and "college" not in parts[0].lower()
        ):
            for i, part in enumerate(parts[1:], 1):
                if any(
                    keyword in part.lower()
                    for keyword in ["university", "college", "institute", "school"]
                ):
                    parsed["department"] = parts[0]
                    parsed["university"] = part
                    if i + 1 < len(parts):
                        parsed["country"] = parts[-1]
                    break

        return parsed

    def save_batch_results(
        self, keyword, papers_data, start_page, end_page, paper_start_index=1
    ):
        try:
            safe_keyword = re.sub(r"[^\w\s-]", "", keyword).replace(" ", "_")
            filename = f"scopus_{safe_keyword}_pages_{start_page}-{end_page}.xlsx"

            if papers_data:
                formatted_data = []
                current_paper_index = paper_start_index

                for paper in papers_data:
                    authors = paper.get("authors", [""])
                    emails = paper.get("emails", [""])
                    affiliations = paper.get("detailed_affiliations", [""])
                    raw_affiliations = paper.get("raw_affiliations", [""])
                    universities = paper.get("universities", [""])
                    countries = paper.get("countries", [""])
                    detected_sentences = paper.get("detected_sentences", "")

                    max_len = max(
                        len(authors),
                        len(emails),
                        len(affiliations),
                        len(raw_affiliations),
                        len(universities),
                        len(countries),
                    )
                    authors.extend([""] * (max_len - len(authors)))
                    emails.extend([""] * (max_len - len(emails)))
                    affiliations.extend([""] * (max_len - len(affiliations)))
                    raw_affiliations.extend([""] * (max_len - len(raw_affiliations)))
                    universities.extend([""] * (max_len - len(universities)))
                    countries.extend([""] * (max_len - len(countries)))

                    for i in range(max_len):
                        formatted_data.append(
                            {
                                "Paper Number": (
                                    current_paper_index if i == 0 else ""
                                ),
                                "Author": authors[i] if i < len(authors) else "",
                                "Email": emails[i] if i < len(emails) else "",
                                "Affiliation (Raw)": (
                                    raw_affiliations[i] if i < len(raw_affiliations) else ""
                                ),
                                "Affiliation (Department)": (
                                    affiliations[i] if i < len(affiliations) else ""
                                ),
                                "Affiliation (University)": (
                                    universities[i] if i < len(universities) else ""
                                ),
                                "Affiliation (Country)": (
                                    countries[i] if i < len(countries) else ""
                                ),
                                "Detected Sentences": (
                                    detected_sentences if i == 0 else ""
                                ),
                                "Paper Link": (
                                    paper.get("link", "") if i == 0 else ""
                                ),
                            }
                        )

                    formatted_data.append(
                        {
                            "Paper Number": "",
                            "Author": "",
                            "Email": "",
                            "Affiliation (Raw)": "",
                            "Affiliation (Department)": "",
                            "Affiliation (University)": "",
                            "Affiliation (Country)": "",
                            "Detected Sentences": "",
                            "Paper Link": "",
                        }
                    )

                    current_paper_index += 1

                df = pd.DataFrame(formatted_data)
                df.to_excel(filename, index=False)

                return current_paper_index

        except Exception as e:
            return paper_start_index

    def crawl_pages(self, keyword, max_pages=200, start_page=1):
        papers_data = []
        paper_index = ((start_page - 1) * 2) + 1

        if not self.search_keyword(keyword):
            return papers_data

        if start_page > 1:
            success = self.navigate_to_page(start_page)
            if not success:
                start_page = 1
                paper_index = 1

        for page_num in range(start_page, max_pages + 1):
            try:
                logger.info(f"Crawling keyword '{keyword}' - page {page_num}/{max_pages}")

                paper_elements = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_all_elements_located(
                        (By.CSS_SELECTOR, "tbody tr.TableItems-module__A6xTk")
                    )
                )

                if not paper_elements:
                    logger.info(f"No more results on page {page_num}.")
                    break

                paper_links = self.extract_paper_links(paper_elements)

                page_papers = []
                
                for j, paper_link in enumerate(paper_links, 1):
                    detailed_info = self.get_detailed_author_info(paper_link)
                    detailed_info["link"] = paper_link

                    if detailed_info.get("detected_sentences") != "No LLM keywords found - skipped":
                        detailed_info["paper_number"] = paper_index
                        paper_index += 1
                    else:
                        detailed_info["paper_number"] = "none"

                    page_papers.append(detailed_info)
                    papers_data.append(detailed_info)

                    self.human_like_delay(2, 4)

                if page_num < max_pages:
                    try:
                        current_url = self.driver.current_url
                        
                        next_button = self.driver.find_element(
                            By.XPATH, "//button[.//span[text()='Next']]"
                        )

                        if next_button.is_enabled() and not next_button.get_attribute(
                            "disabled"
                        ):
                            next_button.click()
                            self.human_like_delay(3, 5)
                            
                            new_url = self.driver.current_url
                            
                            if current_url == new_url:
                                try:
                                    WebDriverWait(self.driver, 10).until(
                                        EC.staleness_of(paper_elements[0])
                                    )
                                except:
                                    pass
                            
                            logger.info(f"Moved to page {page_num + 1}")
                        else:
                            logger.info("No more next pages available.")
                            break

                    except NoSuchElementException:
                        logger.info("Next button not found. Last page reached.")
                        break
                    except Exception as e:
                        logger.error(f"Error navigating to next page: {str(e)}")
                        break

            except Exception as e:
                logger.error(f"Error crawling page {page_num}: {str(e)}")
                continue

        if papers_data:
            self.save_batch_results(keyword, papers_data, start_page, page_num, 1)

        logger.info(f"Keyword '{keyword}' crawling completed: {len(papers_data)} papers")
        return papers_data

    def navigate_to_page(self, target_page):
        try:
            for i in range(target_page - 1):
                try:
                    next_button = self.driver.find_element(
                        By.XPATH, "//button[.//span[text()='Next']]"
                    )
                    if next_button.is_enabled():
                        next_button.click()
                        self.human_like_delay(2, 3)
                    else:
                        return False
                except:
                    return False

            return True

        except Exception:
            return False

    def save_progress(self, keyword, next_page):
        try:
            progress_info = {
                "keyword": keyword,
                "keyword_index": self.keywords.index(keyword),
                "next_page": next_page,
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            }

            with open("scopus_progress.txt", "w", encoding="utf-8") as f:
                f.write(f"Last completed: {progress_info['keyword']}\n")
                f.write(f"Keyword index: {progress_info['keyword_index']}\n")
                f.write(f"Next start page: {progress_info['next_page']}\n")
                f.write(f"Time: {progress_info['timestamp']}\n")
                f.write(f"\nRestart method:\n")
                f.write(
                    f"crawler = ScopusCrawler(start_keyword_index={progress_info['keyword_index']}, start_page={progress_info['next_page']})\n"
                )

        except Exception as e:
            pass

    def save_to_excel(self, filename="scopus_papers_results.xlsx"):
        with pd.ExcelWriter(filename, engine="openpyxl") as writer:
            for keyword, papers_data in self.results_data.items():
                if papers_data:
                    formatted_data = []

                    for paper_index, paper in enumerate(papers_data, 1):
                        authors = paper.get("authors", [""])
                        emails = paper.get("emails", [""])
                        detailed_affiliations = paper.get("detailed_affiliations", [""])
                        raw_affiliations = paper.get("raw_affiliations", [""])
                        universities = paper.get("universities", [""])
                        countries = paper.get("countries", [""])
                        detected_sentences = paper.get("detected_sentences", "")

                        max_len = max(
                            len(authors),
                            len(emails),
                            len(detailed_affiliations),
                            len(raw_affiliations),
                            len(universities),
                            len(countries),
                        )
                        authors.extend([""] * (max_len - len(authors)))
                        emails.extend([""] * (max_len - len(emails)))
                        detailed_affiliations.extend(
                            [""] * (max_len - len(detailed_affiliations))
                        )
                        raw_affiliations.extend([""] * (max_len - len(raw_affiliations)))
                        universities.extend([""] * (max_len - len(universities)))
                        countries.extend([""] * (max_len - len(countries)))

                        for i in range(max_len):
                            formatted_data.append(
                                {
                                    "Paper Number": (
                                        paper_index if i == 0 else ""
                                    ),
                                    "Author": authors[i] if i < len(authors) else "",
                                    "Email": emails[i] if i < len(emails) else "",
                                    "Affiliation (Raw)": (
                                        raw_affiliations[i] if i < len(raw_affiliations) else ""
                                    ),
                                    "Affiliation (Department)": (
                                        detailed_affiliations[i]
                                        if i < len(detailed_affiliations)
                                        else ""
                                    ),
                                    "Affiliation (University)": (
                                        universities[i] if i < len(universities) else ""
                                    ),
                                    "Affiliation (Country)": (
                                        countries[i] if i < len(countries) else ""
                                    ),
                                    "Detected Sentences": (
                                        detected_sentences if i == 0 else ""
                                    ),
                                    "Paper Link": (
                                        paper.get("link", "") if i == 0 else ""
                                    ),
                                }
                            )

                        formatted_data.append(
                            {
                                "Paper Number": "",
                                "Author": "",
                                "Email": "",
                                "Affiliation (Raw)": "",
                                "Affiliation (Department)": "",
                                "Affiliation (University)": "",
                                "Affiliation (Country)": "",
                                "Detected Sentences": "",
                                "Paper Link": "",
                            }
                        )

                    df = pd.DataFrame(formatted_data)

                    safe_keyword = re.sub(r"[^\w\s-]", "", keyword).strip()[
                        :31
                    ]
                    df.to_excel(writer, sheet_name=safe_keyword, index=False)

        logger.info(f"Results saved to {filename}")

    def run(self):
        try:
            self.setup_driver()

            if not self.login_and_access_scopus():
                logger.error("Failed to access Scopus")
                return

            try:
                WebDriverWait(self.driver, 15).until(
                    EC.presence_of_element_located(
                        (
                            By.CSS_SELECTOR,
                            "input[placeholder=' '][class*='styleguide-input_input']",
                        )
                    )
                )
            except:
                input("Please navigate to Scopus search page manually and press Enter: ")

            total_keywords = len(self.keywords)
            for idx, keyword in enumerate(
                self.keywords[self.start_keyword_index :], self.start_keyword_index + 1
            ):
                start_page = (
                    self.start_page if idx == self.start_keyword_index + 1 else 1
                )

                papers_data = self.crawl_pages(
                    keyword, max_pages=200, start_page=start_page
                )
                self.results_data[keyword] = papers_data

                if idx < total_keywords:
                    self.human_like_delay(10, 15)

            self.save_to_excel("scopus_papers_results.xlsx")

            total_papers = sum(len(papers) for papers in self.results_data.values())
            print(f"Crawling completed! Total papers: {total_papers}")

        except Exception as e:
            logger.error(f"Error during crawling: {str(e)}")
        finally:
            if self.driver:
                self.driver.quit()


if __name__ == "__main__":
    crawler = ScopusCrawler()
    crawler.run()