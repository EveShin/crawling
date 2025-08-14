import requests
import pandas as pd
import time
import re
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
import logging

# 로깅 설정
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class ScopusCrawler:
    def __init__(self, start_keyword_index=0, start_page=1):
        """Scopus 크롤러 초기화"""
        # 실제 운영용: 키워드 8개 전체
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
        self.library_url = "https://libs.korea.ac.kr/"  # 고려대 도서관 사이트
        self.driver = None
        self.results_data = {}

        # 재시작 설정
        self.start_keyword_index = start_keyword_index
        self.start_page = start_page

        print(
            f"🔄 시작 설정: 키워드 {start_keyword_index + 1}번째 ('{self.keywords[start_keyword_index]}'), 페이지 {start_page}부터"
        )

    def setup_driver(self):
        """Chrome 드라이버 설정 (봇 감지 우회 강화)"""
        chrome_options = Options()

        # 봇 감지 우회를 위한 고급 설정
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option("useAutomationExtension", False)

        # 실제 사용자처럼 보이게 하는 설정들
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-plugins")
        chrome_options.add_argument(
            "--disable-images"
        )  # 이미지 로딩 비활성화로 속도 향상
        chrome_options.add_argument(
            "--disable-javascript"
        )  # 일부 봇 감지 스크립트 우회

        # 로그 숨기기
        chrome_options.add_argument("--log-level=3")
        chrome_options.add_argument("--disable-logging")
        chrome_options.add_argument("--disable-background-networking")

        # 더 현실적인 User-Agent
        chrome_options.add_argument(
            "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )

        # 윈도우 크기 설정 (봇은 보통 headless이므로 실제 크기 설정)
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--start-maximized")

        # ChromeDriver 자동 관리
        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=chrome_options)

        # JavaScript로 webdriver 속성 숨기기
        self.driver.execute_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )

        # 추가 봇 감지 우회 스크립트
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

        logger.info("Chrome 드라이버 설정 완료 (봇 감지 우회 강화)")

    def extract_author_affiliation_mapping(self):
        """'Show all information' 패널 기준 저자-소속 매핑 정보 추출 (개선된 로직)"""
        author_affiliation_map = {}
        affiliation_dict = {}

        try:
            # ✅ 1. 먼저 소속 정보 수집 (첨자 → 소속 텍스트)
            print("🔍 소속 정보 수집 중...")
            affiliation_elements = self.driver.find_elements(
                By.CSS_SELECTOR,
                "section[data-testid='detailed-information-affiliations'] ul.DetailedInformationFlyout_list__76Ipn li",
            )

            for aff in affiliation_elements:
                try:
                    # 첨자가 있는 경우와 없는 경우 모두 처리
                    try:
                        sup = aff.find_element(By.TAG_NAME, "sup").text.strip()
                        text = aff.find_element(By.TAG_NAME, "span").text.strip()
                        affiliation_dict[sup] = text
                        print(f"🏛️ [{sup}] {text}")
                    except NoSuchElementException:
                        # 첨자가 없는 소속인 경우
                        text = aff.text.strip()
                        if text:  # 빈 텍스트가 아닌 경우만
                            # 기본 키로 저장 (첨자 없음을 나타냄)
                            default_key = "default"
                            # 이미 default가 있으면 번호 추가
                            counter = 1
                            while default_key in affiliation_dict:
                                default_key = f"default{counter}"
                                counter += 1
                            affiliation_dict[default_key] = text
                            print(f"🏛️ [첨자없음] {text} → 키: {default_key}")
                except Exception as e:
                    print(f"⚠️ 소속 파싱 실패: {str(e)}")
                    continue

            # ✅ 2. 저자 정보 추출 (저자명 → 첨자 목록)
            print("👥 저자 정보 수집 중...")
            author_elements = self.driver.find_elements(
                By.CSS_SELECTOR,
                "ul.DetailedInformationFlyout_list__76Ipn li[data-testid='authorItem-button']",
            )

            # 📊 소속 개수에 따른 처리 로직 결정
            num_affiliations = len(affiliation_dict)
            print(f"📊 발견된 소속 수: {num_affiliations}개")

            for i, author_el in enumerate(author_elements, 1):
                try:
                    name = author_el.find_element(
                        By.CSS_SELECTOR, "span.Button_text__0dddp"
                    ).text.strip()

                    # 이메일 생략 필터링
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

                    # 🔄 개선된 소속 할당 로직
                    if num_affiliations == 1:
                        # 소속이 하나뿐이면 모든 저자에게 동일하게 적용
                        superscripts = list(affiliation_dict.keys())
                        print(f"👤 {name} → {superscripts} (단일 소속 - 모든 저자 공통)")
                    elif num_affiliations > 1:
                        # 소속이 여러 개인 경우
                        if not superscripts:
                            # 첨자가 없으면 첫 번째 소속을 기본으로 할당
                            first_affiliation = list(affiliation_dict.keys())[0] if affiliation_dict else ""
                            if first_affiliation:
                                superscripts = [first_affiliation]
                                print(f"👤 {name} → {superscripts} (첨자 없음 - 기본 소속 할당)")
                            else:
                                superscripts = []
                                print(f"👤 {name} → 소속 없음")
                        else:
                            print(f"👤 {name} → {superscripts} (첨자 매핑)")
                    elif num_affiliations == 0:
                        # 소속 정보가 아예 없는 경우
                        superscripts = []
                        print(f"👤 {name} → 소속 정보 전혀 없음")
                    else:
                        # 예외 상황
                        superscripts = []
                        print(f"👤 {name} → 예외 상황 - 소속 없음")

                    author_affiliation_map[name] = superscripts

                except Exception as e:
                    print(f"❌ 저자 {i} 추출 실패: {str(e)}")
                    continue

            # 📈 최종 결과 요약
            print(f"\n📈 매핑 결과 요약:")
            print(f"   - 총 소속 수: {len(affiliation_dict)}개")
            print(f"   - 총 저자 수: {len(author_affiliation_map)}명")
            
            # 소속 없는 저자 확인
            authors_without_affiliation = [name for name, affs in author_affiliation_map.items() if not affs]
            if authors_without_affiliation:
                print(f"   ⚠️ 소속 없는 저자: {len(authors_without_affiliation)}명")
                for name in authors_without_affiliation:
                    print(f"      - {name}")

        except Exception as e:
            print(f"❌ 매핑 전체 실패: {str(e)}")

        return author_affiliation_map, affiliation_dict

    def contains_llm(self, text):
        """LLM 관련 키워드 탐지 (단어 경계 사용)"""
        import re
        text_lower = text.lower()
        
        # 1. LLM 약어들 (단어 경계 적용)
        if re.search(r'\bllms?\b', text_lower):
            return True
        
        # 2. 전체 용어
        if "large language model" in text_lower:
            return True
        
        return False

    def extract_llm_sentences(self, text):
        """LLM이 포함된 문장들 추출"""
        import re
        
        # 문장 분리 (점, 느낌표, 물음표 기준)
        sentences = re.split(r'[.!?]+', text)
        llm_sentences = []
        
        for sentence in sentences:
            sentence = sentence.strip()
            if sentence and self.contains_llm(sentence):
                llm_sentences.append(sentence)
        
        return ' | '.join(llm_sentences)

    def human_like_delay(self, min_seconds=1, max_seconds=3):
        """사람처럼 랜덤한 지연시간"""
        import random

        delay = random.uniform(min_seconds, max_seconds)
        time.sleep(delay)

    def login_and_access_scopus(self):
        """고려대 도서관에서 Scopus 접근 (로그인 과정 생략)"""
        try:
            # 고려대 도서관 사이트로 이동
            print("🌐 고려대학교 도서관 사이트로 이동 중...")
            self.driver.get(self.library_url)
            time.sleep(3)

            # 바로 "학술DB" 버튼 클릭 시도
            print("🔍 학술DB 메뉴를 클릭합니다...")
            try:
                academic_db_button = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable(
                        (By.CSS_SELECTOR, "a[data-target='.home-service-link-group-2']")
                    )
                )
                academic_db_button.click()
                time.sleep(2)
                print("✅ 학술DB 메뉴 클릭 성공!")
            except TimeoutException:
                print("❌ 학술DB 버튼을 찾을 수 없습니다.")
                print("📚 수동으로 학술DB 메뉴를 클릭한 후 Enter를 눌러주세요...")
                input("학술DB 메뉴 클릭 완료 후 Enter: ")

            # Scopus 링크 클릭
            print("📖 Scopus 데이터베이스에 접근합니다...")
            try:
                scopus_link = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable(
                        (By.XPATH, "//a[contains(@href, 'scopus.com') and text()='Scopus']")
                    )
                )

                # 현재 창에서 Scopus 링크 주소 가져오기
                scopus_url = scopus_link.get_attribute("href")
                print(f"🔗 Scopus 페이지로 이동합니다...")

                # Scopus로 이동
                self.driver.get(scopus_url)
                time.sleep(5)  # 페이지 로딩 대기
                print("✅ Scopus 접근 성공!")
                
            except TimeoutException:
                print("❌ Scopus 링크를 찾을 수 없습니다.")
                print("📖 수동으로 Scopus 링크를 클릭한 후 Enter를 눌러주세요...")
                input("Scopus 접근 완료 후 Enter: ")

            # Scopus 로그인이 필요한 경우에만 로그인 요청
            print("\n" + "=" * 60)
            print("🔐 Scopus 접근 확인")
            print("=" * 60)
            print("1. Scopus 페이지가 로딩되었습니다")
            print("2. 만약 로그인이 필요하다면 로그인을 완료해주세요")
            print("3. Scopus 검색 페이지에 접근 가능하면 Enter를 눌러주세요")
            print("=" * 60)
            input("Scopus 사용 준비 완료 후 Enter를 눌러주세요: ")

            print("✅ Scopus 접근 완료!")
            return True

        except Exception as e:
            logger.error(f"Scopus 접근 중 오류: {str(e)}")
            print("❌ 자동 접근에 실패했습니다.")
            print("수동으로 다음 단계를 진행해주세요:")
            print("1. 고려대 도서관 → 학술DB → Scopus 접근")
            print("2. 필요시 Scopus 로그인 완료")
            input("수동으로 Scopus 접근 완료 후 Enter: ")
            return True

    def set_results_per_page(self, count=10):  # 기본값 유지: 10개
        """페이지당 표시할 결과 수 설정"""
        try:
            # Display 드롭다운 찾기
            display_select = WebDriverWait(self.driver, 5).until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, ".Select-module__vDMww")
                )
            )

            from selenium.webdriver.support.ui import Select

            select = Select(display_select)
            try:
                select.select_by_value("10")  # 기본 10개 설정
            except:
                try:
                    select.select_by_value("20")  # 10이 없으면 20개
                except:
                    select.select_by_value("50")   # 20도 없으면 50개

            time.sleep(3)  # 페이지 로딩 대기
            logger.info(f"페이지당 결과 설정 완료 (기본값 사용)")

        except Exception as e:
            logger.warning(f"결과 표시 개수 설정 실패: {str(e)}")
            print("⚠️ 페이지당 결과 수 설정을 건너뜁니다. 기본값 사용.")

    def search_keyword(self, keyword):
        """특정 키워드로 검색 실행"""
        try:
            # 현재 URL 확인 및 정리
            current_url = self.driver.current_url
            print(f"🔗 현재 URL: {current_url}")

            # URL 파싱하여 올바른 기본 URL 구성
            try:
                from urllib.parse import urlparse

                parsed_url = urlparse(current_url)

                # 고려대 프록시 URL만 사용
                base_url = "https://www-scopus-com-ssl.oca.korea.ac.kr"

            except Exception as e:
                print(f"❌ URL 파싱 오류: {e}")
                print(
                    "⚠️ Scopus 페이지가 아닙니다. 수동으로 Scopus 검색 페이지로 이동해주세요."
                )
                input("Scopus 검색 페이지 접근 후 Enter를 눌러주세요: ")
                base_url = "https://www-scopus-com-ssl.oca.korea.ac.kr"  # 기본값

            print(f"🔗 기본 URL: {base_url}")

            # 현재 페이지에서 검색 필드 찾기 시도
            try:
                search_input = WebDriverWait(self.driver, 5).until(
                    EC.presence_of_element_located(
                        (
                            By.CSS_SELECTOR,
                            "input[placeholder=' '][class*='styleguide-input_input']",
                        )
                    )
                )
                print("✅ 현재 페이지에서 검색 필드를 찾았습니다!")

            except TimeoutException:
                # 검색 페이지로 이동 시도
                print("🔍 검색 페이지로 이동을 시도합니다...")
                search_paths = [
                    "/search/form.uri?display=basic",
                    "/search/form.uri",
                    "/document/search.uri",
                ]

                for search_path in search_paths:
                    try:
                        search_url = base_url + search_path
                        print(f"🔗 시도하는 URL: {search_url}")
                        self.driver.get(search_url)
                        time.sleep(3)

                        # 검색 필드가 있는지 확인
                        search_input = WebDriverWait(self.driver, 5).until(
                            EC.presence_of_element_located(
                                (
                                    By.CSS_SELECTOR,
                                    "input[placeholder=' '][class*='styleguide-input_input']",
                                )
                            )
                        )
                        print("✅ 검색 페이지로 이동 성공!")
                        break
                    except Exception as e:
                        print(f"❌ URL 실패: {search_url} - {str(e)}")
                        continue
                else:
                    # 모든 URL 실패시 수동 이동 요청
                    print("❌ 자동으로 검색 페이지를 찾을 수 없습니다.")
                    print("현재 페이지가 이미 검색 페이지라면 그냥 Enter를 눌러주세요.")
                    print(
                        "아니면 수동으로 Scopus 검색 페이지로 이동한 후 Enter를 눌러주세요."
                    )
                    input("Scopus 검색 페이지 확인 후 Enter: ")

                    # 다시 검색 필드 찾기 시도
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
                        print(
                            "❌ 검색 필드를 찾을 수 없습니다. 수동으로 검색을 진행해주세요."
                        )
                        print(f"검색어: {keyword}")
                        input("수동 검색 완료 후 Enter: ")
                        return True

            # 검색어 입력
            print(f"🔍 검색어 입력: {keyword}")
            search_input.clear()
            self.human_like_delay(0.5, 1)  # 타이핑 지연
            search_input.send_keys(keyword)

            # Search within을 Keywords로 설정
            try:
                search_within_dropdown = WebDriverWait(self.driver, 5).until(
                    EC.element_to_be_clickable(
                        (By.CSS_SELECTOR, "select[data-testid='select-search-within']")
                    )
                )

                from selenium.webdriver.support.ui import Select

                select = Select(search_within_dropdown)
                select.select_by_value("KEY")
                print("✅ 검색 범위를 Keywords로 설정")
            except:
                print("⚠️ 검색 범위 설정을 건너뜁니다.")

            # 검색 버튼 클릭
            print("🔍 검색 실행...")
            self.human_like_delay(0.5, 1)  # 클릭 전 지연
            search_button = WebDriverWait(self.driver, 5).until(
                EC.element_to_be_clickable(
                    (By.CSS_SELECTOR, "button[type='submit'][class*='Button_button']")
                )
            )
            search_button.click()

            # 결과 페이지 로딩 대기 (여러 selector 시도)
            result_selectors = [
                "tbody tr.TableItems-module__A6xTk",
                ".result-item",
                "[data-testid='search-results']",
                ".document-result",
                ".search-results-content",
            ]

            print("⏳ 검색 결과 로딩 대기... (최대 15초)")
            for selector in result_selectors:
                try:
                    WebDriverWait(self.driver, 15).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                    )
                    print(f"✅ 검색 결과 페이지 로딩 완료!")
                    break
                except:
                    continue
            else:
                print("⚠️ 자동으로 검색 결과를 확인할 수 없습니다.")
                print("검색 결과가 표시되었는지 확인해주세요.")
                input("검색 결과 확인 후 Enter를 눌러주세요: ")

            # 페이지당 결과 수 설정 (기본값 유지)
            self.set_results_per_page(10)

            logger.info(f"키워드 '{keyword}' 검색 완료")
            return True

        except Exception as e:
            logger.error(f"키워드 '{keyword}' 검색 중 오류: {str(e)}")
            print(f"❌ 자동 검색 실패: {str(e)}")
            print("수동으로 다음을 진행해주세요:")
            print(f"1. 검색어 '{keyword}' 입력")
            print("2. Keywords 범위로 검색")
            print("3. 검색 결과 확인 후 Enter")
            input("수동 검색 완료 후 Enter: ")
            return True

    def extract_paper_links(self, paper_elements):  # 실제 운영: 모든 논문 처리
        """논문 요소들에서 상세 페이지 링크 추출 (모든 논문)"""
        paper_links = []

        # 🔍 디버깅: 모든 논문 링크 출력
        print(f"🔍 현재 페이지의 모든 논문 ({len(paper_elements)}개)")
        for i, element in enumerate(paper_elements):
            try:
                link = element.find_element(By.CSS_SELECTOR, "h3 a").get_attribute('href')
                print(f"📄 논문 {i+1}: {link}")
            except Exception as e:
                print(f"❌ 논문 {i+1} 링크 추출 실패: {str(e)}")

        # 🚀 실제 운영: 모든 논문 처리
        for i, paper_element in enumerate(paper_elements, 1):
            try:
                # 논문 상세 페이지 링크 추출
                title_link = paper_element.find_element(By.CSS_SELECTOR, "h3 a")
                href = title_link.get_attribute("href")

                # 절대 URL인지 상대 URL인지 확인
                if href.startswith("http"):
                    paper_link = href
                else:
                    # 상대 URL인 경우 기본 URL과 결합
                    if "oca.korea.ac.kr" in self.driver.current_url:
                        base_url = "https://www-scopus-com-ssl.oca.korea.ac.kr"
                    else:
                        base_url = "https://www.scopus.com"
                    paper_link = base_url + href

                paper_links.append(paper_link)

            except Exception as e:
                logger.warning(f"논문 링크 추출 중 오류: {str(e)}")
                print(f"❌ [{i}] 논문 링크 추출 실패: {str(e)}")
                continue

        print(f"📋 총 {len(paper_elements)}개 중 {len(paper_links)}개 링크 추출 완료")
        return paper_links

    def get_detailed_author_info(self, paper_link):
        """논문 상세 페이지에서 저자 상세 정보 추출 (Show all information 버튼 클릭 후)"""
        detailed_info = {
            "authors": [],
            "emails": [],
            "detailed_affiliations": [],
            "raw_affiliations": [],  # 원본 소속 정보 (첨자 포함)
            "universities": [],
            "countries": [],
            "detected_sentences": "",  # 🆕 LLM 탐지된 문장들
            "link": paper_link,
        }

        try:
            # 새 탭에서 상세 페이지 열기
            self.driver.execute_script(f"window.open('{paper_link}', '_blank');")
            self.driver.switch_to.window(self.driver.window_handles[-1])

            # 페이지 로딩 대기 - 더 충분한 시간과 명시적 대기
            print("⏳ 논문 상세 페이지 로딩 대기 중...")
            self.human_like_delay(5, 7)  # 더 긴 초기 대기

            # 🔍 제목과 초록에서 LLM 키워드 검사
            print("🔍 제목과 초록에서 LLM 키워드 검사 중...")
            
            # 제목 추출 - 명시적 대기 추가
            title_text = ""
            try:
                print("📝 제목 추출 시도 중...")
                title_element = WebDriverWait(self.driver, 15).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "h2[data-testid='publication-titles']"))
                )
                title_text = title_element.text.strip()
                print(f"📝 제목: {title_text}")
            except TimeoutException:
                print(f"⚠️ 제목 추출 시간 초과")
            except Exception as e:
                print(f"⚠️ 제목 추출 실패: {str(e)}")

            # 초록 추출 - 명시적 대기 추가
            abstract_text = ""
            try:
                print("📄 초록 추출 시도 중...")
                abstract_element = WebDriverWait(self.driver, 15).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "div[id='document-details-abstract']"))
                )
                abstract_text = abstract_element.text.strip()
                print(f"📄 초록: {abstract_text[:100]}..." if len(abstract_text) > 100 else f"📄 초록: {abstract_text}")
            except TimeoutException:
                print(f"⚠️ 초록 추출 시간 초과")
            except Exception as e:
                print(f"⚠️ 초록 추출 실패: {str(e)}")

            # LLM 키워드 검사
            title_has_llm = self.contains_llm(title_text) if title_text else False
            abstract_has_llm = self.contains_llm(abstract_text) if abstract_text else False
            
            if not (title_has_llm or abstract_has_llm):
                print("❌ LLM 키워드가 제목이나 초록에 없음 - 논문 스킵")
                detailed_info["detected_sentences"] = "LLM 키워드 없음 - 스킵됨"
                
                # 탭 닫고 원래 탭으로 돌아가기
                self.driver.close()
                self.driver.switch_to.window(self.driver.window_handles[0])
                return detailed_info

            # LLM 탐지된 경우
            print("✅ LLM 키워드 탐지됨 - 저자 정보 수집 진행")
            
            # 탐지된 문장들 추출
            detected_sentences = []
            if title_has_llm:
                title_sentences = self.extract_llm_sentences(title_text)
                if title_sentences:
                    detected_sentences.append(f"제목: {title_sentences}")
            
            if abstract_has_llm:
                abstract_sentences = self.extract_llm_sentences(abstract_text)
                if abstract_sentences:
                    detected_sentences.append(f"초록: {abstract_sentences}")
            
            detailed_info["detected_sentences"] = " | ".join(detected_sentences)

            # "Show all information" 버튼 클릭
            try:
                show_all_button = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable(
                        (By.XPATH, "//button[.//span[text()='Show all information']]")
                    )
                )
                self.driver.execute_script("arguments[0].click();", show_all_button)
                print("🔍 'Show all information' 버튼 클릭 성공 ")
                time.sleep(5)  # 충분한 시각적 확인 딜레이
            except Exception as e:
                print(f"⚠️ 'Show all information' 버튼 클릭 실패 또는 없음: {str(e)}")

            # ✅ 'Show all information' 패널 기준 추출
            author_affiliation_map, affiliation_dict = (
                self.extract_author_affiliation_mapping()
            )

            for name, superscripts in author_affiliation_map.items():
                detailed_info["authors"].append(name)

                # 이메일 (해당 author block 내에 있을 경우)
                try:
                    email_element = self.driver.find_element(
                        By.XPATH,
                        f"//span[text()='{name}']/ancestor::li//a[starts-with(@href, 'mailto:')]",
                    )
                    email = email_element.get_attribute("href").replace("mailto:", "")
                    detailed_info["emails"].append(email)
                except:
                    detailed_info["emails"].append("")

                # 소속 매핑 - 각 저자별로 해당하는 소속만
                affs, affs_raw, univs, countries = [], [], [], []
                for sup in superscripts:
                    if sup in affiliation_dict:
                        aff_text = affiliation_dict[sup]
                        affs.append(aff_text)
                        
                        # 원본 표시: 첨자가 default류면 [첨자없음], 아니면 [첨자]
                        if sup.startswith("default"):
                            affs_raw.append(f"[첨자없음] {aff_text}")
                        else:
                            affs_raw.append(f"[{sup}] {aff_text}")
                            
                        parsed = self.parse_affiliation(aff_text)
                        univs.append(parsed["university"])
                        countries.append(parsed["country"])

                if not affs:
                    affs = [""]
                    affs_raw = [""]  # 소속 없는 경우 빈값
                    univs = [""]
                    countries = [""]

                detailed_info["detailed_affiliations"].append(" | ".join(affs))
                detailed_info["raw_affiliations"].append(" | ".join(affs_raw))  # 해당 저자의 소속만
                detailed_info["universities"].append(" | ".join(univs))
                detailed_info["countries"].append(" | ".join(countries))

            self.driver.close()
            self.driver.switch_to.window(self.driver.window_handles[0])

        except Exception as e:
            logger.error(f"상세 정보 추출 중 오류: {str(e)}")
            try:
                if len(self.driver.window_handles) > 1:
                    self.driver.close()
                self.driver.switch_to.window(self.driver.window_handles[0])
            except:
                pass

        return detailed_info

    def parse_affiliation(self, affiliation_text):
        """소속 정보를 전공, 대학, 국가로 분리"""
        parsed = {"department": "", "university": "", "country": ""}

        if not affiliation_text:
            return parsed

        # 쉼표로 분리
        parts = [part.strip() for part in affiliation_text.split(",")]

        if len(parts) >= 3:
            # 일반적인 형태: Department, University, Country
            parsed["department"] = parts[0]
            parsed["university"] = parts[1]
            parsed["country"] = parts[-1]  # 마지막이 국가

        elif len(parts) == 2:
            # University, Country 형태
            parsed["university"] = parts[0]
            parsed["country"] = parts[1]

        elif len(parts) == 1:
            # 전체를 대학으로 간주
            parsed["university"] = parts[0]

        # University 키워드가 포함되지 않은 첫 번째 part는 학과로 간주
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

        print(f"📍 소속 분석: {affiliation_text}")
        print(f"   - 전공: {parsed['department']}")
        print(f"   - 대학: {parsed['university']}")
        print(f"   - 국가: {parsed['country']}")

        return parsed

    def save_batch_results(
        self, keyword, papers_data, start_page, end_page, paper_start_index=1
    ):
        """배치 결과 저장 (5페이지씩) - 실제 운영용"""
        try:
            # 안전한 파일명 생성
            safe_keyword = re.sub(r"[^\w\s-]", "", keyword).replace(" ", "_")
            filename = f"scopus_{safe_keyword}_pages_{start_page}-{end_page}.xlsx"

            if papers_data:
                # 데이터 정리
                formatted_data = []
                current_paper_index = paper_start_index

                for paper in papers_data:
                    authors = paper.get("authors", [""])
                    emails = paper.get("emails", [""])
                    affiliations = paper.get("detailed_affiliations", [""])
                    raw_affiliations = paper.get("raw_affiliations", [""])  # 원본 소속
                    universities = paper.get("universities", [""])
                    countries = paper.get("countries", [""])
                    detected_sentences = paper.get("detected_sentences", "")  # 🆕 탐지된 문장

                    max_len = max(
                        len(authors),
                        len(emails),
                        len(affiliations),
                        len(raw_affiliations),  # 원본 소속 길이도 고려
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
                                "논문번호": (
                                    current_paper_index if i == 0 else ""
                                ),  # 첫 번째 저자에만 논문번호 표시
                                "저자": authors[i] if i < len(authors) else "",
                                "이메일": emails[i] if i < len(emails) else "",
                                "소속(원본)": (
                                    raw_affiliations[i] if i < len(raw_affiliations) else ""
                                ),  # 원본 소속 (첨자 포함)
                                "소속(전공)": (
                                    affiliations[i] if i < len(affiliations) else ""
                                ),
                                "소속(대학)": (
                                    universities[i] if i < len(universities) else ""
                                ),
                                "소속(국가)": (
                                    countries[i] if i < len(countries) else ""
                                ),
                                "탐지문장": (
                                    detected_sentences if i == 0 else ""
                                ),  # 🆕 첫 번째 저자에만 탐지문장 표시
                                "논문 링크": (
                                    paper.get("link", "") if i == 0 else ""
                                ),  # 첫 번째 저자에만 링크 표시
                            }
                        )

                    # 논문 구분을 위한 빈 행 추가
                    formatted_data.append(
                        {
                            "논문번호": "",
                            "저자": "",
                            "이메일": "",
                            "소속(원본)": "",
                            "소속(전공)": "",
                            "소속(대학)": "",
                            "소속(국가)": "",
                            "탐지문장": "",  # 🆕
                            "논문 링크": "",
                        }
                    )

                    current_paper_index += 1

                df = pd.DataFrame(formatted_data)
                df.to_excel(filename, index=False)
                print(
                    f"💾 배치 저장 완료: {filename} ({len(papers_data)}개 논문, {start_page}-{end_page}페이지)"
                )

                return current_paper_index  # 다음 논문 번호 반환

        except Exception as e:
            print(f"❌ 배치 저장 실패: {str(e)}")
            return paper_start_index

    def crawl_pages(self, keyword, max_pages=3, start_page=1):  # 🧪 테스트용: 3페이지로 제한
        """특정 키워드에 대해 여러 페이지 크롤링 - 테스트용"""
        papers_data = []

        # 논문 번호 시작값 계산 (페이지당 평균 논문 수를 고려)
        paper_index = ((start_page - 1) * 2) + 1  # 페이지당 2개 논문

        # 첫 검색 실행
        if not self.search_keyword(keyword):
            return papers_data

        # 시작 페이지가 1이 아니면 해당 페이지로 이동
        if start_page > 1:
            print(f"🔄 페이지 {start_page}로 이동 중...")
            success = self.navigate_to_page(start_page)
            if not success:
                print(f"❌ 페이지 {start_page}로 이동 실패. 페이지 1부터 시작합니다.")
                start_page = 1
                paper_index = 1

        for page_num in range(start_page, max_pages + 1):
            try:
                logger.info(
                    f"키워드 '{keyword}' - 페이지 {page_num}/{max_pages} 테스트 크롤링 중..."
                )

                # 현재 페이지의 논문 링크들 수집
                paper_elements = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_all_elements_located(
                        (By.CSS_SELECTOR, "tbody tr.TableItems-module__A6xTk")
                    )
                )

                if not paper_elements:
                    logger.info(f"페이지 {page_num}에서 더 이상 결과가 없습니다.")
                    break

                print(f"📋 페이지 {page_num}: {len(paper_elements)}개 논문 발견 (테스트: 처음 2개만 처리)")

                # 논문 링크들 추출 (실제 운영: 모든 논문)
                paper_links = self.extract_paper_links(paper_elements)  # limit 파라미터 제거

                # 각 논문 상세 페이지에서 저자 정보 추출
                page_papers = []  # 🔧 변수 초기화 추가
                
                for j, paper_link in enumerate(paper_links, 1):
                    print(
                        f"🔍 [{j}/{len(paper_links)}] 논문 상세 정보 확인 중..."
                    )

                    # 상세 정보 가져오기 (LLM 필터링 포함)
                    detailed_info = self.get_detailed_author_info(paper_link)
                    detailed_info["link"] = paper_link

                    # LLM 키워드가 탐지된 경우만 논문 번호 증가
                    if detailed_info.get("detected_sentences") != "LLM 키워드 없음 - 스킵됨":
                        detailed_info["paper_number"] = paper_index
                        print(
                            f"✅ 논문 {paper_index}번: 저자 {len(detailed_info.get('authors', []))}명, 이메일 {len(detailed_info.get('emails', []))}개 수집"
                        )
                        paper_index += 1  # LLM 탐지된 논문만 번호 증가
                    else:
                        detailed_info["paper_number"] = "none"
                        print(f"⏭️ 논문 스킵: LLM 키워드 없음")

                    page_papers.append(detailed_info)
                    papers_data.append(detailed_info)

                    self.human_like_delay(2, 4)

                print(f"📄 페이지 {page_num} 완료: {len(page_papers)}개 논문 수집")

                # 다음 페이지로 이동
                if page_num < max_pages:
                    try:
                        # 🔧 현재 URL 저장 (페이지 이동 확인용)
                        current_url = self.driver.current_url
                        print(f"🔗 현재 URL: {current_url}")
                        
                        next_button = self.driver.find_element(
                            By.XPATH, "//button[.//span[text()='Next']]"
                        )

                        if next_button.is_enabled() and not next_button.get_attribute(
                            "disabled"
                        ):
                            print("🔄 Next 버튼 클릭 중...")
                            next_button.click()
                            self.human_like_delay(3, 5)
                            
                            # 🔧 URL이 실제로 바뀌었는지 확인
                            new_url = self.driver.current_url
                            print(f"🔗 새 URL: {new_url}")
                            
                            if current_url == new_url:
                                print("❌ URL이 바뀌지 않음 - 페이지 이동 실패")
                                # 강제로 새 페이지 요소 대기
                                try:
                                    WebDriverWait(self.driver, 10).until(
                                        EC.staleness_of(paper_elements[0])  # 이전 요소가 사라질 때까지 대기
                                    )
                                    print("✅ 페이지 요소 갱신 확인")
                                except:
                                    print("⚠️ 페이지 요소 갱신 확인 실패")
                            else:
                                print("✅ URL 변경 확인 - 페이지 이동 성공")
                            
                            logger.info(f"페이지 {page_num + 1}로 이동")
                        else:
                            logger.info("더 이상 다음 페이지가 없습니다.")
                            break

                    except NoSuchElementException:
                        logger.info(
                            "Next 버튼을 찾을 수 없습니다. 마지막 페이지인 것 같습니다."
                        )
                        break
                    except Exception as e:
                        logger.error(f"다음 페이지로 이동 중 오류: {str(e)}")
                        break

            except Exception as e:
                logger.error(f"페이지 {page_num} 크롤링 중 오류: {str(e)}")
                continue

        # 테스트 결과 저장
        if papers_data:
            self.save_batch_results(keyword, papers_data, start_page, page_num, 1)

        logger.info(f"키워드 '{keyword}' 테스트 크롤링 완료: {len(papers_data)}개 논문")
        return papers_data

    def navigate_to_page(self, target_page):
        """특정 페이지로 이동"""
        try:
            print(f"📄 페이지 {target_page}로 이동 중...")

            # 간단한 방법: target_page - 1번 Next 버튼 클릭
            for i in range(target_page - 1):
                try:
                    next_button = self.driver.find_element(
                        By.XPATH, "//button[.//span[text()='Next']]"
                    )
                    if next_button.is_enabled():
                        next_button.click()
                        self.human_like_delay(2, 3)
                        print(f"  -> 페이지 {i + 2}로 이동")
                    else:
                        print(f"❌ 페이지 {i + 2}로 이동 실패")
                        return False
                except:
                    print(f"❌ 페이지 {i + 2}로 이동 중 오류")
                    return False

            print(f"✅ 페이지 {target_page} 도착")
            return True

        except Exception as e:
            print(f"❌ 페이지 이동 실패: {str(e)}")
            return False

    def save_progress(self, keyword, next_page):
        """진행 상황 저장"""
        try:
            progress_info = {
                "keyword": keyword,
                "keyword_index": self.keywords.index(keyword),
                "next_page": next_page,
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            }

            with open("scopus_progress.txt", "w", encoding="utf-8") as f:
                f.write(f"마지막 완료: {progress_info['keyword']}\n")
                f.write(f"키워드 번호: {progress_info['keyword_index']}\n")
                f.write(f"다음 시작 페이지: {progress_info['next_page']}\n")
                f.write(f"시간: {progress_info['timestamp']}\n")
                f.write(f"\n재시작 방법:\n")
                f.write(
                    f"crawler = ScopusCrawler(start_keyword_index={progress_info['keyword_index']}, start_page={progress_info['next_page']})\n"
                )

        except Exception as e:
            print(f"❌ 진행 상황 저장 실패: {str(e)}")

    def save_to_excel(self, filename="scopus_papers_results.xlsx"):
        """결과를 엑셀 파일로 저장 - 실제 운영용"""
        with pd.ExcelWriter(filename, engine="openpyxl") as writer:
            for keyword, papers_data in self.results_data.items():
                if papers_data:
                    # 데이터 정리
                    formatted_data = []

                    for paper_index, paper in enumerate(papers_data, 1):
                        # 저자별로 행 분리
                        authors = paper.get("authors", [""])
                        emails = paper.get("emails", [""])
                        detailed_affiliations = paper.get(
                            "detailed_affiliations", [""]
                        )  # 파싱된 소속
                        raw_affiliations = paper.get("raw_affiliations", [""])  # 원본 소속
                        universities = paper.get("universities", [""])  # 파싱된 대학
                        countries = paper.get("countries", [""])  # 파싱된 국가
                        detected_sentences = paper.get("detected_sentences", "")  # 🆕 탐지된 문장

                        # 리스트 길이 맞추기
                        max_len = max(
                            len(authors),
                            len(emails),
                            len(detailed_affiliations),
                            len(raw_affiliations),  # 원본 소속 길이도 고려
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
                                    "논문번호": (
                                        paper_index if i == 0 else ""
                                    ),  # 첫 번째 저자에만 논문번호 표시
                                    "저자": authors[i] if i < len(authors) else "",
                                    "이메일": emails[i] if i < len(emails) else "",
                                    "소속(원본)": (
                                        raw_affiliations[i] if i < len(raw_affiliations) else ""
                                    ),  # 원본 소속 (첨자 포함)
                                    "소속(전공)": (
                                        detailed_affiliations[i]
                                        if i < len(detailed_affiliations)
                                        else ""
                                    ),  # 파싱된 소속
                                    "소속(대학)": (
                                        universities[i] if i < len(universities) else ""
                                    ),  # 파싱된 대학
                                    "소속(국가)": (
                                        countries[i] if i < len(countries) else ""
                                    ),  # 파싱된 국가
                                    "탐지문장": (
                                        detected_sentences if i == 0 else ""
                                    ),  # 🆕 첫 번째 저자에만 탐지문장 표시
                                    "논문 링크": (
                                        paper.get("link", "") if i == 0 else ""
                                    ),  # 첫 번째 저자에만 링크 표시
                                }
                            )

                        # 논문 구분을 위한 빈 행 추가
                        formatted_data.append(
                            {
                                "논문번호": "",
                                "저자": "",
                                "이메일": "",
                                "소속(원본)": "",
                                "소속(전공)": "",
                                "소속(대학)": "",
                                "소속(국가)": "",
                                "탐지문장": "",  # 🆕
                                "논문 링크": "",
                            }
                        )

                    # DataFrame 생성 및 저장
                    df = pd.DataFrame(formatted_data)

                    # 시트 이름에서 특수 문자 제거
                    safe_keyword = re.sub(r"[^\w\s-]", "", keyword).strip()[
                        :31
                    ]  # Excel 시트명 길이 제한
                    df.to_excel(writer, sheet_name=safe_keyword, index=False)

        logger.info(f"결과가 {filename} 파일로 저장되었습니다.")

    def run(self):
        """메인 크롤링 실행 - 실제 운영용"""
        try:
            print("🚀 Scopus 논문 크롤링을 시작합니다!")
            print(f"📋 수집할 키워드: {', '.join(self.keywords)} (총 {len(self.keywords)}개)")
            print(f"📄 각 키워드당 최대 200페이지, 모든 논문 크롤링")
            print(f"🎯 LLM 관련 논문만 선별적으로 수집합니다.")

            self.setup_driver()

            # 고려대 도서관에서 Scopus 자동 접근 (로그인 과정 간소화)
            if not self.login_and_access_scopus():
                logger.error("Scopus 접근 실패로 크롤링을 중단합니다.")
                return

            # Scopus 검색 페이지 접근 확인
            try:
                print("🔍 Scopus 검색 기능 확인 중...")
                WebDriverWait(self.driver, 15).until(
                    EC.presence_of_element_located(
                        (
                            By.CSS_SELECTOR,
                            "input[placeholder=' '][class*='styleguide-input_input']",
                        )
                    )
                )
                print("✅ Scopus 검색 페이지 접근 완료!")
            except:
                print("❌ Scopus 검색 페이지 접근에 문제가 있습니다.")
                print("수동으로 Scopus 검색 페이지로 이동해주세요.")
                input("준비 완료 후 Enter를 눌러주세요: ")

            # 각 키워드별로 크롤링 (시작점부터)
            total_keywords = len(self.keywords)
            for idx, keyword in enumerate(
                self.keywords[self.start_keyword_index :], self.start_keyword_index + 1
            ):
                print(
                    f"\n🔍 [{idx}/{total_keywords}] 키워드 '{keyword}' 크롤링 시작..."
                )
                print("💾 5페이지마다 자동 저장됩니다.")
                print("📊 논문별 번호와 구분 빈 행이 자동으로 추가됩니다.")

                # 첫 번째 키워드면 지정된 페이지부터, 아니면 1페이지부터
                start_page = (
                    self.start_page if idx == self.start_keyword_index + 1 else 1
                )

                papers_data = self.crawl_pages(
                    keyword, max_pages=200, start_page=start_page
                )
                self.results_data[keyword] = papers_data

                print(f"✅ 키워드 '{keyword}' 완료: {len(papers_data)}개 논문 수집")

                if idx < total_keywords:
                    print("⏳ 다음 키워드 크롤링까지 10-15초 대기...")
                    self.human_like_delay(10, 15)

            # 결과 저장
            print(f"\n💾 결과를 엑셀 파일로 저장 중...")
            self.save_to_excel("scopus_papers_results.xlsx")

            # 최종 결과 요약
            total_papers = sum(len(papers) for papers in self.results_data.values())
            print(f"\n🎉 크롤링 완료!")
            print(f"📊 총 수집된 논문: {total_papers}개")
            print(f"📁 저장된 파일: scopus_papers_results.xlsx")
            print(f"✨ 각 논문별로 번호가 매겨져 있고, 구분을 위한 빈 행이 추가되었습니다!")

        except Exception as e:
            logger.error(f"크롤링 중 전체 오류: {str(e)}")
        finally:
            if self.driver:
                print("\n🔒 브라우저를 종료합니다...")
                self.driver.quit()


# 실행
if __name__ == "__main__":
    print("🚀 Scopus 크롤러 실제 운영 버전")
    print("=" * 50)
    print("📋 운영 설정:")
    print("   - 키워드: 8개")
    print("   - 페이지: 200페이지")
    print("   - 논문: 모든 논문 (LLM 관련만 선별)")
    print("   - 총 예상 논문 수: 수천~수만개")
    print("=" * 50)
    
    # 기본 실행 (처음부터 시작)
    crawler = ScopusCrawler()
    
    # 재시작 예시 (주석 해제 후 사용):
    # crawler = ScopusCrawler(start_keyword_index=2, start_page=15)  # 3번째 키워드, 15페이지부터
    # crawler = ScopusCrawler(start_keyword_index=0, start_page=25)  # 1번째 키워드, 25페이지부터
    
    crawler.run()