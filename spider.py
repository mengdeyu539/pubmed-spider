import requests
import pandas as pd
from xml.etree import ElementTree
from tqdm import tqdm
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import html
from progress.bar import Bar

# 设置重试策略
retry_strategy = Retry(
    total=5,
    status_forcelist=[429, 500, 502, 503, 504],
    backoff_factor=1
)
adapter = HTTPAdapter(max_retries=retry_strategy)
http = requests.Session()
http.mount("https://", adapter)
http.mount("http://", adapter)


def fetch_pubmed_ids(query, article_types=None, start_year=None, end_year=None):
    base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
    term = query
    if article_types:
        types_query = " OR ".join([f"{article_type}[Publication Type]" for article_type in article_types])
        term += f" AND ({types_query})"
    if start_year and end_year:
        term += f" AND ({start_year}:{end_year}[dp])"
    params = {
        'db': 'pubmed',
        'term': term,
        'retmax': '1000000',  # Set a very high number to retrieve all results
        'retmode': 'json'
    }
    response = http.get(base_url, params=params)
    data = response.json()
    ids = data['esearchresult']['idlist']
    return ids


def fetch_pubmed_details(ids):
    base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
    params = {
        'db': 'pubmed',
        'id': ','.join(ids),
        'retmode': 'xml'
    }
    response = http.get(base_url, params=params)
    return response.content


def parse_pubmed_details(xml_data):
    root = ElementTree.fromstring(xml_data)
    articles = []
    for article in root.findall('.//PubmedArticle'):
        pubmed_id = article.findtext('.//PMID')
        title = article.findtext('.//ArticleTitle')
        years = article.findall('.//ArticleYear')

        # 处理摘要
        abstract = ''
        abstract_texts = article.findall('.//Abstract/AbstractText')
        if abstract_texts:
            for abstract_text in abstract_texts:
                if abstract_text is not None:
                    parts = list(abstract_text.itertext())
                    part_text = " ".join(parts).strip()
                    part_text = html.unescape(part_text)  # 处理 HTML 实体编码
                    label = abstract_text.attrib.get('Label', '')
                    if label:
                        abstract += f"{label}: {part_text} "
                    else:
                        abstract += f"{part_text} "
            abstract = abstract.strip()
        else:
            # 如果没有结构化摘要，尝试获取非结构化摘要
            abstract = article.findtext('.//Abstract')
            if abstract:
                abstract = html.unescape(abstract)  # 处理 HTML 实体编码

        article_types = [at.text for at in article.findall('.//PublicationTypeList/PublicationType')]

        articles.append({
            'pubmed_id': pubmed_id,
            'title': title,
            'abstract': abstract,
            'years': years
            #'article_types': article_types
        })
    return articles


def save_to_csv(data, filename):
    df = pd.DataFrame(data)
    df.to_csv(filename, index=False)


# 使用示例

def download_pubmed_ids(query, article_types=None, start_year=None, end_year=None):
# 获取 PubMed IDs
    print("Fetching PubMed IDs...")
    pubmed_ids = fetch_pubmed_ids(query, article_types=article_types, start_year=start_year, end_year=end_year)

# 获取详细信息并显示进度条
    print("Fetching PubMed details...")
    details_list = []
    batch_size = 100  # Increase batch size to improve efficiency

    with Bar("downloading:", max=len(pubmed_ids)/batch_size, fill="🏀", suffix='%(percent).1f%%-%(eta)ds') as bar:
        for i in range(0, len(pubmed_ids), batch_size):
            batch_ids = pubmed_ids[i:i + batch_size]
            xml_data = fetch_pubmed_details(batch_ids)
            details_list.extend(parse_pubmed_details(xml_data))
            bar.next()

# 保存为 CSV
        print("Saving to CSV...")
        save_to_csv(details_list, 'filtered_pubmed_{}.csv'.format(query))

        print(f"Saved {len(details_list)} filtered articles to filtered_pubmed_{query}.csv")

if __name__ == '__main__':
    query = "fitness training"
    article_types = ["Clinical Trial", "Randomized Controlled Trial"]
    start_year = 2015
    end_year = 2023
    download_pubmed_ids(query, article_types, start_year, end_year)