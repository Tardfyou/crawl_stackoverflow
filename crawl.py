import requests
from bs4 import BeautifulSoup
import csv
import signal
import time
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

# 全局变量，用于判断是否停止爬取
stop_crawling = False

def signal_handler(sig, frame):
    global stop_crawling
    stop_crawling = True
    print("爬取停止中...")

signal.signal(signal.SIGINT, signal_handler)

def fetch_page(url):
    try:
        print(f"正在请求 {url}...")
        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
        response.raise_for_status()  # 确保请求成功
        return response.text
    except requests.RequestException as e:
        print(f"请求错误: {e}")
        return None

def parse_question_page(html):
    soup = BeautifulSoup(html, 'html.parser')
    question_data = {}
    
    # 提取问题的详细描述
    question_data['description'] = soup.select_one('.js-post-body').text.strip() if soup.select_one('.js-post-body') else '无描述'
    
    # 提取标签
    tags = [tag.text for tag in soup.select('.post-tag')]
    unique_tags = list(set(tags))  # 去重
    question_data['tags'] = ', '.join(unique_tags)
    
    # 提取发布日期
    question_data['date'] = soup.select_one('.user-action-time span').get('title', '无发布日期')

    # 提取回答内容和是否被接受
    accepted_answer_id = None
    accepted_answer = soup.select_one('.accepted-answer')
    if accepted_answer:
        accepted_answer_id = accepted_answer.get('data-answerid')

    answers = soup.select('.answer')
    answers_data = []
    for answer in answers:
        answer_data = {}
        answer_id = answer.get('data-answerid')
        answer_data['content'] = answer.select_one('.js-post-body').text.strip() if answer.select_one('.js-post-body') else '无回答内容'
        answer_data['accepted'] = '是' if answer_id == accepted_answer_id else '否'
        answers_data.append(answer_data)
    
    return question_data, answers_data

def parse_page(html):
    soup = BeautifulSoup(html, 'html.parser')
    questions = soup.select('.s-post-summary')
    data = []
    for question in questions:
        if stop_crawling:
            break

        question_data = {}
        question_data['title'] = question.select_one('.s-link').text.strip()
        question_data['link'] = 'https://stackoverflow.com' + question.select_one('.s-link')['href']
        stats = question.select('.s-post-summary--stats-item-number')
        if len(stats) >= 3:
            votes = stats[0].text.strip()
            answers = stats[1].text.strip()
            views = stats[2].text.strip()
            
            if int(answers) > 0 and int(votes) >= 0:
                question_data['votes'] = votes
                question_data['answers'] = answers
                question_data['views'] = views
                
                data.append(question_data)
    return data

def process_question(question_data):
    if stop_crawling:
        return None

    question_html = fetch_page(question_data['link'])
    if question_html:
        question_details, answers_data = parse_question_page(question_html)
        question_data.update(question_details)
        question_data['answers_data'] = answers_data
        print(f"成功解析问题：{question_data['title']}")
        return question_data
    return None

def save_to_csv(data, filename='stackoverflow_data4000_6000.csv'):
    with open(filename, 'a', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['title', 'link', 'votes', 'answers', 'views', 'description', 'tags', 'date', 'answer_content', 'accepted']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        for row in data:
            answers_data = row.pop('answers_data')
            for answer in answers_data:
                row['answer_content'] = answer['content']
                row['accepted'] = answer['accepted']
                writer.writerow(row)
        print(f"{len(data)} 条数据已写入 {filename}")

def load_last_page(file='last_page.txt'):
    if os.path.exists(file):
        with open(file, 'r') as f:
            last_page = int(f.readline().strip())
            max_page = int(f.readline().strip())
            return last_page, max_page
    return 1, float('inf')

def save_last_page(page, max_page, file='last_page.txt'):
    with open(file, 'w') as f:
        f.write(f"{page}\n")
        f.write(f"{max_page}\n")

def main():
    page, max_page = load_last_page()
    all_data = []
    while not stop_crawling:
        if page > max_page:
            print(f"达到最大页数 {max_page}，停止爬取。")
            break
        print(f"正在爬取第 {page} 页...")
        url = f'https://stackoverflow.com/questions?tab=votes&page={page}'
        html = fetch_page(url)
        if html:
            questions_data = parse_page(html)
            if questions_data:
                with ThreadPoolExecutor(max_workers=5) as executor:
                    future_to_question = {executor.submit(process_question, q): q for q in questions_data}
                    for future in as_completed(future_to_question):
                        question_data = future.result()
                        if question_data:
                            all_data.append(question_data)
                print(f"第 {page} 页爬取完成。")
                save_last_page(page, max_page)  # 保存当前页数和最大页数
            else:
                print(f"第 {page} 页没有数据。")
            page += 1
            time.sleep(1)  # 添加延时，防止请求过于频繁被限制
        else:
            print(f"无法获取第 {page} 页的内容。")
            break
    
    # 在程序结束或被终止时写入所有数据
    save_to_csv(all_data)
    print("爬取已停止。")

if __name__ == '__main__':
    # 检查CSV文件是否存在，不存在则创建并添加表头
    if not os.path.exists('stackoverflow_data4000_6000.csv'):
        with open('stackoverflow_data4000_6000.csv', 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['title', 'link', 'votes', 'answers', 'views', 'description', 'tags', 'date', 'answer_content', 'accepted']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
    
    main()
