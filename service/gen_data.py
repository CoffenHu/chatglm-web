import os
from whoosh.fields import *
from jieba.analyse import ChineseAnalyzer
from whoosh.filedb.filestore import FileStorage

import PyPDF2  # PDF 解析库
from docx import Document  # DOCX 解析库

from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm  # 进度条库


def process_file(file_path, file):
    """处理单个文件的解析逻辑"""
    try:
        if file.lower().endswith('.pdf'):
            with open(file_path, 'rb') as f:
                pdf_reader = PyPDF2.PdfReader(f)
                return file, '\n'.join(page.extract_text() for page in pdf_reader.pages)
        
        elif file.lower().endswith(('.docx', '.doc')):
            doc = Document(file_path)
            return file, '\n'.join(paragraph.text for paragraph in doc.paragraphs)
        
        elif file.lower().endswith('.txt'):
            with open(file_path, "r", encoding='utf-8') as f:
                return file, f.read()
        
        else:
            print(f"跳过不支持的文件格式: {file}")
            return None, None
    
    except Exception as e:
        print(f"读取文件失败 {file}: {str(e)}")
        return None, None


def gen_whoosh_data():
    floder = 'knowledge'
    files = os.listdir(floder)
    analyzer = ChineseAnalyzer()
    schema = Schema(title=TEXT(stored=True), content=TEXT(stored=True, analyzer=analyzer))
    storage = FileStorage('knowdata')
    if not os.path.exists('knowdata'):
        os.mkdir('knowdata')
        ix = storage.create_index(schema)
    else:
        ix = storage.open_index()
    
    writer = ix.writer()
    
    # 并行处理文件（最大线程数根据CPU核心数调整）
    with ThreadPoolExecutor(max_workers=12) as executor:
        futures = []
        for file in files:
            file_path = os.path.join(floder, file)
            futures.append(executor.submit(process_file, file_path, file))
        
        # 使用 tqdm 显示进度条
        for future in tqdm(as_completed(futures), total=len(futures), desc="处理进度"):
            title, content = future.result()
            if title and content:
                writer.add_document(title=title, content=content)
    
    writer.commit()  # 提交
    print("读取知识库文件完成")


if __name__ == "__main__":
    gen_whoosh_data()
