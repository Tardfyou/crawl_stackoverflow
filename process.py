import csv
from tqdm import tqdm

def process_csv(input_filename, output_filename):
    # 增加CSV字段大小限制
    max_field_size = 10**7
    csv.field_size_limit(max_field_size)
    
    # 读取输入CSV文件并统计行数
    with open(input_filename, 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        fieldnames = reader.fieldnames
        
        # 统计行数以显示进度条
        total_rows = sum(1 for _ in reader)
        csvfile.seek(0)  # 重置文件指针到文件开头
        reader = csv.DictReader(csvfile)

        # 创建输出CSV文件
        with open(output_filename, 'w', newline='', encoding='utf-8') as csvfile_out:
            writer = csv.DictWriter(csvfile_out, fieldnames=fieldnames)
            writer.writeheader()
            
            # 使用 tqdm 显示进度条
            for row in tqdm(reader, total=total_rows, desc="Processing rows"):
                if row['accepted'] == '是':
                    writer.writerow(row)

    print(f"筛选数据已成功写入 {output_filename}")

if __name__ == '__main__':
    input_csv = 'stackoverflow_data4000_6000.csv'  # 输入CSV文件名
    output_csv = 'accepted_answers4000_6000.csv'  # 输出CSV文件名
    process_csv(input_csv, output_csv)
