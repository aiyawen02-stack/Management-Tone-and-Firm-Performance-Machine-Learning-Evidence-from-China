import sys
import subprocess

try:
    import requests
    import pandas as pd
except ImportError:
    print("检测到缺少所需的库。正在尝试自动安装...")
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "requests", "pandas"])
        print("库安装成功，请重新运行脚本。")
    except Exception as e:
        print(f"自动安装失败: {e}")
        print("请手动安装'requests'和'pandas'库后重试，可以运行以下命令：")
        print("pip install requests pandas")
    sys.exit(1)

import requests
import pandas as pd
import json
import time

def get_announcements(page_num, keyword, start_date, end_date):
    """
    获取指定页码的公告数据。
    """
    url = "https://www.cninfo.com.cn/new/hisAnnouncement/query"
    headers = {
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36"
    }
    data = {
        "pageNum": page_num,
        "pageSize": 300,
        "column": "szse",
        "tabName": "fulltext",
        "sortName": "",
        "sortType": "",
        "limit": "",
        "showFullText": "true",
        "searchkey": keyword,
        "seDate": f"{start_date}~{end_date}",
        "category": "category_ndbg_szsh", # 精确筛选年度报告
        "isHLtitle": "true"
    }
    try:
        response = requests.post(url, headers=headers, data=data)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"请求失败: {e}")
        return None
    except json.JSONDecodeError:
        print(f"无法解析JSON响应: {response.text}")
        return None

def main():
    """
    主函数，用于按年份爬取所有分页数据并保存到CSV文件。
    """
    keyword = "年度报告"
    all_reports = []
    
    for year in range(2015, 2024):
        start_date = f"{year}-01-01"
        end_date = f"{year}-12-31"
        yearly_announcements = []
        
        print(f"======== 开始爬取 {year} 年的数据 ========")
        # 每年最多爬取 600 条，即 20 页
        for page_num in range(1, 21):
            print(f"正在爬取 {year} 年第 {page_num} 页...")
            result = get_announcements(page_num, keyword, start_date, end_date)
            
            if result and result.get('announcements'):
                announcements = result['announcements']
                # 过滤标题，只保留完全匹配“年度报告”的项
                filtered_announcements = [
                    ann for ann in announcements if '年度报告' in ann.get('announcementTitle', '')
                ]
                yearly_announcements.extend(filtered_announcements)
                
                # 如果当年已获取足够数据或已无更多数据，则停止
                if len(yearly_announcements) >= 600 or not result.get('hasMore'):
                    if not result.get('hasMore'):
                        print(f"{year} 年已到达最后一页。")
                    break
                
                time.sleep(1)
            else:
                print(f"{year} 年未能获取到数据或已到达末尾。")
                break
        
        # 截取前600条
        all_reports.extend(yearly_announcements[:600])
        print(f"======== {year} 年数据爬取完成，共获取 {len(yearly_announcements[:600])} 条 ========\n")

    if all_reports:
        df = pd.DataFrame(all_reports)
        
        # 选择并重命名列
        columns_to_keep = {
            'announcementId': 'ann_id',
            'secCode': '股票代码',
            'secName': 'org_name',
            'orgId': 'org_id',
            'announcementTitle': '公告标题',
            'announcementTime': 'announce_data',
            'adjunctUrl': '公告链接'
        }
        
        # 确保所有需要的列都存在，不存在的列用None填充
        for col in columns_to_keep.keys():
            if col not in df.columns:
                df[col] = None
        
        df = df[list(columns_to_keep.keys())]
        df.rename(columns=columns_to_keep, inplace=True)
        
        # 将时间戳转换为日期
        df['announce_data'] = pd.to_datetime(df['announce_data'], unit='ms')
        
        # 提取年份
        df['year'] = df['announce_data'].dt.year
        
        # 根据股票代码判断交易所
        def get_exchange(sec_code):
            if pd.isna(sec_code):
                return '未知'
            sec_code_str = str(sec_code)
            if sec_code_str.startswith('6'):
                return '科创板'
            elif sec_code_str.startswith('3'):
                return '创业板'
            else:
                return '未知'
        
        df['exchange'] = df['股票代码'].apply(get_exchange)
        
        # 生成完整的公告链接
        df['公告链接'] = 'http://static.cninfo.com.cn/' + df['公告链接']

        output_file = "cninfo_annual_reports_2015_2023-1.csv"
        df.to_csv(output_file, index=False, encoding='utf-8-sig')
        print(f"所有数据已成功保存到 {output_file}")
    else:
        print("没有爬取到任何数据。")

if __name__ == "__main__":
    main()
