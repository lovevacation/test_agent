import json
from collections import Counter
import xlrd

def scan_duplicates(path: str):
    wb = xlrd.open_workbook(path)
    ws = wb.sheet_by_index(0)

    headers = [str(ws.cell_value(0, col)).strip() for col in range(ws.ncols)]
    rows = []
    for row_idx in range(1, ws.nrows):
        row = {headers[col]: ws.cell_value(row_idx, col) for col in range(ws.ncols)}
        if any(str(v).strip() for v in row.values()):
            rows.append(row)
    wb.release_resources()

    total = len(rows)
    print(f"总条数：{total}\n")

    # 9个字段全部相同才算真正重复
    KEY_FIELDS = ["岗位名称", "地址", "薪资范围", "公司名称",
                  "所属行业", "公司规模", "公司类型", "岗位编码", "岗位详情"]

    key_counter = Counter()
    key_rows    = {}  # key → 第一次出现的行号

    for idx, r in enumerate(rows):
        key = "|".join(str(r.get(f) or "").strip() for f in KEY_FIELDS)
        key_counter[key] += 1
        if key not in key_rows:
            key_rows[key] = idx + 2  # +2 因为第1行是表头，从第2行开始

    dups = {k: v for k, v in key_counter.items() if v > 1}

    print(f"完全重复的记录组数：{len(dups)} 组")
    print(f"涉及冗余行数：{sum(v - 1 for v in dups.values())} 行")
    print(f"去重后剩余：{total - sum(v - 1 for v in dups.values())} 条")

    if dups:
        print(f"\nTop10重复组（按重复次数排序）：")
        for key, cnt in sorted(dups.items(), key=lambda x: -x[1])[:10]:
            parts = key.split("|")
            print(f"\n  出现 {cnt} 次，首次出现在第 {key_rows[key]} 行")
            for field, val in zip(KEY_FIELDS, parts):
                display = val[:50] + "..." if len(val) > 50 else val
                print(f"    {field:10s}: {display}")

if __name__ == "__main__":
    scan_duplicates("jobs.xls")