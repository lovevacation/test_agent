import xlrd
from collections import defaultdict

def scan_same_code(path: str):
    wb = xlrd.open_workbook(path)
    ws = wb.sheet_by_index(0)
    headers = [str(ws.cell_value(0, col)).strip() for col in range(ws.ncols)]
    rows = []
    for row_idx in range(1, ws.nrows):
        row = {headers[col]: ws.cell_value(row_idx, col) for col in range(ws.ncols)}
        if any(str(v).strip() for v in row.values()):
            rows.append(row)
    wb.release_resources()

    # 按job_code分组
    by_code = defaultdict(list)
    for r in rows:
        code = str(r.get("岗位编码") or "").strip()
        by_code[code].append(r)

    # 找出job_code相同但内容不同的
    diff_groups = {}
    for code, group in by_code.items():
        if len(group) < 2:
            continue
        # 检查是否完全相同
        keys = ["岗位名称", "地址", "薪资范围", "公司名称",
                "所属行业", "公司规模", "公司类型", "岗位详情"]
        sigs = set("|".join(str(r.get(f) or "").strip() for f in keys)
                   for r in group)
        if len(sigs) > 1:  # 有差异
            diff_groups[code] = group

    print(f"job_code相同但内容不同：{len(diff_groups)} 组")
    for code, group in list(diff_groups.items())[:3]:
        print(f"\n  job_code: {code}")
        for r in group:
            print(f"    岗位名称: {r.get('岗位名称')}")
            print(f"    地址:     {r.get('地址')}")
            print(f"    薪资:     {r.get('薪资范围')}")
            print(f"    公司:     {r.get('公司名称')}")
            print(f"    ----")

if __name__ == "__main__":
    scan_same_code("jobs.xls")