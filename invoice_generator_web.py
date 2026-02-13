from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import A4
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
import os
from datetime import datetime
from io import BytesIO

def setup_fonts():
    # Try common Windows font first
    font_path = "C:/Windows/Fonts/msgothic.ttc"
    if os.path.exists(font_path):
        try:
            pdfmetrics.registerFont(TTFont('Gothic', font_path))
            return 'Gothic'
        except:
            pass
    return 'Helvetica' # Fallback

def generate_invoice_pdf(tenant_data):
    """Generates a PDF and returns the bytes."""
    buffer = BytesIO()
    c = canvas.Canvas(buffer, pagesize=A4)
    width, height = A4
    font_name = setup_fonts()
    
    # --- 1. Date of Issue & Property ID (Top-Right) ---
    c.setFont(font_name, 10)
    issue_date = datetime.now().strftime("%Y年%m月%d日")
    c.drawRightString(width - 50, height - 40, f"発行日: {issue_date}")
    c.drawRightString(width - 50, height - 55, f"物件管理番号: {tenant_data.get('PropertyID', '')}")
    
    # --- 2. Window Envelope Address (Top-Left) ---
    left_margin = 40
    top_pos = height - 60
    
    c.setFont(font_name, 11)
    zip_code = str(tenant_data.get('Zip', '')).strip()
    c.drawString(left_margin, top_pos, f"〒 {zip_code}")
    address_str = str(tenant_data.get('Address', '')).strip()
    c.drawString(left_margin, top_pos - 15, address_str)
    c.setFont(font_name, 16)
    name_str = str(tenant_data.get('Name', '')).strip()
    c.drawString(left_margin, top_pos - 40, f"{name_str} 様")

    # --- 3. Title (Below Address) ---
    c.setFont(font_name, 18)
    c.drawCentredString(width / 2, top_pos - 100, "お支払い期日のお知らせ（請求書）")
    
    # --- 4. Main Statement ---
    c.setFont(font_name, 14)
    # Property ID moved to top-right
    c.drawString(50, top_pos - 130, f"ご請求合計金額:   ¥ {int(tenant_data['TotalDue']):,} -")
    c.setFont(font_name, 10)
    c.drawString(50, top_pos - 155, "(未払残高 + 翌月分家賃の合計です)") 
    
    # --- 5. Billing Details (請求明細) ---
    c.setFont(font_name, 12)
    c.drawString(50, top_pos - 190, "【請求明細】")
    
    y = top_pos - 215
    c.setFont(font_name, 10)
    # Header
    c.drawString(70, y, "該当年月")
    c.drawString(150, y, "家賃額")
    c.drawString(250, y, "既入金額")
    c.drawString(350, y, "差引不足額")
    c.line(50, y-10, width-50, y-10) # separator below header
    
    y -= 35
    history = tenant_data.get('History', [])
    if not history: history = []

    for h in history:
        balance = int(h['amount'] - h['paid'])
        # Show if not fully paid OR if it's the latest month (next month)
        is_next_month = (h == history[-1])
        if balance <= 0 and not is_next_month:
            continue

        ts = h['month']
        if isinstance(ts, str): # Handle string logic if not timestamp
             month_str = ts
        else:
             month_str = ts.strftime("%Y年%m月分")

        c.drawString(70, y, month_str)
        c.drawString(150, y, f"¥ {int(h['amount']):,}")
        c.drawString(250, y, f"¥ {int(h['paid']):,}")
        if balance > 0:
            c.setFillColorRGB(0.8, 0, 0) # Red for overdue
        c.drawString(350, y, f"¥ {balance:,}")
        c.setFillColorRGB(0, 0, 0) # Back to black
        
        y -= 10
        c.line(50, y, width-50, y) # separator
        y -= 25 
        if y < 200: # Earlier break to avoid overlap with footer
            c.showPage()
            y = height - 50
            c.setFont(font_name, 10)
        
    # --- 6. Recent Payments Received (from Ledger) ---
    y -= 20
    if y < 180: 
        c.showPage()
        y = height - 50
    
    c.setFont(font_name, 12)
    c.drawString(50, y, "【直近の入金履歴】")
    y -= 25
    c.setFont(font_name, 10)
    c.drawString(70, y, "入金日")
    c.drawString(170, y, "金額")
    c.drawString(270, y, "摘要／充当内容")
    c.line(50, y-10, width-50, y-10)
    
    y -= 30
    for p in tenant_data.get('LedgerHistory', []):
        row_y = y
        date_val = p['Date']
        if not isinstance(date_val, str):
            date_str = date_val.strftime("%Y/%m/%d")
        else:
            date_str = date_val
            
        c.drawString(70, row_y, date_str)
        c.drawString(170, row_y, f"¥ {int(p['Amount']):,}")
        
        # Wrapped Multi-line Allocation Description
        desc = p.get('AllocationDesc', '')
        from reportlab.lib.utils import simpleSplit
        lines = simpleSplit(desc, font_name, 10, width - 320) # 270px width
        
        line_y = row_y
        for line in lines:
            c.drawString(270, line_y, line)
            line_y -= 12
        
        # Update y for next row based on number of lines
        y = min(y - 25, line_y - 13)
        
        if y < 150: # Leave space for footer
            c.drawString(50, y, "...(履歴が多い場合は省略されます)")
            break

    # --- 7. Footer / Bank Info ---
    footer_y = 120
    c.setDash(1, 2)
    c.line(50, footer_y + 15, width-50, footer_y + 15)
    c.setDash()
    c.setFont(font_name, 10)
    c.drawString(50, footer_y, "【お振込先】")
    c.setFont(font_name, 11)
    c.drawString(70, footer_y - 20, "りそな銀行 住道（ｽﾐﾉﾄﾞｳ）支店 普通 3041570 サカグチ ゲンタ")
    
    c.setFont(font_name, 9)
    c.drawString(50, footer_y - 45, "※ 本状と行き違いでお支払い済みの場合は、何卒ご容赦ください。")
    c.drawString(50, footer_y - 60, "※ お振込み手数料はお客様のご負担にてお願い申し上げます。")
    
    c.showPage()
    c.save()
    
    buffer.seek(0)
    return buffer.getvalue()
