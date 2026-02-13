from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import A4
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
import os
from datetime import datetime

# Registration for Japanese fonts might vary by environment. 
# We'll try to find a system font or use a generic one if possible.
# For simplicity in this environment, we'll try to use a standard Japanese font path if it exists,
# but for robust code we'll fallback to a basic placeholder if fonts aren't found.
def setup_fonts():
    # Common Windows Japanese font path
    font_path = "C:/Windows/Fonts/msgothic.ttc"
    if os.path.exists(font_path):
        pdfmetrics.registerFont(TTFont('Gothic', font_path))
        return 'Gothic'
    return 'Helvetica'

def create_invoice(tenant_data, output_path):
    c = canvas.Canvas(output_path, pagesize=A4)
    width, height = A4
    font_name = setup_fonts()
    
    # --- 1. Date of Issue (Top-Right) ---
    c.setFont(font_name, 10)
    issue_date = datetime.now().strftime("%Y年%m月%d日")
    c.drawRightString(width - 50, height - 50, f"発行日: {issue_date}")
    
    # --- 2. Window Envelope Address (Top-Left) ---
    # Most window envelopes have the window starting 12-15mm from top.
    left_margin = 80
    top_pos = height - 80
    
    c.setFont(font_name, 11)
    c.drawString(left_margin, top_pos, f"〒 {tenant_data['Zip']}")
    c.drawString(left_margin, top_pos - 15, tenant_data['Address'])
    c.setFont(font_name, 16)
    c.drawString(left_margin, top_pos - 40, f"{tenant_data['Name']} 様")

    # --- 3. Title (Below Address) ---
    c.setFont(font_name, 18)
    c.drawCentredString(width / 2, top_pos - 100, "お支払い期日のお知らせ（請求書）")
    
    # --- 4. Main Statement ---
    c.setFont(font_name, 12)
    c.drawString(50, top_pos - 140, f"物件管理番号: {tenant_data['PropertyID']}")
    c.setFont(font_name, 14)
    c.drawString(50, top_pos - 130, f"ご請求合計金額:   ¥ {int(tenant_data['TotalDue']):,} -")
    c.setFont(font_name, 10)
    c.drawString(50, top_pos - 150, "(未払残高 + 翌月分家賃の合計です)")
    
    # --- 5. Billing Details (請求明細) ---
    c.setFont(font_name, 12)
    c.drawString(50, top_pos - 190, "【請求明細】")
    
    y = top_pos - 210
    c.setFont(font_name, 10)
    # Header
    c.drawString(70, y, "該当年月")
    c.drawString(150, y, "家賃額")
    c.drawString(250, y, "既入金額")
    c.drawString(350, y, "差引不足額")
    c.line(50, y-5, width-50, y-5)
    
    y -= 25
    for h in tenant_data['History']:
        balance = int(h['amount'] - h['paid'])
        # Show only if not fully paid OR if it's the latest month in the history (next month)
        is_next_month = (h == tenant_data['History'][-1])
        if balance <= 0 and not is_next_month:
            continue

        month_str = h['month'].strftime("%Y年%m月分")
        c.drawString(70, y, month_str)
        c.drawString(150, y, f"¥ {int(h['amount']):,}")
        c.drawString(250, y, f"¥ {int(h['paid']):,}")
        if balance > 0:
            c.setFillColorRGB(0.8, 0, 0) # Red for overdue
        c.drawString(350, y, f"¥ {balance:,}")
        c.setFillColorRGB(0, 0, 0) # Back to black
        y -= 20
        c.line(50, y+5, width-50, y+5) # separator
        if y < 150: break
        
    # --- 6. Recent Payments Received (from Ledger) ---
    y -= 30
    if y < 100: 
        c.showPage()
        y = height - 50
        c.setFont(font_name, 10)
    
    c.setFont(font_name, 12)
    c.drawString(50, y, "【直近の入金履歴】")
    y -= 20
    c.setFont(font_name, 10)
    c.drawString(70, y, "入金日")
    c.drawString(170, y, "金額")
    c.drawString(270, y, "摘要")
    c.line(50, y-5, width-50, y-5)
    
    y -= 20
    for p in tenant_data.get('LedgerHistory', []):
        c.drawString(70, y, p['Date'].strftime("%Y/%m/%d"))
        c.drawString(170, y, f"¥ {int(p['Amount']):,}")
        # Allocation details
        c.drawString(270, y, p.get('AllocationDesc', ''))
        y -= 15
        if y < 50: break

    # --- 7. Footer / Instructions ---
    c.setFont(font_name, 10)
    footer_y = 100
    c.drawString(50, footer_y, "※ 本状と行き違いでお支払い済みの場合は、何卒ご容赦ください。")
    c.drawString(50, footer_y - 15, "※ お振込みは原則として月末までにお願い申し上げます。")
    
    c.showPage()
    c.save()

if __name__ == "__main__":
    # Test generation
    mock_tenant = {
        'Zip': '100-0001',
        'Address': '東京都千代田区千代田1-1',
        'Name': '田中 太郎',
        'PropertyID': '101',
        'TotalDue': 50000,
        'History': [
            {'month': datetime(2025, 1, 1), 'amount': 50000, 'paid': 50000},
            {'month': datetime(2025, 2, 1), 'amount': 50000, 'paid': 0},
        ]
    }
    # Ensure private_data exists for test
    if not os.path.exists('private_data'): os.makedirs('private_data')
    create_invoice(mock_tenant, "private_data/test_invoice.pdf")
    print("Test invoice generated: private_data/test_invoice.pdf")
